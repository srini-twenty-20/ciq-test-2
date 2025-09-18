"""
Raw Data Assets

This module contains assets for raw data extraction from external sources.
Raw assets are the entry point to the TTB pipeline and handle data ingestion.
"""
import os
import tempfile
import urllib3
import hashlib
from datetime import date, datetime, timedelta
from typing import Dict, Any

import requests
from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetMaterialization,
    MetadataValue,
    AssetExecutionContext
)
from dagster_aws.s3 import S3Resource

from ..utils.ttb_utils import TTBIDUtils, TTBSequenceTracker
from ..config.ttb_config import TTBExtractionConfig
from ..config.ttb_partitions import ttb_partitions

# Disable SSL warnings for requests with verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TTB Error Page Detection
# Hash of the standard TTB error page returned for invalid TTB IDs
TTB_ERROR_PAGE_HASH = "50fa048f9cf8200c3d82d60add59b3b1f78f9e3ebc67f9395051595fc830a9e3"


def is_ttb_error_page(content: bytes) -> bool:
    """
    Check if the response content is a TTB error page by comparing its hash.

    Args:
        content: Raw response content bytes

    Returns:
        True if the content matches the known TTB error page hash
    """
    content_hash = hashlib.sha256(content).hexdigest()
    return content_hash == TTB_ERROR_PAGE_HASH


@asset(
    partitions_def=ttb_partitions,
    group_name="ttb_raw",
    description="Raw TTB data extraction partitioned by date, receipt method, and data type",
    metadata={
        "data_type": "raw",
        "source": "ttbonline.gov",
        "format": "html"
    }
)
def ttb_raw_data(
    context: AssetExecutionContext,
    config: TTBExtractionConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Extract raw TTB data for a specific date, receipt method, and data type partition.

    This asset implements:
    - Rate limiting (0.5s between requests)
    - Intelligent sequence detection (stop after configurable consecutive failures)
    - Comprehensive logging and metadata
    - S3 storage with organized key structure
    - Support for both COLA detail and certificate data

    Partition key format: date|method_type
    Examples:
    - "2024-01-01|001-cola-detail"
    - "2024-01-01|001-certificate"

    Returns:
        Dictionary with processing statistics and results
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    # Parse method_type (format: "001-cola-detail")
    receipt_method_str, data_type = method_type_str.split("-", 1)

    # Parse partition data
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    receipt_method = int(receipt_method_str)

    # Configure based on data type
    if data_type == "cola-detail":
        action_param = "publicDisplaySearchAdvanced"
    elif data_type == "certificate":
        action_param = "publicFormDisplay"
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

    # Use configured S3 prefix for raw data
    s3_prefix = config.raw_data_prefix

    logger.info(f"Processing TTB {data_type} data for date: {partition_date}, receipt method: {receipt_method}")

    # Initialize tracking
    sequence_tracker = TTBSequenceTracker(max_consecutive_failures=config.consecutive_failure_threshold)
    s3_client = s3_resource.get_client()
    successful_downloads = []
    failed_ttb_ids = []

    # Generate TTB IDs for this partition
    julian_day = TTBIDUtils.date_to_julian(partition_date)

    try:
        sequence = 1
        while sequence <= config.max_sequence_per_batch:
            # Build TTB ID
            ttb_id = TTBIDUtils.build_ttb_id(
                year=partition_date.year,
                julian_day=julian_day,
                receipt_method=receipt_method,
                sequence=sequence
            )

            # Build URL with appropriate action parameter
            url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action={action_param}&ttbid={ttb_id}"

            logger.info(f"Requesting TTB {data_type} ID: {ttb_id} (sequence {sequence})")

            try:
                # Rate limiting
                TTBIDUtils.rate_limit_sleep()

                # Make request
                response = requests.get(url, stream=True, verify=False, timeout=30)

                if 200 <= response.status_code < 300:
                    # Get full response content to check for error page
                    content = response.content

                    # Check if this is a TTB error page
                    if is_ttb_error_page(content):
                        # This is an error page - treat as failure
                        sequence_tracker.record_failure()
                        failed_ttb_ids.append({
                            "ttb_id": ttb_id,
                            "sequence": sequence,
                            "status_code": response.status_code,
                            "error": "TTB error page detected"
                        })
                        logger.info(f"TTB error page detected for {ttb_id} (consecutive failures: {sequence_tracker.consecutive_failures})")

                        if sequence_tracker.should_stop():
                            logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive error pages")
                            break
                    else:
                        # Valid content - save to S3
                        sequence_tracker.record_success()

                        # Create S3 key following our naming convention
                        s3_key = f"{s3_prefix}/{date_str}/{method_type_str}/{ttb_id}.html"

                        # Upload to S3
                        s3_client.put_object(
                            Bucket=config.s3_bucket,
                            Key=s3_key,
                            Body=content,
                            ContentType='text/html'
                        )

                        successful_downloads.append({
                            "ttb_id": ttb_id,
                            "sequence": sequence,
                            "s3_key": s3_key,
                            "size_bytes": len(content)
                        })

                        logger.info(f"Successfully saved TTB {data_type} ID: {ttb_id} to S3: {s3_key}")
                else:
                    # HTTP error
                    sequence_tracker.record_failure()
                    failed_ttb_ids.append({
                        "ttb_id": ttb_id,
                        "sequence": sequence,
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code}"
                    })

                    if sequence_tracker.should_stop():
                        logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive failures")
                        break

            except Exception as e:
                sequence_tracker.record_failure()
                failed_ttb_ids.append({
                    "ttb_id": ttb_id,
                    "sequence": sequence,
                    "error": str(e)
                })
                logger.error(f"Error processing TTB ID {ttb_id}: {e}")

                if sequence_tracker.should_stop():
                    logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive failures")
                    break

            sequence += 1

    except Exception as e:
        logger.error(f"Critical error in TTB extraction: {e}")
        raise

    # Calculate statistics
    total_processed = len(successful_downloads) + len(failed_ttb_ids)
    success_rate = len(successful_downloads) / total_processed if total_processed > 0 else 0

    # Log comprehensive results
    logger.info(f"TTB {data_type} extraction complete for {partition_date}")
    logger.info(f"Successful downloads: {len(successful_downloads)}")
    logger.info(f"Failed attempts: {len(failed_ttb_ids)}")
    logger.info(f"Success rate: {success_rate:.2%}")

    # Record asset materialization with metadata
    context.log_event(
        AssetMaterialization(
            asset_key=context.asset_key,
            metadata={
                "partition_date": MetadataValue.text(date_str),
                "receipt_method": MetadataValue.int(receipt_method),
                "data_type": MetadataValue.text(data_type),
                "successful_downloads": MetadataValue.int(len(successful_downloads)),
                "failed_attempts": MetadataValue.int(len(failed_ttb_ids)),
                "success_rate": MetadataValue.float(success_rate),
                "total_sequences_processed": MetadataValue.int(total_processed),
                "consecutive_failures_at_stop": MetadataValue.int(sequence_tracker.consecutive_failures)
            }
        )
    )

    return {
        "partition_date": date_str,
        "receipt_method": receipt_method,
        "data_type": data_type,
        "successful_downloads": successful_downloads,
        "failed_ttb_ids": failed_ttb_ids,
        "total_processed": total_processed,
        "success_rate": success_rate,
        "consecutive_failures": sequence_tracker.consecutive_failures
    }