"""
Partitioned assets for TTB COLA data extraction.

This module implements modern Dagster partitioned assets for systematically
extracting TTB COLA data with intelligent rate limiting and sequence detection.
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
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    AssetMaterialization,
    MetadataValue,
    EnvVar
)
from dagster_aws.s3 import S3Resource

from .ttb_utils import TTBIDUtils, TTBSequenceTracker
from .ttb_config import TTBExtractionConfig

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



# Import centralized partition definitions
from .ttb_partitions import daily_partitions, method_type_partitions, ttb_partitions


@asset(
    partitions_def=ttb_partitions,
    description="TTB data partitioned by date, receipt method, and data type with intelligent sequence detection"
)
def ttb_partitioned(
    context,
    config: TTBExtractionConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Extract TTB data for a specific date, receipt method, and data type partition.

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
                        logger.info(f"TTB ID {ttb_id} returned error page - skipping upload")

                        # Check if we should stop due to consecutive error pages
                        if sequence_tracker.should_stop():
                            logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive error pages")
                            break
                    else:
                        # Valid content - process and upload
                        sequence_tracker.record_success()

                        # Generate S3 key with organized structure using configured prefix
                        s3_key = f"{s3_prefix}/data_type={data_type}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/receipt_method={receipt_method:03d}/{ttb_id}.html"

                        # Upload to S3 using temporary file
                        with tempfile.NamedTemporaryFile() as tmp_file:
                            # Write response content to temporary file
                            tmp_file.write(content)
                            tmp_file.flush()

                            # Upload to S3
                            s3_client.upload_file(tmp_file.name, config.s3_bucket, s3_key)

                        successful_downloads.append({
                            "ttb_id": ttb_id,
                            "s3_key": s3_key,
                            "sequence": sequence,
                            "status_code": response.status_code
                        })

                        logger.info(f"Successfully uploaded TTB ID {ttb_id} to s3://{config.s3_bucket}/{s3_key}")

                else:
                    # HTTP error - record failure
                    sequence_tracker.record_failure()
                    failed_ttb_ids.append({
                        "ttb_id": ttb_id,
                        "sequence": sequence,
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code}"
                    })

                    logger.warning(f"HTTP {response.status_code} for TTB ID {ttb_id}")

                    # Check if we should stop
                    if sequence_tracker.should_stop():
                        logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive failures")
                        break

            except requests.RequestException as e:
                # Request error - record failure
                sequence_tracker.record_failure()
                failed_ttb_ids.append({
                    "ttb_id": ttb_id,
                    "sequence": sequence,
                    "status_code": None,
                    "error": str(e)
                })

                logger.error(f"Request error for TTB ID {ttb_id}: {str(e)}")

                # Check if we should stop
                if sequence_tracker.should_stop():
                    logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive failures")
                    break

            sequence += 1

        # Gather final statistics
        stats = sequence_tracker.get_stats()

        # Log summary
        logger.info(f"Partition processing complete for {partition_date} receipt method {receipt_method} {data_type}")
        logger.info(f"Processed: {stats['total_processed']}, Success: {stats['total_success']}, Failed: {stats['total_failures']}")
        logger.info(f"Success rate: {stats['success_rate']:.2%}")

        # Create metadata for the asset
        context.add_output_metadata({
            "total_processed": MetadataValue.int(stats['total_processed']),
            "successful_downloads": MetadataValue.int(stats['total_success']),
            "failed_requests": MetadataValue.int(stats['total_failures']),
            "success_rate": MetadataValue.float(stats['success_rate']),
            "partition_date": MetadataValue.text(str(partition_date)),
            "receipt_method": MetadataValue.int(receipt_method),
            "data_type": MetadataValue.text(data_type),
            "consecutive_failures_at_end": MetadataValue.int(stats['consecutive_failures']),
            "s3_bucket": MetadataValue.text(config.s3_bucket),
            "s3_prefix": MetadataValue.text(s3_prefix),
            "action_param": MetadataValue.text(action_param),
            "files_uploaded": MetadataValue.json(successful_downloads[:10]),  # First 10 for brevity
        })

        # Return comprehensive results
        return {
            "partition_date": str(partition_date),
            "receipt_method": receipt_method,
            "data_type": data_type,
            "action_param": action_param,
            "s3_prefix": s3_prefix,
            "statistics": stats,
            "successful_downloads": successful_downloads,
            "failed_ttb_ids": failed_ttb_ids,
            "s3_bucket": config.s3_bucket,
            "processing_timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Unexpected error processing partition {partition_key}: {str(e)}")
        raise


@asset(
    description="Summary statistics across all TTB partitions (COLA and certificate data)"
)
def ttb_summary(ttb_partitioned) -> Dict[str, Any]:
    """
    Aggregate summary statistics across all TTB partitions.

    This downstream asset provides overall metrics and insights
    across all processed partitions for both COLA detail and certificate data.
    """
    logger = get_dagster_logger()

    # This would aggregate data from all partitions
    # For now, return a placeholder that demonstrates the concept
    logger.info("Generating TTB summary statistics")

    return {
        "summary_generated_at": datetime.now().isoformat(),
        "note": "This asset aggregates statistics from all TTB partitions (COLA detail and certificate)",
        "total_partitions_available": "Calculated from all materialized partitions",
        "data_types_supported": ["cola-detail", "certificate"]
    }