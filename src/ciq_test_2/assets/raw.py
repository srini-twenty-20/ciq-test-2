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
from typing import Dict, Any, List

import requests
from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetMaterialization,
    MetadataValue,
    AssetExecutionContext
)

from ..utils.ttb_utils import TTBIDUtils, TTBSequenceTracker
from ..config.ttb_config import TTBExtractionConfig
from ..config.ttb_partitions import daily_partitions

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
    partitions_def=daily_partitions,
    group_name="ttb_raw",
    description="Raw TTB data extraction partitioned by date for all data types and receipt methods",
    metadata={
        "data_type": "raw",
        "source": "ttbonline.gov",
        "format": "html"
    }
)
def ttb_raw_data(
    context: AssetExecutionContext,
    config: TTBExtractionConfig
) -> List[Dict[str, Any]]:
    """
    Extract raw TTB data for a specific date, processing all receipt methods and data types.

    This asset implements:
    - Rate limiting (0.5s between requests)
    - Intelligent sequence detection (stop after configurable consecutive failures)
    - Comprehensive logging and metadata
    - Returns raw HTML data for IO Manager to store
    - Processes both COLA detail and certificate data for all receipt methods

    Partition key format: date (e.g. "2024-01-01")

    Returns:
        List of dictionaries containing TTB records with HTML content
    """
    logger = get_dagster_logger()

    # Get partition information (now just a date string)
    date_str = context.partition_key
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    logger.info(f"Processing TTB data for date: {partition_date}")

    # Configure data types and receipt methods for daily processing
    data_types = ["cola-detail", "certificate"]
    receipt_methods = [1]  # Primary receipt method

    logger.info(f"Data types: {data_types}")
    logger.info(f"Receipt methods: {receipt_methods}")

    # Initialize tracking
    all_extracted_records = []
    total_failed_count = 0

    # Generate TTB IDs for this partition
    julian_day = TTBIDUtils.date_to_julian(partition_date)

    try:
        # Process each combination of receipt method and data type
        for receipt_method in receipt_methods:
            for data_type in data_types:
                logger.info(f"Processing {data_type} data for receipt method {receipt_method}")

                # Configure action parameter based on data type
                if data_type == "cola-detail":
                    action_param = "publicDisplaySearchAdvanced"
                elif data_type == "certificate":
                    action_param = "publicFormDisplay"
                else:
                    logger.warning(f"Unknown data_type: {data_type}, skipping...")
                    continue

                # Initialize tracking for this combination
                sequence_tracker = TTBSequenceTracker(max_consecutive_failures=config.consecutive_failure_threshold)
                failed_count = 0

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
                                failed_count += 1

                                if sequence_tracker.should_stop():
                                    logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive error pages")
                                    break
                            else:
                                # Valid content - collect the data
                                sequence_tracker.record_success()

                                # Store the extracted record with metadata
                                record = {
                                    "ttb_id": ttb_id,
                                    "sequence": sequence,
                                    "html_content": content.decode('utf-8', errors='ignore'),
                                    "partition_date": date_str,
                                    "receipt_method": receipt_method,
                                    "data_type": data_type,
                                    "extraction_timestamp": datetime.now().isoformat(),
                                    "url": url,
                                    "size_bytes": len(content),
                                    "status_code": response.status_code
                                }

                                all_extracted_records.append(record)
                                logger.debug(f"Successfully extracted TTB {data_type} ID: {ttb_id}")
                        else:
                            # HTTP error
                            sequence_tracker.record_failure()
                            failed_count += 1

                            if sequence_tracker.should_stop():
                                logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive failures")
                                break

                    except Exception as e:
                        sequence_tracker.record_failure()
                        failed_count += 1
                        logger.error(f"Error processing TTB ID {ttb_id}: {e}")

                        if sequence_tracker.should_stop():
                            logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive failures")
                            break

                    sequence += 1

                total_failed_count += failed_count
                logger.info(f"Completed {data_type}/{receipt_method}: {len([r for r in all_extracted_records if r['data_type'] == data_type and r['receipt_method'] == receipt_method])} successful, {failed_count} failed")

    except Exception as e:
        logger.error(f"Critical error in TTB extraction: {e}")
        raise

    # Calculate overall statistics
    total_processed = len(all_extracted_records) + total_failed_count
    success_rate = len(all_extracted_records) / total_processed if total_processed > 0 else 0

    # Count by data type for metadata
    cert_count = len([r for r in all_extracted_records if r['data_type'] == 'certificate'])
    cola_count = len([r for r in all_extracted_records if r['data_type'] == 'cola-detail'])

    # Log comprehensive results
    logger.info(f"TTB extraction complete for {partition_date}")
    logger.info(f"Total successful extractions: {len(all_extracted_records)} (cert: {cert_count}, cola-detail: {cola_count})")
    logger.info(f"Total failed attempts: {total_failed_count}")
    logger.info(f"Overall success rate: {success_rate:.2%}")

    # Add metadata to context
    context.add_output_metadata({
        "partition_date": MetadataValue.text(date_str),
        "successful_extractions": MetadataValue.int(len(all_extracted_records)),
        "certificate_extractions": MetadataValue.int(cert_count),
        "cola_detail_extractions": MetadataValue.int(cola_count),
        "failed_attempts": MetadataValue.int(total_failed_count),
        "success_rate": MetadataValue.float(success_rate),
        "total_sequences_processed": MetadataValue.int(total_processed),
        "data_types_processed": MetadataValue.text(",".join(data_types)),
        "receipt_methods_processed": MetadataValue.text(",".join(map(str, receipt_methods)))
    })

    return all_extracted_records