"""
TTB COLA Extraction Assets

This module implements the core extraction logic for TTB COLA certificates,
fetching both detail and certificate views, preserving raw HTML, and creating
consolidated structured data.
"""
import requests
import urllib3
from datetime import datetime
from typing import Dict, Any, List, Tuple

from dagster import (
    multi_asset,
    AssetOut,
    AssetExecutionContext,
    get_dagster_logger,
    MetadataValue
)
from dagster_aws.s3 import S3Resource

from ..config.partitions import ttb_partitions
from ..config.settings import TTBExtractionConfig
from ..utils.ttb_utils import (
    TTBIDUtils,
    TTBSequenceTracker,
    is_ttb_error_page,
    validate_cola_data_consistency
)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@multi_asset(
    outs={
        "ttb_cola_detail_html": AssetOut(
            partitions_def=ttb_partitions,
            io_manager_key="s3_html_io_manager"
        ),
        "ttb_cola_certificate_html": AssetOut(
            partitions_def=ttb_partitions,
            io_manager_key="s3_html_io_manager"
        ),
        "ttb_cola_consolidated_data": AssetOut(
            partitions_def=ttb_partitions,
            io_manager_key="s3_parquet_io_manager"
        )
    },
    group_name="ttb_extraction",
    description="Extract complete TTB COLA certificates with both detail and certificate views"
)
def ttb_cola_extraction(
    context: AssetExecutionContext,
    config: TTBExtractionConfig,
    s3_resource: S3Resource
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Extract complete TTB COLA certificates including both views and consolidated data.

    For each TTB ID in the partition:
    1. Fetches detail view (product information, ingredients, etc.)
    2. Fetches certificate view (approval status, dates, etc.)
    3. Preserves raw HTML content for both views
    4. Creates consolidated structured data combining both views
    5. Validates data consistency across views

    Partition format: "date|method"
    Example: "2024-01-01|001"

    Returns:
        Tuple of (detail_html_list, certificate_html_list, consolidated_data_list)
    """
    logger = get_dagster_logger()

    # Parse partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_str = partition_key.keys_by_dimension["method"]

    # Parse partition data
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    receipt_method = int(method_str)

    logger.info(f"Extracting TTB COLA data for date: {partition_date}, receipt method: {receipt_method}")

    # Initialize tracking and results
    sequence_tracker = TTBSequenceTracker(max_consecutive_failures=config.consecutive_failure_threshold)
    detail_htmls = []
    certificate_htmls = []
    consolidated_data = []

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

            logger.info(f"Processing TTB ID: {ttb_id} (sequence {sequence})")

            # Extract both views for this TTB ID
            detail_result, cert_result = extract_cola_views(ttb_id, config, logger)

            if detail_result["success"] and cert_result["success"]:
                # Both views successful - process complete COLA entity
                sequence_tracker.record_success()

                # Store raw HTML data
                detail_html = {
                    "ttb_id": ttb_id,
                    "url": detail_result["url"],
                    "html_content": detail_result["content"],
                    "fetch_timestamp": datetime.now().isoformat(),
                    "content_hash": detail_result["content_hash"],
                    "content_size": len(detail_result["content"])
                }

                cert_html = {
                    "ttb_id": ttb_id,
                    "url": cert_result["url"],
                    "html_content": cert_result["content"],
                    "fetch_timestamp": datetime.now().isoformat(),
                    "content_hash": cert_result["content_hash"],
                    "content_size": len(cert_result["content"])
                }

                # Parse structured data (basic parsing - can be enhanced)
                detail_parsed = parse_cola_detail_view(detail_result["content"])
                cert_parsed = parse_cola_certificate_view(cert_result["content"])

                # Validate consistency
                validation_results = validate_cola_data_consistency(detail_parsed, cert_parsed)

                # Create consolidated record
                consolidated = {
                    "ttb_id": ttb_id,
                    "extraction_date": date_str,
                    "receipt_method": receipt_method,
                    "sequence": sequence,

                    # Basic entity data (to be enhanced with actual parsing)
                    "product_name": detail_parsed.get("product_name"),
                    "applicant_name": detail_parsed.get("applicant_name"),
                    "approval_date": cert_parsed.get("approval_date"),
                    "certificate_number": cert_parsed.get("certificate_number"),
                    "status": cert_parsed.get("status"),

                    # Data quality and consistency
                    "data_consistency": validation_results,
                    "has_both_views": True,

                    # Source tracking
                    "source_detail_hash": detail_html["content_hash"],
                    "source_certificate_hash": cert_html["content_hash"],
                    "fetch_timestamp": datetime.now().isoformat()
                }

                detail_htmls.append(detail_html)
                certificate_htmls.append(cert_html)
                consolidated_data.append(consolidated)

                logger.info(f"Successfully processed complete COLA entity: {ttb_id}")

            else:
                # One or both views failed
                sequence_tracker.record_failure()

                # Log the failure details
                if not detail_result["success"]:
                    logger.info(f"Detail view failed for {ttb_id}: {detail_result.get('error', 'Unknown error')}")
                if not cert_result["success"]:
                    logger.info(f"Certificate view failed for {ttb_id}: {cert_result.get('error', 'Unknown error')}")

                # Check if we should stop
                if sequence_tracker.should_stop():
                    logger.info(f"Stopping after {sequence_tracker.consecutive_failures} consecutive failures")
                    break

            sequence += 1

    except Exception as e:
        logger.error(f"Critical error in TTB COLA extraction: {e}")
        raise

    # Calculate statistics
    total_processed = len(consolidated_data) + sequence_tracker.total_failures
    success_rate = len(consolidated_data) / total_processed if total_processed > 0 else 0

    # Log results
    logger.info(f"TTB COLA extraction complete for {partition_date}")
    logger.info(f"Successful complete entities: {len(consolidated_data)}")
    logger.info(f"Failed attempts: {sequence_tracker.total_failures}")
    logger.info(f"Success rate: {success_rate:.2%}")

    # Add metadata to context
    context.add_output_metadata(
        metadata={
            "partition_date": MetadataValue.text(date_str),
            "receipt_method": MetadataValue.int(receipt_method),
            "successful_entities": MetadataValue.int(len(consolidated_data)),
            "failed_attempts": MetadataValue.int(sequence_tracker.total_failures),
            "success_rate": MetadataValue.float(success_rate),
            "total_sequences_processed": MetadataValue.int(total_processed)
        },
        output_name="ttb_cola_consolidated_data"
    )

    return detail_htmls, certificate_htmls, consolidated_data


def extract_cola_views(ttb_id: str, config: TTBExtractionConfig, logger) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Extract both detail and certificate views for a single TTB ID.

    Returns:
        Tuple of (detail_result, certificate_result) dictionaries
    """
    # Build URLs for both views
    detail_url = f"{config.base_url}?action={config.detail_action}&ttbid={ttb_id}"
    cert_url = f"{config.base_url}?action={config.certificate_action}&ttbid={ttb_id}"

    # Extract detail view
    detail_result = extract_single_view(detail_url, "detail", config, logger)

    # Rate limiting between requests
    TTBIDUtils.rate_limit_sleep(config.request_delay_seconds)

    # Extract certificate view
    cert_result = extract_single_view(cert_url, "certificate", config, logger)

    return detail_result, cert_result


def extract_single_view(url: str, view_type: str, config: TTBExtractionConfig, logger) -> Dict[str, Any]:
    """
    Extract a single view (detail or certificate) from TTB.

    Returns:
        Dictionary with success status, content, and metadata
    """
    try:
        response = requests.get(
            url,
            verify=config.ssl_verify,
            timeout=30,
            stream=True
        )

        if 200 <= response.status_code < 300:
            content = response.content

            # Check if this is a TTB error page
            if is_ttb_error_page(content):
                return {
                    "success": False,
                    "url": url,
                    "view_type": view_type,
                    "error": "TTB error page detected",
                    "status_code": response.status_code
                }

            # Success - return content and metadata
            import hashlib
            return {
                "success": True,
                "url": url,
                "view_type": view_type,
                "content": content.decode('utf-8'),
                "content_hash": hashlib.sha256(content).hexdigest(),
                "status_code": response.status_code
            }

        else:
            return {
                "success": False,
                "url": url,
                "view_type": view_type,
                "error": f"HTTP {response.status_code}",
                "status_code": response.status_code
            }

    except Exception as e:
        return {
            "success": False,
            "url": url,
            "view_type": view_type,
            "error": str(e)
        }


def parse_cola_detail_view(html_content: str) -> Dict[str, Any]:
    """
    Parse the detail view HTML to extract structured data.

    This is a placeholder implementation - should be enhanced with actual parsing logic.
    """
    # TODO: Implement actual HTML parsing using BeautifulSoup or similar
    # For now, return basic structure
    return {
        "product_name": None,  # Extract from HTML
        "applicant_name": None,  # Extract from HTML
        "brand_name": None,  # Extract from HTML
        "product_type": None,  # Extract from HTML
        "parse_status": "placeholder_implementation"
    }


def parse_cola_certificate_view(html_content: str) -> Dict[str, Any]:
    """
    Parse the certificate view HTML to extract structured data.

    This is a placeholder implementation - should be enhanced with actual parsing logic.
    """
    # TODO: Implement actual HTML parsing using BeautifulSoup or similar
    # For now, return basic structure
    return {
        "approval_date": None,  # Extract from HTML
        "certificate_number": None,  # Extract from HTML
        "status": None,  # Extract from HTML
        "approval_type": None,  # Extract from HTML
        "parse_status": "placeholder_implementation"
    }