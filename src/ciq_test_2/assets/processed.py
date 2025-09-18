"""
Processed Data Assets

This module contains assets for data processing and transformation stages.
These assets take raw data and convert it into structured, validated formats.
"""
import tempfile
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    AssetDep
)
from dagster_aws.s3 import S3Resource

from .raw import ttb_raw_data
from ..utils.ttb_data_extraction import parse_ttb_html
from ..config.ttb_partitions import ttb_partitions
from ..utils.ttb_transformations import apply_field_transformations, transformation_registry, load_ttb_reference_data
from ..utils.ttb_schema import (
    schema_registry,
    create_parquet_dataset,
    validate_records_against_schema
)


class TTBProcessingConfig(Config):
    """Configuration for TTB processing assets."""
    s3_bucket: str = "ciq-dagster"
    s3_input_prefix: str = "1-ttb-raw-data"
    s3_output_prefix: str = "2-ttb-processed-data"
    processing_batch_size: int = 100
    enable_parsing_validation: bool = True
    data_quality_analysis: bool = True


class TTBCleaningConfig(Config):
    """Configuration for TTB cleaning assets."""
    s3_bucket: str = "ciq-dagster"
    s3_output_prefix: str = "2-ttb-processed-data"
    enable_deduplication: bool = True
    data_quality_analysis: bool = True
    create_partitioned_output: bool = True


class TTBStructuringConfig(Config):
    """Configuration for TTB structuring assets."""
    s3_bucket: str = "ciq-dagster"
    s3_output_prefix: str = "2-ttb-processed-data"
    create_partitioned_output: bool = True
    validate_schema: bool = True
    data_quality_analysis: bool = True


@asset(
    partitions_def=ttb_partitions,
    group_name="ttb_processing",
    description="Extracted structured data from raw TTB HTML files",
    deps=[AssetDep(ttb_raw_data)],
    metadata={
        "data_type": "extracted",
        "stage": "processing",
        "format": "json"
    }
)
def ttb_extracted_data(
    context: AssetExecutionContext,
    config: TTBProcessingConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Extract structured data from raw TTB HTML files.

    This asset processes raw HTML data downloaded from TTB and extracts
    structured fields using parsing logic.

    Returns:
        Dictionary containing extracted structured data
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    # Parse method_type (format: "001-cola-detail")
    receipt_method_str, data_type = method_type_str.split("-", 1)
    receipt_method = int(receipt_method_str)

    logger.info(f"Processing TTB {data_type} extraction for {date_str}, method {receipt_method}")

    s3_client = s3_resource.get_client()

    # List files in the raw data partition
    s3_prefix = f"{config.s3_input_prefix}/{date_str}/{method_type_str}/"

    try:
        # List objects in S3
        response = s3_client.list_objects_v2(
            Bucket=config.s3_bucket,
            Prefix=s3_prefix
        )

        files = response.get('Contents', [])
        logger.info(f"Found {len(files)} raw files to process")

        extracted_records = []
        processing_stats = {
            "total_files": len(files),
            "successful_extractions": 0,
            "failed_extractions": 0,
            "total_fields_extracted": 0
        }

        for file_obj in files:
            try:
                # Download file content
                s3_key = file_obj['Key']
                file_response = s3_client.get_object(Bucket=config.s3_bucket, Key=s3_key)
                html_content = file_response['Body'].read().decode('utf-8')

                # Extract TTB ID from filename
                filename = Path(s3_key).stem
                ttb_id = filename.replace('.html', '')

                # Parse HTML content
                extracted_data = parse_ttb_html(html_content, data_type)

                if extracted_data:
                    # Add metadata
                    extracted_data['ttb_id'] = ttb_id
                    extracted_data['source_file'] = s3_key
                    extracted_data['extraction_timestamp'] = datetime.now().isoformat()
                    extracted_data['partition_date'] = date_str
                    extracted_data['receipt_method'] = receipt_method
                    extracted_data['data_type'] = data_type

                    extracted_records.append(extracted_data)
                    processing_stats["successful_extractions"] += 1
                    processing_stats["total_fields_extracted"] += len(extracted_data)
                else:
                    logger.warning(f"No data extracted from {s3_key}")
                    processing_stats["failed_extractions"] += 1

            except Exception as e:
                logger.error(f"Error processing {s3_key}: {e}")
                processing_stats["failed_extractions"] += 1

        # Save extracted data to S3
        if extracted_records:
            output_key = f"{config.s3_output_prefix}/extracted/{date_str}/{method_type_str}/extracted_data.json"

            import json
            json_content = json.dumps(extracted_records, indent=2, default=str)

            s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=output_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )

            logger.info(f"Saved {len(extracted_records)} extracted records to {output_key}")

        # Add metadata
        context.add_output_metadata({
            "total_files": MetadataValue.int(processing_stats["total_files"]),
            "successful_extractions": MetadataValue.int(processing_stats["successful_extractions"]),
            "failed_extractions": MetadataValue.int(processing_stats["failed_extractions"]),
            "success_rate": MetadataValue.float(
                processing_stats["successful_extractions"] / max(processing_stats["total_files"], 1)
            ),
            "total_fields_extracted": MetadataValue.int(processing_stats["total_fields_extracted"]),
            "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{config.s3_output_prefix}/extracted/{date_str}/{method_type_str}/")
        })

        return {
            "partition_date": date_str,
            "receipt_method": receipt_method,
            "data_type": data_type,
            "extracted_records": extracted_records,
            "processing_stats": processing_stats
        }

    except Exception as e:
        logger.error(f"Critical error in extraction: {e}")
        raise


@asset(
    partitions_def=ttb_partitions,
    group_name="ttb_processing",
    description="Cleaned and validated TTB data with transformations applied",
    deps=[AssetDep(ttb_extracted_data)],
    metadata={
        "data_type": "cleaned",
        "stage": "processing",
        "format": "parquet"
    }
)
def ttb_cleaned_data(
    context: AssetExecutionContext,
    config: TTBCleaningConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Clean and validate extracted TTB data.

    This asset applies data cleaning transformations, deduplication,
    and validation to the extracted data.

    Returns:
        Dictionary containing cleaned data statistics
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    logger.info(f"Cleaning TTB data for {date_str}, method_type {method_type_str}")

    # Implement cleaning logic here
    # This is a placeholder for the actual cleaning implementation

    cleaning_stats = {
        "records_processed": 0,
        "records_cleaned": 0,
        "duplicates_removed": 0,
        "validation_errors": 0
    }

    # Add metadata
    context.add_output_metadata({
        "records_processed": MetadataValue.int(cleaning_stats["records_processed"]),
        "records_cleaned": MetadataValue.int(cleaning_stats["records_cleaned"]),
        "duplicates_removed": MetadataValue.int(cleaning_stats["duplicates_removed"]),
        "validation_errors": MetadataValue.int(cleaning_stats["validation_errors"])
    })

    return {
        "partition_date": date_str,
        "method_type": method_type_str,
        "cleaning_stats": cleaning_stats
    }


@asset(
    partitions_def=ttb_partitions,
    group_name="ttb_processing",
    description="Structured TTB data in final schema format",
    deps=[AssetDep(ttb_cleaned_data)],
    metadata={
        "data_type": "structured",
        "stage": "processing",
        "format": "parquet"
    }
)
def ttb_structured_data(
    context: AssetExecutionContext,
    config: TTBStructuringConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Structure cleaned TTB data into final schema format.

    This asset applies final schema transformations and validates
    the data against the target schema.

    Returns:
        Dictionary containing structuring statistics
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    logger.info(f"Structuring TTB data for {date_str}, method_type {method_type_str}")

    # Implement structuring logic here
    # This is a placeholder for the actual structuring implementation

    structuring_stats = {
        "records_structured": 0,
        "schema_violations": 0,
        "data_quality_score": 0.0
    }

    # Add metadata
    context.add_output_metadata({
        "records_structured": MetadataValue.int(structuring_stats["records_structured"]),
        "schema_violations": MetadataValue.int(structuring_stats["schema_violations"]),
        "data_quality_score": MetadataValue.float(structuring_stats["data_quality_score"])
    })

    return {
        "partition_date": date_str,
        "method_type": method_type_str,
        "structuring_stats": structuring_stats
    }