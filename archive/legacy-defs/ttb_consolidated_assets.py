"""
Consolidated TTB assets that merge COLA detail and certificate data into unified tables.
"""
import os
import tempfile
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    DailyPartitionsDefinition,
    AssetDep,
    AllPartitionMapping
)
from dagster_aws.s3 import S3Resource

from .ttb_consolidated_schema import (
    get_consolidated_ttb_schema,
    get_dimension_schemas,
    get_data_quality_tiers
)


class ConsolidatedConfig(Config):
    """Configuration for consolidated TTB assets."""
    bucket_name: str = "ciq-dagster"
    s3_output_prefix: str = "3-ttb-consolidated"
    create_partitioned_output: bool = True
    data_quality_analysis: bool = True
    deduplication_enabled: bool = True


# Import centralized partition definitions
from .ttb_partitions import daily_partitions, method_type_partitions, ttb_partitions


@asset(
    partitions_def=daily_partitions,  # Consolidate by date across all method_types
    deps=[
        AssetDep("ttb_structured_output", partition_mapping=AllPartitionMapping())
    ],
    description="Consolidated flat table combining COLA detail and certificate data by date"
)
def ttb_consolidated_data(
    context: AssetExecutionContext,
    config: ConsolidatedConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Consolidate COLA detail and certificate data into a single flat table.

    This asset reads structured output from both COLA detail and certificate
    partitions for a given date and creates a unified table with TTB ID as the key.
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    logger.info(f"Consolidating TTB data for date: {partition_date}")

    s3_client = s3_resource.get_client()
    schema = get_consolidated_ttb_schema()

    # Read data from all method_type partitions for this date
    consolidated_records = []
    processing_stats = {
        'partitions_processed': 0,
        'cola_records_found': 0,
        'cert_records_found': 0,
        'consolidated_records': 0,
        'duplicate_ttb_ids': 0,
        'data_quality_scores': {},
        'processing_errors': []
    }

    try:
        # Define receipt methods and data types to process
        receipt_methods = ["001", "002", "003", "000"]  # e-filed, mailed, overnight, hand delivered
        data_types = ["cola-detail", "certificate"]

        # Process all receipt methods and data types for this date
        for receipt_method in receipt_methods:
            for data_type in data_types:
                method_type = f"{receipt_method}-{data_type}"

                try:
                    # Read structured data from S3
                    s3_prefix = f"2-ttb-processed-data/data_type={data_type}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/receipt_method={receipt_method}"

                    # List parquet files in this partition
                    response = s3_client.list_objects_v2(
                        Bucket=config.bucket_name,
                        Prefix=s3_prefix
                    )

                    if 'Contents' not in response:
                        logger.info(f"No data found for {partition_date} {method_type}")
                        continue

                    parquet_files = [obj['Key'] for obj in response['Contents']
                                   if obj['Key'].endswith('.parquet')]

                    for parquet_file in parquet_files:
                        logger.info(f"Reading {parquet_file}")

                        # Download and read parquet file
                        with tempfile.NamedTemporaryFile() as tmp_file:
                            s3_client.download_file(config.bucket_name, parquet_file, tmp_file.name)
                            df = pd.read_parquet(tmp_file.name)

                            if data_type == "cola-detail":
                                processing_stats['cola_records_found'] += len(df)
                                processed_records = _process_cola_records(df, receipt_method, partition_date)
                            else:  # certificate
                                processing_stats['cert_records_found'] += len(df)
                                processed_records = _process_certificate_records(df, receipt_method, partition_date)

                            consolidated_records.extend(processed_records)

                    processing_stats['partitions_processed'] += 1

                except Exception as e:
                    error_msg = f"Error processing {method_type} for {partition_date}: {str(e)}"
                    logger.warning(error_msg)
                    processing_stats['processing_errors'].append(error_msg)

        # Merge records by TTB ID
        logger.info(f"Merging {len(consolidated_records)} records by TTB ID")
        merged_records = _merge_records_by_ttb_id(consolidated_records, config)
        processing_stats['consolidated_records'] = len(merged_records)

        # Apply data quality analysis
        if config.data_quality_analysis:
            merged_records = _apply_data_quality_analysis(merged_records)

        # Calculate processing statistics
        quality_tiers = {}
        for record in merged_records:
            tier = record.get('data_quality_tier', 'unknown')
            quality_tiers[tier] = quality_tiers.get(tier, 0) + 1
        processing_stats['data_quality_scores'] = quality_tiers

        # Write consolidated data to S3
        if merged_records:
            output_s3_key = _write_consolidated_data_to_s3(
                merged_records, schema, config, s3_client, partition_date
            )

            logger.info(f"Successfully wrote {len(merged_records)} consolidated records to {output_s3_key}")
        else:
            logger.warning(f"No consolidated records to write for {partition_date}")

        # Create metadata
        context.add_output_metadata({
            "consolidated_records": MetadataValue.int(processing_stats['consolidated_records']),
            "partitions_processed": MetadataValue.int(processing_stats['partitions_processed']),
            "cola_records_found": MetadataValue.int(processing_stats['cola_records_found']),
            "cert_records_found": MetadataValue.int(processing_stats['cert_records_found']),
            "data_quality_tiers": MetadataValue.json(processing_stats['data_quality_scores']),
            "partition_date": MetadataValue.text(partition_date_str),
            "s3_bucket": MetadataValue.text(config.bucket_name),
            "s3_output_prefix": MetadataValue.text(config.s3_output_prefix),
            "processing_errors_count": MetadataValue.int(len(processing_stats['processing_errors']))
        })

        return {
            'partition_date': partition_date_str,
            'processing_stats': processing_stats,
            'consolidated_records_count': len(merged_records) if merged_records else 0,
            'output_s3_key': output_s3_key if merged_records else None,
            'processing_timestamp': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Critical error in consolidation: {str(e)}")
        raise


def _process_cola_records(df: pd.DataFrame, receipt_method: str, partition_date: date) -> List[Dict[str, Any]]:
    """Process COLA detail records into consolidated format."""
    records = []

    for _, row in df.iterrows():
        record = {
            # Primary identifiers
            'ttb_id': row.get('ttb_id'),
            'partition_date': partition_date,
            'receipt_method': int(receipt_method),

            # Data source indicators
            'has_cola_detail': True,
            'has_certificate': False,

            # COLA Detail fields
            'cola_serial_number': row.get('serial_number'),
            'cola_filing_date': _parse_date(row.get('filing_date')),
            'cola_approval_date': _parse_date(row.get('approval_date')),
            'cola_expiration_date': _parse_date(row.get('expiration_date')),
            'cola_brand_name': row.get('brand_name'),
            'cola_fanciful_name': row.get('fanciful_name'),
            'cola_product_description': row.get('product_description'),
            'cola_net_contents': row.get('net_contents'),
            'cola_alcohol_content': row.get('alcohol_content'),
            'cola_product_class_type': row.get('product_class_type'),
            'cola_origin_code': row.get('origin_code'),
            'cola_formula_number': row.get('formula_number'),
            'cola_label_images_count': row.get('label_images_count', 0),
            'cola_qualifications': row.get('qualifications'),

            # Common applicant info
            'applicant_business_name': row.get('applicant_business_name'),
            'applicant_mailing_address': row.get('applicant_mailing_address'),
            'applicant_phone': row.get('applicant_phone'),
            'applicant_email': row.get('applicant_email'),
            'applicant_fax': row.get('applicant_fax'),

            # Validation results
            'ttb_id_valid': row.get('ttb_id_valid', False),
            'product_class_validation_is_valid': row.get('product_class_validation', {}).get('is_valid', False),
            'product_class_validation_code': row.get('product_class_validation', {}).get('code'),
            'product_class_validation_description': row.get('product_class_validation', {}).get('description'),
            'origin_code_validation_is_valid': row.get('origin_code_validation', {}).get('is_valid', False),
            'origin_code_validation_code': row.get('origin_code_validation', {}).get('code'),
            'origin_code_validation_description': row.get('origin_code_validation', {}).get('description'),

            # Processing metadata
            'cola_source_s3_key': row.get('source_s3_key'),
            'processing_timestamp': datetime.now(),
        }

        records.append(record)

    return records


def _process_certificate_records(df: pd.DataFrame, receipt_method: str, partition_date: date) -> List[Dict[str, Any]]:
    """Process certificate records into consolidated format."""
    records = []

    for _, row in df.iterrows():
        record = {
            # Primary identifiers
            'ttb_id': row.get('ttb_id'),
            'partition_date': partition_date,
            'receipt_method': int(receipt_method),

            # Data source indicators
            'has_cola_detail': False,
            'has_certificate': True,

            # Certificate fields
            'cert_certificate_number': row.get('certificate_number'),
            'cert_application_date': _parse_date(row.get('application_date')),
            'cert_approval_date': _parse_date(row.get('approval_date')),
            'cert_certificate_status': row.get('certificate_status'),
            'cert_certificate_type': row.get('certificate_type'),
            'cert_applicant_signature_present': row.get('applicant_signature_present', False),
            'cert_authorized_signature_present': row.get('authorized_signature_present', False),
            'cert_authorized_signature_url': row.get('authorized_signature_url'),
            'cert_plant_registry_number': row.get('plant_registry_number'),

            # Common applicant info
            'applicant_business_name': row.get('applicant_business_name'),
            'applicant_mailing_address': row.get('applicant_mailing_address'),
            'applicant_phone': row.get('applicant_phone'),
            'applicant_email': row.get('applicant_email'),
            'applicant_fax': row.get('applicant_fax'),

            # Validation results
            'ttb_id_valid': row.get('ttb_id_valid', False),

            # Processing metadata
            'cert_source_s3_key': row.get('source_s3_key'),
            'processing_timestamp': datetime.now(),
        }

        records.append(record)

    return records


def _merge_records_by_ttb_id(records: List[Dict[str, Any]], config: ConsolidatedConfig) -> List[Dict[str, Any]]:
    """Merge COLA and certificate records by TTB ID."""
    ttb_id_map = {}

    # Group records by TTB ID
    for record in records:
        ttb_id = record.get('ttb_id')
        if not ttb_id:
            continue

        if ttb_id not in ttb_id_map:
            ttb_id_map[ttb_id] = {
                'cola_record': None,
                'cert_record': None
            }

        if record.get('has_cola_detail'):
            ttb_id_map[ttb_id]['cola_record'] = record
        elif record.get('has_certificate'):
            ttb_id_map[ttb_id]['cert_record'] = record

    # Merge records
    merged_records = []
    for ttb_id, record_pair in ttb_id_map.items():
        cola_record = record_pair['cola_record']
        cert_record = record_pair['cert_record']

        # Create merged record
        merged = _create_merged_record(cola_record, cert_record, ttb_id)
        merged_records.append(merged)

    return merged_records


def _create_merged_record(cola_record: Optional[Dict], cert_record: Optional[Dict], ttb_id: str) -> Dict[str, Any]:
    """Create a single merged record from COLA and certificate data."""
    # Start with the record that has data, or create empty structure
    if cola_record:
        merged = cola_record.copy()
    elif cert_record:
        merged = cert_record.copy()
    else:
        merged = {'ttb_id': ttb_id}

    # Update data source indicators
    merged['has_cola_detail'] = cola_record is not None
    merged['has_certificate'] = cert_record is not None

    # Merge certificate data if available
    if cert_record and cola_record:
        # Update certificate fields
        for key, value in cert_record.items():
            if key.startswith('cert_') or key in ['has_certificate']:
                merged[key] = value

        # Merge applicant info (prefer COLA data, fallback to certificate)
        for field in ['applicant_business_name', 'applicant_mailing_address',
                     'applicant_phone', 'applicant_email', 'applicant_fax']:
            if not merged.get(field) and cert_record.get(field):
                merged[field] = cert_record[field]

    # Calculate data completeness score
    merged['data_completeness_score'] = _calculate_completeness_score(merged)

    # Add computed fields
    merged.update(_calculate_computed_fields(merged))

    return merged


def _calculate_completeness_score(record: Dict[str, Any]) -> float:
    """Calculate data completeness score (0.0 to 1.0)."""
    important_fields = [
        'ttb_id', 'applicant_business_name', 'applicant_mailing_address',
        'cola_brand_name', 'cola_product_description', 'cola_approval_date'
    ]

    filled_fields = sum(1 for field in important_fields
                       if record.get(field) is not None and record.get(field) != '')

    return filled_fields / len(important_fields)


def _calculate_computed_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate computed analytics fields."""
    computed = {}

    # Calculate days to approval
    filing_date = record.get('cola_filing_date')
    approval_date = record.get('cola_approval_date')
    if filing_date and approval_date:
        computed['days_to_approval'] = (approval_date - filing_date).days

    # Product type flags
    product_class = str(record.get('cola_product_class_type', '')).lower()
    computed['is_wine'] = 'wine' in product_class
    computed['is_beer'] = 'beer' in product_class or 'malt' in product_class
    computed['is_spirits'] = 'spirit' in product_class or 'distilled' in product_class

    # Applicant info completeness
    applicant_fields = ['applicant_business_name', 'applicant_mailing_address',
                       'applicant_phone', 'applicant_email']
    has_complete_info = all(record.get(field) for field in applicant_fields)
    computed['has_complete_applicant_info'] = has_complete_info

    return computed


def _apply_data_quality_analysis(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply data quality tier analysis to records."""
    quality_tiers = get_data_quality_tiers()

    for record in records:
        completeness_score = record.get('data_completeness_score', 0.0)

        # Determine quality tier
        if completeness_score >= quality_tiers['premium']['min_completeness_score']:
            if _meets_requirements(record, quality_tiers['premium']):
                record['data_quality_tier'] = 'premium'
            else:
                record['data_quality_tier'] = 'standard'
        elif completeness_score >= quality_tiers['standard']['min_completeness_score']:
            if _meets_requirements(record, quality_tiers['standard']):
                record['data_quality_tier'] = 'standard'
            else:
                record['data_quality_tier'] = 'basic'
        else:
            record['data_quality_tier'] = 'basic'

    return records


def _meets_requirements(record: Dict[str, Any], requirements: Dict[str, Any]) -> bool:
    """Check if record meets quality tier requirements."""
    # Check required fields
    for field in requirements['required_fields']:
        if not record.get(field):
            return False

    # Check validation requirements
    for validation in requirements['validation_requirements']:
        if not record.get(validation, False):
            return False

    return True


def _write_consolidated_data_to_s3(
    records: List[Dict[str, Any]],
    schema: pa.Schema,
    config: ConsolidatedConfig,
    s3_client,
    partition_date: date
) -> str:
    """Write consolidated data to S3 as Parquet."""
    # Convert to PyArrow table
    df = pd.DataFrame(records)

    # Ensure schema compliance
    for field in schema:
        if field.name not in df.columns:
            if field.type == pa.bool_():
                df[field.name] = False
            elif field.type in [pa.int32(), pa.int64()]:
                df[field.name] = None
            elif field.type == pa.float32():
                df[field.name] = None
            else:
                df[field.name] = None

    # Reorder columns to match schema
    df = df.reindex(columns=[field.name for field in schema])

    # Convert to PyArrow table with schema
    table = pa.Table.from_pandas(df, schema=schema)

    # Write to temporary file then upload
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
        pq.write_table(table, tmp_file.name)

        s3_key = f"{config.s3_output_prefix}/partition_date={partition_date}/consolidated_data.parquet"
        s3_client.upload_file(tmp_file.name, config.bucket_name, s3_key)

        return s3_key


def _parse_date(date_str: Any) -> Optional[date]:
    """Parse date string into date object."""
    if not date_str:
        return None

    if isinstance(date_str, date):
        return date_str

    try:
        if isinstance(date_str, str):
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
    except Exception:
        pass

    return None