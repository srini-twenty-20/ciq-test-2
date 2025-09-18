"""
TTB data parsing assets for structured field extraction.

This module defines Dagster assets that process downloaded TTB HTML files
and extract structured data fields.
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
    MetadataValue
)
from dagster_aws.s3 import S3Resource

from .ttb_data_extraction import parse_ttb_html
from .partitioned_assets import ttb_partitioned, ttb_partitions
from .ttb_transformations import apply_field_transformations, transformation_registry, load_ttb_reference_data
from .ttb_schema import (
    schema_registry,
    create_parquet_dataset,
    validate_records_against_schema
)


class TTBParsingConfig(Config):
    """Configuration for TTB parsing assets."""
    bucket_name: str = "ciq-dagster"
    processing_batch_size: int = 100  # Process files in batches
    include_image_metadata: bool = True  # Include image URLs and metadata
    apply_transformations: bool = True  # Apply data transformations and cleaning
    output_format: str = "parquet"  # Output format: 'parquet', 'json', 'csv'
    create_partitioned_output: bool = True  # Create partitioned Parquet output
    upload_to_s3: bool = True  # Upload processed data to S3
    s3_output_prefix: str = "2-ttb-processed-data"  # S3 prefix for processed data


@asset(
    description="TTB reference data including origin codes and product class types for validation"
)
def ttb_reference_data(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Download and cache TTB reference data from lookup URLs.

    This asset provides:
    - Origin codes (232 entries): State/country codes for product origins
    - Product class types (531 entries): Beverage type classifications

    Data is used for validation during transformation pipeline.

    Returns:
        Dictionary containing structured reference data with lookup mappings
    """
    logger = get_dagster_logger()

    logger.info("🔍 Downloading TTB reference data...")

    try:
        # Load reference data using existing function
        reference_data = load_ttb_reference_data()

        # Extract statistics for metadata
        origin_codes = reference_data.get('origin_codes', {})
        product_types = reference_data.get('product_class_types', {})

        origin_count = len(origin_codes.get('all_codes', []))
        product_count = len(product_types.get('all_codes', []))

        logger.info(f"✅ Reference data loaded: {origin_count} origin codes, {product_count} product types")

        # Add metadata to the asset
        context.add_output_metadata({
            "origin_codes_count": MetadataValue.int(origin_count),
            "product_class_types_count": MetadataValue.int(product_count),
            "data_sources": MetadataValue.json({
                "origin_codes": "https://ttbonline.gov/colasonline/lookupOriginCode.do?action=search&display=all",
                "product_class_types": "https://ttbonline.gov/colasonline/lookupProductClassTypeCode.do?action=search&display=all"
            }),
            "sample_origin_codes": MetadataValue.json(dict(list(origin_codes.get('by_code', {}).items())[:5])),
            "sample_product_types": MetadataValue.json(dict(list(product_types.get('by_code', {}).items())[:5])),
            "cache_status": MetadataValue.text("Fresh data downloaded from TTB website"),
            "last_updated": MetadataValue.text(datetime.now().isoformat())
        })

        return reference_data

    except Exception as e:
        logger.error(f"❌ Failed to load TTB reference data: {str(e)}")
        context.add_output_metadata({
            "error": MetadataValue.text(str(e)),
            "status": MetadataValue.text("FAILED")
        })
        raise


@asset(
    partitions_def=ttb_partitions,
    deps=[ttb_partitioned],
    description="Extract structured data fields from TTB HTML files"
)
def ttb_raw_extraction(
    context: AssetExecutionContext,
    config: TTBParsingConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Parse TTB HTML files and extract raw structured data.

    This asset reads HTML files from S3, parses them using our extraction utilities,
    and returns structured field data with validation and error tracking.
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    # Parse method_type (format: "001-cola-detail")
    receipt_method_str, data_type = method_type_str.split("-", 1)

    logger.info(f"Processing TTB raw extraction for {date_str} | {method_type_str}")

    # Build S3 paths for HTML files
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    receipt_method = int(receipt_method_str)

    # Use numbered prefix for stage ordering
    s3_prefix = "1-ttb-raw-data"

    s3_path = f"{s3_prefix}/data_type={data_type}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/receipt_method={receipt_method:03d}/"

    logger.info(f"Reading HTML files from S3 path: s3://{config.bucket_name}/{s3_path}")

    # Initialize S3 client and tracking
    s3_client = s3_resource.get_client()
    parsing_results = []
    processing_stats = {
        'total_files': 0,
        'successful_parses': 0,
        'failed_parses': 0,
        'total_fields_extracted': 0,
        'processing_errors': []
    }

    try:
        # List HTML files in S3 path
        response = s3_client.list_objects_v2(
            Bucket=config.bucket_name,
            Prefix=s3_path,
            MaxKeys=config.processing_batch_size
        )

        if 'Contents' not in response:
            logger.warning(f"No HTML files found in {s3_path}")
            processing_stats['total_files'] = 0
        else:
            html_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.html')]
            processing_stats['total_files'] = len(html_files)

            logger.info(f"Found {len(html_files)} HTML files to process")

            # Process each HTML file
            for i, s3_obj in enumerate(html_files):
                s3_key = s3_obj['Key']
                file_size = s3_obj['Size']

                # Extract TTB ID from filename
                filename = Path(s3_key).name
                ttb_id = filename.replace('.html', '')

                logger.info(f"Processing file {i+1}/{len(html_files)}: {filename} ({file_size} bytes)")

                try:
                    # Download HTML content from S3
                    with tempfile.NamedTemporaryFile(mode='w+', suffix='.html', delete=False) as tmp_file:
                        s3_client.download_file(config.bucket_name, s3_key, tmp_file.name)

                        # Read and parse HTML content
                        with open(tmp_file.name, 'r', encoding='utf-8', errors='ignore') as f:
                            html_content = f.read()

                    # Parse the HTML using our extraction utilities
                    parsed_data = parse_ttb_html(html_content, data_type)

                    # Add metadata
                    parsed_data.update({
                        'partition_date': date_str,
                        'receipt_method': receipt_method,
                        'data_type': data_type,
                        'source_s3_key': s3_key,
                        'source_file_size': file_size,
                        'extraction_timestamp': datetime.now().isoformat(),
                        'ttb_id_from_filename': ttb_id
                    })

                    # Validate TTB ID consistency
                    extracted_ttb_id = parsed_data.get('ttb_id')
                    if extracted_ttb_id and extracted_ttb_id != ttb_id:
                        logger.warning(f"TTB ID mismatch: filename={ttb_id}, extracted={extracted_ttb_id}")
                        parsed_data['ttb_id_mismatch'] = True

                    # Count non-empty fields
                    non_empty_fields = sum(1 for k, v in parsed_data.items()
                                         if v is not None and v != '' and v != [])
                    parsed_data['field_count'] = non_empty_fields
                    processing_stats['total_fields_extracted'] += non_empty_fields

                    # Remove image metadata if not requested
                    if not config.include_image_metadata:
                        parsed_data.pop('label_images', None)
                        parsed_data.pop('signatures', None)

                    parsing_results.append(parsed_data)
                    processing_stats['successful_parses'] += 1

                    logger.info(f"✅ Successfully parsed {filename}: {non_empty_fields} fields extracted")

                except Exception as e:
                    error_msg = f"Failed to parse {filename}: {str(e)}"
                    logger.error(error_msg)

                    processing_stats['failed_parses'] += 1
                    processing_stats['processing_errors'].append({
                        'filename': filename,
                        'ttb_id': ttb_id,
                        's3_key': s3_key,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    })

                    # Create minimal error record
                    error_record = {
                        'partition_date': date_str,
                        'receipt_method': receipt_method,
                        'data_type': data_type,
                        'ttb_id': ttb_id,
                        'source_s3_key': s3_key,
                        'parsing_success': False,
                        'extraction_errors': [error_msg],
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    parsing_results.append(error_record)

    except Exception as e:
        logger.error(f"Critical error in raw extraction: {str(e)}")
        processing_stats['processing_errors'].append({
            'error': f"Critical extraction error: {str(e)}",
            'timestamp': datetime.now().isoformat()
        })
        raise

    # Calculate processing statistics
    success_rate = (processing_stats['successful_parses'] / processing_stats['total_files']
                   if processing_stats['total_files'] > 0 else 0)
    avg_fields_per_record = (processing_stats['total_fields_extracted'] / processing_stats['successful_parses']
                           if processing_stats['successful_parses'] > 0 else 0)

    # Log final statistics
    logger.info(f"Raw extraction complete for {date_str} | {method_type_str}")
    logger.info(f"Files processed: {processing_stats['total_files']}")
    logger.info(f"Successful parses: {processing_stats['successful_parses']}")
    logger.info(f"Failed parses: {processing_stats['failed_parses']}")
    logger.info(f"Success rate: {success_rate:.2%}")
    logger.info(f"Average fields per record: {avg_fields_per_record:.1f}")

    # Create asset metadata
    context.add_output_metadata({
        "total_files_processed": MetadataValue.int(processing_stats['total_files']),
        "successful_parses": MetadataValue.int(processing_stats['successful_parses']),
        "failed_parses": MetadataValue.int(processing_stats['failed_parses']),
        "success_rate": MetadataValue.float(success_rate),
        "total_fields_extracted": MetadataValue.int(processing_stats['total_fields_extracted']),
        "average_fields_per_record": MetadataValue.float(avg_fields_per_record),
        "partition_date": MetadataValue.text(date_str),
        "receipt_method": MetadataValue.int(receipt_method),
        "data_type": MetadataValue.text(data_type),
        "s3_source_path": MetadataValue.text(f"s3://{config.bucket_name}/{s3_path}"),
        "processing_errors_count": MetadataValue.int(len(processing_stats['processing_errors']))
    })

    # Return comprehensive results
    return {
        'partition_info': {
            'partition_date': date_str,
            'receipt_method': receipt_method,
            'data_type': data_type,
            'method_type_str': method_type_str
        },
        'processing_stats': processing_stats,
        'parsed_records': parsing_results,
        'extraction_metadata': {
            'total_records': len(parsing_results),
            'success_rate': success_rate,
            'avg_fields_per_record': avg_fields_per_record,
            'extraction_timestamp': datetime.now().isoformat(),
            'config': {
                'batch_size': config.processing_batch_size,
                'include_image_metadata': config.include_image_metadata
            }
        }
    }


@asset(
    deps=[ttb_raw_extraction],
    description="Aggregate extraction statistics across partitions"
)
def ttb_extraction_summary(ttb_raw_extraction) -> Dict[str, Any]:
    """
    Create summary statistics from raw extraction results.

    This asset aggregates data quality metrics, field coverage statistics,
    and processing performance across all partitions.
    """
    logger = get_dagster_logger()

    # Extract key metrics from raw extraction
    stats = ttb_raw_extraction['processing_stats']
    extraction_metadata = ttb_raw_extraction['extraction_metadata']
    partition_info = ttb_raw_extraction['partition_info']

    logger.info(f"Generating extraction summary for {partition_info['data_type']} data")

    # Analyze parsed records for field coverage
    parsed_records = ttb_raw_extraction['parsed_records']
    field_coverage = {}

    if parsed_records:
        # Count how often each field appears
        all_fields = set()
        for record in parsed_records:
            if record.get('parsing_success', True):  # Only count successful parses
                all_fields.update(record.keys())

        # Calculate coverage percentage for each field
        successful_records = [r for r in parsed_records if r.get('parsing_success', True)]
        total_successful = len(successful_records)

        if total_successful > 0:
            for field in all_fields:
                # Skip metadata fields
                if field in ['extraction_timestamp', 'source_s3_key', 'partition_date',
                           'receipt_method', 'data_type', 'parsing_success']:
                    continue

                # Count non-empty values
                non_empty_count = sum(1 for r in successful_records
                                    if r.get(field) is not None and r.get(field) != '' and r.get(field) != [])
                coverage_pct = (non_empty_count / total_successful) * 100
                field_coverage[field] = {
                    'coverage_percentage': coverage_pct,
                    'non_empty_count': non_empty_count,
                    'total_records': total_successful
                }

    # Create summary
    summary = {
        'partition_info': partition_info,
        'processing_summary': {
            'total_files': stats['total_files'],
            'successful_parses': stats['successful_parses'],
            'failed_parses': stats['failed_parses'],
            'success_rate': extraction_metadata['success_rate'],
            'avg_fields_per_record': extraction_metadata['avg_fields_per_record']
        },
        'field_coverage': field_coverage,
        'data_quality': {
            'high_coverage_fields': [k for k, v in field_coverage.items() if v['coverage_percentage'] > 80],
            'medium_coverage_fields': [k for k, v in field_coverage.items() if 50 <= v['coverage_percentage'] <= 80],
            'low_coverage_fields': [k for k, v in field_coverage.items() if v['coverage_percentage'] < 50],
            'total_unique_fields': len(field_coverage)
        },
        'processing_errors': stats['processing_errors'],
        'summary_timestamp': datetime.now().isoformat()
    }

    # Log key insights
    logger.info(f"Extraction Summary Generated:")
    logger.info(f"  Success Rate: {summary['processing_summary']['success_rate']:.2%}")
    logger.info(f"  Total Unique Fields: {summary['data_quality']['total_unique_fields']}")
    logger.info(f"  High Coverage Fields: {len(summary['data_quality']['high_coverage_fields'])}")
    logger.info(f"  Processing Errors: {len(summary['processing_errors'])}")

    return summary


@asset(
    partitions_def=ttb_partitions,
    deps=[ttb_raw_extraction, ttb_reference_data],
    description="Clean and transform raw TTB extraction data"
)
def ttb_cleaned_data(
    context: AssetExecutionContext,
    config: TTBParsingConfig,
    ttb_raw_extraction,
    ttb_reference_data
) -> Dict[str, Any]:
    """
    Apply data transformations and cleaning to raw extraction results.

    This asset takes the raw extracted data and applies standardized transformations
    including date parsing, text cleaning, validation, and business logic rules.
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    # Parse method_type (format: "001-cola-detail")
    receipt_method_str, data_type = method_type_str.split("-", 1)

    logger.info(f"Applying transformations to {data_type} data for {date_str} | {method_type_str}")

    # Get raw extraction results
    raw_records = ttb_raw_extraction['parsed_records']
    partition_info = ttb_raw_extraction['partition_info']

    # Define transformation rules based on data type
    if data_type == "cola-detail":
        field_rules = {
            # Dates
            'filing_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],
            'approval_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],
            'expiration_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],

            # Text fields
            'brand_name': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ],
            'applicant_business_name': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ],
            'applicant_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ],

            # Contact info
            'applicant_phone': [
                {'transform': 'clean_phone'}
            ],
            'applicant_email': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ],

            # Alcohol content
            'alcohol_content': [
                {'transform': 'extract_alcohol_content'}
            ],

            # Container size
            'net_contents': [
                {'transform': 'standardize_container_size'}
            ],

            # Classification
            'wine_appellation': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ],
            'wine_vintage': [
                {'transform': 'extract_numbers', 'include_decimals': False}
            ]
        }

    elif data_type == "certificate":
        field_rules = {
            # Dates
            'effective_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],
            'expiration_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],

            # Business info
            'trade_name': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ],
            'premises_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ],
            'mailing_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ],

            # Contact
            'phone': [
                {'transform': 'clean_phone'}
            ],
            'email': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_whitespace'}
            ]
        }
    else:
        field_rules = {}

    # Apply transformations to each record
    transformed_records = []
    transformation_stats = {
        'total_records': len(raw_records),
        'transformed_records': 0,
        'transformation_errors': [],
        'field_transformation_counts': {}
    }

    for i, record in enumerate(raw_records):
        try:
            if not config.apply_transformations:
                # Skip transformations if disabled
                transformed_record = record.copy()
            else:
                # Apply field transformations
                transformed_record = apply_field_transformations(record, field_rules)

                # Add transformation metadata
                transformed_record['_transformation_applied'] = True
                transformed_record['_transformation_timestamp'] = datetime.now().isoformat()

                # Count transformations applied
                for field_name in field_rules.keys():
                    if field_name in record:
                        transformation_stats['field_transformation_counts'][field_name] = \
                            transformation_stats['field_transformation_counts'].get(field_name, 0) + 1

            # Add business logic classifications
            if data_type == "cola-detail":
                # Classify beverage type
                product_info = ' '.join([
                    str(transformed_record.get('brand_name', '')),
                    str(transformed_record.get('wine_appellation', '')),
                    str(transformed_record.get('product_description', ''))
                ])
                transformed_record['beverage_type'] = transformation_registry.apply(
                    'classify_beverage_type', product_info
                )

                # Validate TTB ID
                ttb_id = transformed_record.get('ttb_id')
                if ttb_id:
                    transformed_record['ttb_id_valid'] = transformation_registry.apply(
                        'validate_ttb_id', ttb_id
                    )

                # Load reference data for validation (cached after first load)
                try:
                    reference_data = transformation_registry.apply('load_reference_data', None)

                    # Validate product class/type code if present
                    beverage_type = transformed_record.get('beverage_type') or transformed_record.get('product_class_type')
                    if beverage_type:
                        validation_result = transformation_registry.apply(
                            'validate_product_class', beverage_type, reference_data=reference_data
                        )
                        transformed_record['product_class_validation'] = validation_result
                        if validation_result.get('is_valid'):
                            transformed_record['product_class_description'] = validation_result.get('description')

                    # Validate origin code if present
                    origin_code = transformed_record.get('origin_code') or transformed_record.get('wine_appellation_code')
                    if origin_code:
                        validation_result = transformation_registry.apply(
                            'validate_origin_code', origin_code, reference_data=reference_data
                        )
                        transformed_record['origin_code_validation'] = validation_result
                        if validation_result.get('is_valid'):
                            transformed_record['origin_description'] = validation_result.get('description')

                except Exception as e:
                    logger.warning(f"Reference data validation failed for record {i}: {str(e)}")
                    # Continue processing without reference validation

            transformed_records.append(transformed_record)
            transformation_stats['transformed_records'] += 1

        except Exception as e:
            error_msg = f"Transformation error for record {i}: {str(e)}"
            logger.warning(error_msg)

            transformation_stats['transformation_errors'].append({
                'record_index': i,
                'ttb_id': record.get('ttb_id', 'unknown'),
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

            # Keep original record if transformation fails
            transformed_records.append(record)

    # Calculate transformation success rate
    success_rate = (transformation_stats['transformed_records'] /
                   transformation_stats['total_records'] if transformation_stats['total_records'] > 0 else 0)

    # Log transformation results
    logger.info(f"Data transformation complete for {date_str} | {method_type_str}")
    logger.info(f"Records processed: {transformation_stats['total_records']}")
    logger.info(f"Successfully transformed: {transformation_stats['transformed_records']}")
    logger.info(f"Transformation errors: {len(transformation_stats['transformation_errors'])}")
    logger.info(f"Success rate: {success_rate:.2%}")

    # Log field transformation counts
    for field, count in transformation_stats['field_transformation_counts'].items():
        logger.info(f"  {field}: {count} transformations")

    # Create asset metadata
    context.add_output_metadata({
        "total_records": MetadataValue.int(transformation_stats['total_records']),
        "transformed_records": MetadataValue.int(transformation_stats['transformed_records']),
        "transformation_success_rate": MetadataValue.float(success_rate),
        "transformation_errors_count": MetadataValue.int(len(transformation_stats['transformation_errors'])),
        "transformations_applied": MetadataValue.bool(config.apply_transformations),
        "field_transformations": MetadataValue.json(transformation_stats['field_transformation_counts']),
        "partition_date": MetadataValue.text(date_str),
        "data_type": MetadataValue.text(data_type),
        "available_transformations": MetadataValue.json(transformation_registry.get_available())
    })

    # Return transformed results
    return {
        'partition_info': partition_info,
        'transformation_stats': transformation_stats,
        'transformed_records': transformed_records,
        'transformation_metadata': {
            'transformations_enabled': config.apply_transformations,
            'field_rules_applied': list(field_rules.keys()),
            'success_rate': success_rate,
            'total_transformations': sum(transformation_stats['field_transformation_counts'].values()),
            'transformation_timestamp': datetime.now().isoformat()
        }
    }


@asset(
    partitions_def=ttb_partitions,
    deps=[ttb_cleaned_data, ttb_reference_data],
    description="Create structured Parquet output from cleaned TTB data"
)
def ttb_structured_output(
    context: AssetExecutionContext,
    config: TTBParsingConfig,
    ttb_cleaned_data,
    ttb_reference_data,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create structured, schema-compliant output from cleaned TTB data.

    This asset takes the cleaned data and creates well-structured Parquet files
    with enforced schemas for downstream analytics and reporting.
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    method_type_str = partition_key.keys_by_dimension["method_type"]

    # Parse method_type (format: "001-cola-detail")
    receipt_method_str, data_type = method_type_str.split("-", 1)

    logger.info(f"Creating structured output for {data_type} data: {date_str} | {method_type_str}")

    # Get cleaned data
    transformed_records = ttb_cleaned_data['transformed_records']
    partition_info = ttb_cleaned_data['partition_info']
    transformation_metadata = ttb_cleaned_data['transformation_metadata']

    # Get appropriate schema
    try:
        schema = schema_registry.get_schema(data_type)
        logger.info(f"Using schema for {data_type} with {len(schema)} fields")
    except ValueError as e:
        logger.error(f"Schema error: {e}")
        raise

    # Validate records against schema
    logger.info(f"Validating {len(transformed_records)} records against schema...")
    validation_results = validate_records_against_schema(transformed_records, schema)

    logger.info(f"Schema validation complete:")
    logger.info(f"  Valid records: {validation_results['valid_records']}/{validation_results['total_records']}")
    logger.info(f"  Validation errors: {len(validation_results['validation_errors'])}")

    if validation_results['missing_fields']:
        logger.warning(f"  Missing fields: {validation_results['missing_fields']}")
    if validation_results['extra_fields']:
        logger.info(f"  Extra fields (will be ignored): {validation_results['extra_fields']}")

    # Create output directory path
    base_output_path = f"/tmp/ttb_structured_output/{data_type}"
    partition_path = f"{base_output_path}/partition_date={date_str}/receipt_method={receipt_method_str}"

    # Create structured output
    output_stats = {}

    if config.output_format == "parquet":
        logger.info(f"Creating Parquet output at {partition_path}")

        # Add data_type to records for schema compliance (but no Parquet partitioning when uploading to S3)
        for record in transformed_records:
            record['data_type'] = data_type

        # Create Parquet dataset without partitioning (S3 upload handles partitioning structure)
        partition_cols = None if config.upload_to_s3 else (['data_type'] if config.create_partitioned_output else None)

        parquet_stats = create_parquet_dataset(
            records=transformed_records,
            schema=schema,
            output_path=partition_path,
            partition_cols=partition_cols
        )

        output_stats['parquet'] = parquet_stats
        logger.info(f"✅ Parquet output created: {parquet_stats['files_written']} files, {parquet_stats['total_rows']} rows")

        # Upload to S3 if enabled
        if config.upload_to_s3:
            s3_client = s3_resource.get_client()
            s3_upload_stats = upload_parquet_to_s3(
                local_path=partition_path,
                s3_resource=s3_client,
                bucket_name=config.bucket_name,
                s3_prefix=config.s3_output_prefix,
                data_type=data_type,
                date_str=date_str,
                receipt_method=receipt_method_str,
                logger=logger
            )
            output_stats['s3_upload'] = s3_upload_stats
            logger.info(f"✅ S3 upload completed: {s3_upload_stats['files_uploaded']} files, {s3_upload_stats['total_bytes']:,} bytes")

    # Create additional output formats if requested
    if config.output_format in ["json", "all"]:
        import json
        json_path = f"{partition_path}/data.json"
        Path(json_path).parent.mkdir(parents=True, exist_ok=True)

        with open(json_path, 'w') as f:
            json.dump(transformed_records, f, indent=2, default=str)

        output_stats['json'] = {
            'file_path': json_path,
            'total_rows': len(transformed_records),
            'file_size_bytes': Path(json_path).stat().st_size
        }
        logger.info(f"✅ JSON output created: {json_path}")

    if config.output_format in ["csv", "all"]:
        import pandas as pd
        csv_path = f"{partition_path}/data.csv"
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(transformed_records)
        df.to_csv(csv_path, index=False)

        output_stats['csv'] = {
            'file_path': csv_path,
            'total_rows': len(df),
            'file_size_bytes': Path(csv_path).stat().st_size
        }
        logger.info(f"✅ CSV output created: {csv_path}")

    # Calculate quality metrics
    quality_metrics = {
        'schema_compliance_rate': validation_results['valid_records'] / validation_results['total_records'],
        'field_completeness': {},
        'data_quality_score': 0.0
    }

    # Calculate field completeness
    for field_name, stats in validation_results['field_validation'].items():
        total = stats['non_null_count'] + stats['null_count']
        completeness = stats['non_null_count'] / total if total > 0 else 0
        quality_metrics['field_completeness'][field_name] = completeness

    # Calculate overall data quality score (average of compliance rate and field completeness)
    avg_completeness = sum(quality_metrics['field_completeness'].values()) / len(quality_metrics['field_completeness']) if quality_metrics['field_completeness'] else 0
    quality_metrics['data_quality_score'] = (quality_metrics['schema_compliance_rate'] + avg_completeness) / 2

    # Log quality metrics
    logger.info(f"Data Quality Metrics:")
    logger.info(f"  Schema compliance: {quality_metrics['schema_compliance_rate']:.2%}")
    logger.info(f"  Average field completeness: {avg_completeness:.2%}")
    logger.info(f"  Overall quality score: {quality_metrics['data_quality_score']:.2%}")

    # Create asset metadata
    context.add_output_metadata({
        "total_records": MetadataValue.int(len(transformed_records)),
        "valid_records": MetadataValue.int(validation_results['valid_records']),
        "schema_compliance_rate": MetadataValue.float(quality_metrics['schema_compliance_rate']),
        "data_quality_score": MetadataValue.float(quality_metrics['data_quality_score']),
        "output_format": MetadataValue.text(config.output_format),
        "output_path": MetadataValue.text(partition_path),
        "schema_fields": MetadataValue.int(len(schema)),
        "validation_errors": MetadataValue.int(len(validation_results['validation_errors'])),
        "partition_date": MetadataValue.text(date_str),
        "data_type": MetadataValue.text(data_type),
        "file_outputs": MetadataValue.json(output_stats),
        "field_completeness": MetadataValue.json(quality_metrics['field_completeness'])
    })

    # Return comprehensive results
    return {
        'partition_info': partition_info,
        'output_stats': output_stats,
        'validation_results': validation_results,
        'quality_metrics': quality_metrics,
        'schema_metadata': {
            'data_type': data_type,
            'schema_fields': [field.name for field in schema],
            'total_schema_fields': len(schema),
            'output_path': partition_path,
            'partitioned': config.create_partitioned_output
        },
        'processing_summary': {
            'input_records': len(transformed_records),
            'output_records': validation_results['valid_records'],
            'transformation_metadata': transformation_metadata,
            'processing_timestamp': datetime.now().isoformat(),
            'output_formats_created': list(output_stats.keys())
        }
    }


def upload_parquet_to_s3(
    local_path: str,
    s3_resource,
    bucket_name: str,
    s3_prefix: str,
    data_type: str,
    date_str: str,
    receipt_method: str,
    logger
) -> Dict[str, Any]:
    """
    Upload Parquet files from local path to S3 with proper partitioning structure.

    Args:
        local_path: Local directory containing Parquet files
        s3_resource: S3 resource object for uploads
        bucket_name: Target S3 bucket name
        s3_prefix: S3 prefix for the data (e.g., "ttb-processed-data")
        data_type: Data type (cola-detail, certificate)
        date_str: Partition date string (YYYY-MM-DD)
        receipt_method: Receipt method (001, 002, 003, 000)
        logger: Logger instance

    Returns:
        Dict with upload statistics
    """
    try:
        import boto3
        from pathlib import Path

        # Initialize S3 client if not provided
        if s3_resource is None:
            s3_resource = boto3.client('s3')

        upload_stats = {
            'files_uploaded': 0,
            'total_bytes': 0,
            'failed_uploads': 0,
            'upload_errors': [],
            's3_paths': []
        }

        local_path_obj = Path(local_path)
        if not local_path_obj.exists():
            logger.warning(f"Local path does not exist: {local_path}")
            return upload_stats

        # Find Parquet files - only direct files, not from subdirectories to avoid duplicate partitioning
        parquet_files = list(local_path_obj.glob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} Parquet files to upload from {local_path}")

        # If no direct files found, look for partitioned files in subdirectories
        if not parquet_files:
            parquet_files = list(local_path_obj.rglob("*.parquet"))
            logger.info(f"Found {len(parquet_files)} partitioned Parquet files to upload from subdirectories")

        for parquet_file in parquet_files:
            try:
                # Calculate relative path from the local_path
                relative_path = parquet_file.relative_to(local_path_obj)

                # Construct S3 key with proper partitioning structure
                # Format: s3_prefix/data_type=X/partition_date=YYYY-MM-DD/receipt_method=XXX/filename.parquet
                s3_key = f"{s3_prefix}/data_type={data_type}/partition_date={date_str}/receipt_method={receipt_method}/{relative_path}"

                # Upload file
                logger.debug(f"Uploading {parquet_file} to s3://{bucket_name}/{s3_key}")

                s3_resource.upload_file(
                    str(parquet_file),
                    bucket_name,
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'application/octet-stream',
                        'Metadata': {
                            'data_type': data_type,
                            'partition_date': date_str,
                            'receipt_method': receipt_method,
                            'upload_timestamp': datetime.now().isoformat()
                        }
                    }
                )

                file_size = parquet_file.stat().st_size
                upload_stats['files_uploaded'] += 1
                upload_stats['total_bytes'] += file_size
                upload_stats['s3_paths'].append(f"s3://{bucket_name}/{s3_key}")

                logger.debug(f"✅ Uploaded {parquet_file.name} ({file_size:,} bytes)")

            except Exception as e:
                upload_stats['failed_uploads'] += 1
                upload_stats['upload_errors'].append({
                    'file': str(parquet_file),
                    'error': str(e)
                })
                logger.error(f"Failed to upload {parquet_file}: {e}")

        logger.info(f"S3 Upload Summary:")
        logger.info(f"  Files uploaded: {upload_stats['files_uploaded']}")
        logger.info(f"  Total bytes: {upload_stats['total_bytes']:,}")
        logger.info(f"  Failed uploads: {upload_stats['failed_uploads']}")

        return upload_stats

    except Exception as e:
        logger.error(f"S3 upload failed with error: {e}")
        return {
            'files_uploaded': 0,
            'total_bytes': 0,
            'failed_uploads': 0,
            'upload_errors': [{'error': str(e)}],
            's3_paths': []
        }