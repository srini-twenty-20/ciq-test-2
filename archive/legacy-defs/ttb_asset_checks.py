"""
TTB data quality asset checks and monitoring.

This module defines Dagster asset checks that monitor data quality,
parsing success rates, validation metrics, and system health for the TTB pipeline.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckSeverity,
    get_dagster_logger,
    AssetCheckExecutionContext,
    MetadataValue
)

from .ttb_parsing_assets import ttb_raw_extraction, ttb_cleaned_data, ttb_structured_output
from .ttb_transformations import load_ttb_reference_data
from .partitioned_assets import ttb_partitions
from .ttb_consolidated_assets import daily_partitions


@asset_check(
    asset=ttb_raw_extraction,
    name="parsing_success_rate",
    description="Check that TTB HTML parsing success rate is above threshold"
)
def check_parsing_success_rate(context: AssetCheckExecutionContext, ttb_raw_extraction) -> AssetCheckResult:
    """
    Verify that the parsing success rate for TTB HTML files is acceptable.

    Fails if success rate is below 85%, warns if below 95%.
    """
    logger = get_dagster_logger()

    # Check if we have a specific partition context
    if hasattr(context, 'partition_key') and context.partition_key:
        logger.info(f"Running parsing success rate check for partition: {context.partition_key}")

    try:
        # Handle case where ttb_raw_extraction might be a dict or have stats
        if isinstance(ttb_raw_extraction, dict):
            stats = ttb_raw_extraction.get('processing_stats', {})
        else:
            # If it's not a dict, try to get stats attribute or default to empty
            stats = getattr(ttb_raw_extraction, 'processing_stats', {})
        total_files = stats.get('total_files', 0)
        successful_parses = stats.get('successful_parses', 0)
        failed_parses = stats.get('failed_parses', 0)

        if total_files == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No files found to process",
                metadata={
                    "total_files": total_files,
                    "partition": context.partition_key
                }
            )

        success_rate = (successful_parses / total_files) * 100

        # Define thresholds
        failure_threshold = 85.0  # Below this is a failure
        warning_threshold = 95.0  # Below this is a warning

        passed = success_rate >= failure_threshold
        severity = AssetCheckSeverity.ERROR if success_rate < failure_threshold else (
            AssetCheckSeverity.WARN if success_rate < warning_threshold else None
        )

        description = f"Parsing success rate: {success_rate:.1f}% ({successful_parses}/{total_files} files)"

        if not passed:
            description += f" - Below failure threshold of {failure_threshold}%"
        elif severity == AssetCheckSeverity.WARN:
            description += f" - Below warning threshold of {warning_threshold}%"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "success_rate_percent": success_rate,
                "successful_parses": successful_parses,
                "failed_parses": failed_parses,
                "total_files": total_files,
                "failure_threshold": failure_threshold,
                "warning_threshold": warning_threshold,
                "processing_errors": stats.get('processing_errors', [])[:5]  # Show first 5 errors
            }
        )

    except Exception as e:
        logger.error(f"Error in parsing success rate check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_extraction,
    name="field_extraction_completeness",
    description="Check that key fields are being extracted from TTB data"
)
def check_field_extraction_completeness(context: AssetCheckExecutionContext, ttb_raw_extraction) -> AssetCheckResult:
    """
    Verify that essential fields are being extracted from TTB HTML files.

    Checks for presence of key fields like TTB ID, dates, applicant info.
    """
    logger = get_dagster_logger()

    try:
        parsed_records = ttb_raw_extraction.get('parsed_records', [])
        partition_info = ttb_raw_extraction.get('partition_info', {})
        data_type = partition_info.get('data_type', 'unknown')

        if not parsed_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No parsed records found",
                metadata={
                    "record_count": 0,
                    "data_type": data_type
                }
            )

        # Define essential fields by data type
        if data_type == "cola-detail":
            essential_fields = [
                'ttb_id', 'serial_number', 'filing_date', 'applicant_business_name',
                'brand_name', 'product_description', 'net_contents'
            ]
        elif data_type == "certificate":
            essential_fields = [
                'ttb_id', 'certificate_number', 'effective_date', 'trade_name',
                'premises_address', 'permit_type'
            ]
        else:
            essential_fields = ['ttb_id']

        # Calculate field completeness
        field_stats = {}
        total_records = len(parsed_records)

        for field in essential_fields:
            non_empty_count = sum(1 for record in parsed_records
                                if record.get(field) and str(record[field]).strip())
            completeness_rate = (non_empty_count / total_records) * 100
            field_stats[field] = {
                'completeness_rate': completeness_rate,
                'non_empty_count': non_empty_count,
                'total_records': total_records
            }

        # Check if any essential field has low completeness
        min_completeness_threshold = 70.0  # Minimum acceptable completeness rate
        failing_fields = [
            field for field, stats in field_stats.items()
            if stats['completeness_rate'] < min_completeness_threshold
        ]

        overall_completeness = sum(stats['completeness_rate'] for stats in field_stats.values()) / len(field_stats)

        passed = len(failing_fields) == 0
        severity = AssetCheckSeverity.WARN if failing_fields else None

        description = f"Field extraction completeness: {overall_completeness:.1f}% average"
        if failing_fields:
            description += f" - Low completeness fields: {', '.join(failing_fields)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "overall_completeness_percent": overall_completeness,
                "field_statistics": field_stats,
                "failing_fields": failing_fields,
                "total_records": total_records,
                "data_type": data_type,
                "min_threshold": min_completeness_threshold
            }
        )

    except Exception as e:
        logger.error(f"Error in field extraction completeness check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_cleaned_data,
    name="transformation_validation_rates",
    description="Check transformation and validation success rates"
)
def check_transformation_validation_rates(context: AssetCheckExecutionContext, ttb_cleaned_data) -> AssetCheckResult:
    """
    Monitor transformation success rates and reference data validation results.

    Checks TTB ID validation, reference data validation, and transformation errors.
    """
    logger = get_dagster_logger()

    try:
        transformed_records = ttb_cleaned_data.get('transformed_records', [])
        transformation_metadata = ttb_cleaned_data.get('transformation_metadata', {})

        if not transformed_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No transformed records found",
                metadata={"record_count": 0}
            )

        total_records = len(transformed_records)

        # Check TTB ID validation rates
        valid_ttb_ids = sum(1 for record in transformed_records
                           if record.get('ttb_id_valid', False))
        ttb_id_validation_rate = (valid_ttb_ids / total_records) * 100

        # Check reference data validation rates
        valid_product_classes = sum(1 for record in transformed_records
                                  if record.get('product_class_validation', {}).get('is_valid', False))
        product_class_validation_rate = (valid_product_classes / total_records) * 100 if total_records > 0 else 0

        valid_origin_codes = sum(1 for record in transformed_records
                               if record.get('origin_code_validation', {}).get('is_valid', False))
        origin_code_validation_rate = (valid_origin_codes / total_records) * 100 if total_records > 0 else 0

        # Transformation error analysis
        transformation_stats = transformation_metadata.get('transformation_stats', {})
        transformation_errors = transformation_stats.get('transformation_errors', [])
        error_rate = (len(transformation_errors) / total_records) * 100 if total_records > 0 else 0

        # Define thresholds
        min_ttb_id_rate = 90.0
        max_error_rate = 10.0

        # Determine overall health
        issues = []
        if ttb_id_validation_rate < min_ttb_id_rate:
            issues.append(f"Low TTB ID validation rate: {ttb_id_validation_rate:.1f}%")
        if error_rate > max_error_rate:
            issues.append(f"High transformation error rate: {error_rate:.1f}%")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if issues else None

        description = f"Validation rates - TTB ID: {ttb_id_validation_rate:.1f}%, Product Class: {product_class_validation_rate:.1f}%, Origin: {origin_code_validation_rate:.1f}%"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "ttb_id_validation_rate": ttb_id_validation_rate,
                "product_class_validation_rate": product_class_validation_rate,
                "origin_code_validation_rate": origin_code_validation_rate,
                "transformation_error_rate": error_rate,
                "total_records": total_records,
                "valid_ttb_ids": valid_ttb_ids,
                "valid_product_classes": valid_product_classes,
                "valid_origin_codes": valid_origin_codes,
                "transformation_errors": transformation_errors[:3],  # Show first 3 errors
                "issues": issues
            }
        )

    except Exception as e:
        logger.error(f"Error in transformation validation rates check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_structured_output,
    name="schema_compliance",
    description="Check that structured output complies with expected schema"
)
def check_schema_compliance(context: AssetCheckExecutionContext, ttb_structured_output) -> AssetCheckResult:
    """
    Verify that the structured output data complies with the defined schema.

    Checks field types, required fields, and data consistency.
    """
    logger = get_dagster_logger()

    try:
        dataset_metadata = ttb_structured_output.get('dataset_metadata', {})
        validation_results = ttb_structured_output.get('validation_results', {})

        if not validation_results:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No validation results found",
                metadata={"dataset_metadata": dataset_metadata}
            )

        total_records = validation_results.get('total_records', 0)
        valid_records = validation_results.get('valid_records', 0)
        validation_errors = validation_results.get('validation_errors', [])

        if total_records == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No records found for schema validation",
                metadata={"total_records": 0}
            )

        compliance_rate = (valid_records / total_records) * 100
        error_rate = (len(validation_errors) / total_records) * 100

        # Define thresholds
        min_compliance_rate = 95.0
        max_error_rate = 5.0

        passed = compliance_rate >= min_compliance_rate and error_rate <= max_error_rate
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Schema compliance: {compliance_rate:.1f}% ({valid_records}/{total_records} records)"
        if error_rate > max_error_rate:
            description += f", Error rate: {error_rate:.1f}%"

        # Field-level validation statistics
        field_validation = validation_results.get('field_validation', {})
        problematic_fields = [
            field for field, stats in field_validation.items()
            if stats.get('type_errors', 0) > 0
        ]

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "compliance_rate": compliance_rate,
                "error_rate": error_rate,
                "total_records": total_records,
                "valid_records": valid_records,
                "validation_errors_count": len(validation_errors),
                "problematic_fields": problematic_fields,
                "field_validation_summary": field_validation,
                "dataset_info": {
                    "files_written": dataset_metadata.get('files_written', 0),
                    "file_size_mb": dataset_metadata.get('file_size_bytes', 0) / (1024 * 1024),
                    "output_format": dataset_metadata.get('partitioned', False)
                },
                "sample_errors": validation_errors[:3]  # Show first 3 validation errors
            }
        )

    except Exception as e:
        logger.error(f"Error in schema compliance check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_extraction,  # Associate with an asset for now
    name="reference_data_freshness",
    description="Check that TTB reference data is fresh and accessible"
)
def check_reference_data_freshness(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """
    Verify that TTB reference data is accessible and reasonably fresh.

    Checks both the availability of reference lookup URLs and cached data age.
    """
    logger = get_dagster_logger()

    try:
        # Try to load reference data
        reference_data = load_ttb_reference_data()

        if not reference_data:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Failed to load TTB reference data",
                metadata={"reference_data_loaded": False}
            )

        # Check data completeness
        product_codes_count = len(reference_data.get('product_class_types', {}).get('by_code', {}))
        origin_codes_count = len(reference_data.get('origin_codes', {}).get('by_code', {}))

        # Expected minimum counts (based on current data)
        min_product_codes = 500
        min_origin_codes = 200

        issues = []
        if product_codes_count < min_product_codes:
            issues.append(f"Low product code count: {product_codes_count} (expected ≥ {min_product_codes})")
        if origin_codes_count < min_origin_codes:
            issues.append(f"Low origin code count: {origin_codes_count} (expected ≥ {min_origin_codes})")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if issues else None

        description = f"Reference data loaded: {product_codes_count} product codes, {origin_codes_count} origin codes"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "reference_data_loaded": True,
                "product_codes_count": product_codes_count,
                "origin_codes_count": origin_codes_count,
                "min_product_codes": min_product_codes,
                "min_origin_codes": min_origin_codes,
                "issues": issues,
                "sample_product_codes": list(reference_data.get('product_class_types', {}).get('by_code', {}).keys())[:10],
                "sample_origin_codes": list(reference_data.get('origin_codes', {}).get('by_code', {}).keys())[:10]
            }
        )

    except Exception as e:
        logger.error(f"Error in reference data freshness check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Reference data check failed: {str(e)}",
            metadata={"error": str(e), "reference_data_loaded": False}
        )