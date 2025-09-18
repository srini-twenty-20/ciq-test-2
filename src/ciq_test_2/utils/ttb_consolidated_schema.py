"""
Consolidated TTB schema that combines COLA detail and certificate data.
"""
import pyarrow as pa
from typing import Dict, Any


def get_consolidated_ttb_schema() -> pa.Schema:
    """
    Define the consolidated schema that merges COLA detail and certificate data.

    This schema represents a flat table where each row contains all available
    information for a TTB ID from both COLA detail and certificate sources.
    """
    return pa.schema([
        # Primary identifiers
        ('ttb_id', pa.string()),
        ('partition_date', pa.date32()),
        ('receipt_method', pa.int32()),

        # Data source indicators
        ('has_cola_detail', pa.bool_()),
        ('has_certificate', pa.bool_()),
        ('data_completeness_score', pa.float32()),

        # COLA Detail fields (prefixed with cola_)
        ('cola_serial_number', pa.string()),
        ('cola_filing_date', pa.date32()),
        ('cola_approval_date', pa.date32()),
        ('cola_expiration_date', pa.date32()),
        ('cola_brand_name', pa.string()),
        ('cola_fanciful_name', pa.string()),
        ('cola_product_description', pa.string()),
        ('cola_net_contents', pa.string()),
        ('cola_alcohol_content', pa.string()),
        ('cola_product_class_type', pa.string()),
        ('cola_origin_code', pa.string()),
        ('cola_formula_number', pa.string()),
        ('cola_label_images_count', pa.int32()),
        ('cola_qualifications', pa.string()),

        # Certificate fields (prefixed with cert_)
        ('cert_certificate_number', pa.string()),
        ('cert_application_date', pa.date32()),
        ('cert_approval_date', pa.date32()),
        ('cert_certificate_status', pa.string()),
        ('cert_certificate_type', pa.string()),
        ('cert_applicant_signature_present', pa.bool_()),
        ('cert_authorized_signature_present', pa.bool_()),
        ('cert_authorized_signature_url', pa.string()),
        ('cert_plant_registry_number', pa.string()),

        # Common applicant information (consolidated from both sources)
        ('applicant_business_name', pa.string()),
        ('applicant_mailing_address', pa.string()),
        ('applicant_phone', pa.string()),
        ('applicant_email', pa.string()),
        ('applicant_fax', pa.string()),

        # Validation results
        ('ttb_id_valid', pa.bool_()),
        ('product_class_validation_is_valid', pa.bool_()),
        ('product_class_validation_code', pa.string()),
        ('product_class_validation_description', pa.string()),
        ('origin_code_validation_is_valid', pa.bool_()),
        ('origin_code_validation_code', pa.string()),
        ('origin_code_validation_description', pa.string()),

        # Processing metadata
        ('cola_source_s3_key', pa.string()),
        ('cert_source_s3_key', pa.string()),
        ('processing_timestamp', pa.timestamp('ms')),
        ('extraction_success_rate', pa.float32()),
        ('validation_errors_count', pa.int32()),

        # Computed fields for analytics
        ('days_to_approval', pa.int32()),  # Filing to approval duration
        ('is_wine', pa.bool_()),
        ('is_beer', pa.bool_()),
        ('is_spirits', pa.bool_()),
        ('has_complete_applicant_info', pa.bool_()),
        ('data_quality_tier', pa.string()),  # 'premium', 'standard', 'basic'
    ])


def get_dimension_schemas() -> Dict[str, pa.Schema]:
    """Define schemas for dimensional model tables."""

    return {
        'dim_companies': pa.schema([
            ('company_id', pa.int64()),
            ('company_name', pa.string()),
            ('mailing_address', pa.string()),
            ('phone', pa.string()),
            ('email', pa.string()),
            ('fax', pa.string()),
            ('plant_registry_number', pa.string()),
            ('created_date', pa.timestamp('ms')),
            ('modified_date', pa.timestamp('ms')),
            ('is_current', pa.bool_()),
            ('source_ttb_ids', pa.list_(pa.string())),
        ]),

        'dim_locations': pa.schema([
            ('location_id', pa.int64()),
            ('origin_code', pa.string()),
            ('origin_description', pa.string()),
            ('state', pa.string()),
            ('country', pa.string()),
            ('region', pa.string()),
            ('is_domestic', pa.bool_()),
            ('created_date', pa.timestamp('ms')),
        ]),

        'dim_product_types': pa.schema([
            ('product_type_id', pa.int64()),
            ('product_class_code', pa.string()),
            ('product_class_description', pa.string()),
            ('category', pa.string()),  # 'Wine', 'Beer', 'Spirits', etc.
            ('subcategory', pa.string()),
            ('is_alcoholic', pa.bool_()),
            ('created_date', pa.timestamp('ms')),
        ]),

        'dim_dates': pa.schema([
            ('date_id', pa.int32()),  # YYYYMMDD format
            ('date', pa.date32()),
            ('year', pa.int32()),
            ('month', pa.int32()),
            ('quarter', pa.int32()),
            ('day_of_year', pa.int32()),
            ('week_of_year', pa.int32()),
            ('day_of_week', pa.int32()),
            ('is_weekend', pa.bool_()),
            ('fiscal_year', pa.int32()),
            ('fiscal_quarter', pa.int32()),
        ]),

        'fact_products': pa.schema([
            ('product_id', pa.int64()),
            ('ttb_id', pa.string()),
            ('company_id', pa.int64()),
            ('location_id', pa.int64()),
            ('product_type_id', pa.int64()),
            ('filing_date_id', pa.int32()),
            ('approval_date_id', pa.int32()),
            ('expiration_date_id', pa.int32()),
            ('brand_name', pa.string()),
            ('fanciful_name', pa.string()),
            ('product_description', pa.string()),
            ('net_contents', pa.string()),
            ('alcohol_content', pa.string()),
            ('formula_number', pa.string()),
            ('serial_number', pa.string()),
            ('label_images_count', pa.int32()),
            ('days_to_approval', pa.int32()),
            ('data_quality_score', pa.float32()),
            ('created_date', pa.timestamp('ms')),
        ]),

        'fact_certificates': pa.schema([
            ('certificate_id', pa.int64()),
            ('ttb_id', pa.string()),
            ('company_id', pa.int64()),
            ('application_date_id', pa.int32()),
            ('approval_date_id', pa.int32()),
            ('certificate_number', pa.string()),
            ('certificate_status', pa.string()),
            ('certificate_type', pa.string()),
            ('plant_registry_number', pa.string()),
            ('applicant_signature_present', pa.bool_()),
            ('authorized_signature_present', pa.bool_()),
            ('authorized_signature_url', pa.string()),
            ('days_to_approval', pa.int32()),
            ('created_date', pa.timestamp('ms')),
        ]),
    }


def get_data_quality_tiers() -> Dict[str, Dict[str, Any]]:
    """Define data quality tier criteria."""
    return {
        'premium': {
            'min_completeness_score': 0.9,
            'required_fields': [
                'ttb_id', 'applicant_business_name', 'applicant_mailing_address',
                'cola_brand_name', 'cola_product_description', 'cola_approval_date'
            ],
            'validation_requirements': ['ttb_id_valid', 'product_class_validation_is_valid']
        },
        'standard': {
            'min_completeness_score': 0.7,
            'required_fields': [
                'ttb_id', 'applicant_business_name', 'cola_brand_name'
            ],
            'validation_requirements': ['ttb_id_valid']
        },
        'basic': {
            'min_completeness_score': 0.4,
            'required_fields': ['ttb_id'],
            'validation_requirements': []
        }
    }