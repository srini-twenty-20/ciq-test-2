"""
Supabase Export Assets

Assets for exporting TTB data from S3 to Supabase tables for analytics and visualization.
"""
from typing import Dict, Any
from dagster import (
    asset,
    get_dagster_logger,
    AssetIn,
    Output,
    Config
)

from ciq_test_2.config.ttb_partitions import daily_partitions


class SupabaseExportConfig(Config):
    """Configuration for Supabase export operations."""
    batch_size: int = 1000
    enable_validation: bool = True


@asset(
    group_name="ttb_supabase_export",
    description="Export TTB reference data to Supabase tables",
    ins={"ttb_reference_data": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_reference_data(context, config: SupabaseExportConfig, ttb_reference_data: Dict[str, Any]) -> Dict[str, Any]:
    """Export reference data to Supabase tables."""
    logger = get_dagster_logger()

    logger.info("Exporting TTB reference data to Supabase")

    # Transform reference data for Supabase schema
    export_data = {
        'product_class_types': [],
        'origin_codes': []
    }

    # Transform product class types
    if 'product_class_types' in ttb_reference_data:
        product_types = ttb_reference_data['product_class_types']
        if isinstance(product_types, dict):
            # Convert dictionary to list of code/description pairs
            for code, description in product_types.items():
                export_data['product_class_types'].append({
                    'code': code,
                    'description': description
                })
        elif isinstance(product_types, list):
            # Handle list format
            for item in product_types:
                if isinstance(item, dict):
                    export_data['product_class_types'].append({
                        'code': item.get('code', ''),
                        'description': item.get('description', '')
                    })

    # Transform origin codes
    if 'origin_codes' in ttb_reference_data:
        origin_codes = ttb_reference_data['origin_codes']
        if isinstance(origin_codes, dict):
            # Convert dictionary to list of code/description pairs
            for code, description in origin_codes.items():
                export_data['origin_codes'].append({
                    'code': code,
                    'description': description
                })
        elif isinstance(origin_codes, list):
            # Handle list format
            for item in origin_codes:
                if isinstance(item, dict):
                    export_data['origin_codes'].append({
                        'code': item.get('code', ''),
                        'description': item.get('description', '')
                    })

    logger.info(f"Transformed {len(export_data['product_class_types'])} product class types")
    logger.info(f"Transformed {len(export_data['origin_codes'])} origin codes")

    return export_data


@asset(
    group_name="ttb_supabase_export",
    description="Export date dimension to Supabase",
    ins={"dim_dates": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_dates(context, config: SupabaseExportConfig, dim_dates: Dict[str, Any]) -> Dict[str, Any]:
    """Export date dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info("Exporting date dimension to Supabase")

    if 'records' not in dim_dates:
        logger.warning("No records found in date dimension")
        return {"records": []}

    records = dim_dates['records']
    logger.info(f"Exporting {len(records)} date dimension records")

    # Transform date records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'date_id': record.get('date_id'),
            'date': record.get('date'),
            'year': record.get('year'),
            'quarter': record.get('quarter'),
            'month': record.get('month'),
            'day': record.get('day'),
            'day_of_week': record.get('day_of_week'),
            'day_of_year': record.get('day_of_year'),
            'week_of_year': record.get('week_of_year'),
            'fiscal_year': record.get('fiscal_year'),
            'fiscal_quarter': record.get('fiscal_quarter'),
            'is_weekend': record.get('is_weekend'),
            'is_holiday': record.get('is_holiday'),
            'month_name': record.get('month_name'),
            'day_name': record.get('day_name'),
            'quarter_name': record.get('quarter_name'),
            'season': record.get('season'),
            'days_from_epoch': record.get('days_from_epoch')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export company dimension to Supabase",
    ins={"dim_companies": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_companies(context, config: SupabaseExportConfig, dim_companies: Dict[str, Any]) -> Dict[str, Any]:
    """Export company dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting company dimension to Supabase for partition {context.partition_key}")

    if 'records' not in dim_companies:
        logger.warning("No records found in company dimension")
        return {"records": []}

    records = dim_companies['records']
    logger.info(f"Exporting {len(records)} company dimension records")

    # Transform company records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'company_id': record.get('company_id'),
            'business_name': record.get('business_name'),
            'mailing_address': record.get('mailing_address'),
            'phone': record.get('phone'),
            'email': record.get('email'),
            'fax': record.get('fax'),
            'first_seen_date': record.get('first_seen_date'),
            'last_seen_date': record.get('last_seen_date'),
            'total_applications': record.get('total_applications'),
            'data_quality_score': record.get('data_quality_score'),
            'source_ttb_ids': record.get('source_ttb_ids', []),
            'partition_date': context.partition_key
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export product dimension to Supabase",
    ins={"dim_products": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_products(context, config: SupabaseExportConfig, dim_products: Dict[str, Any]) -> Dict[str, Any]:
    """Export product dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting product dimension to Supabase for partition {context.partition_key}")

    if 'records' not in dim_products:
        logger.warning("No records found in product dimension")
        return {"records": []}

    records = dim_products['records']
    logger.info(f"Exporting {len(records)} product dimension records")

    # Transform product records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'product_id': record.get('product_id'),
            'brand_name': record.get('brand_name'),
            'fanciful_name': record.get('fanciful_name'),
            'product_description': record.get('product_description'),
            'class_type_code': record.get('class_type_code'),
            'origin_code': record.get('origin_code'),
            'product_category': record.get('product_category'),
            'grape_varietals': record.get('grape_varietals'),
            'wine_appellation': record.get('wine_appellation'),
            'alcohol_content': record.get('alcohol_content'),
            'net_contents': record.get('net_contents'),
            'first_seen_date': record.get('first_seen_date'),
            'last_seen_date': record.get('last_seen_date'),
            'total_labels': record.get('total_labels'),
            'data_quality_score': record.get('data_quality_score'),
            'source_ttb_ids': record.get('source_ttb_ids', []),
            'partition_date': context.partition_key
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export certificate facts to Supabase",
    ins={"fact_certificates": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_fact_certificates(context, config: SupabaseExportConfig, fact_certificates: Dict[str, Any]) -> Dict[str, Any]:
    """Export certificate facts to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting certificate facts to Supabase for partition {context.partition_key}")

    if 'records' not in fact_certificates:
        logger.warning("No records found in certificate facts")
        return {"records": []}

    records = fact_certificates['records']
    logger.info(f"Exporting {len(records)} certificate fact records")

    # Transform certificate fact records for Supabase (match table schema)
    transformed_records = []
    for record in records:
        transformed_record = {
            'certificate_fact_id': record.get('certificate_fact_id'),
            'ttb_id': record.get('ttb_id'),
            'company_id': record.get('company_id'),
            'product_id': record.get('product_id'),
            'filing_date_id': record.get('filing_date_id'),
            'approval_date_id': record.get('approval_date_id'),
            'expiration_date_id': record.get('expiration_date_id'),
            'final_quality_score': record.get('final_quality_score'),
            'data_completeness_score': record.get('data_completeness_score'),
            'days_to_approval': record.get('days_to_approval'),
            'status': record.get('status'),
            'serial_number': record.get('serial_number'),
            'vendor_code': record.get('vendor_code'),
            'filing_date': record.get('filing_date'),
            'approval_date': record.get('approval_date'),
            'expiration_date': record.get('expiration_date'),
            'partition_date': record.get('partition_date'),
            'fact_creation_timestamp': record.get('fact_creation_timestamp'),
            'source_extraction_timestamp': record.get('source_extraction_timestamp'),
            'source_cleaning_timestamp': record.get('source_cleaning_timestamp'),
            'source_structuring_timestamp': record.get('source_structuring_timestamp')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export product facts to Supabase",
    ins={"fact_products": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_fact_products(context, config: SupabaseExportConfig, fact_products: Dict[str, Any]) -> Dict[str, Any]:
    """Export product facts to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting product facts to Supabase for partition {context.partition_key}")

    if 'records' not in fact_products:
        logger.warning("No records found in product facts")
        return {"records": []}

    records = fact_products['records']
    logger.info(f"Exporting {len(records)} product fact records")

    # Transform fact records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'product_fact_id': record.get('product_fact_id'),
            'ttb_id': record.get('ttb_id'),
            'company_id': record.get('company_id'),
            'product_id': record.get('product_id'),
            'filing_date_id': record.get('filing_date_id'),
            'approval_date_id': record.get('approval_date_id'),
            'expiration_date_id': record.get('expiration_date_id'),
            'final_quality_score': record.get('final_quality_score'),
            'data_completeness_score': record.get('data_completeness_score'),
            'days_to_approval': record.get('days_to_approval'),
            'has_certificate_data': record.get('has_certificate_data'),
            'has_cola_detail_data': record.get('has_cola_detail_data'),
            'class_type_code': record.get('class_type_code'),
            'origin_code': record.get('origin_code'),
            'product_category': record.get('product_category'),
            'status': record.get('status'),
            'serial_number': record.get('serial_number'),
            'vendor_code': record.get('vendor_code'),
            'filing_date': record.get('filing_date'),
            'approval_date': record.get('approval_date'),
            'expiration_date': record.get('expiration_date'),
            'partition_date': record.get('partition_date'),
            'fact_creation_timestamp': record.get('fact_creation_timestamp'),
            'source_extraction_timestamp': record.get('source_extraction_timestamp'),
            'source_cleaning_timestamp': record.get('source_cleaning_timestamp'),
            'source_structuring_timestamp': record.get('source_structuring_timestamp')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


# Full Supabase export job
@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Complete TTB data export to Supabase",
    ins={
        "supabase_reference_data": AssetIn(),
        "supabase_dim_dates": AssetIn(),
        "supabase_dim_companies": AssetIn(),
        "supabase_dim_products": AssetIn(),
        "supabase_fact_products": AssetIn(),
        "supabase_fact_certificates": AssetIn()
    }
)
def ttb_supabase_export_complete(
    context,
    config: SupabaseExportConfig,
    supabase_reference_data: Dict[str, Any],
    supabase_dim_dates: Dict[str, Any],
    supabase_dim_companies: Dict[str, Any],
    supabase_dim_products: Dict[str, Any],
    supabase_fact_products: Dict[str, Any],
    supabase_fact_certificates: Dict[str, Any]
) -> Dict[str, Any]:
    """Summary asset indicating complete TTB export to Supabase."""
    logger = get_dagster_logger()

    export_summary = {
        'partition_date': context.partition_key,
        'reference_data': {
            'product_class_types': len(supabase_reference_data.get('product_class_types', [])),
            'origin_codes': len(supabase_reference_data.get('origin_codes', []))
        },
        'dimensions': {
            'dates': len(supabase_dim_dates.get('records', [])),
            'companies': len(supabase_dim_companies.get('records', [])),
            'products': len(supabase_dim_products.get('records', []))
        },
        'facts': {
            'products': len(supabase_fact_products.get('records', [])),
            'certificates': len(supabase_fact_certificates.get('records', []))
        },
        'export_timestamp': context.run_id
    }

    logger.info(f"Complete TTB Supabase export summary: {export_summary}")

    return export_summary