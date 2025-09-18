"""
Fact table assets for TTB dimensional model.
Creates fact_products and fact_certificates from consolidated data with foreign key lookups.
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import hashlib
import logging

from dagster import (
    asset,
    AssetExecutionContext,
    get_dagster_logger,
    DailyPartitionsDefinition
)
from dagster_aws.s3 import S3Resource

from .ttb_consolidated_schema import get_dimension_schemas
from .ttb_config import TTBGlobalConfig
from .ttb_partitions import daily_partitions


# Remove the local FactConfig class - we'll use TTBGlobalConfig instead


def _create_date_id(date_val) -> Optional[int]:
    """Convert date to date_id format (YYYYMMDD)."""
    if pd.isna(date_val) or date_val is None:
        return None

    if isinstance(date_val, str):
        try:
            date_val = pd.to_datetime(date_val).date()
        except:
            return None
    elif isinstance(date_val, datetime):
        date_val = date_val.date()

    if isinstance(date_val, date):
        return int(date_val.strftime('%Y%m%d'))

    return None


def _create_company_id(business_name: str, mailing_address: str) -> Optional[int]:
    """Create company_id hash matching dim_companies logic."""
    if pd.isna(business_name) or business_name is None:
        return None

    company_hash = hashlib.md5(
        f"{business_name}|{mailing_address}".encode()
    ).hexdigest()[:16]

    return int(company_hash[:8], 16)


def _create_location_id(origin_code: str) -> Optional[int]:
    """Create location_id hash matching dim_locations logic."""
    if pd.isna(origin_code) or origin_code is None:
        return None

    return int(hashlib.md5(origin_code.encode()).hexdigest()[:8], 16)


def _create_product_type_id(product_class_type: str) -> Optional[int]:
    """Create product_type_id hash matching dim_product_types logic."""
    if pd.isna(product_class_type) or product_class_type is None:
        return None

    return int(hashlib.md5(product_class_type.encode()).hexdigest()[:8], 16)


def _calculate_days_to_approval(filing_date, approval_date) -> Optional[int]:
    """Calculate days between filing and approval."""
    if pd.isna(filing_date) or pd.isna(approval_date):
        return None

    try:
        if isinstance(filing_date, str):
            filing_date = pd.to_datetime(filing_date).date()
        if isinstance(approval_date, str):
            approval_date = pd.to_datetime(approval_date).date()

        return (approval_date - filing_date).days
    except:
        return None


def _calculate_data_quality_score(row: pd.Series) -> float:
    """Calculate data quality score for a record."""
    critical_fields = [
        'ttb_id', 'applicant_business_name', 'cola_brand_name',
        'cola_product_description', 'cola_approval_date'
    ]

    important_fields = [
        'cola_filing_date', 'cola_net_contents', 'cola_alcohol_content',
        'cola_product_class_type', 'cola_origin_code'
    ]

    optional_fields = [
        'cola_fanciful_name', 'cola_formula_number', 'applicant_phone',
        'applicant_email', 'applicant_mailing_address'
    ]

    # Critical fields count double
    critical_score = sum(1 for field in critical_fields if not pd.isna(row.get(field)))
    important_score = sum(1 for field in important_fields if not pd.isna(row.get(field)))
    optional_score = sum(1 for field in optional_fields if not pd.isna(row.get(field)))

    max_score = len(critical_fields) * 2 + len(important_fields) + len(optional_fields)
    actual_score = critical_score * 2 + important_score + optional_score

    return actual_score / max_score if max_score > 0 else 0.0


@asset(
    deps=["ttb_consolidated_data", "dim_companies", "dim_locations", "dim_product_types", "dim_dates"],
    partitions_def=daily_partitions,
    description="Product fact table with foreign keys to dimension tables"
)
def fact_products(
    context: AssetExecutionContext,
    config: TTBGlobalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Create fact_products table from consolidated data with dimension lookups."""
    logger = get_dagster_logger()

    # Read consolidated data from S3
    partition_date = context.partition_key
    s3_path = f"{config.processed_data_prefix}/ttb_consolidated_data/partition_date={partition_date}"

    consolidated_df = pd.DataFrame()

    s3_client = s3_resource.get_client()
    response = s3_client.list_objects_v2(
        Bucket=config.s3_bucket,
        Prefix=s3_path
    )

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                parquet_obj = s3_client.get_object(Bucket=config.s3_bucket, Key=obj['Key'])
                df_chunk = pd.read_parquet(parquet_obj['Body'])
                consolidated_df = pd.concat([consolidated_df, df_chunk], ignore_index=True)

    if consolidated_df.empty:
        logger.warning(f"No consolidated data found for partition {partition_date}")
        return {"records_processed": 0, "products_created": 0}

    # Filter to records with COLA detail data (products)
    products_df = consolidated_df[consolidated_df['has_cola_detail'] == True].copy()

    if products_df.empty:
        logger.warning(f"No COLA detail records found for partition {partition_date}")
        return {"records_processed": len(consolidated_df), "products_created": 0}

    # Create fact_products records
    fact_products_data = []

    for idx, row in products_df.iterrows():
        # Generate unique product_id
        product_id = int(hashlib.md5(f"{row['ttb_id']}_product".encode()).hexdigest()[:8], 16)

        # Create foreign key IDs
        company_id = _create_company_id(row.get('applicant_business_name'), row.get('applicant_mailing_address'))
        location_id = _create_location_id(row.get('cola_origin_code'))
        product_type_id = _create_product_type_id(row.get('cola_product_class_type'))

        # Create date IDs
        filing_date_id = _create_date_id(row.get('cola_filing_date'))
        approval_date_id = _create_date_id(row.get('cola_approval_date'))
        expiration_date_id = _create_date_id(row.get('cola_expiration_date'))

        # Calculate derived fields
        days_to_approval = _calculate_days_to_approval(row.get('cola_filing_date'), row.get('cola_approval_date'))
        data_quality_score = _calculate_data_quality_score(row)

        fact_record = {
            'product_id': product_id,
            'ttb_id': row.get('ttb_id'),
            'company_id': company_id,
            'location_id': location_id,
            'product_type_id': product_type_id,
            'filing_date_id': filing_date_id,
            'approval_date_id': approval_date_id,
            'expiration_date_id': expiration_date_id,
            'brand_name': row.get('cola_brand_name'),
            'fanciful_name': row.get('cola_fanciful_name'),
            'product_description': row.get('cola_product_description'),
            'net_contents': row.get('cola_net_contents'),
            'alcohol_content': row.get('cola_alcohol_content'),
            'formula_number': row.get('cola_formula_number'),
            'serial_number': row.get('cola_serial_number'),
            'label_images_count': row.get('cola_label_images_count'),
            'days_to_approval': days_to_approval,
            'data_quality_score': data_quality_score,
            'created_date': datetime.now()
        }

        fact_products_data.append(fact_record)

    fact_products_df = pd.DataFrame(fact_products_data)

    logger.info(f"Created {len(fact_products_df)} product fact records from {len(products_df)} COLA detail records")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['fact_products']
    table = pa.Table.from_pandas(fact_products_df, schema=schema)

    s3_key = f"{config.dimensional_data_prefix}/fact_products/partition_date={partition_date}/products.parquet"

    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue().to_pybytes()
    )

    logger.info(f"Uploaded products fact table to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(consolidated_df),
        "cola_records_found": len(products_df),
        "products_created": len(fact_products_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "avg_data_quality_score": fact_products_df['data_quality_score'].mean(),
        "schema_fields": len(schema)
    }


@asset(
    deps=["ttb_consolidated_data", "dim_companies", "dim_dates"],
    partitions_def=daily_partitions,
    description="Certificate fact table with foreign keys to dimension tables"
)
def fact_certificates(
    context: AssetExecutionContext,
    config: TTBGlobalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Create fact_certificates table from consolidated data with dimension lookups."""
    logger = get_dagster_logger()

    # Read consolidated data from S3
    partition_date = context.partition_key
    s3_path = f"{config.processed_data_prefix}/ttb_consolidated_data/partition_date={partition_date}"

    consolidated_df = pd.DataFrame()

    s3_client = s3_resource.get_client()
    response = s3_client.list_objects_v2(
        Bucket=config.s3_bucket,
        Prefix=s3_path
    )

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                parquet_obj = s3_client.get_object(Bucket=config.s3_bucket, Key=obj['Key'])
                df_chunk = pd.read_parquet(parquet_obj['Body'])
                consolidated_df = pd.concat([consolidated_df, df_chunk], ignore_index=True)

    if consolidated_df.empty:
        logger.warning(f"No consolidated data found for partition {partition_date}")
        return {"records_processed": 0, "certificates_created": 0}

    # Filter to records with certificate data
    certificates_df = consolidated_df[consolidated_df['has_certificate'] == True].copy()

    if certificates_df.empty:
        logger.warning(f"No certificate records found for partition {partition_date}")
        return {"records_processed": len(consolidated_df), "certificates_created": 0}

    # Create fact_certificates records
    fact_certificates_data = []

    for idx, row in certificates_df.iterrows():
        # Generate unique certificate_id
        certificate_id = int(hashlib.md5(f"{row['ttb_id']}_certificate".encode()).hexdigest()[:8], 16)

        # Create foreign key IDs
        company_id = _create_company_id(row.get('applicant_business_name'), row.get('applicant_mailing_address'))

        # Create date IDs
        application_date_id = _create_date_id(row.get('cert_application_date'))
        approval_date_id = _create_date_id(row.get('cert_approval_date'))

        # Calculate derived fields
        days_to_approval = _calculate_days_to_approval(row.get('cert_application_date'), row.get('cert_approval_date'))

        fact_record = {
            'certificate_id': certificate_id,
            'ttb_id': row.get('ttb_id'),
            'company_id': company_id,
            'application_date_id': application_date_id,
            'approval_date_id': approval_date_id,
            'certificate_number': row.get('cert_certificate_number'),
            'certificate_status': row.get('cert_certificate_status'),
            'certificate_type': row.get('cert_certificate_type'),
            'plant_registry_number': row.get('cert_plant_registry_number'),
            'applicant_signature_present': row.get('cert_applicant_signature_present'),
            'authorized_signature_present': row.get('cert_authorized_signature_present'),
            'authorized_signature_url': row.get('cert_authorized_signature_url'),
            'days_to_approval': days_to_approval,
            'created_date': datetime.now()
        }

        fact_certificates_data.append(fact_record)

    fact_certificates_df = pd.DataFrame(fact_certificates_data)

    logger.info(f"Created {len(fact_certificates_df)} certificate fact records from {len(certificates_df)} certificate records")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['fact_certificates']
    table = pa.Table.from_pandas(fact_certificates_df, schema=schema)

    s3_key = f"{config.dimensional_data_prefix}/fact_certificates/partition_date={partition_date}/certificates.parquet"

    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue().to_pybytes()
    )

    logger.info(f"Uploaded certificates fact table to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(consolidated_df),
        "certificate_records_found": len(certificates_df),
        "certificates_created": len(fact_certificates_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "avg_days_to_approval": fact_certificates_df['days_to_approval'].mean() if len(fact_certificates_df) > 0 else None,
        "schema_fields": len(schema)
    }