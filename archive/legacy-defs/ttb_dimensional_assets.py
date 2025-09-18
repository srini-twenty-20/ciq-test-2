"""
Dimensional modeling assets for TTB data.
Creates star schema with fact and dimension tables from consolidated data.
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
    Config,
    get_dagster_logger,
    DailyPartitionsDefinition
)
from dagster_aws.s3 import S3Resource

from .ttb_consolidated_schema import get_dimension_schemas
from .ttb_partitions import daily_partitions


class DimensionalConfig(Config):
    """Configuration for dimensional modeling assets."""
    s3_bucket: str = "ciq-dagster"
    output_prefix: str = "3-ttb-dimensional-data"
    date_dimension_start_year: int = 2015
    date_dimension_end_year: int = 2030


def _generate_date_dimension(start_year: int, end_year: int) -> pd.DataFrame:
    """Generate a comprehensive date dimension table."""
    dates = pd.date_range(
        start=f"{start_year}-01-01",
        end=f"{end_year}-12-31",
        freq='D'
    )

    date_data = []
    for dt in dates:
        date_id = int(dt.strftime('%Y%m%d'))
        fiscal_year = dt.year if dt.month >= 10 else dt.year - 1  # Oct-Sep fiscal year
        fiscal_quarter = ((dt.month - 10) % 12) // 3 + 1 if dt.month >= 10 else ((dt.month + 2) // 3)

        date_data.append({
            'date_id': date_id,
            'date': dt.date(),
            'year': dt.year,
            'month': dt.month,
            'quarter': (dt.month - 1) // 3 + 1,
            'day_of_year': dt.dayofyear,
            'week_of_year': dt.isocalendar()[1],
            'day_of_week': dt.dayofweek + 1,  # 1-7 instead of 0-6
            'is_weekend': dt.dayofweek >= 5,
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
        })

    return pd.DataFrame(date_data)


def _extract_companies_from_consolidated(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and deduplicate company information."""
    company_cols = [
        'applicant_business_name', 'applicant_mailing_address',
        'applicant_phone', 'applicant_email', 'applicant_fax'
    ]

    # Filter to rows with valid company data
    company_df = df[company_cols + ['cert_plant_registry_number', 'processing_timestamp']].copy()
    company_df = company_df.dropna(subset=['applicant_business_name'])

    # Create company hash for deduplication
    company_df['company_hash'] = company_df.apply(
        lambda row: hashlib.md5(
            f"{row['applicant_business_name']}|{row['applicant_mailing_address']}".encode()
        ).hexdigest()[:16], axis=1
    )

    # Group by company hash and aggregate
    company_groups = company_df.groupby('company_hash').agg({
        'applicant_business_name': 'first',
        'applicant_mailing_address': 'first',
        'applicant_phone': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
        'applicant_email': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
        'applicant_fax': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
        'cert_plant_registry_number': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
        'processing_timestamp': 'min'
    }).reset_index()

    # Create company_id from hash
    company_groups['company_id'] = company_groups['company_hash'].apply(
        lambda x: int(x[:8], 16)  # Convert first 8 hex chars to int
    )

    # Collect all TTB IDs for each company
    ttb_ids_by_company = df.groupby(
        df.apply(lambda row: hashlib.md5(
            f"{row['applicant_business_name']}|{row['applicant_mailing_address']}".encode()
        ).hexdigest()[:16], axis=1)
    )['ttb_id'].apply(list).to_dict()

    company_groups['source_ttb_ids'] = company_groups['company_hash'].map(ttb_ids_by_company)

    return company_groups[[
        'company_id', 'applicant_business_name', 'applicant_mailing_address',
        'applicant_phone', 'applicant_email', 'applicant_fax',
        'cert_plant_registry_number', 'processing_timestamp', 'source_ttb_ids'
    ]].rename(columns={
        'applicant_business_name': 'company_name',
        'applicant_mailing_address': 'mailing_address',
        'applicant_phone': 'phone',
        'applicant_email': 'email',
        'applicant_fax': 'fax',
        'cert_plant_registry_number': 'plant_registry_number',
        'processing_timestamp': 'created_date'
    }).assign(
        modified_date=lambda x: x['created_date'],
        is_current=True
    )


def _extract_locations_from_consolidated(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and deduplicate location/origin information."""
    # Filter to rows with valid origin data
    location_df = df[['cola_origin_code', 'origin_code_validation_description']].copy()
    location_df = location_df.dropna(subset=['cola_origin_code'])
    location_df = location_df.drop_duplicates(subset=['cola_origin_code'])

    # Create location_id from origin code hash
    location_df['location_id'] = location_df['cola_origin_code'].apply(
        lambda x: int(hashlib.md5(x.encode()).hexdigest()[:8], 16)
    )

    # Parse location information (simplified - would need more sophisticated parsing)
    location_df['state'] = location_df['origin_code_validation_description'].str.extract(r'\b([A-Z]{2})\b')
    location_df['country'] = location_df['origin_code_validation_description'].apply(
        lambda x: 'USA' if any(state in str(x) for state in ['CA', 'NY', 'TX', 'FL', 'WA']) else 'Other'
    )
    location_df['region'] = location_df['state']  # Simplified
    location_df['is_domestic'] = location_df['country'] == 'USA'
    location_df['created_date'] = datetime.now()

    return location_df[[
        'location_id', 'cola_origin_code', 'origin_code_validation_description',
        'state', 'country', 'region', 'is_domestic', 'created_date'
    ]].rename(columns={
        'cola_origin_code': 'origin_code',
        'origin_code_validation_description': 'origin_description'
    })


def _extract_product_types_from_consolidated(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and deduplicate product type information."""
    product_type_df = df[['cola_product_class_type', 'product_class_validation_description']].copy()
    product_type_df = product_type_df.dropna(subset=['cola_product_class_type'])
    product_type_df = product_type_df.drop_duplicates(subset=['cola_product_class_type'])

    # Create product_type_id from class code hash
    product_type_df['product_type_id'] = product_type_df['cola_product_class_type'].apply(
        lambda x: int(hashlib.md5(x.encode()).hexdigest()[:8], 16)
    )

    # Categorize products (simplified logic)
    def categorize_product(class_type: str, description: str) -> tuple:
        description_lower = str(description).lower()
        class_lower = str(class_type).lower()

        if any(term in description_lower for term in ['wine', 'grape']):
            return 'Wine', 'Table Wine' if 'table' in description_lower else 'Specialty Wine'
        elif any(term in description_lower for term in ['beer', 'malt', 'ale', 'lager']):
            return 'Beer', 'Standard Beer' if 'beer' in description_lower else 'Specialty Beer'
        elif any(term in description_lower for term in ['spirit', 'whiskey', 'vodka', 'gin', 'rum']):
            return 'Spirits', 'Distilled Spirits'
        else:
            return 'Other', 'Specialty Product'

    product_type_df[['category', 'subcategory']] = product_type_df.apply(
        lambda row: categorize_product(row['cola_product_class_type'], row['product_class_validation_description']),
        axis=1, result_type='expand'
    )

    product_type_df['is_alcoholic'] = True  # All TTB products are alcoholic
    product_type_df['created_date'] = datetime.now()

    return product_type_df[[
        'product_type_id', 'cola_product_class_type', 'product_class_validation_description',
        'category', 'subcategory', 'is_alcoholic', 'created_date'
    ]].rename(columns={
        'cola_product_class_type': 'product_class_code',
        'product_class_validation_description': 'product_class_description'
    })


@asset(
    description="Date dimension table covering all relevant years for TTB data"
)
def dim_dates(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Generate and store date dimension table."""
    logger = get_dagster_logger()

    # Generate date dimension
    date_df = _generate_date_dimension(
        config.date_dimension_start_year,
        config.date_dimension_end_year
    )

    logger.info(f"Generated {len(date_df)} date records from {config.date_dimension_start_year} to {config.date_dimension_end_year}")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['dim_dates']
    table = pa.Table.from_pandas(date_df, schema=schema)

    s3_key = f"{config.output_prefix}/dim_dates/dim_dates.parquet"

    # Write to S3
    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue().to_pybytes()
    )

    logger.info(f"Uploaded date dimension to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(date_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "date_range": f"{config.date_dimension_start_year}-{config.date_dimension_end_year}",
        "schema_fields": len(schema)
    }


@asset(
    deps=["ttb_consolidated_data"],
    partitions_def=daily_partitions,
    description="Company dimension table extracted from consolidated TTB data"
)
def dim_companies(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Extract and deduplicate company information from consolidated data."""
    logger = get_dagster_logger()

    # Read consolidated data from S3
    partition_date = context.partition_key
    s3_path = f"2-ttb-processed-data/ttb_consolidated_data/partition_date={partition_date}"

    consolidated_df = pd.DataFrame()

    s3_client = s3_resource.get_client()
    # List all parquet files for this partition
    response = s3_client.list_objects_v2(
        Bucket=config.s3_bucket,
        Prefix=s3_path
    )

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                # Read each parquet file
                parquet_obj = s3_client.get_object(Bucket=config.s3_bucket, Key=obj['Key'])
                df_chunk = pd.read_parquet(parquet_obj['Body'])
                consolidated_df = pd.concat([consolidated_df, df_chunk], ignore_index=True)

    if consolidated_df.empty:
        logger.warning(f"No consolidated data found for partition {partition_date}")
        return {"records_processed": 0, "companies_extracted": 0}

    # Extract companies
    companies_df = _extract_companies_from_consolidated(consolidated_df)

    logger.info(f"Extracted {len(companies_df)} unique companies from {len(consolidated_df)} consolidated records")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['dim_companies']
    table = pa.Table.from_pandas(companies_df, schema=schema)

    s3_key = f"{config.output_prefix}/dim_companies/partition_date={partition_date}/companies.parquet"

    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue().to_pybytes()
        )

    logger.info(f"Uploaded company dimension to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(consolidated_df),
        "companies_extracted": len(companies_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "schema_fields": len(schema)
    }


@asset(
    deps=["ttb_consolidated_data"],
    partitions_def=daily_partitions,
    description="Location dimension table extracted from consolidated TTB data"
)
def dim_locations(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Extract and deduplicate location information from consolidated data."""
    logger = get_dagster_logger()

    # Read consolidated data from S3 (similar pattern as dim_companies)
    partition_date = context.partition_key
    s3_path = f"2-ttb-processed-data/ttb_consolidated_data/partition_date={partition_date}"

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
        return {"records_processed": 0, "locations_extracted": 0}

    # Extract locations
    locations_df = _extract_locations_from_consolidated(consolidated_df)

    logger.info(f"Extracted {len(locations_df)} unique locations from {len(consolidated_df)} consolidated records")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['dim_locations']
    table = pa.Table.from_pandas(locations_df, schema=schema)

    s3_key = f"{config.output_prefix}/dim_locations/partition_date={partition_date}/locations.parquet"

    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue().to_pybytes()
        )

    logger.info(f"Uploaded location dimension to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(consolidated_df),
        "locations_extracted": len(locations_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "schema_fields": len(schema)
    }


@asset(
    deps=["ttb_consolidated_data"],
    partitions_def=daily_partitions,
    description="Product type dimension table extracted from consolidated TTB data"
)
def dim_product_types(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """Extract and deduplicate product type information from consolidated data."""
    logger = get_dagster_logger()

    # Read consolidated data from S3 (similar pattern)
    partition_date = context.partition_key
    s3_path = f"2-ttb-processed-data/ttb_consolidated_data/partition_date={partition_date}"

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
        return {"records_processed": 0, "product_types_extracted": 0}

    # Extract product types
    product_types_df = _extract_product_types_from_consolidated(consolidated_df)

    logger.info(f"Extracted {len(product_types_df)} unique product types from {len(consolidated_df)} consolidated records")

    # Convert to PyArrow and write to S3
    schema = get_dimension_schemas()['dim_product_types']
    table = pa.Table.from_pandas(product_types_df, schema=schema)

    s3_key = f"{config.output_prefix}/dim_product_types/partition_date={partition_date}/product_types.parquet"

    s3_client = s3_resource.get_client()
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue().to_pybytes()
        )

    logger.info(f"Uploaded product type dimension to s3://{config.s3_bucket}/{s3_key}")

    return {
        "records_processed": len(consolidated_df),
        "product_types_extracted": len(product_types_df),
        "s3_location": f"s3://{config.s3_bucket}/{s3_key}",
        "schema_fields": len(schema)
    }