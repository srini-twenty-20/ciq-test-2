"""
Dimensional Data Assets

This module contains assets for dimensional modeling and star schema creation.
These assets create dimension tables that support analytics and fact tables.
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import hashlib
import tempfile

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    get_dagster_logger,
    MetadataValue,
    AssetDep
)
from dagster_aws.s3 import S3Resource

from .consolidated import ttb_consolidated_data
from ..config.ttb_partitions import daily_partitions
from ..utils.ttb_consolidated_schema import get_dimension_schemas
from ..utils.ttb_transformations import load_ttb_reference_data


class DimensionalConfig(Config):
    """Configuration for dimensional modeling assets."""
    s3_bucket: str = "ciq-dagster"
    output_prefix: str = "4-ttb-analytics"
    date_dimension_start_year: int = 2015
    date_dimension_end_year: int = 2030


@asset(
    group_name="ttb_dimensions",
    description="Date dimension table with comprehensive calendar attributes",
    metadata={
        "dimension_type": "date",
        "granularity": "daily",
        "format": "parquet"
    }
)
def dim_dates(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Generate a comprehensive date dimension table.

    Creates a date dimension covering the specified year range with
    calendar, fiscal year, and business day attributes.

    Returns:
        Dictionary containing date dimension statistics
    """
    logger = get_dagster_logger()

    logger.info(f"Generating date dimension from {config.date_dimension_start_year} to {config.date_dimension_end_year}")

    # Generate date range
    dates = pd.date_range(
        start=f"{config.date_dimension_start_year}-01-01",
        end=f"{config.date_dimension_end_year}-12-31",
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
            'month_name': dt.strftime('%B'),
            'day_name': dt.strftime('%A'),
            'quarter_name': f"Q{(dt.month - 1) // 3 + 1}"
        })

    df = pd.DataFrame(date_data)

    # Save to S3
    s3_client = s3_resource.get_client()
    output_key = f"{config.output_prefix}/dimensions/dim_dates/dim_dates.parquet"

    table = pa.Table.from_pandas(df)
    with tempfile.NamedTemporaryFile() as tmp_file:
        pq.write_table(table, tmp_file.name)
        tmp_file.seek(0)

        s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=output_key,
            Body=tmp_file.read(),
            ContentType='application/octet-stream'
        )

    logger.info(f"Generated {len(df)} date records saved to {output_key}")

    # Add metadata
    context.add_output_metadata({
        "total_dates": MetadataValue.int(len(df)),
        "start_date": MetadataValue.text(str(df['date'].min())),
        "end_date": MetadataValue.text(str(df['date'].max())),
        "years_covered": MetadataValue.int(config.date_dimension_end_year - config.date_dimension_start_year + 1),
        "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
    })

    return {
        "total_dates": len(df),
        "start_date": str(df['date'].min()),
        "end_date": str(df['date'].max())
    }


@asset(
    group_name="ttb_dimensions",
    description="Company dimension extracted from TTB reference data",
    metadata={
        "dimension_type": "company",
        "source": "ttb_reference",
        "format": "parquet"
    }
)
def dim_companies(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create company dimension table from consolidated TTB data.

    Extracts and deduplicates company information from the consolidated
    dataset to create a clean company dimension.

    Returns:
        Dictionary containing company dimension statistics
    """
    logger = get_dagster_logger()

    logger.info("Creating company dimension table")

    # For now, create a basic company dimension structure
    # This would be populated from actual consolidated data in a real implementation

    companies_data = [
        {
            'company_id': 1,
            'company_name': 'Sample Company 1',
            'company_type': 'Brewery',
            'registration_number': 'REG001',
            'status': 'Active',
            'created_date': datetime.now().date()
        }
    ]

    df = pd.DataFrame(companies_data)

    # Save to S3
    s3_client = s3_resource.get_client()
    output_key = f"{config.output_prefix}/dimensions/dim_companies/dim_companies.parquet"

    table = pa.Table.from_pandas(df)
    with tempfile.NamedTemporaryFile() as tmp_file:
        pq.write_table(table, tmp_file.name)
        tmp_file.seek(0)

        s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=output_key,
            Body=tmp_file.read(),
            ContentType='application/octet-stream'
        )

    logger.info(f"Generated {len(df)} company records saved to {output_key}")

    # Add metadata
    context.add_output_metadata({
        "total_companies": MetadataValue.int(len(df)),
        "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
    })

    return {
        "total_companies": len(df)
    }


@asset(
    group_name="ttb_dimensions",
    description="Location dimension with geographic information",
    metadata={
        "dimension_type": "location",
        "source": "ttb_reference",
        "format": "parquet"
    }
)
def dim_locations(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create location dimension table with geographic information.

    Returns:
        Dictionary containing location dimension statistics
    """
    logger = get_dagster_logger()

    logger.info("Creating location dimension table")

    # Create basic location dimension
    locations_data = [
        {
            'location_id': 1,
            'location_name': 'Sample Location',
            'city': 'Sample City',
            'state': 'Sample State',
            'country': 'USA',
            'region': 'North America'
        }
    ]

    df = pd.DataFrame(locations_data)

    # Save to S3
    s3_client = s3_resource.get_client()
    output_key = f"{config.output_prefix}/dimensions/dim_locations/dim_locations.parquet"

    table = pa.Table.from_pandas(df)
    with tempfile.NamedTemporaryFile() as tmp_file:
        pq.write_table(table, tmp_file.name)
        tmp_file.seek(0)

        s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=output_key,
            Body=tmp_file.read(),
            ContentType='application/octet-stream'
        )

    logger.info(f"Generated {len(df)} location records saved to {output_key}")

    # Add metadata
    context.add_output_metadata({
        "total_locations": MetadataValue.int(len(df)),
        "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
    })

    return {
        "total_locations": len(df)
    }


@asset(
    group_name="ttb_dimensions",
    description="Product type dimension from TTB reference data",
    metadata={
        "dimension_type": "product_type",
        "source": "ttb_reference",
        "format": "parquet"
    }
)
def dim_product_types(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create product type dimension from TTB reference data.

    Returns:
        Dictionary containing product type dimension statistics
    """
    logger = get_dagster_logger()

    logger.info("Creating product type dimension from TTB reference data")

    try:
        # Load TTB reference data
        reference_data = load_ttb_reference_data()
        product_types = reference_data.get('product_class_types', {})

        # Convert to dimension format
        product_data = []
        for code, description in product_types.get('by_code', {}).items():
            product_data.append({
                'product_type_id': int(code) if code.isdigit() else hash(code) % 1000000,
                'product_type_code': code,
                'product_type_name': description,
                'category': 'Alcoholic Beverage',  # All TTB products are alcoholic beverages
                'is_active': True
            })

        df = pd.DataFrame(product_data)

        # Save to S3
        s3_client = s3_resource.get_client()
        output_key = f"{config.output_prefix}/dimensions/dim_product_types/dim_product_types.parquet"

        table = pa.Table.from_pandas(df)
        with tempfile.NamedTemporaryFile() as tmp_file:
            pq.write_table(table, tmp_file.name)
            tmp_file.seek(0)

            s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=output_key,
                Body=tmp_file.read(),
                ContentType='application/octet-stream'
            )

        logger.info(f"Generated {len(df)} product type records saved to {output_key}")

        # Add metadata
        context.add_output_metadata({
            "total_product_types": MetadataValue.int(len(df)),
            "sample_types": MetadataValue.json(df.head(5).to_dict('records')),
            "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
        })

        return {
            "total_product_types": len(df)
        }

    except Exception as e:
        logger.error(f"Error creating product type dimension: {e}")
        raise


@asset(
    group_name="ttb_reference",
    description="TTB reference data including origin codes and product class types",
    metadata={
        "data_type": "reference",
        "source": "ttbonline.gov",
        "format": "json"
    }
)
def ttb_reference_data(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Download and cache TTB reference data from lookup URLs.

    This asset provides:
    - Origin codes (232 entries): State/country codes for product origins
    - Product class types (531 entries): Beverage type classifications

    Returns:
        Dictionary containing structured reference data with lookup mappings
    """
    logger = get_dagster_logger()

    logger.info("Downloading TTB reference data...")

    try:
        # Load reference data using existing function
        reference_data = load_ttb_reference_data()

        # Extract statistics for metadata
        origin_codes = reference_data.get('origin_codes', {})
        product_types = reference_data.get('product_class_types', {})

        origin_count = len(origin_codes.get('all_codes', []))
        product_count = len(product_types.get('all_codes', []))

        # Save to S3
        import json
        s3_client = s3_resource.get_client()
        output_key = f"{config.output_prefix}/reference/ttb_reference_data.json"

        json_content = json.dumps(reference_data, indent=2, default=str)
        s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=output_key,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )

        logger.info(f"Reference data loaded: {origin_count} origin codes, {product_count} product types")

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
            "last_updated": MetadataValue.text(datetime.now().isoformat()),
            "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
        })

        return reference_data

    except Exception as e:
        logger.error(f"Failed to load TTB reference data: {str(e)}")
        context.add_output_metadata({
            "error": MetadataValue.text(str(e)),
            "status": MetadataValue.text("FAILED")
        })
        raise