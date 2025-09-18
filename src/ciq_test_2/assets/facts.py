"""
Fact Table Assets

This module contains fact tables that form the core of the star schema.
Fact tables contain quantitative measures and foreign keys to dimensions.
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
    get_dagster_logger,
    Config,
    MetadataValue,
    AssetDep
)
from dagster_aws.s3 import S3Resource

from .consolidated import ttb_consolidated_data
from .dimensional import dim_dates, dim_companies, dim_locations, dim_product_types
from ..config.ttb_partitions import daily_partitions
from ..utils.ttb_consolidated_schema import get_dimension_schemas


class FactConfig(Config):
    """Configuration for fact table assets."""
    s3_bucket: str = "ciq-dagster"
    output_prefix: str = "4-ttb-analytics"


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
        'cola_origin_code', 'cola_product_class_type'
    ]

    # Count non-null critical fields
    critical_score = sum(1 for field in critical_fields if field in row and pd.notna(row[field])) / len(critical_fields)

    # Count non-null important fields
    important_score = sum(1 for field in important_fields if field in row and pd.notna(row[field])) / len(important_fields)

    # Weighted average (critical fields worth more)
    return (critical_score * 0.7) + (important_score * 0.3)


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_facts",
    description="Product fact table with measures and dimension keys",
    deps=[
        AssetDep(ttb_consolidated_data),
        AssetDep(dim_dates),
        AssetDep(dim_companies),
        AssetDep(dim_locations),
        AssetDep(dim_product_types)
    ],
    metadata={
        "fact_type": "products",
        "grain": "ttb_id",
        "format": "parquet"
    }
)
def fact_products(
    context: AssetExecutionContext,
    config: FactConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create product fact table from consolidated TTB data.

    This fact table contains one record per TTB product with measures
    and foreign keys to dimension tables.

    Returns:
        Dictionary containing fact table statistics
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    logger.info(f"Creating product facts for date: {partition_date}")

    s3_client = s3_resource.get_client()

    try:
        # Read consolidated data from S3
        consolidated_key = f"3-ttb-consolidated/consolidated/{partition_date_str}/consolidated_data.parquet"

        try:
            file_response = s3_client.get_object(Bucket=config.s3_bucket, Key=consolidated_key)
            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(file_response['Body'].read())
                tmp_file.flush()
                df = pd.read_parquet(tmp_file.name)

            logger.info(f"Loaded {len(df)} consolidated records")

        except Exception as e:
            logger.warning(f"No consolidated data found for {partition_date_str}: {e}")
            # Create empty fact table
            df = pd.DataFrame()

        if not df.empty:
            # Create fact_products from consolidated data
            fact_data = []

            for _, row in df.iterrows():
                # Create dimension foreign keys
                date_id = _create_date_id(row.get('cola_approval_date'))
                company_id = _create_company_id(
                    row.get('applicant_business_name'),
                    row.get('applicant_mailing_address')
                )

                # Calculate measures
                days_to_approval = _calculate_days_to_approval(
                    row.get('cola_filing_date'),
                    row.get('cola_approval_date')
                )
                data_quality_score = _calculate_data_quality_score(row)

                fact_record = {
                    'ttb_id': row.get('ttb_id'),
                    'date_id': date_id,
                    'company_id': company_id,
                    'product_type_id': 1,  # Placeholder for now
                    'location_id': 1,      # Placeholder for now

                    # Measures
                    'alcohol_content': row.get('cola_alcohol_content'),
                    'net_contents_ml': row.get('cola_net_contents'),
                    'days_to_approval': days_to_approval,
                    'data_quality_score': data_quality_score,

                    # Product attributes
                    'brand_name': row.get('cola_brand_name'),
                    'product_description': row.get('cola_product_description'),
                    'approval_status': row.get('cola_approval_status', 'Unknown'),

                    # Metadata
                    'created_date': datetime.now().date(),
                    'partition_date': partition_date
                }

                fact_data.append(fact_record)

            fact_df = pd.DataFrame(fact_data)

            # Save to S3
            output_key = f"{config.output_prefix}/facts/fact_products/partition_date={partition_date_str}/fact_products.parquet"

            table = pa.Table.from_pandas(fact_df)
            with tempfile.NamedTemporaryFile() as tmp_file:
                pq.write_table(table, tmp_file.name)
                tmp_file.seek(0)

                s3_client.put_object(
                    Bucket=config.s3_bucket,
                    Key=output_key,
                    Body=tmp_file.read(),
                    ContentType='application/octet-stream'
                )

            logger.info(f"Created {len(fact_df)} product facts saved to {output_key}")

            # Calculate statistics
            avg_data_quality = fact_df['data_quality_score'].mean()
            avg_days_to_approval = fact_df['days_to_approval'].mean() if fact_df['days_to_approval'].notna().any() else 0

            # Add metadata
            context.add_output_metadata({
                "partition_date": MetadataValue.text(partition_date_str),
                "total_products": MetadataValue.int(len(fact_df)),
                "avg_data_quality_score": MetadataValue.float(avg_data_quality),
                "avg_days_to_approval": MetadataValue.float(avg_days_to_approval),
                "unique_companies": MetadataValue.int(fact_df['company_id'].nunique()),
                "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
            })

            return {
                "partition_date": partition_date_str,
                "total_products": len(fact_df),
                "avg_data_quality_score": avg_data_quality,
                "avg_days_to_approval": avg_days_to_approval
            }

        else:
            # No data case
            context.add_output_metadata({
                "partition_date": MetadataValue.text(partition_date_str),
                "total_products": MetadataValue.int(0),
                "status": MetadataValue.text("No consolidated data available")
            })

            return {
                "partition_date": partition_date_str,
                "total_products": 0,
                "status": "No data"
            }

    except Exception as e:
        logger.error(f"Error creating product facts: {e}")
        raise


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_facts",
    description="Certificate fact table with regulatory measures",
    deps=[
        AssetDep(ttb_consolidated_data),
        AssetDep(dim_dates),
        AssetDep(dim_companies)
    ],
    metadata={
        "fact_type": "certificates",
        "grain": "certificate_id",
        "format": "parquet"
    }
)
def fact_certificates(
    context: AssetExecutionContext,
    config: FactConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Create certificate fact table from consolidated TTB data.

    This fact table contains certificate-specific measures and
    regulatory information.

    Returns:
        Dictionary containing certificate fact statistics
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    logger.info(f"Creating certificate facts for date: {partition_date}")

    s3_client = s3_resource.get_client()

    try:
        # Read consolidated data from S3 (filtered for certificate data)
        consolidated_key = f"3-ttb-consolidated/consolidated/{partition_date_str}/consolidated_data.parquet"

        try:
            file_response = s3_client.get_object(Bucket=config.s3_bucket, Key=consolidated_key)
            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(file_response['Body'].read())
                tmp_file.flush()
                df = pd.read_parquet(tmp_file.name)

            # Filter for certificate records (could have cert-specific fields)
            cert_df = df[df.get('data_type', '') == 'certificate'] if 'data_type' in df.columns else df

            logger.info(f"Loaded {len(cert_df)} certificate records")

        except Exception as e:
            logger.warning(f"No certificate data found for {partition_date_str}: {e}")
            cert_df = pd.DataFrame()

        if not cert_df.empty:
            # Create fact_certificates from certificate data
            fact_data = []

            for _, row in cert_df.iterrows():
                # Create dimension foreign keys
                date_id = _create_date_id(row.get('cert_approval_date'))
                company_id = _create_company_id(
                    row.get('applicant_business_name'),
                    row.get('applicant_mailing_address')
                )

                fact_record = {
                    'certificate_id': row.get('ttb_id'),  # Use TTB ID as certificate ID
                    'date_id': date_id,
                    'company_id': company_id,

                    # Certificate-specific measures
                    'plant_registry_number': row.get('cert_plant_registry_number'),
                    'certificate_type': row.get('cert_type', 'Unknown'),
                    'approval_status': row.get('cert_approval_status', 'Unknown'),

                    # Metadata
                    'created_date': datetime.now().date(),
                    'partition_date': partition_date
                }

                fact_data.append(fact_record)

            fact_df = pd.DataFrame(fact_data)

            # Save to S3
            output_key = f"{config.output_prefix}/facts/fact_certificates/partition_date={partition_date_str}/fact_certificates.parquet"

            table = pa.Table.from_pandas(fact_df)
            with tempfile.NamedTemporaryFile() as tmp_file:
                pq.write_table(table, tmp_file.name)
                tmp_file.seek(0)

                s3_client.put_object(
                    Bucket=config.s3_bucket,
                    Key=output_key,
                    Body=tmp_file.read(),
                    ContentType='application/octet-stream'
                )

            logger.info(f"Created {len(fact_df)} certificate facts saved to {output_key}")

            # Add metadata
            context.add_output_metadata({
                "partition_date": MetadataValue.text(partition_date_str),
                "total_certificates": MetadataValue.int(len(fact_df)),
                "unique_companies": MetadataValue.int(fact_df['company_id'].nunique()),
                "output_location": MetadataValue.text(f"s3://{config.s3_bucket}/{output_key}")
            })

            return {
                "partition_date": partition_date_str,
                "total_certificates": len(fact_df)
            }

        else:
            # No data case
            context.add_output_metadata({
                "partition_date": MetadataValue.text(partition_date_str),
                "total_certificates": MetadataValue.int(0),
                "status": MetadataValue.text("No certificate data available")
            })

            return {
                "partition_date": partition_date_str,
                "total_certificates": 0,
                "status": "No data"
            }

    except Exception as e:
        logger.error(f"Error creating certificate facts: {e}")
        raise