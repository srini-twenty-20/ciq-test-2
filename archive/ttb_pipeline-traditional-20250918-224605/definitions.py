"""
TTB Pipeline Definitions

Main entry point for the TTB COLA pipeline, defining all assets, resources,
and configuration for the Dagster deployment.
"""
from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource

from .assets.cola_extraction import ttb_cola_extraction
from .config.settings import TTBExtractionConfig
from .resources.io_managers import S3HTMLIOManager, S3ParquetIOManager


def create_ttb_definitions() -> Definitions:
    """Create the complete TTB pipeline definitions."""

    # S3 Resource
    s3_resource = S3Resource(
        region_name=EnvVar("AWS_REGION").get_value("us-east-1"),
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY")
    )

    # Configuration
    extraction_config = TTBExtractionConfig.from_env()

    # IO Managers
    s3_html_io_manager = S3HTMLIOManager(
        s3_resource=s3_resource,
        bucket=extraction_config.s3_bucket,
        prefix=extraction_config.raw_html_prefix
    )

    s3_parquet_io_manager = S3ParquetIOManager(
        s3_resource=s3_resource,
        bucket=extraction_config.s3_bucket,
        prefix=extraction_config.processed_data_prefix
    )

    return Definitions(
        assets=[
            ttb_cola_extraction
        ],
        resources={
            "s3_resource": s3_resource,
            "config": extraction_config,
            "s3_html_io_manager": s3_html_io_manager,
            "s3_parquet_io_manager": s3_parquet_io_manager
        }
    )


# Create the definitions for this module
defs = create_ttb_definitions()