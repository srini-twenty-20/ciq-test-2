"""
IO Manager Definitions

Standardized IO managers for the TTB pipeline following Dagster best practices.
"""
import tempfile
from typing import Any, Dict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

from dagster import (
    IOManager,
    io_manager,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    EnvVar
)
from dagster_aws.s3 import S3PickleIOManager


class TTBS3IOManager(ConfigurableIOManager):
    """
    Custom S3 IO Manager for TTB pipeline data.

    Handles different data formats (JSON, Parquet, CSV) based on asset metadata
    and provides standardized S3 key patterns compatible with legacy structure.
    """

    bucket_name: str = "ciq-dagster"
    region_name: str = "us-east-1"

    # Legacy-compatible S3 prefixes (configurable via environment)
    raw_data_prefix: str = "1-ttb-raw-data"
    processed_data_prefix: str = "2-ttb-processed-data"
    dimensional_data_prefix: str = "3-ttb-dimensional-data"

    def _get_s3_client(self):
        """Get configured S3 client."""
        import boto3
        return boto3.client('s3', region_name=self.region_name)

    def _get_s3_key(self, context: OutputContext) -> str:
        """
        Generate S3 key based on asset metadata and partition using legacy-compatible paths.

        Uses asset group name to determine the appropriate S3 path prefix.
        """
        asset_key = context.asset_key
        asset_metadata = context.metadata or {}

        # Simple asset name-based mapping (reliable and straightforward)
        asset_name = asset_key.path[-1]

        # Determine S3 prefix based on asset type
        if asset_name in ['ttb_raw_data']:
            s3_prefix = self.raw_data_prefix
        elif asset_name in ['ttb_extracted_data', 'ttb_cleaned_data', 'ttb_structured_data', 'ttb_consolidated_data']:
            s3_prefix = self.processed_data_prefix
        elif asset_name in ['dim_dates', 'dim_companies', 'dim_locations', 'dim_product_types', 'fact_products', 'fact_certificates', 'ttb_reference_data']:
            s3_prefix = self.dimensional_data_prefix
        else:
            s3_prefix = self.processed_data_prefix  # Default fallback

        format_type = asset_metadata.get("format", "parquet")

        # Build S3 key - skip IO manager output for raw data assets since they handle their own S3 storage
        if asset_name == 'ttb_raw_data':
            # Raw data assets handle their own S3 storage, so skip IO manager processing
            return None

        # Build S3 key for other assets
        if hasattr(context, 'partition_key') and context.partition_key:
            if context.has_partition_key:
                # Handle daily partitioned assets
                return f"{s3_prefix}/partition_date={context.partition_key}/{asset_name}.{format_type}"
            else:
                return f"{s3_prefix}/{asset_name}.{format_type}"
        else:
            # Non-partitioned assets
            return f"{s3_prefix}/{asset_name}.{format_type}"

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output to S3 based on data type."""
        s3_client = self._get_s3_client()
        s3_key = self._get_s3_key(context)

        # Skip IO manager processing if s3_key is None (raw assets handle their own storage)
        if s3_key is None:
            context.log.info("Skipping IO manager output - asset handles its own S3 storage")
            return

        format_type = context.metadata.get("format", "json")

        context.log.info(f"Writing {format_type} data to s3://{self.bucket_name}/{s3_key}")

        if format_type == "parquet":
            # Handle DataFrame or dict to Parquet
            if isinstance(obj, pd.DataFrame):
                df = obj
            elif isinstance(obj, dict) and "data" in obj:
                df = pd.DataFrame(obj["data"])
            else:
                # Convert dict to single-row DataFrame
                df = pd.DataFrame([obj] if isinstance(obj, dict) else obj)

            table = pa.Table.from_pandas(df)
            with tempfile.NamedTemporaryFile() as tmp_file:
                pq.write_table(table, tmp_file.name)
                tmp_file.seek(0)

                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=tmp_file.read(),
                    ContentType='application/octet-stream'
                )

        elif format_type == "json":
            # Handle JSON output
            json_content = json.dumps(obj, indent=2, default=str)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )

        elif format_type == "csv":
            # Handle CSV output
            if isinstance(obj, pd.DataFrame):
                csv_content = obj.to_csv(index=False)
            else:
                df = pd.DataFrame([obj] if isinstance(obj, dict) else obj)
                csv_content = df.to_csv(index=False)

            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )

        else:
            # Default to JSON for unknown formats
            json_content = json.dumps(obj, indent=2, default=str)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )

        # Add S3 location metadata
        context.add_output_metadata({
            "s3_location": f"s3://{self.bucket_name}/{s3_key}",
            "s3_key": s3_key,
            "format": format_type
        })

    def load_input(self, context: InputContext) -> Any:
        """Load input from S3 by reconstructing the S3 path from context."""
        s3_client = self._get_s3_client()

        # Reconstruct S3 key using same logic as _get_s3_key but for InputContext
        asset_key = context.asset_key
        asset_name = asset_key.path[-1]

        # Determine S3 prefix based on asset type
        if asset_name in ['ttb_raw_data']:
            s3_prefix = self.raw_data_prefix
            format_type = "html"  # Raw data is HTML
        elif asset_name in ['ttb_extracted_data', 'ttb_cleaned_data', 'ttb_structured_data', 'ttb_consolidated_data']:
            s3_prefix = self.processed_data_prefix
            format_type = "parquet"  # Processed data is parquet
        elif asset_name in ['dim_dates', 'dim_companies', 'dim_locations', 'dim_product_types', 'fact_products', 'fact_certificates', 'ttb_reference_data']:
            s3_prefix = self.dimensional_data_prefix
            format_type = "parquet"  # Dimensional data is parquet
        else:
            s3_prefix = self.processed_data_prefix  # Default fallback
            format_type = "parquet"

        # Build S3 key for daily partitioned assets only
        if hasattr(context, 'asset_partition_key') and context.asset_partition_key:
            # Daily partition
            s3_key = f"{s3_prefix}/partition_date={context.asset_partition_key}/{asset_name}.{format_type}"
        else:
            # Non-partitioned assets
            s3_key = f"{s3_prefix}/{asset_name}.{format_type}"

        context.log.info(f"Loading {format_type} data from s3://{self.bucket_name}/{s3_key}")

        try:
            file_response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)

            if format_type == "parquet":
                with tempfile.NamedTemporaryFile() as tmp_file:
                    tmp_file.write(file_response['Body'].read())
                    tmp_file.flush()
                    return pd.read_parquet(tmp_file.name)

            elif format_type == "json":
                content = file_response['Body'].read().decode('utf-8')
                return json.loads(content)

            elif format_type == "csv":
                content = file_response['Body'].read().decode('utf-8')
                return pd.read_csv(content)

            else:
                # Default to string content for HTML files
                return file_response['Body'].read().decode('utf-8')

        except Exception as e:
            context.log.error(f"Failed to load data from S3 key {s3_key}: {str(e)}")
            raise ValueError(f"Could not load input {context.asset_key} from s3://{self.bucket_name}/{s3_key}: {str(e)}")


@io_manager(
    config_schema={
        "bucket_name": str,
        "region_name": str
    }
)
def ttb_s3_io_manager(context) -> TTBS3IOManager:
    """
    TTB S3 IO Manager factory.

    Provides standardized S3 storage for TTB pipeline assets.
    """
    return TTBS3IOManager(
        bucket_name=context.resource_config["bucket_name"],
        region_name=context.resource_config["region_name"]
    )


@io_manager(
    config_schema={
        "bucket_name": str,
        "region_name": str
    }
)
def ttb_parquet_io_manager(context) -> TTBS3IOManager:
    """
    TTB Parquet-specific IO Manager factory.

    Optimized for Parquet format with automatic partitioning support.
    """
    return TTBS3IOManager(
        bucket_name=context.resource_config["bucket_name"],
        region_name=context.resource_config["region_name"]
    )