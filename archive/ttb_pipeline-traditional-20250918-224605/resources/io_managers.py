"""
IO Managers for TTB pipeline data storage.
"""
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagster import IOManager, OutputContext, InputContext
import json
import pickle
from typing import Any


class S3HTMLIOManager(IOManager):
    """IO Manager for storing raw HTML files in S3."""

    def __init__(self, s3_resource: S3Resource, bucket: str, prefix: str):
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store HTML data to S3."""
        s3_client = self.s3_resource.get_client()
        partition_key = context.partition_key

        if hasattr(partition_key, 'keys_by_dimension'):
            # Multi-dimensional partition
            date_str = partition_key.keys_by_dimension["date"]
            method_str = partition_key.keys_by_dimension["method"]
            key_path = f"{self.prefix}/{date_str}/{method_str}/{context.asset_key.path[-1]}.pkl"
        else:
            # Single partition
            key_path = f"{self.prefix}/{partition_key}/{context.asset_key.path[-1]}.pkl"

        # Serialize the HTML data
        serialized_data = pickle.dumps(obj)

        s3_client.put_object(
            Bucket=self.bucket,
            Key=key_path,
            Body=serialized_data,
            ContentType='application/octet-stream'
        )

        context.log.info(f"Stored HTML data to s3://{self.bucket}/{key_path}")

    def load_input(self, context: InputContext) -> Any:
        """Load HTML data from S3."""
        s3_client = self.s3_resource.get_client()
        partition_key = context.upstream_output.partition_key

        if hasattr(partition_key, 'keys_by_dimension'):
            # Multi-dimensional partition
            date_str = partition_key.keys_by_dimension["date"]
            method_str = partition_key.keys_by_dimension["method"]
            key_path = f"{self.prefix}/{date_str}/{method_str}/{context.upstream_output.asset_key.path[-1]}.pkl"
        else:
            # Single partition
            key_path = f"{self.prefix}/{partition_key}/{context.upstream_output.asset_key.path[-1]}.pkl"

        response = s3_client.get_object(Bucket=self.bucket, Key=key_path)
        return pickle.loads(response['Body'].read())


class S3ParquetIOManager(IOManager):
    """IO Manager for storing processed data as Parquet in S3."""

    def __init__(self, s3_resource: S3Resource, bucket: str, prefix: str):
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store processed data to S3 as Parquet."""
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO

        s3_client = self.s3_resource.get_client()
        partition_key = context.partition_key

        if hasattr(partition_key, 'keys_by_dimension'):
            # Multi-dimensional partition
            date_str = partition_key.keys_by_dimension["date"]
            method_str = partition_key.keys_by_dimension["method"]
            key_path = f"{self.prefix}/{date_str}/{method_str}/{context.asset_key.path[-1]}.parquet"
        else:
            # Single partition
            key_path = f"{self.prefix}/{partition_key}/{context.asset_key.path[-1]}.parquet"

        # Convert to DataFrame and save as Parquet
        if isinstance(obj, list):
            df = pd.DataFrame(obj)
        elif isinstance(obj, dict):
            df = pd.DataFrame([obj])
        else:
            df = obj

        # Write to bytes buffer
        buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        buffer.seek(0)

        s3_client.put_object(
            Bucket=self.bucket,
            Key=key_path,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        context.log.info(f"Stored processed data to s3://{self.bucket}/{key_path}")

    def load_input(self, context: InputContext) -> Any:
        """Load processed data from S3."""
        import pandas as pd
        from io import BytesIO

        s3_client = self.s3_resource.get_client()
        partition_key = context.upstream_output.partition_key

        if hasattr(partition_key, 'keys_by_dimension'):
            # Multi-dimensional partition
            date_str = partition_key.keys_by_dimension["date"]
            method_str = partition_key.keys_by_dimension["method"]
            key_path = f"{self.prefix}/{date_str}/{method_str}/{context.upstream_output.asset_key.path[-1]}.parquet"
        else:
            # Single partition
            key_path = f"{self.prefix}/{partition_key}/{context.upstream_output.asset_key.path[-1]}.parquet"

        response = s3_client.get_object(Bucket=self.bucket, Key=key_path)
        return pd.read_parquet(BytesIO(response['Body'].read()))