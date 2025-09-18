from dagster_aws.s3 import S3Resource
from dagster import EnvVar
import os


def get_s3_resource() -> S3Resource:
    """
    Configure S3 resource with environment variables.
    Supports both regular AWS and LocalStack configurations.
    """
    # Base configuration
    config = {
        "aws_access_key_id": EnvVar("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY"),
        "region_name": EnvVar("AWS_REGION"),
    }

    # Add endpoint_url if LocalStack is configured
    localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint:
        config["endpoint_url"] = localstack_endpoint

    return S3Resource(**config)
