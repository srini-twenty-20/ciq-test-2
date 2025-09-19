"""
Configuration settings for TTB pipeline.
"""
from dagster import Config, EnvVar
from typing import List


class TTBExtractionConfig(Config):
    """Configuration for TTB COLA data extraction."""

    # S3 settings
    s3_bucket: str = "ciq-dagster"
    raw_html_prefix: str = "ttb-raw-html"
    processed_data_prefix: str = "ttb-processed-data"

    # Extraction behavior
    max_sequence_per_batch: int = 1000
    consecutive_failure_threshold: int = 10
    request_delay_seconds: float = 0.5
    max_retries: int = 3
    ssl_verify: bool = False

    # TTB endpoints
    base_url: str = "https://ttbonline.gov/colasonline/viewColaDetails.do"
    detail_action: str = "publicDisplaySearchAdvanced"
    certificate_action: str = "publicFormDisplay"

    @classmethod
    def from_env(cls) -> "TTBExtractionConfig":
        """Create config from environment variables."""
        return cls(
            s3_bucket=EnvVar("TTB_S3_BUCKET").get_value("ciq-dagster"),
            max_sequence_per_batch=int(EnvVar("TTB_MAX_SEQUENCE_PER_BATCH").get_value("1000")),
            consecutive_failure_threshold=int(EnvVar("TTB_CONSECUTIVE_FAILURE_THRESHOLD").get_value("10")),
            request_delay_seconds=float(EnvVar("TTB_REQUEST_DELAY_SECONDS").get_value("0.5")),
        )