"""
TTB Pipeline Configuration

Centralized configuration management for all TTB pipeline components
using Dagster's Config system and environment variables.
"""
from dagster import Config, EnvVar
from typing import Optional, List


class TTBPipelineConfig(Config):
    """
    Global TTB pipeline configuration.

    This is the master configuration that contains all pipeline-wide settings.
    Other config classes inherit from or reference this configuration.
    """
    # Environment settings
    environment: str = "development"
    debug_mode: bool = False

    # S3 Storage settings
    s3_bucket: str = "ciq-dagster"
    s3_region: str = "us-east-1"

    # Data storage prefixes
    raw_data_prefix: str = "1-ttb-raw-data"
    processed_data_prefix: str = "2-ttb-processed-data"
    consolidated_data_prefix: str = "3-ttb-consolidated"
    analytics_data_prefix: str = "4-ttb-analytics"

    # Pipeline behavior
    enable_data_quality_checks: bool = True
    enable_notifications: bool = False
    max_concurrent_assets: int = 10


class TTBExtractionConfig(Config):
    """
    Configuration for TTB data extraction assets.

    Controls raw data extraction behavior, rate limiting, and error handling.
    """
    # S3 settings
    s3_bucket: str = "ciq-dagster"
    raw_data_prefix: str = "1-ttb-raw-data"

    # Extraction behavior
    max_sequence_per_batch: int = 1000
    consecutive_failure_threshold: int = 10
    request_delay_seconds: float = 0.5
    max_retries: int = 3
    ssl_verify: bool = False

    # Error handling
    stop_on_consecutive_failures: bool = True
    save_error_responses: bool = True

    @classmethod
    def from_env(cls) -> "TTBExtractionConfig":
        """Create config from environment variables."""
        return cls(
            s3_bucket=EnvVar("TTB_S3_BUCKET").get_value("ciq-dagster"),
            max_sequence_per_batch=int(EnvVar("TTB_MAX_SEQUENCE_PER_BATCH").get_value("1000")),
            consecutive_failure_threshold=int(EnvVar("TTB_CONSECUTIVE_FAILURE_THRESHOLD").get_value("10")),
            request_delay_seconds=float(EnvVar("TTB_REQUEST_DELAY_SECONDS").get_value("0.5")),
            ssl_verify=EnvVar("TTB_SSL_VERIFY").get_value("false").lower() == "true"
        )


class TTBProcessingConfig(Config):
    """
    Configuration for TTB data processing assets.

    Controls data transformation, cleaning, and validation behavior.
    """
    # S3 settings
    s3_bucket: str = "ciq-dagster"
    s3_input_prefix: str = "1-ttb-raw-data"
    s3_output_prefix: str = "2-ttb-processed-data"

    # Processing behavior
    processing_batch_size: int = 100
    enable_parsing_validation: bool = True
    enable_deduplication: bool = True
    data_quality_analysis: bool = True
    create_partitioned_output: bool = True

    # Data formats
    output_format: str = "parquet"  # parquet, json, csv
    compression: str = "snappy"

    # Validation settings
    validate_schema: bool = True
    strict_validation: bool = False
    max_validation_errors: int = 100


class TTBConsolidationConfig(Config):
    """
    Configuration for TTB data consolidation assets.

    Controls cross-partition data consolidation and deduplication.
    """
    # S3 settings
    bucket_name: str = "ciq-dagster"
    s3_output_prefix: str = "3-ttb-consolidated"

    # Consolidation behavior
    create_partitioned_output: bool = True
    data_quality_analysis: bool = True
    deduplication_enabled: bool = True

    # Data processing
    merge_strategy: str = "latest"  # latest, oldest, merge
    conflict_resolution: str = "prefer_cola_detail"  # prefer_cola_detail, prefer_certificate, merge


class TTBAnalyticsConfig(Config):
    """
    Configuration for TTB analytics and dimensional modeling assets.

    Controls fact and dimension table creation and analytics processing.
    """
    # S3 settings
    s3_bucket: str = "ciq-dagster"
    output_prefix: str = "4-ttb-analytics"

    # Dimensional modeling
    date_dimension_start_year: int = 2015
    date_dimension_end_year: int = 2030
    create_surrogate_keys: bool = True
    enable_slowly_changing_dimensions: bool = False

    # Analytics behavior
    enable_aggregations: bool = True
    create_summary_tables: bool = True
    data_retention_days: int = 2555  # ~7 years


class TTBPartitionConfig(Config):
    """
    Configuration for TTB partition definitions.

    Controls partition behavior for testing vs production environments.
    """
    # Date partitions
    partition_start_date: str = "2024-01-01"
    partition_end_date: str = "2024-01-01"

    # Method/type partitions
    receipt_methods: List[str] = ["001"]  # Default to testing with single method
    data_types: List[str] = ["cola-detail"]  # Default to testing with single type

    @classmethod
    def for_testing(cls) -> "TTBPartitionConfig":
        """Get minimal partition config for testing."""
        return cls(
            partition_start_date=EnvVar("TTB_PARTITION_START_DATE").get_value("2024-01-01"),
            partition_end_date=EnvVar("TTB_PARTITION_END_DATE").get_value("2024-01-01"),
            receipt_methods=EnvVar("TTB_RECEIPT_METHODS").get_value("001").split(","),
            data_types=EnvVar("TTB_DATA_TYPES").get_value("cola-detail").split(",")
        )

    @classmethod
    def for_production(cls) -> "TTBPartitionConfig":
        """Get full partition config for production."""
        return cls(
            partition_start_date=EnvVar("TTB_PARTITION_START_DATE").get_value("2015-01-01"),
            partition_end_date=EnvVar("TTB_PARTITION_END_DATE").get_value("2025-12-31"),
            receipt_methods=EnvVar("TTB_RECEIPT_METHODS").get_value("001,002,003,000").split(","),
            data_types=EnvVar("TTB_DATA_TYPES").get_value("cola-detail,certificate").split(",")
        )