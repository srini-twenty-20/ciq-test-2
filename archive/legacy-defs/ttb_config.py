"""
Centralized configuration for TTB pipeline.

This module provides a unified configuration system that supports:
- Environment variable overrides
- YAML configuration files
- Deployment-specific settings
- Default values for development
"""
import os
from typing import Optional
from dagster import Config
from pydantic import Field


class TTBGlobalConfig(Config):
    """Global TTB pipeline configuration with environment variable support."""

    # S3 Configuration
    s3_bucket: str = Field(
        default_factory=lambda: os.getenv("TTB_S3_BUCKET", "ciq-dagster"),
        description="S3 bucket for TTB data storage"
    )

    # S3 Prefixes for different stages
    raw_data_prefix: str = Field(
        default_factory=lambda: os.getenv("TTB_RAW_PREFIX", "1-ttb-raw-data"),
        description="S3 prefix for raw TTB data"
    )

    processed_data_prefix: str = Field(
        default_factory=lambda: os.getenv("TTB_PROCESSED_PREFIX", "2-ttb-processed-data"),
        description="S3 prefix for processed TTB data"
    )

    dimensional_data_prefix: str = Field(
        default_factory=lambda: os.getenv("TTB_DIMENSIONAL_PREFIX", "3-ttb-dimensional-data"),
        description="S3 prefix for dimensional model data"
    )

    # Processing Configuration
    max_sequence_per_batch: int = Field(
        default_factory=lambda: int(os.getenv("TTB_MAX_SEQUENCE", "100")),
        description="Maximum sequence numbers to process per batch"
    )

    receipt_methods: list[int] = Field(
        default=[1, 2, 3, 0],
        description="TTB receipt methods to process (1=e-filed, 2=mailed, 3=overnight, 0=hand-delivered)"
    )

    # Rate Limiting
    request_delay_seconds: float = Field(
        default_factory=lambda: float(os.getenv("TTB_REQUEST_DELAY", "0.5")),
        description="Delay between TTB requests in seconds"
    )

    # Data Quality
    enable_data_quality_checks: bool = Field(
        default_factory=lambda: os.getenv("TTB_ENABLE_QUALITY_CHECKS", "true").lower() == "true",
        description="Enable data quality validation"
    )

    # AWS Region
    aws_region: str = Field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"),
        description="AWS region for S3 operations"
    )


class TTBExtractionConfig(TTBGlobalConfig):
    """Configuration specific to TTB data extraction assets."""

    # Override max sequences for extraction
    max_sequence_per_batch: int = Field(
        default_factory=lambda: int(os.getenv("TTB_EXTRACTION_MAX_SEQUENCE", "100")),
        description="Maximum sequence numbers for extraction phase"
    )

    # Extraction specific settings
    ssl_verify: bool = Field(
        default_factory=lambda: os.getenv("TTB_SSL_VERIFY", "false").lower() == "true",
        description="Verify SSL certificates for TTB requests"
    )

    max_retries: int = Field(
        default_factory=lambda: int(os.getenv("TTB_MAX_RETRIES", "3")),
        description="Maximum retries for failed TTB requests"
    )

    consecutive_failure_threshold: int = Field(
        default_factory=lambda: int(os.getenv("TTB_CONSECUTIVE_FAILURE_THRESHOLD", "10")),
        description="Stop extraction after this many consecutive failures (error pages, HTTP errors, etc.)"
    )


class TTBProcessingConfig(TTBGlobalConfig):
    """Configuration specific to TTB data processing assets."""

    # Processing specific settings
    enable_deduplication: bool = Field(
        default_factory=lambda: os.getenv("TTB_ENABLE_DEDUP", "true").lower() == "true",
        description="Enable record deduplication"
    )

    create_partitioned_output: bool = Field(
        default_factory=lambda: os.getenv("TTB_CREATE_PARTITIONED", "true").lower() == "true",
        description="Create partitioned output files"
    )


class TTBDimensionalConfig(TTBGlobalConfig):
    """Configuration specific to TTB dimensional modeling assets."""

    # Dimensional modeling settings
    date_dimension_start_year: int = Field(
        default_factory=lambda: int(os.getenv("TTB_DATE_START_YEAR", "2015")),
        description="Start year for date dimension"
    )

    date_dimension_end_year: int = Field(
        default_factory=lambda: int(os.getenv("TTB_DATE_END_YEAR", "2030")),
        description="End year for date dimension"
    )

    enable_surrogate_keys: bool = Field(
        default_factory=lambda: os.getenv("TTB_SURROGATE_KEYS", "true").lower() == "true",
        description="Use surrogate keys in dimensional model"
    )


def get_ttb_config() -> TTBGlobalConfig:
    """
    Get TTB configuration with environment variable overrides.

    This function provides a central way to access TTB configuration
    that respects environment variables and deployment settings.
    """
    return TTBGlobalConfig()


def get_extraction_config() -> TTBExtractionConfig:
    """Get configuration for TTB extraction assets."""
    return TTBExtractionConfig()


def get_processing_config() -> TTBProcessingConfig:
    """Get configuration for TTB processing assets."""
    return TTBProcessingConfig()


def get_dimensional_config() -> TTBDimensionalConfig:
    """Get configuration for TTB dimensional modeling assets."""
    return TTBDimensionalConfig()


# Deployment-specific configuration helpers
class DeploymentConfig:
    """Helper class for deployment-specific configuration."""

    @staticmethod
    def is_production() -> bool:
        """Check if running in production environment."""
        return os.getenv("DAGSTER_DEPLOYMENT", "dev").lower() == "prod"

    @staticmethod
    def is_staging() -> bool:
        """Check if running in staging environment."""
        return os.getenv("DAGSTER_DEPLOYMENT", "dev").lower() == "staging"

    @staticmethod
    def is_development() -> bool:
        """Check if running in development environment."""
        return os.getenv("DAGSTER_DEPLOYMENT", "dev").lower() == "dev"

    @staticmethod
    def get_environment() -> str:
        """Get current deployment environment."""
        return os.getenv("DAGSTER_DEPLOYMENT", "dev").lower()