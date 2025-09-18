"""
Environment-specific Configuration

Provides pre-configured settings for different deployment environments.
"""
from dagster import RunConfig
from typing import Dict, Any

from .ttb_config import (
    TTBPipelineConfig,
    TTBExtractionConfig,
    TTBProcessingConfig,
    TTBConsolidationConfig,
    TTBAnalyticsConfig,
    TTBPartitionConfig
)


def get_development_config() -> Dict[str, Any]:
    """
    Get configuration for development environment.

    Optimized for:
    - Fast iteration
    - Minimal data processing
    - Local testing
    - Single partition processing
    """
    return {
        "resources": {
            "s3_resource": {
                "config": {
                    "region_name": "us-east-1"
                }
            },
            "ttb_storage": {
                "config": {
                    "bucket_name": "ciq-dagster",
                    "region_name": "us-east-1"
                }
            }
        },
        "ops": {
            "ttb_raw_data": {
                "config": TTBExtractionConfig(
                    max_sequence_per_batch=100,  # Small batch for testing
                    consecutive_failure_threshold=5,
                    request_delay_seconds=0.1,   # Faster for development
                    ssl_verify=False
                )
            },
            "ttb_extracted_data": {
                "config": TTBProcessingConfig(
                    processing_batch_size=50,
                    enable_parsing_validation=True,
                    data_quality_analysis=False,  # Skip for speed
                    output_format="json"          # Easier to inspect
                )
            },
            "ttb_cleaned_data": {
                "config": TTBProcessingConfig(
                    enable_deduplication=False,   # Skip for speed
                    data_quality_analysis=False,
                    strict_validation=False
                )
            },
            "ttb_consolidated_data": {
                "config": TTBConsolidationConfig(
                    data_quality_analysis=False,
                    deduplication_enabled=False
                )
            }
        }
    }


def get_test_config() -> Dict[str, Any]:
    """
    Get configuration for testing environment.

    Optimized for:
    - Reliable testing
    - Minimal external dependencies
    - Fast execution
    - Comprehensive validation
    """
    return {
        "resources": {
            "s3_resource": {
                "config": {
                    "region_name": "us-east-1"
                }
            }
        },
        "ops": {
            "ttb_raw_data": {
                "config": TTBExtractionConfig(
                    max_sequence_per_batch=10,   # Very small for tests
                    consecutive_failure_threshold=3,
                    request_delay_seconds=0.05,  # Minimal delay
                    ssl_verify=False,
                    save_error_responses=True
                )
            },
            "ttb_extracted_data": {
                "config": TTBProcessingConfig(
                    processing_batch_size=10,
                    enable_parsing_validation=True,
                    validate_schema=True,
                    strict_validation=True,       # Strict for testing
                    output_format="json"
                )
            },
            "ttb_cleaned_data": {
                "config": TTBProcessingConfig(
                    enable_deduplication=True,
                    data_quality_analysis=True,
                    validate_schema=True,
                    max_validation_errors=5      # Low tolerance for tests
                )
            }
        }
    }


def get_production_config() -> Dict[str, Any]:
    """
    Get configuration for production environment.

    Optimized for:
    - Data quality and reliability
    - Performance and scalability
    - Comprehensive monitoring
    - Full feature set
    """
    return {
        "resources": {
            "s3_resource": {
                "config": {
                    "region_name": "us-east-1"
                }
            },
            "ttb_storage": {
                "config": {
                    "bucket_name": "ciq-dagster",
                    "region_name": "us-east-1"
                }
            }
        },
        "ops": {
            "ttb_raw_data": {
                "config": TTBExtractionConfig(
                    max_sequence_per_batch=10000,  # Large batches for efficiency
                    consecutive_failure_threshold=15,
                    request_delay_seconds=0.5,     # Respectful rate limiting
                    ssl_verify=True,               # Security in production
                    max_retries=5,
                    save_error_responses=True
                )
            },
            "ttb_extracted_data": {
                "config": TTBProcessingConfig(
                    processing_batch_size=500,
                    enable_parsing_validation=True,
                    data_quality_analysis=True,
                    create_partitioned_output=True,
                    output_format="parquet",       # Efficient storage
                    compression="snappy"
                )
            },
            "ttb_cleaned_data": {
                "config": TTBProcessingConfig(
                    enable_deduplication=True,
                    data_quality_analysis=True,
                    validate_schema=True,
                    strict_validation=False,       # More lenient in production
                    max_validation_errors=1000
                )
            },
            "ttb_structured_data": {
                "config": TTBProcessingConfig(
                    create_partitioned_output=True,
                    validate_schema=True,
                    data_quality_analysis=True
                )
            },
            "ttb_consolidated_data": {
                "config": TTBConsolidationConfig(
                    create_partitioned_output=True,
                    data_quality_analysis=True,
                    deduplication_enabled=True,
                    merge_strategy="latest"
                )
            },
            "dim_dates": {
                "config": TTBAnalyticsConfig(
                    date_dimension_start_year=2015,
                    date_dimension_end_year=2030
                )
            },
            "fact_products": {
                "config": TTBAnalyticsConfig(
                    create_surrogate_keys=True,
                    enable_aggregations=True,
                    create_summary_tables=True
                )
            },
            "fact_certificates": {
                "config": TTBAnalyticsConfig(
                    create_surrogate_keys=True,
                    enable_aggregations=True
                )
            }
        }
    }


def get_config_for_environment(environment: str) -> Dict[str, Any]:
    """
    Get configuration for the specified environment.

    Args:
        environment: Environment name (development, test, production)

    Returns:
        Dictionary containing environment-specific configuration

    Raises:
        ValueError: If environment is not recognized
    """
    config_map = {
        "development": get_development_config,
        "dev": get_development_config,
        "test": get_test_config,
        "testing": get_test_config,
        "production": get_production_config,
        "prod": get_production_config
    }

    if environment.lower() not in config_map:
        raise ValueError(
            f"Unknown environment: {environment}. "
            f"Valid options: {list(config_map.keys())}"
        )

    return config_map[environment.lower()]()