"""
Centralized partition definitions for TTB pipeline.

This module provides configurable partition definitions that can be used
across all TTB assets to ensure consistency and enable testing with minimal partitions.
"""
from dagster import (
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    EnvVar
)


def get_configurable_daily_partitions() -> DailyPartitionsDefinition:
    """
    Get configurable daily partitions based on environment variables.

    Environment Variables:
        TTB_PARTITION_START_DATE: Start date (default: 2024-01-01 for testing)
        TTB_PARTITION_END_DATE: End date (default: 2024-01-01 for testing)

    Logic: Both start_date and end_date are always included, along with all days in between.

    For production, set:
        TTB_PARTITION_START_DATE=2015-01-01
        TTB_PARTITION_END_DATE=2025-12-31
    """
    start_date = EnvVar("TTB_PARTITION_START_DATE").get_value("2024-01-01")
    end_date = EnvVar("TTB_PARTITION_END_DATE").get_value("2024-01-01")

    from datetime import datetime, timedelta

    # Parse dates and calculate proper end_date
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Add one day to end_date to make it inclusive
    inclusive_end_date = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    return DailyPartitionsDefinition(
        start_date=start_date,
        end_date=inclusive_end_date,
    )


def get_configurable_method_type_partitions() -> StaticPartitionsDefinition:
    """
    Get configurable receipt method + data type combinations.

    Environment Variables:
        TTB_RECEIPT_METHODS: Comma-separated receipt methods (default: "001" for testing)
        TTB_DATA_TYPES: Comma-separated data types (default: "cola-detail" for testing)

    For production, set:
        TTB_RECEIPT_METHODS=001,002,003,000
        TTB_DATA_TYPES=cola-detail,certificate
    """
    receipt_methods_str = EnvVar("TTB_RECEIPT_METHODS").get_value("001")
    data_types_str = EnvVar("TTB_DATA_TYPES").get_value("cola-detail")

    receipt_methods = receipt_methods_str.split(",")
    data_types = data_types_str.split(",")

    method_type_combinations = []
    for receipt_method in receipt_methods:
        for data_type in data_types:
            method_type_combinations.append(f"{receipt_method.strip()}-{data_type.strip()}")

    return StaticPartitionsDefinition(method_type_combinations)


def get_configurable_ttb_partitions() -> MultiPartitionsDefinition:
    """
    Get configurable multi-dimensional TTB partitions.

    Combines daily partitions with receipt method + data type combinations.
    """
    return MultiPartitionsDefinition({
        "date": get_configurable_daily_partitions(),
        "method_type": get_configurable_method_type_partitions(),
    })


# Create the actual partition definitions that assets will use
daily_partitions = get_configurable_daily_partitions()
method_type_partitions = get_configurable_method_type_partitions()
ttb_partitions = get_configurable_ttb_partitions()


# For backward compatibility and convenience
__all__ = [
    "daily_partitions",
    "method_type_partitions",
    "ttb_partitions",
    "get_configurable_daily_partitions",
    "get_configurable_method_type_partitions",
    "get_configurable_ttb_partitions"
]