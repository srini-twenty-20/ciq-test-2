"""
Partition definitions for TTB COLA pipeline.

Defines configurable partitions for dates and receipt methods that can be
adjusted for testing vs production environments.
"""
from dagster import (
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    EnvVar
)
from datetime import datetime, timedelta


def get_daily_partitions() -> DailyPartitionsDefinition:
    """
    Get configurable daily partitions.

    Environment Variables:
        TTB_START_DATE: Start date (default: 2024-01-01 for testing)
        TTB_END_DATE: End date (default: 2024-01-01 for testing)

    For production:
        TTB_START_DATE=2015-01-01
        TTB_END_DATE=2025-12-31
    """
    start_date = EnvVar("TTB_START_DATE").get_value("2024-01-01")
    end_date = EnvVar("TTB_END_DATE").get_value("2024-01-01")

    # Parse dates and add one day to end_date to make it inclusive
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    inclusive_end_date = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    return DailyPartitionsDefinition(
        start_date=start_date,
        end_date=inclusive_end_date,
    )


def get_receipt_method_partitions() -> StaticPartitionsDefinition:
    """
    Get configurable receipt method partitions.

    Environment Variables:
        TTB_RECEIPT_METHODS: Comma-separated methods (default: "001" for testing)

    For production:
        TTB_RECEIPT_METHODS=001,002,003,000
    """
    methods_str = EnvVar("TTB_RECEIPT_METHODS").get_value("001")
    methods = [method.strip() for method in methods_str.split(",")]
    return StaticPartitionsDefinition(methods)


def get_ttb_partitions() -> MultiPartitionsDefinition:
    """
    Get multi-dimensional TTB partitions combining dates and receipt methods.

    Partition key format: "date|method"
    Example: "2024-01-01|001"
    """
    return MultiPartitionsDefinition({
        "date": get_daily_partitions(),
        "method": get_receipt_method_partitions(),
    })


# Main partition definition used by assets
ttb_partitions = get_ttb_partitions()