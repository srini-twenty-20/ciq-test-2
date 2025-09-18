#!/usr/bin/env python3
"""
Example of enhanced partitioning with data type as a partition dimension.
"""

from dagster import (
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    asset,
    Config
)

# Enhanced partition definitions
data_type_partitions = StaticPartitionsDefinition([
    "cola-detail",  # action=publicDisplaySearchAdvanced
    "certificate",  # action=publicFormDisplay
])

receipt_method_partitions = StaticPartitionsDefinition([
    "001",  # e-filed
    "002",  # mailed
    "003",  # overnight
    "000",  # hand delivered
])

daily_partitions = DailyPartitionsDefinition(
    start_date="2015-01-01",
    end_date_offset=1,
)

# Multi-dimensional partitions: Date × Receipt Method × Data Type
ttb_enhanced_partitions = MultiPartitionsDefinition({
    "date": daily_partitions,
    "receipt_method": receipt_method_partitions,
    "data_type": data_type_partitions,
})


class TTBEnhancedConfig(Config):
    """Configuration for enhanced TTB partitioned assets."""
    bucket_name: str = "ciq-dagster"
    max_sequence_per_batch: int = 100


@asset(
    partitions_def=ttb_enhanced_partitions,
    description="TTB data partitioned by date, receipt method, and data type"
)
def ttb_enhanced_partitioned(
    context,
    config: TTBEnhancedConfig,
    s3_resource: S3Resource
):
    """
    Enhanced TTB data extraction with data type as partition dimension.

    Partition key format: date|receipt_method|data_type
    Examples:
    - "2024-01-01|001|cola-detail"
    - "2024-01-01|001|certificate"
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    receipt_method_str = partition_key.keys_by_dimension["receipt_method"]
    data_type = partition_key.keys_by_dimension["data_type"]

    # Parse partition data
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    receipt_method = int(receipt_method_str)

    logger.info(f"Processing TTB {data_type} data for date: {partition_date}, receipt method: {receipt_method}")

    # Configure based on data type
    if data_type == "cola-detail":
        action_param = "publicDisplaySearchAdvanced"
        s3_prefix = "ttb-cola-data"
    elif data_type == "certificate":
        action_param = "publicFormDisplay"
        s3_prefix = "ttb-certificate-data"
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

    # Build URL with appropriate action
    # url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action={action_param}&ttbid={ttb_id}"

    # Build S3 key
    # s3_key = f"{s3_prefix}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/receipt_method={receipt_method:03d}/{ttb_id}.html"

    # ... rest of processing logic remains the same

    return {
        "partition_date": str(partition_date),
        "receipt_method": receipt_method,
        "data_type": data_type,
        # ... other return data
    }


# Example partition keys:
partition_examples = [
    "2024-01-01|001|cola-detail",    # Jan 1, 2024, e-filed, COLA detail
    "2024-01-01|001|certificate",    # Jan 1, 2024, e-filed, certificate
    "2024-01-01|002|cola-detail",    # Jan 1, 2024, mailed, COLA detail
    "2024-01-01|002|certificate",    # Jan 1, 2024, mailed, certificate
]

# Job examples:
# 1. Process all data types for specific date/method
# 2. Process only COLA data for date range
# 3. Process only certificates for specific method
# 4. Full backfill of both data types