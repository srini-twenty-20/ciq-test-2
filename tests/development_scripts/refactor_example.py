#!/usr/bin/env python3
"""
Example of how the TTB asset could be refactored to be configurable.
This shows a more elegant approach using configuration instead of duplicate assets.
"""

from dagster import Config, asset
from typing import Literal

class TTBFlexibleConfig(Config):
    """Flexible configuration for TTB data extraction."""
    bucket_name: str = "ciq-dagster"
    max_sequence_per_batch: int = 100
    receipt_methods: list[int] = [1, 2, 3, 0]

    # URL configuration
    action_type: Literal["cola", "certificate"] = "cola"
    custom_action: str = None  # Override for custom action parameters
    s3_prefix: str = None      # Override for S3 path prefix

    def get_action_param(self) -> str:
        """Get the action parameter for the URL."""
        if self.custom_action:
            return self.custom_action

        if self.action_type == "cola":
            return "publicDisplaySearchAdvanced"
        elif self.action_type == "certificate":
            return "publicFormDisplay"
        else:
            raise ValueError(f"Unknown action_type: {self.action_type}")

    def get_s3_prefix(self) -> str:
        """Get the S3 prefix for storage."""
        if self.s3_prefix:
            return self.s3_prefix

        if self.action_type == "cola":
            return "ttb-cola-data"
        elif self.action_type == "certificate":
            return "ttb-certificate-data"
        else:
            raise ValueError(f"Unknown action_type: {self.action_type}")


@asset(
    partitions_def=ttb_partitions,
    description="Flexible TTB data partitioned by date and receipt method - configurable for COLA or certificate data"
)
def ttb_flexible_partitioned(
    context,
    config: TTBFlexibleConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Flexible TTB data extraction asset.

    Can be configured to extract either:
    - COLA data (action=publicDisplaySearchAdvanced)
    - Certificate data (action=publicFormDisplay)
    - Custom action types via configuration
    """
    logger = get_dagster_logger()

    # Get configuration
    action_param = config.get_action_param()
    s3_prefix = config.get_s3_prefix()

    # Get partition information
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    receipt_method_str = partition_key.keys_by_dimension["receipt_method"]

    # Parse partition data
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    receipt_method = int(receipt_method_str)

    logger.info(f"Processing TTB {config.action_type} data for date: {partition_date}, receipt method: {receipt_method}")

    # ... same processing logic as before, but using:
    # url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action={action_param}&ttbid={ttb_id}"
    # s3_key = f"{s3_prefix}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/receipt_method={receipt_method:03d}/{ttb_id}.html"


# Example job configurations:
ttb_cola_job = define_asset_job(
    name="ttb_cola_job",
    selection=[ttb_flexible_partitioned],
    config={
        "ops": {
            "ttb_flexible_partitioned": {
                "config": {
                    "action_type": "cola"
                }
            }
        }
    }
)

ttb_certificate_job = define_asset_job(
    name="ttb_certificate_job",
    selection=[ttb_flexible_partitioned],
    config={
        "ops": {
            "ttb_flexible_partitioned": {
                "config": {
                    "action_type": "certificate"
                }
            }
        }
    }
)

# Even more flexible - custom action:
ttb_custom_job = define_asset_job(
    name="ttb_custom_job",
    selection=[ttb_flexible_partitioned],
    config={
        "ops": {
            "ttb_flexible_partitioned": {
                "config": {
                    "custom_action": "someOtherAction",
                    "s3_prefix": "ttb-custom-data"
                }
            }
        }
    }
)