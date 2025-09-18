#!/usr/bin/env python3
"""
Test script to run TTB certificate partitioned asset for a single partition.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster import materialize, DagsterInstance
from ciq_test_2.defs.partitioned_assets import ttb_certificate_partitioned
from ciq_test_2.defs.resources import get_s3_resource


def test_certificate_partition():
    """Test running a single partition of the TTB certificate asset."""

    # Define the partition key we want to test
    partition_key = "2023-01-01|001"  # January 1, 2023, e-filed (using your example)

    print(f"🚀 Testing TTB certificate partitioned asset for partition: {partition_key}")
    print(f"📋 This will process TTB certificate IDs for January 1, 2023 with receipt method 001 (e-filed)")
    print(f"🔗 Using URL pattern: action=publicFormDisplay")

    try:
        # Create an ephemeral instance
        instance = DagsterInstance.ephemeral()

        # Materialize the asset for the specific partition with resources
        result = materialize(
            [ttb_certificate_partitioned],
            instance=instance,
            partition_key=partition_key,
            resources={
                "s3_resource": get_s3_resource()
            },
            tags={
                "test_run": "true",
                "partition_key": partition_key,
                "data_type": "certificate"
            }
        )

        if result.success:
            print(f"✅ Successfully processed certificate partition: {partition_key}")
            print(f"📊 Check S3 bucket for files in: ttb-certificate-data/year=2023/month=01/day=01/receipt_method=001/")
        else:
            print(f"❌ Failed to process certificate partition: {partition_key}")

    except Exception as e:
        print(f"❌ Error processing certificate partition {partition_key}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(test_certificate_partition())