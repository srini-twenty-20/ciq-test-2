#!/usr/bin/env python3
"""
Test script to run TTB partitioned asset for a single partition.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster import materialize, DagsterInstance
from ciq_test_2.defs.partitioned_assets import ttb_cola_partitioned
from ciq_test_2.defs.resources import get_s3_resource


def test_single_partition():
    """Test running a single partition of the TTB asset."""

    # Define the partition key we want to test
    partition_key = "2024-01-01|000"  # January 1, 2024, hand-delivered

    print(f"🚀 Testing TTB partitioned asset for partition: {partition_key}")
    print(f"📋 This will process TTB IDs for January 1, 2024 with receipt method 000 (hand-delivered)")

    try:
        # Create an ephemeral instance
        instance = DagsterInstance.ephemeral()

        # Materialize the asset for the specific partition with resources
        result = materialize(
            [ttb_cola_partitioned],
            instance=instance,
            partition_key=partition_key,
            resources={
                "s3_resource": get_s3_resource()
            },
            tags={
                "test_run": "true",
                "partition_key": partition_key
            }
        )

        if result.success:
            print(f"✅ Successfully processed partition: {partition_key}")
            print(f"📊 Asset events: {len(result.asset_materializations)}")

            # Show some details about what was processed
            for materialization in result.asset_materializations:
                print(f"   🎯 Materialized: {materialization.asset_key}")
                if materialization.metadata:
                    for key, value in materialization.metadata.items():
                        print(f"      📈 {key}: {value}")
        else:
            print(f"❌ Failed to process partition: {partition_key}")

    except Exception as e:
        print(f"❌ Error processing partition {partition_key}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(test_single_partition())