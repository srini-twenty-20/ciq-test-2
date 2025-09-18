#!/usr/bin/env python3
"""
Script to run TTB COLA data extraction for a specific date.
"""
import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster import DagsterInstance, execute_job
from ciq_test_2.definitions import defs
from ciq_test_2.defs.ttb_jobs import generate_partition_keys_for_date_range


def run_for_specific_date(target_date: date, receipt_methods=None):
    """
    Run TTB data extraction for a specific date.

    Args:
        target_date: Date to extract data for
        receipt_methods: List of receipt methods (default: ["001", "002", "003"])
    """
    if receipt_methods is None:
        receipt_methods = ["001", "002", "003"]

    print(f"🎯 Running TTB data extraction for {target_date}")
    print(f"📋 Receipt methods: {receipt_methods}")

    # Generate partition keys for the specific date
    partition_keys = generate_partition_keys_for_date_range(
        start_date=target_date,
        end_date=target_date,
        receipt_methods=receipt_methods
    )

    print(f"🔑 Generated {len(partition_keys)} partition keys:")
    for i, key in enumerate(partition_keys, 1):
        print(f"   {i}: {key}")

    # Get the job using the newer API
    job = defs.resolve_job_def("ttb_test_single_partition")

    # Execute for each partition
    instance = DagsterInstance.ephemeral()

    for partition_key in partition_keys:
        print(f"\n🚀 Processing partition: {partition_key}")

        try:
            # Execute the job with the specific partition
            result = job.execute_in_process(
                instance=instance,
                tags={
                    "partition_key": partition_key,
                    "target_date": str(target_date),
                    "manual_run": "true"
                }
            )

            if result.success:
                print(f"   ✅ Successfully processed {partition_key}")
            else:
                print(f"   ❌ Failed to process {partition_key}")

        except Exception as e:
            print(f"   ❌ Error processing {partition_key}: {e}")

    print(f"\n🎉 Completed processing for {target_date}")


def main():
    """Main function to run for specific dates."""
    print("🔧 TTB COLA Data Extraction - Specific Date Runner\n")

    # Example: Run for January 1, 2024
    target_date = date(2024, 1, 1)

    # You can also specify specific receipt methods
    receipt_methods = ["001", "000"]  # e-filed and hand-delivered for testing

    try:
        run_for_specific_date(target_date, receipt_methods)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())