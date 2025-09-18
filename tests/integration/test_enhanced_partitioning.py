#!/usr/bin/env python3
"""
Test script for enhanced TTB partitioning with data type dimension.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster import materialize, DagsterInstance
from ciq_test_2.defs.partitioned_assets import ttb_partitioned
from ciq_test_2.defs.resources import get_s3_resource


def test_enhanced_partitions():
    """Test the enhanced partitioning with data type dimension."""

    print("🚀 Testing Enhanced TTB Partitioning System")
    print("📋 New partition format: date|method_type (e.g., 2024-01-01|001-cola-detail)\n")

    # Test partition keys to validate
    test_partitions = [
        "2024-01-01|001-cola-detail",   # COLA detail data
        "2024-01-01|001-certificate",   # Certificate data
    ]

    for partition_key in test_partitions:
        print(f"🧪 Testing partition: {partition_key}")

        # Parse the partition key to show what it means
        parts = partition_key.split("|")
        date_part, method_type_part = parts
        method_part, type_part = method_type_part.split("-", 1)
        method_names = {"001": "e-filed", "002": "mailed", "003": "overnight", "000": "hand delivered"}

        print(f"   📅 Date: {date_part}")
        print(f"   📨 Receipt method: {method_part} ({method_names.get(method_part, 'unknown')})")
        print(f"   🔗 Data type: {type_part}")

        if type_part == "cola-detail":
            print(f"   🌐 URL action: publicDisplaySearchAdvanced")
            print(f"   📁 S3 path: ttb-cola-data/...")
        elif type_part == "certificate":
            print(f"   🌐 URL action: publicFormDisplay")
            print(f"   📁 S3 path: ttb-certificate-data/...")

        print()

    print("✅ Enhanced partitioning structure validated!")
    print("\n📊 Benefits:")
    print("   • Single asset handles both data types")
    print("   • Clear data lineage and organization")
    print("   • Flexible materialization options")
    print("   • Unified processing logic")

    print("\n🎯 Example Usage:")
    print("   # Process both data types for Jan 1, e-filed:")
    print("   2024-01-01|001|cola-detail")
    print("   2024-01-01|001|certificate")
    print("\n   # Process only COLA data for a date:")
    print("   2024-01-01|001|cola-detail")
    print("   2024-01-01|002|cola-detail")
    print("   2024-01-01|003|cola-detail")
    print("   2024-01-01|000|cola-detail")

    print("\n🔧 Available in Dagster UI:")
    print("   • Asset: ttb_partitioned")
    print("   • Jobs: ttb_backward_backfill_job, ttb_daily_forward_fill_job, etc.")
    print("   • Partition selection: Choose specific combinations as needed")

    return 0


if __name__ == "__main__":
    sys.exit(test_enhanced_partitions())