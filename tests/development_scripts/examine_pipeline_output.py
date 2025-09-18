#!/usr/bin/env python3
"""
Examine the output of the TTB pipeline run to understand data shape.
"""
import sys
sys.path.insert(0, 'src')

import json
import pandas as pd
from pathlib import Path
import glob

def examine_pipeline_output():
    """Examine the output from the TTB pipeline run."""

    print("🔍 Examining TTB Pipeline Output")
    print("=" * 50)

    # Look for dagster storage directory
    storage_dirs = glob.glob(".tmp_dagster_home_*/storage")
    if not storage_dirs:
        print("❌ No Dagster storage directory found")
        return

    storage_dir = Path(storage_dirs[0])
    print(f"📁 Found storage directory: {storage_dir}")

    # Look for TTB assets
    asset_dirs = [
        "ttb_raw_extraction",
        "ttb_cleaned_data",
        "ttb_structured_output"
    ]

    for asset_name in asset_dirs:
        asset_dir = storage_dir / asset_name
        if asset_dir.exists():
            print(f"\n📋 Examining {asset_name}...")

            # Find partition directories
            partition_dirs = [d for d in asset_dir.iterdir() if d.is_dir()]
            print(f"   Found {len(partition_dirs)} partitions: {[d.name for d in partition_dirs]}")

            for partition_dir in partition_dirs:
                print(f"\n   📊 Partition: {partition_dir.name}")

                # Look for data files
                data_files = list(partition_dir.iterdir())
                for data_file in data_files:
                    if data_file.is_file():
                        print(f"      📄 File: {data_file.name} ({data_file.stat().st_size} bytes)")

                        # Try to examine the content
                        try:
                            if data_file.suffix == '.json':
                                with open(data_file, 'r') as f:
                                    data = json.load(f)
                                print(f"         JSON keys: {list(data.keys()) if isinstance(data, dict) else f'List with {len(data)} items'}")

                            elif data_file.suffix == '.parquet':
                                df = pd.read_parquet(data_file)
                                print(f"         Parquet shape: {df.shape}")
                                print(f"         Columns: {list(df.columns)}")

                            else:
                                # Try to load as pickle (Dagster's default)
                                import pickle
                                with open(data_file, 'rb') as f:
                                    data = pickle.load(f)

                                if isinstance(data, dict):
                                    print(f"         Dict keys: {list(data.keys())}")

                                    # If it's the structured output, show details
                                    if asset_name == "ttb_structured_output":
                                        dataset_metadata = data.get('dataset_metadata', {})
                                        validation_results = data.get('validation_results', {})

                                        print(f"         Dataset files: {dataset_metadata.get('files_written', 0)}")
                                        print(f"         Total rows: {dataset_metadata.get('total_rows', 0)}")
                                        print(f"         File size: {dataset_metadata.get('file_size_bytes', 0):,} bytes")
                                        print(f"         Valid records: {validation_results.get('valid_records', 0)}")
                                        print(f"         Validation errors: {len(validation_results.get('validation_errors', []))}")

                                        # Show output path
                                        output_path = dataset_metadata.get('output_path')
                                        if output_path:
                                            print(f"         📁 Output path: {output_path}")
                                            # List files in output path
                                            try:
                                                output_dir = Path(output_path)
                                                if output_dir.exists():
                                                    parquet_files = list(output_dir.rglob("*.parquet"))
                                                    print(f"         📄 Parquet files: {len(parquet_files)}")
                                                    for pf in parquet_files[:3]:  # Show first 3
                                                        print(f"            - {pf.name} ({pf.stat().st_size:,} bytes)")
                                            except Exception as e:
                                                print(f"         Error listing output files: {e}")

                                    # If it's cleaned data, show sample
                                    elif asset_name == "ttb_cleaned_data":
                                        transformed_records = data.get('transformed_records', [])
                                        print(f"         Transformed records: {len(transformed_records)}")
                                        if transformed_records:
                                            sample_record = transformed_records[0]
                                            print(f"         Sample record keys: {list(sample_record.keys())}")

                                            # Show validation results
                                            ttb_id_valid = sample_record.get('ttb_id_valid')
                                            product_validation = sample_record.get('product_class_validation')
                                            origin_validation = sample_record.get('origin_code_validation')

                                            print(f"         TTB ID valid: {ttb_id_valid}")
                                            if product_validation:
                                                print(f"         Product class valid: {product_validation.get('is_valid')}")
                                            if origin_validation:
                                                print(f"         Origin code valid: {origin_validation.get('is_valid')}")

                                    # If it's raw data, show stats
                                    elif asset_name == "ttb_raw_extraction":
                                        parsed_records = data.get('parsed_records', [])
                                        processing_stats = data.get('processing_stats', {})

                                        print(f"         Parsed records: {len(parsed_records)}")
                                        print(f"         Processing stats: {processing_stats}")

                                        if parsed_records:
                                            sample_record = parsed_records[0]
                                            print(f"         Sample record keys: {list(sample_record.keys())}")

                                elif isinstance(data, list):
                                    print(f"         List with {len(data)} items")
                                    if data:
                                        print(f"         First item type: {type(data[0])}")
                                else:
                                    print(f"         Data type: {type(data)}")

                        except Exception as e:
                            print(f"         Error reading file: {e}")
        else:
            print(f"\n❌ {asset_name} directory not found")

    # Look for any parquet files in tmp
    print(f"\n🔍 Looking for Parquet files...")
    parquet_files = list(Path("/tmp").rglob("*.parquet"))
    for pf in parquet_files:
        if "ttb" in str(pf).lower():
            print(f"   📄 {pf} ({pf.stat().st_size:,} bytes)")
            try:
                df = pd.read_parquet(pf)
                print(f"      Shape: {df.shape}")
                print(f"      Columns: {list(df.columns)[:10]}...")  # First 10 columns
            except Exception as e:
                print(f"      Error reading: {e}")

if __name__ == "__main__":
    examine_pipeline_output()
    print("\n✅ Pipeline output examination complete!")