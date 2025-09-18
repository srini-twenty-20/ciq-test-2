#!/usr/bin/env python3
"""
Display TTB origin codes and product class types reference data.
"""
import sys
sys.path.insert(0, 'src')

from src.ciq_test_2.defs.ttb_transformations import load_ttb_reference_data

def show_reference_data():
    """Download and display TTB reference data."""
    print("🔍 Downloading TTB Reference Data...")
    print("=" * 60)

    try:
        reference_data = load_ttb_reference_data()

        print("\n📋 ORIGIN CODES")
        print("-" * 30)
        origin_codes = reference_data.get('origin_codes', {})
        if 'all_codes' in origin_codes:
            total_origins = len(origin_codes['all_codes'])
            print(f"Total Origin Codes: {total_origins}")
            print("\nSample Origin Codes:")
            for i, code in enumerate(origin_codes['all_codes'][:10]):
                description = origin_codes['by_code'].get(code, 'Unknown')
                print(f"  {code}: {description}")
            if total_origins > 10:
                print(f"  ... and {total_origins - 10} more")

        print("\n🏷️  PRODUCT CLASS TYPES")
        print("-" * 30)
        product_types = reference_data.get('product_class_types', {})
        if 'all_codes' in product_types:
            total_types = len(product_types['all_codes'])
            print(f"Total Product Class Types: {total_types}")
            print("\nSample Product Class Types:")
            for i, code in enumerate(product_types['all_codes'][:10]):
                description = product_types['by_code'].get(code, 'Unknown')
                print(f"  {code}: {description}")
            if total_types > 10:
                print(f"  ... and {total_types - 10} more")

        print(f"\n✅ Reference data downloaded successfully!")

        # Show data structure
        print(f"\n📊 Data Structure:")
        for data_type, data in reference_data.items():
            print(f"  {data_type}:")
            print(f"    - by_code: {len(data.get('by_code', {})) if isinstance(data.get('by_code'), dict) else 0} entries")
            print(f"    - by_description: {len(data.get('by_description', {})) if isinstance(data.get('by_description'), dict) else 0} entries")
            print(f"    - all_codes: {len(data.get('all_codes', [])) if isinstance(data.get('all_codes'), list) else 0} codes")

        return reference_data

    except Exception as e:
        print(f"❌ Error downloading reference data: {e}")
        return None

if __name__ == "__main__":
    data = show_reference_data()