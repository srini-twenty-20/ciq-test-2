#!/usr/bin/env python3
"""
Test script to validate TTB reference data integration.
"""
import sys
sys.path.insert(0, 'src')

from ciq_test_2.defs.ttb_transformations import (
    transformation_registry,
    load_ttb_reference_data,
    validate_product_class_type,
    validate_origin_code
)

def test_reference_data_validation():
    """Test TTB reference data validation functionality."""

    print("🧪 Testing TTB Reference Data Validation")
    print("=" * 50)

    # Test loading reference data
    print("\n1. Loading reference data...")
    reference_data = load_ttb_reference_data()

    if reference_data:
        print("✅ Reference data loaded successfully")

        # Show sample data
        if 'product_class_types' in reference_data:
            product_codes = list(reference_data['product_class_types']['by_code'].keys())[:10]
            print(f"   Sample product class codes: {product_codes}")

        if 'origin_codes' in reference_data:
            origin_codes = list(reference_data['origin_codes']['by_code'].keys())[:10]
            print(f"   Sample origin codes: {origin_codes}")
    else:
        print("❌ Failed to load reference data")
        return

    # Test product class validation
    print("\n2. Testing product class validation...")

    test_product_codes = ['100', '901', '80', '999', 'INVALID']

    for code in test_product_codes:
        result = validate_product_class_type(code, reference_data)
        status = "✅" if result['is_valid'] else "❌"
        description = result.get('description', 'N/A')
        print(f"   {status} Code '{code}': {result['is_valid']} - {description}")

    # Test origin code validation
    print("\n3. Testing origin code validation...")

    test_origin_codes = ['01', '02', '50', '00', 'XX', '999']

    for code in test_origin_codes:
        result = validate_origin_code(code, reference_data)
        status = "✅" if result['is_valid'] else "❌"
        description = result.get('description', 'N/A')
        print(f"   {status} Code '{code}': {result['is_valid']} - {description}")

    # Test transformation registry integration
    print("\n4. Testing transformation registry integration...")

    try:
        # Test via registry
        registry_ref_data = transformation_registry.apply('load_reference_data', None)
        registry_result = transformation_registry.apply('validate_product_class', '100', reference_data=registry_ref_data)

        print(f"   ✅ Registry integration: Code '100' -> {registry_result['is_valid']} ({registry_result.get('description', 'N/A')})")

    except Exception as e:
        print(f"   ❌ Registry integration failed: {e}")

    print("\n✅ Reference data validation testing complete!")


if __name__ == "__main__":
    test_reference_data_validation()