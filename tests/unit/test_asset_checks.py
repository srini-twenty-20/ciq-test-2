#!/usr/bin/env python3
"""
Test TTB asset checks functionality.
"""
import sys
sys.path.insert(0, 'src')

from ciq_test_2.defs.ttb_transformations import load_ttb_reference_data
from dagster import AssetCheckSeverity
from unittest.mock import MagicMock

def test_reference_data_check():
    """Test the reference data freshness check logic."""
    print("🧪 Testing TTB Asset Checks")
    print("=" * 50)

    print("\n1. Testing reference data loading...")
    try:
        reference_data = load_ttb_reference_data()

        if not reference_data:
            print("❌ Failed to load reference data")
            return

        product_codes_count = len(reference_data.get('product_class_types', {}).get('by_code', {}))
        origin_codes_count = len(reference_data.get('origin_codes', {}).get('by_code', {}))

        print(f"✅ Reference data loaded: {product_codes_count} product codes, {origin_codes_count} origin codes")

        # Test the check logic
        min_product_codes = 500
        min_origin_codes = 200

        issues = []
        if product_codes_count < min_product_codes:
            issues.append(f"Low product code count: {product_codes_count} (expected ≥ {min_product_codes})")
        if origin_codes_count < min_origin_codes:
            issues.append(f"Low origin code count: {origin_codes_count} (expected ≥ {min_origin_codes})")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if issues else None

        print(f"\n2. Check results:")
        print(f"   Passed: {passed}")
        print(f"   Severity: {severity}")
        print(f"   Issues: {issues if issues else 'None'}")

        # Sample some data
        if reference_data.get('product_class_types', {}).get('by_code'):
            sample_products = list(reference_data['product_class_types']['by_code'].items())[:5]
            print(f"\n   Sample product codes: {sample_products}")

        if reference_data.get('origin_codes', {}).get('by_code'):
            sample_origins = list(reference_data['origin_codes']['by_code'].items())[:5]
            print(f"   Sample origin codes: {sample_origins}")

    except Exception as e:
        print(f"❌ Error in reference data check: {str(e)}")

def test_mock_parsing_stats():
    """Test parsing success rate check with mock data."""
    print("\n3. Testing parsing success rate check logic...")

    # Mock different scenarios
    test_cases = [
        {"total": 100, "success": 98, "expected_pass": True, "desc": "High success rate"},
        {"total": 100, "success": 90, "expected_pass": True, "desc": "Moderate success rate (warning)"},
        {"total": 100, "success": 80, "expected_pass": False, "desc": "Low success rate (failure)"},
        {"total": 0, "success": 0, "expected_pass": False, "desc": "No files found"},
    ]

    for case in test_cases:
        total_files = case["total"]
        successful_parses = case["success"]
        failed_parses = total_files - successful_parses

        if total_files == 0:
            passed = False
            success_rate = 0
        else:
            success_rate = (successful_parses / total_files) * 100
            failure_threshold = 85.0
            passed = success_rate >= failure_threshold

        print(f"   {case['desc']}: {success_rate:.1f}% success → {'PASS' if passed else 'FAIL'} (expected {'PASS' if case['expected_pass'] else 'FAIL'})")

def test_mock_validation_rates():
    """Test transformation validation rates with mock data."""
    print("\n4. Testing validation rates check logic...")

    # Mock transformed records
    mock_records = [
        {"ttb_id_valid": True, "product_class_validation": {"is_valid": True}, "origin_code_validation": {"is_valid": True}},
        {"ttb_id_valid": True, "product_class_validation": {"is_valid": False}, "origin_code_validation": {"is_valid": True}},
        {"ttb_id_valid": False, "product_class_validation": {"is_valid": True}, "origin_code_validation": {"is_valid": False}},
        {"ttb_id_valid": True, "product_class_validation": {"is_valid": True}, "origin_code_validation": {"is_valid": True}},
    ]

    total_records = len(mock_records)
    valid_ttb_ids = sum(1 for r in mock_records if r.get('ttb_id_valid', False))
    valid_product_classes = sum(1 for r in mock_records if r.get('product_class_validation', {}).get('is_valid', False))
    valid_origin_codes = sum(1 for r in mock_records if r.get('origin_code_validation', {}).get('is_valid', False))

    ttb_id_rate = (valid_ttb_ids / total_records) * 100
    product_rate = (valid_product_classes / total_records) * 100
    origin_rate = (valid_origin_codes / total_records) * 100

    print(f"   Validation rates:")
    print(f"     TTB ID: {ttb_id_rate:.1f}% ({valid_ttb_ids}/{total_records})")
    print(f"     Product Class: {product_rate:.1f}% ({valid_product_classes}/{total_records})")
    print(f"     Origin Code: {origin_rate:.1f}% ({valid_origin_codes}/{total_records})")

    min_ttb_id_rate = 90.0
    issues = []
    if ttb_id_rate < min_ttb_id_rate:
        issues.append(f"Low TTB ID validation rate: {ttb_id_rate:.1f}%")

    passed = len(issues) == 0
    print(f"     Overall: {'PASS' if passed else 'FAIL'} - {issues if issues else 'No issues'}")

if __name__ == "__main__":
    test_reference_data_check()
    test_mock_parsing_stats()
    test_mock_validation_rates()
    print("\n✅ Asset checks testing complete!")