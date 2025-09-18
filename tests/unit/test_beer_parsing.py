#!/usr/bin/env python3
"""
Test script for TTB HTML parsing on beer COLA.
"""
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ciq_test_2.defs.ttb_data_extraction import parse_ttb_html


def test_beer_parsing():
    """Test TTB HTML parsing on beer COLA sample."""

    print("🍺 Testing Beer COLA Parsing")
    print("=" * 40)

    try:
        with open('/tmp/beer_cola.html', 'r', encoding='utf-8') as f:
            beer_html = f.read()

        beer_data = parse_ttb_html(beer_html, 'cola-detail')

        print(f"✅ Beer COLA parsing successful!")
        print(f"📊 Extracted {len([k for k, v in beer_data.items() if v is not None and v != ''])} non-empty fields")

        # Expected values from the image
        expected_values = {
            'ttb_id': '23001001000001',
            'status': 'APPROVED',
            'vendor_code': '50197',
            'serial_number': '23TRIP',
            'class_type_code': 'BEER',
            'origin_code': 'NEW YORK',
            'brand_name': 'PARADOX BREWERY',
            'fanciful_name': 'TIPPLE TRIPEL',
            'type_of_application': 'LABEL APPROVAL',
            'approval_date': '01/09/2023'
        }

        print("\n📝 Field Validation (Expected vs Extracted):")
        total_fields = len(expected_values)
        correct_fields = 0

        for field, expected in expected_values.items():
            extracted = beer_data.get(field)
            match = extracted == expected
            if match:
                correct_fields += 1

            status = "✅" if match else "❌"
            print(f"   {status} {field}: Expected '{expected}' | Got '{extracted}'")

        accuracy = (correct_fields / total_fields) * 100
        print(f"\n📊 Accuracy: {correct_fields}/{total_fields} fields correct ({accuracy:.1f}%)")

        # Show all extracted fields
        print(f"\n📋 All Extracted Fields:")
        for field, value in beer_data.items():
            if value is not None and value != '' and field not in ['extraction_errors', 'parsing_success', 'data_type']:
                print(f"   {field}: {value}")

        # Show errors if any
        if beer_data.get('extraction_errors'):
            print(f"\n⚠️  Extraction Errors ({len(beer_data['extraction_errors'])}):")
            for error in beer_data['extraction_errors']:
                print(f"   - {error}")

        # Save detailed output
        with open('beer_parsing_output.json', 'w') as f:
            json.dump(beer_data, f, indent=2, default=str)

        print(f"\n📁 Detailed output saved to: beer_parsing_output.json")

        return accuracy >= 80  # 80% accuracy threshold

    except Exception as e:
        print(f"❌ Beer parsing failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_beer_parsing()
    sys.exit(0 if success else 1)