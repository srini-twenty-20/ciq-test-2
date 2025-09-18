#!/usr/bin/env python3
"""
Test script for TTB HTML parsing utilities.
"""
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ciq_test_2.defs.ttb_data_extraction import parse_ttb_html


def test_parsing():
    """Test TTB HTML parsing on sample files."""

    print("🧪 Testing TTB HTML Parsing Utilities")
    print("=" * 50)

    # Test COLA detail parsing
    print("\n📋 Testing COLA Detail Parser...")
    try:
        with open('/tmp/sample_cola.html', 'r', encoding='utf-8') as f:
            cola_html = f.read()

        cola_data = parse_ttb_html(cola_html, 'cola-detail')

        print(f"✅ COLA parsing successful!")
        print(f"📊 Extracted {len([k for k, v in cola_data.items() if v is not None and v != ''])} non-empty fields")

        # Show key extracted fields
        key_fields = [
            'ttb_id', 'status', 'brand_name', 'fanciful_name',
            'approval_date', 'contact_full_name', 'company_name'
        ]

        print("\n📝 Key COLA Fields Extracted:")
        for field in key_fields:
            value = cola_data.get(field)
            if value:
                print(f"   {field}: {value}")
            else:
                print(f"   {field}: (not found)")

        # Show errors if any
        if cola_data.get('extraction_errors'):
            print(f"\n⚠️  Extraction Errors ({len(cola_data['extraction_errors'])}):")
            for error in cola_data['extraction_errors'][:5]:  # Show first 5
                print(f"   - {error}")

    except Exception as e:
        print(f"❌ COLA parsing failed: {str(e)}")
        return False

    # Test Certificate parsing
    print("\n📋 Testing Certificate Parser...")
    try:
        with open('/tmp/sample_certificate.html', 'r', encoding='utf-8') as f:
            cert_html = f.read()

        cert_data = parse_ttb_html(cert_html, 'certificate')

        print(f"✅ Certificate parsing successful!")
        print(f"📊 Extracted {len([k for k, v in cert_data.items() if v is not None and v != ''])} non-empty fields")

        # Show key extracted fields
        key_fields = [
            'ttb_id', 'serial_number', 'brand_name', 'fanciful_name',
            'application_date', 'approval_date', 'wine_appellation'
        ]

        print("\n📝 Key Certificate Fields Extracted:")
        for field in key_fields:
            value = cert_data.get(field)
            if value:
                print(f"   {field}: {value}")
            else:
                print(f"   {field}: (not found)")

        # Show checkbox data
        if cert_data.get('source_of_product'):
            print(f"\n☑️  Source of Product: {cert_data['source_of_product']}")
        if cert_data.get('product_type'):
            print(f"☑️  Product Type: {cert_data['product_type']}")
        if cert_data.get('application_type_checkboxes'):
            print(f"☑️  Application Type: {cert_data['application_type_checkboxes']}")

        # Show image information
        if cert_data.get('label_images'):
            print(f"\n🖼️  Label Images Found: {len(cert_data['label_images'])}")
            for i, img in enumerate(cert_data['label_images']):
                print(f"   Image {i+1}: {img.get('type', 'Unknown')}")
                print(f"     URL: {img.get('original_url', 'N/A')}")
                if img.get('physical_dimensions'):
                    dims = img['physical_dimensions']
                    print(f"     Size: {dims.get('width_inches')}\" × {dims.get('height_inches')}\"")

        # Show errors if any
        if cert_data.get('extraction_errors'):
            print(f"\n⚠️  Extraction Errors ({len(cert_data['extraction_errors'])}):")
            for error in cert_data['extraction_errors'][:5]:  # Show first 5
                print(f"   - {error}")

    except Exception as e:
        print(f"❌ Certificate parsing failed: {str(e)}")
        return False

    print("\n" + "=" * 50)
    print("✅ Phase 1 Complete: HTML Parsing Utilities Working!")

    # Write detailed output to files for review
    try:
        with open('cola_parsing_output.json', 'w') as f:
            json.dump(cola_data, f, indent=2, default=str)

        with open('certificate_parsing_output.json', 'w') as f:
            json.dump(cert_data, f, indent=2, default=str)

        print("📁 Detailed parsing results saved to:")
        print("   - cola_parsing_output.json")
        print("   - certificate_parsing_output.json")

    except Exception as e:
        print(f"⚠️  Could not save detailed output: {str(e)}")

    return True


if __name__ == "__main__":
    success = test_parsing()
    sys.exit(0 if success else 1)