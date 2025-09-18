#!/usr/bin/env python3
"""
Parse TTB reference data into clean structured format.
"""
import requests
import urllib3
from bs4 import BeautifulSoup
import json
import re

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parse_ttb_reference_data():
    """Parse TTB reference data from lookup pages."""

    print("🔍 Parsing TTB Reference Data")
    print("=" * 50)

    # Create session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })

    # Reference data URLs
    urls = {
        'product_class_types': 'https://ttbonline.gov/colasonline/lookupProductClassTypeCode.do?action=search&display=all',
        'origin_codes': 'https://ttbonline.gov/colasonline/lookupOriginCode.do?action=search&display=all'
    }

    all_reference_data = {}

    for data_type, url in urls.items():
        print(f"\n📋 Processing {data_type.replace('_', ' ').title()}...")

        try:
            response = session.get(url, verify=False, timeout=30)
            print(f"✅ Status: HTTP {response.status_code}")

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Parse the specific structure
                if data_type == 'product_class_types':
                    parsed_data = parse_product_class_types(soup)
                elif data_type == 'origin_codes':
                    parsed_data = parse_origin_codes(soup)
                else:
                    parsed_data = []

                all_reference_data[data_type] = parsed_data
                print(f"📊 Extracted {len(parsed_data)} records")

                # Save data
                filename = f"/tmp/{data_type}_clean.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(parsed_data, f, indent=2)
                print(f"💾 Saved: {filename}")

                # Show sample
                if parsed_data:
                    print(f"📋 Sample records:")
                    for i, record in enumerate(parsed_data[:5]):
                        print(f"  {i+1}. {record}")

            else:
                print(f"❌ Failed: HTTP {response.status_code}")

        except Exception as e:
            print(f"❌ Error: {str(e)}")

    return all_reference_data


def parse_product_class_types(soup):
    """Parse product class type codes from HTML."""

    records = []

    # Find the table with class/type codes
    # Look for table that contains "Class/Type Code" header
    tables = soup.find_all('table')

    for table in tables:
        # Check if this table has the headers we want
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]
        if 'Class/Type Code' in header_texts and 'Description' in header_texts:
            print(f"  Found product class table with headers: {header_texts}")

            # Parse data rows
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'product_class_type'
                        })

            break  # Found the right table

    return records


def parse_origin_codes(soup):
    """Parse origin codes from HTML."""

    records = []

    # Find tables
    tables = soup.find_all('table')

    for table in tables:
        # Check headers
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]

        # Look for origin code related headers
        if any('origin' in h.lower() or 'code' in h.lower() for h in header_texts):
            print(f"  Found potential origin table with headers: {header_texts}")

            # Parse data rows
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'origin_code'
                        })

            if records:  # Found data, break
                break

    return records


def create_validation_lookups():
    """Create lookup dictionaries for validation."""

    print(f"\n🔍 Creating Validation Lookups")
    print("=" * 30)

    lookups = {}

    # Load the parsed data
    for data_type in ['product_class_types', 'origin_codes']:
        try:
            filename = f"/tmp/{data_type}_clean.json"
            with open(filename, 'r') as f:
                data = json.load(f)

            # Create lookup dictionaries
            lookups[data_type] = {
                'by_code': {record['code']: record['description'] for record in data},
                'by_description': {record['description']: record['code'] for record in data},
                'all_codes': [record['code'] for record in data],
                'all_descriptions': [record['description'] for record in data]
            }

            print(f"✅ {data_type}: {len(data)} records")
            print(f"   Codes: {len(lookups[data_type]['by_code'])}")
            print(f"   Sample codes: {list(lookups[data_type]['by_code'].keys())[:10]}")

        except FileNotFoundError:
            print(f"❌ No data found for {data_type}")
        except Exception as e:
            print(f"❌ Error processing {data_type}: {e}")

    # Save lookups
    lookup_filename = "/tmp/ttb_reference_lookups.json"
    with open(lookup_filename, 'w') as f:
        json.dump(lookups, f, indent=2)
    print(f"\n💾 Validation lookups saved: {lookup_filename}")

    return lookups


def validate_sample_data(lookups):
    """Test validation with sample data."""

    print(f"\n🧪 Testing Validation")
    print("=" * 25)

    # Sample codes to test
    sample_codes = ['100', '141', '80', '901']  # Common beverage codes

    if 'product_class_types' in lookups:
        product_lookup = lookups['product_class_types']['by_code']

        print("Product Class Type Validation:")
        for code in sample_codes:
            if code in product_lookup:
                print(f"  ✅ {code}: {product_lookup[code]}")
            else:
                print(f"  ❌ {code}: Not found")

    # Count different beverage categories
    if 'product_class_types' in lookups:
        descriptions = lookups['product_class_types']['all_descriptions']

        categories = {
            'WHISKY': len([d for d in descriptions if 'WHISKY' in d.upper()]),
            'WINE': len([d for d in descriptions if 'WINE' in d.upper()]),
            'BEER': len([d for d in descriptions if 'BEER' in d.upper()]),
            'VODKA': len([d for d in descriptions if 'VODKA' in d.upper()]),
            'GIN': len([d for d in descriptions if 'GIN' in d.upper()]),
            'RUM': len([d for d in descriptions if 'RUM' in d.upper()]),
        }

        print(f"\n📊 Beverage Category Counts:")
        for category, count in categories.items():
            print(f"  {category}: {count} codes")


if __name__ == "__main__":
    # Parse reference data
    reference_data = parse_ttb_reference_data()

    # Create validation lookups
    lookups = create_validation_lookups()

    # Test validation
    validate_sample_data(lookups)

    print(f"\n✅ Reference data parsing complete!")
    print(f"Use /tmp/ttb_reference_lookups.json for validation in your pipeline")