#!/usr/bin/env python3
"""
Test script to download TTB reference data and analyze structure.
"""
import requests
import urllib3
from bs4 import BeautifulSoup
import json

# Disable SSL warnings since TTB has certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_reference_data():
    """Download and analyze TTB reference data."""

    print("🔍 Downloading TTB Reference Data")
    print("=" * 50)

    # Create session with proper headers
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

    reference_data = {}

    for data_type, url in urls.items():
        print(f"\n📋 Fetching {data_type.replace('_', ' ').title()}...")
        print(f"URL: {url}")

        try:
            response = session.get(url, verify=False, timeout=30)
            print(f"✅ Status: HTTP {response.status_code}")
            print(f"Content-Type: {response.headers.get('content-type')}")
            print(f"Content-Length: {len(response.content)} bytes")

            if response.status_code == 200:
                # Parse HTML content
                soup = BeautifulSoup(response.text, 'html.parser')

                # Save raw HTML for analysis
                html_filename = f"/tmp/{data_type}_raw.html"
                with open(html_filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print(f"💾 Raw HTML saved: {html_filename}")

                # Try to extract structured data
                extracted_data = extract_reference_data(soup, data_type)
                reference_data[data_type] = extracted_data

                print(f"📊 Extracted {len(extracted_data)} records")

                # Save structured data
                json_filename = f"/tmp/{data_type}_structured.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(extracted_data, f, indent=2)
                print(f"💾 Structured data saved: {json_filename}")

            else:
                print(f"❌ Failed: HTTP {response.status_code}")

        except Exception as e:
            print(f"❌ Error: {str(e)}")

    return reference_data


def extract_reference_data(soup, data_type):
    """Extract structured data from reference pages."""

    print(f"  🔍 Analyzing HTML structure for {data_type}...")

    # Look for common table structures
    tables = soup.find_all('table')
    print(f"  Found {len(tables)} tables")

    extracted_records = []

    for i, table in enumerate(tables):
        print(f"  📋 Analyzing table {i+1}...")

        # Look for header row
        headers = []
        header_row = table.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            print(f"    Headers: {headers}")

        # Extract data rows
        rows = table.find_all('tr')[1:]  # Skip header
        print(f"    Data rows: {len(rows)}")

        for j, row in enumerate(rows[:5]):  # Show first 5 rows
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if cells:
                record = {}
                for k, cell in enumerate(cells):
                    header = headers[k] if k < len(headers) else f"col_{k}"
                    record[header] = cell
                extracted_records.append(record)

                if j < 3:  # Show first 3 records
                    print(f"    Row {j+1}: {record}")

        if len(rows) > 5:
            print(f"    ... and {len(rows) - 5} more rows")

    # Also look for other structures like select options
    selects = soup.find_all('select')
    if selects:
        print(f"  Found {len(selects)} select elements")
        for select in selects:
            options = select.find_all('option')
            if options:
                print(f"    Select with {len(options)} options:")
                for option in options[:5]:
                    value = option.get('value', '')
                    text = option.get_text(strip=True)
                    print(f"      {value}: {text}")

    return extracted_records


def analyze_sample_data():
    """Show sample of what we extracted."""

    print("\n🔍 Analysis Summary")
    print("=" * 50)

    # Try to load the structured data
    for data_type in ['product_class_types', 'origin_codes']:
        try:
            filename = f"/tmp/{data_type}_structured.json"
            with open(filename, 'r') as f:
                data = json.load(f)

            print(f"\n📊 {data_type.replace('_', ' ').title()}:")
            print(f"  Total records: {len(data)}")

            if data:
                print(f"  Sample record:")
                sample = data[0]
                for key, value in sample.items():
                    print(f"    {key}: {value}")

                # Look for common fields
                if len(data) > 1:
                    all_keys = set()
                    for record in data:
                        all_keys.update(record.keys())
                    print(f"  All fields: {sorted(all_keys)}")

        except FileNotFoundError:
            print(f"❌ No data file found for {data_type}")
        except Exception as e:
            print(f"❌ Error analyzing {data_type}: {e}")


if __name__ == "__main__":
    # Download reference data
    reference_data = download_reference_data()

    # Analyze what we got
    analyze_sample_data()

    print(f"\n✅ Reference data download complete!")
    print(f"Check /tmp/ for raw HTML and structured JSON files")