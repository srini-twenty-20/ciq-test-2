"""
TTB data transformation utilities for cleaning and normalizing extracted fields.

This module provides a registry-based system for applying standardized transformations
to raw extracted TTB data, including date normalization, text cleaning, validation,
and field enrichment.
"""
from typing import Dict, Any, List, Callable, Optional, Union
from datetime import datetime, date
import re
from decimal import Decimal, InvalidOperation


class TransformationRegistry:
    """Registry for managing and applying data transformations."""

    def __init__(self):
        self._transformations: Dict[str, Callable] = {}
        self._register_default_transformations()

    def register(self, name: str, func: Callable):
        """Register a transformation function."""
        self._transformations[name] = func

    def apply(self, name: str, value: Any, **kwargs) -> Any:
        """Apply a transformation by name."""
        if name not in self._transformations:
            raise ValueError(f"Unknown transformation: {name}")
        return self._transformations[name](value, **kwargs)

    def get_available(self) -> List[str]:
        """Get list of available transformation names."""
        return list(self._transformations.keys())

    def _register_default_transformations(self):
        """Register all default transformations."""
        # Text cleaning
        self.register('clean_text', clean_text)
        self.register('normalize_whitespace', normalize_whitespace)
        self.register('extract_numbers', extract_numbers)
        self.register('clean_phone', clean_phone_number)

        # Date transformations
        self.register('parse_date', parse_flexible_date)
        self.register('format_date_iso', format_date_iso)

        # Numeric transformations
        self.register('parse_decimal', parse_decimal)
        self.register('parse_percentage', parse_percentage)
        self.register('clean_currency', clean_currency)

        # Validation
        self.register('validate_ttb_id', validate_ttb_id)
        self.register('validate_email', validate_email)
        self.register('validate_url', validate_url)

        # Address normalization
        self.register('normalize_address', normalize_address)
        self.register('extract_zip_code', extract_zip_code)
        self.register('normalize_state', normalize_state)

        # Business logic
        self.register('classify_beverage_type', classify_beverage_type)
        self.register('extract_alcohol_content', extract_alcohol_content)
        self.register('standardize_container_size', standardize_container_size)

        # Reference data validation
        self.register('validate_product_class', validate_product_class_type)
        self.register('validate_origin_code', validate_origin_code)
        self.register('load_reference_data', load_ttb_reference_data)


# Text cleaning transformations
def clean_text(value: Any, strip_html: bool = True, normalize_spaces: bool = True) -> str:
    """Clean and normalize text content."""
    if not value:
        return ""

    text = str(value)

    # Remove HTML tags if requested
    if strip_html:
        text = re.sub(r'<[^>]+>', '', text)

    # Normalize whitespace
    if normalize_spaces:
        text = re.sub(r'\s+', ' ', text).strip()

    # Remove common artifacts
    text = text.replace('\xa0', ' ')  # Non-breaking space
    text = text.replace('\r\n', '\n').replace('\r', '\n')  # Normalize line endings

    return text


def normalize_whitespace(value: Any) -> str:
    """Normalize whitespace in text."""
    if not value:
        return ""
    return re.sub(r'\s+', ' ', str(value)).strip()


def extract_numbers(value: Any, include_decimals: bool = True) -> List[str]:
    """Extract numeric values from text."""
    if not value:
        return []

    text = str(value)
    if include_decimals:
        pattern = r'\d+\.?\d*'
    else:
        pattern = r'\d+'

    return re.findall(pattern, text)


def clean_phone_number(value: Any) -> str:
    """Clean and format phone numbers."""
    if not value:
        return ""

    # Extract digits only
    digits = re.sub(r'\D', '', str(value))

    # Format common patterns
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"1-({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        return digits


# Date transformations
def parse_flexible_date(value: Any, formats: Optional[List[str]] = None) -> Optional[date]:
    """Parse date from various string formats."""
    if not value:
        return None

    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()

    # Common date formats in TTB data
    if formats is None:
        formats = [
            '%m/%d/%Y',
            '%m-%d-%Y',
            '%Y-%m-%d',
            '%B %d, %Y',
            '%b %d, %Y',
            '%m/%d/%y',
            '%m-%d-%y'
        ]

    text = clean_text(value)
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def format_date_iso(value: Any) -> Optional[str]:
    """Format date as ISO string."""
    parsed_date = parse_flexible_date(value)
    return parsed_date.isoformat() if parsed_date else None


# Numeric transformations
def parse_decimal(value: Any, precision: int = 2) -> Optional[Decimal]:
    """Parse decimal from text, handling various formats."""
    if not value:
        return None

    try:
        # Clean the value
        text = re.sub(r'[^\d.-]', '', str(value))
        if not text:
            return None

        decimal_val = Decimal(text)
        return decimal_val.quantize(Decimal('0.' + '0' * precision))
    except (InvalidOperation, ValueError):
        return None


def parse_percentage(value: Any) -> Optional[Decimal]:
    """Parse percentage values."""
    if not value:
        return None

    text = str(value).replace('%', '').strip()
    decimal_val = parse_decimal(text)

    # If value is > 1, assume it's already in percentage form
    # If value is <= 1, assume it's decimal form and convert
    if decimal_val is not None:
        if decimal_val <= 1:
            return decimal_val * 100
        return decimal_val

    return None


def clean_currency(value: Any) -> Optional[Decimal]:
    """Extract and parse currency values."""
    if not value:
        return None

    # Remove currency symbols and formatting
    text = re.sub(r'[\$,]', '', str(value))
    return parse_decimal(text)


# Validation functions
def validate_ttb_id(value: Any) -> bool:
    """Validate TTB ID format (14 digits: YYJJJRRRSSSSS)."""
    if not value:
        return False

    text = str(value).strip()
    return bool(re.match(r'^\d{14}$', text))


def validate_email(value: Any) -> bool:
    """Basic email validation."""
    if not value:
        return False

    text = str(value).strip()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, text))


def validate_url(value: Any) -> bool:
    """Basic URL validation."""
    if not value:
        return False

    text = str(value).strip()
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(pattern, text, re.IGNORECASE))


# Address normalization
def normalize_address(value: Any) -> str:
    """Normalize address formatting."""
    if not value:
        return ""

    text = clean_text(value)

    # Common address abbreviations
    replacements = {
        r'\bSTREET\b': 'ST',
        r'\bAVENUE\b': 'AVE',
        r'\bBOULEVARD\b': 'BLVD',
        r'\bROAD\b': 'RD',
        r'\bDRIVE\b': 'DR',
        r'\bCOURT\b': 'CT',
        r'\bLANE\b': 'LN',
        r'\bPLACE\b': 'PL',
        r'\bNORTH\b': 'N',
        r'\bSOUTH\b': 'S',
        r'\bEAST\b': 'E',
        r'\bWEST\b': 'W'
    }

    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    return text.upper()


def extract_zip_code(value: Any) -> Optional[str]:
    """Extract ZIP code from address text."""
    if not value:
        return None

    text = str(value)
    # Look for 5-digit ZIP or 5+4 format
    match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', text)
    return match.group(1) if match else None


def normalize_state(value: Any) -> Optional[str]:
    """Normalize state names/abbreviations."""
    if not value:
        return None

    text = str(value).strip().upper()

    # State abbreviation mapping (sample)
    state_map = {
        'CALIFORNIA': 'CA',
        'NEW YORK': 'NY',
        'TEXAS': 'TX',
        'FLORIDA': 'FL',
        # Add more as needed
    }

    # If it's already a 2-letter code, return as-is
    if len(text) == 2 and text.isalpha():
        return text

    # Look up full name
    return state_map.get(text, text)


# Business logic transformations
def classify_beverage_type(value: Any) -> Optional[str]:
    """Classify beverage type from text description."""
    if not value:
        return None

    text = str(value).lower()

    # Classification rules
    if any(word in text for word in ['wine', 'vintner', 'winery', 'grape']):
        return 'WINE'
    elif any(word in text for word in ['beer', 'brewery', 'ale', 'lager', 'stout']):
        return 'BEER'
    elif any(word in text for word in ['spirit', 'whiskey', 'vodka', 'rum', 'gin', 'distillery']):
        return 'SPIRITS'
    elif any(word in text for word in ['cider', 'mead', 'sake']):
        return 'OTHER_FERMENTED'
    else:
        return 'UNKNOWN'


def extract_alcohol_content(value: Any) -> Optional[Decimal]:
    """Extract alcohol by volume percentage."""
    if not value:
        return None

    text = str(value).lower()

    # Look for patterns like "12.5%", "12.5% ABV", "alcohol 12.5%"
    patterns = [
        r'(\d+\.?\d*)\s*%\s*(?:abv|alcohol)',
        r'(?:abv|alcohol)\s*:?\s*(\d+\.?\d*)\s*%?',
        r'(\d+\.?\d*)\s*%'
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return Decimal(match.group(1))
            except (ValueError, InvalidOperation):
                continue

    return None


def standardize_container_size(value: Any) -> Optional[str]:
    """Standardize container size descriptions."""
    if not value:
        return None

    text = str(value).lower()

    # Common size patterns
    if re.search(r'750\s*ml', text):
        return '750ml'
    elif re.search(r'375\s*ml', text):
        return '375ml'
    elif re.search(r'1\.5\s*l|1500\s*ml', text):
        return '1.5L'
    elif re.search(r'3\s*l|3000\s*ml', text):
        return '3L'
    elif re.search(r'12\s*oz', text):
        return '12oz'
    elif re.search(r'16\s*oz', text):
        return '16oz'
    elif re.search(r'22\s*oz', text):
        return '22oz'
    else:
        # Extract any volume measurement
        volume_match = re.search(r'(\d+\.?\d*)\s*(ml|l|oz|gallon)', text)
        if volume_match:
            return f"{volume_match.group(1)}{volume_match.group(2)}"

    return None




def apply_field_transformations(
    data: Dict[str, Any],
    field_rules: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Any]:
    """
    Apply transformation rules to data fields.

    Args:
        data: Raw data dictionary
        field_rules: Rules mapping field names to transformation configs

    Example field_rules:
        {
            'filing_date': [
                {'transform': 'parse_date'},
                {'transform': 'format_date_iso'}
            ],
            'applicant_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ]
        }
    """
    transformed_data = data.copy()

    for field_name, rules in field_rules.items():
        if field_name in transformed_data:
            value = transformed_data[field_name]

            # Apply each transformation in sequence
            for rule in rules:
                transform_name = rule['transform']
                kwargs = {k: v for k, v in rule.items() if k != 'transform'}

                try:
                    value = transformation_registry.apply(transform_name, value, **kwargs)
                except Exception as e:
                    # Log error but continue processing
                    print(f"Error applying {transform_name} to {field_name}: {e}")
                    break

            transformed_data[field_name] = value

    return transformed_data


# TTB Reference Data Validation Functions

def load_ttb_reference_data(unused_value: Any = None) -> Dict[str, Any]:
    """Load TTB reference data for validation."""
    import requests
    import urllib3
    from bs4 import BeautifulSoup
    import json
    import tempfile
    import os

    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    reference_data = {}

    # Check if cached reference data exists
    temp_dir = tempfile.gettempdir()
    cache_file = os.path.join(temp_dir, 'ttb_reference_lookups.json')

    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                reference_data = json.load(f)
            return reference_data
        except Exception:
            pass

    # Download fresh reference data
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    })

    urls = {
        'product_class_types': 'https://ttbonline.gov/colasonline/lookupProductClassTypeCode.do?action=search&display=all',
        'origin_codes': 'https://ttbonline.gov/colasonline/lookupOriginCode.do?action=search&display=all'
    }

    all_data = {}

    for data_type, url in urls.items():
        try:
            response = session.get(url, verify=False, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                if data_type == 'product_class_types':
                    parsed_data = parse_product_class_types(soup)
                elif data_type == 'origin_codes':
                    parsed_data = parse_origin_codes(soup)
                else:
                    parsed_data = []

                # Create lookup dictionaries
                all_data[data_type] = {
                    'by_code': {record['code']: record['description'] for record in parsed_data if len(record.get('code', '')) <= 10},
                    'by_description': {record['description']: record['code'] for record in parsed_data if len(record.get('code', '')) <= 10},
                    'all_codes': [record['code'] for record in parsed_data if len(record.get('code', '')) <= 10]
                }

        except Exception:
            # Return empty structure on error
            all_data[data_type] = {
                'by_code': {},
                'by_description': {},
                'all_codes': []
            }

    # Cache the data
    try:
        with open(cache_file, 'w') as f:
            json.dump(all_data, f)
    except Exception:
        pass

    return all_data


def parse_product_class_types(soup):
    """Parse product class type codes from HTML."""
    records = []
    tables = soup.find_all('table')

    for table in tables:
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]
        if 'Class/Type Code' in header_texts and 'Description' in header_texts:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description and len(code) <= 10:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'product_class_type'
                        })
            break

    return records


def parse_origin_codes(soup):
    """Parse origin codes from HTML."""
    records = []
    tables = soup.find_all('table')

    for table in tables:
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]
        if 'Origin Code' in header_texts and 'Description' in header_texts:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description and len(code) <= 10:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'origin_code'
                        })
            break

    return records


def validate_product_class_type(value: Any, reference_data: Optional[Dict] = None) -> Dict[str, Any]:
    """Validate product class/type code against TTB reference data."""
    if not value:
        return {
            'is_valid': False,
            'original_value': value,
            'validated_code': None,
            'description': None,
            'validation_error': 'Empty value'
        }

    code = str(value).strip()

    if not reference_data:
        reference_data = load_ttb_reference_data()

    product_lookup = reference_data.get('product_class_types', {}).get('by_code', {})

    if code in product_lookup:
        return {
            'is_valid': True,
            'original_value': value,
            'validated_code': code,
            'description': product_lookup[code],
            'validation_error': None
        }
    else:
        return {
            'is_valid': False,
            'original_value': value,
            'validated_code': None,
            'description': None,
            'validation_error': f'Unknown product class type code: {code}'
        }


def validate_origin_code(value: Any, reference_data: Optional[Dict] = None) -> Dict[str, Any]:
    """Validate origin code against TTB reference data."""
    if not value:
        return {
            'is_valid': False,
            'original_value': value,
            'validated_code': None,
            'description': None,
            'validation_error': 'Empty value'
        }

    code = str(value).strip()

    if not reference_data:
        reference_data = load_ttb_reference_data()

    origin_lookup = reference_data.get('origin_codes', {}).get('by_code', {})

    if code in origin_lookup:
        return {
            'is_valid': True,
            'original_value': value,
            'validated_code': code,
            'description': origin_lookup[code],
            'validation_error': None
        }
    else:
        return {
            'is_valid': False,
            'original_value': value,
            'validated_code': None,
            'description': None,
            'validation_error': f'Unknown origin code: {code}'
        }


# Create global registry instance after all functions are defined
transformation_registry = TransformationRegistry()