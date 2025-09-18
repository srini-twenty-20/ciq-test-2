"""
TTB data schema definitions for structured output formats.

This module defines Parquet schemas and data models for TTB COLA and certificate data,
ensuring consistent structure and data types for downstream processing.
"""
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal


class TTBSchemaRegistry:
    """Registry for TTB data schemas and conversions."""

    def __init__(self):
        self._schemas = {}
        self._register_default_schemas()

    def register_schema(self, data_type: str, schema: pa.Schema):
        """Register a PyArrow schema for a data type."""
        self._schemas[data_type] = schema

    def get_schema(self, data_type: str) -> pa.Schema:
        """Get schema for a data type."""
        if data_type not in self._schemas:
            raise ValueError(f"Unknown data type: {data_type}")
        return self._schemas[data_type]

    def get_available_schemas(self) -> List[str]:
        """Get list of available schema names."""
        return list(self._schemas.keys())

    def _register_default_schemas(self):
        """Register default schemas for TTB data types."""
        self.register_schema('cola-detail', self._create_cola_detail_schema())
        self.register_schema('certificate', self._create_certificate_schema())

    def _create_cola_detail_schema(self) -> pa.Schema:
        """Create PyArrow schema for COLA detail data."""
        return pa.schema([
            # Identification
            ('ttb_id', pa.string()),
            ('ttb_id_valid', pa.bool_()),
            ('serial_number', pa.string()),
            ('sequence_number', pa.int64()),

            # Dates
            ('filing_date', pa.date32()),
            ('approval_date', pa.date32()),
            ('expiration_date', pa.date32()),

            # Applicant information
            ('applicant_business_name', pa.string()),
            ('applicant_mailing_address', pa.string()),
            ('applicant_phone', pa.string()),
            ('applicant_email', pa.string()),
            ('applicant_fax', pa.string()),

            # Product information
            ('brand_name', pa.string()),
            ('fanciful_name', pa.string()),
            ('formula_id', pa.string()),
            ('product_description', pa.string()),
            ('beverage_type', pa.string()),

            # Wine-specific fields
            ('wine_appellation', pa.string()),
            ('wine_vintage', pa.string()),
            ('wine_variety_blend', pa.string()),
            ('wine_alcohol_content', pa.float64()),
            ('wine_pH', pa.float64()),

            # Container information
            ('net_contents', pa.string()),
            ('container_type', pa.string()),

            # Alcohol content
            ('alcohol_content', pa.float64()),
            ('alcohol_content_unit', pa.string()),

            # Government warnings and statements
            ('government_warning', pa.string()),
            ('sulfite_declaration', pa.string()),
            ('health_claim_statement', pa.string()),

            # Labels and images
            ('label_images_count', pa.int64()),
            ('label_images_metadata', pa.string()),  # JSON string

            # Processing metadata
            ('partition_date', pa.string()),
            ('receipt_method', pa.int64()),
            ('data_type', pa.string()),
            ('source_s3_key', pa.string()),
            ('source_file_size', pa.int64()),
            ('extraction_timestamp', pa.timestamp('us')),
            ('transformation_applied', pa.bool_()),
            ('transformation_timestamp', pa.timestamp('us')),
            ('field_count', pa.int64()),
            ('parsing_success', pa.bool_()),
        ])

    def _create_certificate_schema(self) -> pa.Schema:
        """Create PyArrow schema for certificate data."""
        return pa.schema([
            # Identification
            ('ttb_id', pa.string()),
            ('ttb_id_valid', pa.bool_()),
            ('certificate_number', pa.string()),

            # Dates
            ('effective_date', pa.date32()),
            ('expiration_date', pa.date32()),

            # Business information
            ('trade_name', pa.string()),
            ('dba_name', pa.string()),
            ('registry_id', pa.string()),

            # Addresses
            ('premises_address', pa.string()),
            ('premises_city', pa.string()),
            ('premises_state', pa.string()),
            ('premises_zip', pa.string()),
            ('mailing_address', pa.string()),
            ('mailing_city', pa.string()),
            ('mailing_state', pa.string()),
            ('mailing_zip', pa.string()),

            # Contact information
            ('phone', pa.string()),
            ('email', pa.string()),
            ('fax', pa.string()),

            # Permit information
            ('permit_type', pa.string()),
            ('permit_class', pa.string()),
            ('basic_permit_number', pa.string()),

            # Processing metadata
            ('partition_date', pa.string()),
            ('receipt_method', pa.int64()),
            ('data_type', pa.string()),
            ('source_s3_key', pa.string()),
            ('source_file_size', pa.int64()),
            ('extraction_timestamp', pa.timestamp('us')),
            ('transformation_applied', pa.bool_()),
            ('transformation_timestamp', pa.timestamp('us')),
            ('field_count', pa.int64()),
            ('parsing_success', pa.bool_()),
        ])


def convert_to_schema_compliant(
    records: List[Dict[str, Any]],
    schema: pa.Schema
) -> List[Dict[str, Any]]:
    """
    Convert records to be compliant with the given schema.

    This function handles type conversion and ensures all schema fields are present.
    """
    schema_fields = {field.name: field.type for field in schema}
    compliant_records = []

    for record in records:
        compliant_record = {}

        for field_name, field_type in schema_fields.items():
            value = record.get(field_name)

            # Convert value to schema-compliant type
            converted_value = convert_value_to_type(value, field_type)
            compliant_record[field_name] = converted_value

        compliant_records.append(compliant_record)

    return compliant_records


def convert_value_to_type(value: Any, pa_type: pa.DataType) -> Any:
    """Convert a value to match a PyArrow data type."""
    if value is None or value == '' or value == []:
        return None

    try:
        if pa.types.is_string(pa_type):
            return str(value) if value is not None else None

        elif pa.types.is_boolean(pa_type):
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ['true', 'yes', '1', 'on']
            else:
                return bool(value)

        elif pa.types.is_integer(pa_type):
            if isinstance(value, (int, float)):
                return int(value)
            elif isinstance(value, str):
                # Extract first number from string
                import re
                match = re.search(r'\d+', str(value))
                return int(match.group()) if match else None
            else:
                return None

        elif pa.types.is_floating(pa_type):
            if isinstance(value, (int, float, Decimal)):
                return float(value)
            elif isinstance(value, str):
                # Extract first decimal number from string
                import re
                match = re.search(r'\d+\.?\d*', str(value))
                return float(match.group()) if match else None
            else:
                return None

        elif pa.types.is_date32(pa_type):
            if isinstance(value, date):
                return value
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, str):
                # Try to parse ISO date format
                try:
                    return datetime.fromisoformat(value).date()
                except ValueError:
                    return None
            else:
                return None

        elif pa.types.is_timestamp(pa_type):
            if isinstance(value, datetime):
                return value
            elif isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    return None
            else:
                return None

        else:
            # Default to string conversion
            return str(value) if value is not None else None

    except (ValueError, TypeError):
        return None


def create_parquet_dataset(
    records: List[Dict[str, Any]],
    schema: pa.Schema,
    output_path: str,
    partition_cols: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Create a Parquet dataset from records.

    Args:
        records: List of data records
        schema: PyArrow schema to enforce
        output_path: Path where to write the Parquet files
        partition_cols: Columns to partition by

    Returns:
        Metadata about the created dataset
    """
    if not records:
        return {
            'files_written': 0,
            'total_rows': 0,
            'output_path': output_path,
            'schema_fields': len(schema)
        }

    # Convert records to schema-compliant format
    compliant_records = convert_to_schema_compliant(records, schema)

    # Create DataFrame
    df = pd.DataFrame(compliant_records)

    # Ensure all schema columns exist
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None

    # Reorder columns to match schema
    df = df[[field.name for field in schema]]

    # Convert to PyArrow table with enforced schema
    table = pa.Table.from_pandas(df, schema=schema)

    # Write to Parquet
    output_path_obj = Path(output_path)
    output_path_obj.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        # Write partitioned dataset
        pq.write_to_dataset(
            table,
            root_path=str(output_path_obj),
            partition_cols=partition_cols,
            existing_data_behavior='overwrite_or_ignore'
        )
        files_written = len(list(output_path_obj.rglob('*.parquet')))
    else:
        # Write single file
        filename = output_path_obj / 'data.parquet'
        pq.write_table(table, filename)
        files_written = 1

    return {
        'files_written': files_written,
        'total_rows': len(table),
        'output_path': str(output_path_obj),
        'schema_fields': len(schema),
        'file_size_bytes': sum(f.stat().st_size for f in output_path_obj.rglob('*.parquet')),
        'columns': [field.name for field in schema],
        'partitioned': bool(partition_cols),
        'partition_columns': partition_cols or []
    }


# Create global schema registry
schema_registry = TTBSchemaRegistry()


def validate_records_against_schema(
    records: List[Dict[str, Any]],
    schema: pa.Schema
) -> Dict[str, Any]:
    """
    Validate records against a schema and return validation results.
    """
    validation_results = {
        'total_records': len(records),
        'valid_records': 0,
        'validation_errors': [],
        'field_validation': {},
        'missing_fields': set(),
        'extra_fields': set()
    }

    schema_fields = {field.name for field in schema}

    for i, record in enumerate(records):
        record_fields = set(record.keys())
        record_errors = []

        # Check for missing required fields
        missing = schema_fields - record_fields
        if missing:
            validation_results['missing_fields'].update(missing)
            record_errors.append(f"Missing fields: {missing}")

        # Check for extra fields
        extra = record_fields - schema_fields
        if extra:
            validation_results['extra_fields'].update(extra)

        # Validate field types
        for field in schema:
            field_name = field.name
            if field_name in record:
                value = record[field_name]
                converted_value = convert_value_to_type(value, field.type)

                if converted_value is None and value is not None:
                    record_errors.append(f"Invalid type for {field_name}: {type(value).__name__}")

                # Track field validation stats
                if field_name not in validation_results['field_validation']:
                    validation_results['field_validation'][field_name] = {
                        'non_null_count': 0,
                        'null_count': 0,
                        'type_errors': 0
                    }

                if converted_value is not None:
                    validation_results['field_validation'][field_name]['non_null_count'] += 1
                else:
                    validation_results['field_validation'][field_name]['null_count'] += 1

                if converted_value is None and value is not None:
                    validation_results['field_validation'][field_name]['type_errors'] += 1

        if record_errors:
            validation_results['validation_errors'].append({
                'record_index': i,
                'ttb_id': record.get('ttb_id', 'unknown'),
                'errors': record_errors
            })
        else:
            validation_results['valid_records'] += 1

    return validation_results