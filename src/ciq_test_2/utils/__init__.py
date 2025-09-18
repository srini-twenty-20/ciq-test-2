"""
Utility Modules

This module contains utility functions and classes that support the TTB pipeline.
Utilities are organized by functional area.
"""

from .ttb_utils import TTBIDUtils, TTBSequenceTracker
from .ttb_data_extraction import parse_ttb_html
from .ttb_transformations import apply_field_transformations, transformation_registry, load_ttb_reference_data
from .ttb_schema import schema_registry, create_parquet_dataset, validate_records_against_schema

__all__ = [
    # TTB utilities
    "TTBIDUtils",
    "TTBSequenceTracker",

    # Data extraction
    "parse_ttb_html",

    # Transformations
    "apply_field_transformations",
    "transformation_registry",
    "load_ttb_reference_data",

    # Schema utilities
    "schema_registry",
    "create_parquet_dataset",
    "validate_records_against_schema"
]