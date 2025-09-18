"""
TTB Pipeline Assets

This module organizes all TTB pipeline assets into logical groups following Dagster best practices.
Assets are organized by pipeline stage and data type for better maintainability and understanding.

Asset Groups:
- ttb_raw: Raw data extraction from external sources
- ttb_processing: Data processing and transformation stages
- ttb_consolidated: Cross-dimensional data consolidation
- ttb_dimensions: Dimensional modeling for analytics
- ttb_facts: Fact tables for star schema
- ttb_reference: Reference and lookup data
"""

from .raw import ttb_raw_data
from .processed import ttb_extracted_data, ttb_cleaned_data, ttb_structured_data
from .consolidated import ttb_consolidated_data
from .dimensional import (
    dim_dates,
    dim_companies,
    dim_locations,
    dim_product_types,
    ttb_reference_data
)
from .facts import fact_products, fact_certificates

__all__ = [
    # Raw data extraction (Group: ttb_raw)
    "ttb_raw_data",

    # Processed data stages (Group: ttb_processing)
    "ttb_extracted_data",
    "ttb_cleaned_data",
    "ttb_structured_data",

    # Consolidated data (Group: ttb_consolidated)
    "ttb_consolidated_data",

    # Dimensional data (Group: ttb_dimensions, ttb_reference)
    "dim_dates",
    "dim_companies",
    "dim_locations",
    "dim_product_types",
    "ttb_reference_data",

    # Fact tables (Group: ttb_facts)
    "fact_products",
    "fact_certificates"
]