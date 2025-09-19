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
- ttb_supabase_export: Export data to Supabase for analytics
"""

from .raw import ttb_raw_data
from .processed import ttb_extracted_data, ttb_cleaned_data, ttb_structured_data
from .reference import ttb_product_class_types, ttb_origin_codes, ttb_reference_data
from .dimensional import dim_dates, dim_companies, dim_products
from .facts import fact_products, fact_certificates
from .supabase_export import (
    supabase_reference_data,
    supabase_dim_dates,
    supabase_dim_companies,
    supabase_dim_products,
    supabase_fact_products,
    ttb_supabase_export_complete
)

__all__ = [
    # Raw data extraction (Group: ttb_raw)
    "ttb_raw_data",

    # Processed data stages (Group: ttb_processing)
    "ttb_extracted_data",
    "ttb_cleaned_data",
    "ttb_structured_data",

    # Reference data (Group: ttb_reference)
    "ttb_product_class_types",
    "ttb_origin_codes",
    "ttb_reference_data",

    # Dimensional data (Group: ttb_dimensions)
    "dim_dates",
    "dim_companies",
    "dim_products",

    # Fact tables (Group: ttb_facts)
    "fact_products",
    "fact_certificates",

    # Supabase export (Group: ttb_supabase_export)
    "supabase_reference_data",
    "supabase_dim_dates",
    "supabase_dim_companies",
    "supabase_dim_products",
    "supabase_fact_products",
    "ttb_supabase_export_complete",
]