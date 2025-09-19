"""
TTB COLA Pipeline

A Dagster pipeline for extracting, processing, and consolidating TTB COLA certificate data.
This pipeline fetches both detail and certificate views of COLA records, preserves raw HTML,
and creates consolidated structured data.
"""

__version__ = "2.0.0"