"""
Resource Definitions

This module provides standardized resource configurations following Dagster best practices.
Resources are properly configured for different environments and use cases.
"""

from .s3_resources import get_s3_resource, TTBStorageResource
from .io_managers import ttb_s3_io_manager, ttb_parquet_io_manager
from .supabase_resources import SupabaseResource, SupabaseIOManager

__all__ = [
    "get_s3_resource",
    "TTBStorageResource",
    "ttb_s3_io_manager",
    "ttb_parquet_io_manager",
    "SupabaseResource",
    "SupabaseIOManager"
]