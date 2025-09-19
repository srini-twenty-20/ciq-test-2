"""
Consolidated TTB Pipeline Jobs and Schedules.

This module provides a single, clean scheduling system for the complete TTB pipeline:
- Daily end-to-end refresh (extraction → facts & dimensions)
- Historical backfill capabilities
- Simple, maintainable scheduling
"""
from dagster import (
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Config
)


class TTBPipelineConfig(Config):
    """Unified configuration for TTB pipeline jobs."""
    max_sequence_per_batch: int = 99999  # Large batch for production
    enable_asset_checks: bool = True


# ============================================================================
# Asset Selections for Different Pipeline Stages
# ============================================================================

# Raw data extraction assets (multi-partitioned)
raw_extraction_assets = AssetSelection.assets(
    "ttb_raw_data",
    "ttb_extracted_data",
    "ttb_cleaned_data",
    "ttb_structured_data"
)

# Consolidated and dimensional assets (daily partitioned)
analytical_assets = AssetSelection.assets(
    "dim_dates",
    "dim_companies",
    "dim_products",
    "fact_products",
    "fact_certificates"
)

# Supabase export assets (daily partitioned)
supabase_export_assets = AssetSelection.assets(
    "supabase_reference_data",
    "supabase_dim_dates",
    "supabase_dim_companies",
    "supabase_dim_products",
    "supabase_fact_products",
    "ttb_supabase_export_complete"
)

# Complete end-to-end pipeline - NOTE: Cannot mix different partition definitions in a single job
# This will be split into separate jobs for different partition types
# complete_pipeline_assets = raw_extraction_assets | analytical_assets | AssetSelection.assets("ttb_reference_data")


# ============================================================================
# Production Jobs
# ============================================================================

# Raw data pipeline (multi-partitioned assets)
ttb_raw_pipeline = define_asset_job(
    name="ttb_raw_pipeline",
    selection=raw_extraction_assets,
    description="TTB raw data extraction and processing pipeline",
    tags={
        "pipeline_type": "raw_extraction",
        "team": "data-engineering"
    }
)

# Analytics pipeline (daily partitioned assets + reference data)
ttb_analytics_pipeline = define_asset_job(
    name="ttb_analytics_pipeline",
    selection=analytical_assets | AssetSelection.assets("ttb_reference_data", "ttb_product_class_types", "ttb_origin_codes"),
    description="TTB analytics pipeline: dimensions & facts with reference data",
    tags={
        "pipeline_type": "analytics_daily",
        "team": "data-engineering"
    }
)

# Raw data extraction only (for troubleshooting)
ttb_extraction_only = define_asset_job(
    name="ttb_extraction_only",
    selection=raw_extraction_assets,
    description="TTB raw data extraction only (troubleshooting)",
    tags={
        "pipeline_type": "extraction_debug",
        "team": "data-engineering"
    }
)

# Analytics refresh only (assumes raw data exists)
ttb_analytics_only = define_asset_job(
    name="ttb_analytics_only",
    selection=analytical_assets,
    description="TTB analytics refresh: consolidation → facts & dimensions",
    tags={
        "pipeline_type": "analytics_refresh",
        "team": "data-engineering"
    }
)

# Complete Supabase export pipeline (will materialize entire upstream dependency graph)
ttb_supabase_export = define_asset_job(
    name="ttb_supabase_export",
    selection=supabase_export_assets,
    description="Complete TTB export to Supabase (materializes entire upstream pipeline)",
    tags={
        "pipeline_type": "supabase_export",
        "team": "data-engineering"
    }
)


# ============================================================================
# Production Schedules
# ============================================================================

# Primary daily schedule - runs analytics pipeline at 6 AM UTC
ttb_daily_schedule = ScheduleDefinition(
    name="ttb_daily_schedule",  # Explicit name to prevent conflicts
    job=ttb_analytics_pipeline,
    cron_schedule="0 6 * * *",  # 6 AM UTC daily
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,  # Start manually in production
    description="Daily TTB analytics pipeline at 6 AM UTC for previous day's data"
)

# Analytics-only refresh at 8 AM UTC (backup/catch-up)
ttb_analytics_schedule = ScheduleDefinition(
    name="ttb_analytics_schedule",  # Explicit name to prevent conflicts
    job=ttb_analytics_only,
    cron_schedule="0 8 * * *",  # 8 AM UTC daily
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily analytics refresh at 8 AM UTC (backup schedule)"
)

# Weekend catch-up for any missed data
ttb_weekend_schedule = ScheduleDefinition(
    name="ttb_weekend_schedule",  # Explicit name to prevent conflicts
    job=ttb_analytics_pipeline,
    cron_schedule="0 10 * * 0",  # 10 AM UTC on Sundays
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekly catch-up for missed TTB data"
)


# ============================================================================
# Legacy/Backfill Jobs (kept for historical data loading)
# ============================================================================

# Historical backfill for specific date ranges
ttb_backfill = define_asset_job(
    name="ttb_backfill",
    selection=raw_extraction_assets,
    description="Historical TTB data backfill for specific date ranges"
)


# Export all definitions
__all__ = [
    # Production jobs
    "ttb_raw_pipeline",
    "ttb_analytics_pipeline",
    "ttb_extraction_only",
    "ttb_analytics_only",
    "ttb_supabase_export",

    # Production schedules
    "ttb_daily_schedule",
    "ttb_analytics_schedule",
    "ttb_weekend_schedule",

    # Legacy/backfill
    "ttb_backfill",

    # Config
    "TTBPipelineConfig"
]