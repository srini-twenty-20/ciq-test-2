# Legacy Files Archive

This folder contains the original files from the `src/ciq_test_2/defs/` directory that have been successfully migrated to the new organized structure following Dagster best practices.

## Archive Date
September 17, 2025

## Migration Status
All files in this archive have been **successfully migrated** to the new organized structure and are **no longer actively used** by the pipeline.

## Archived Files

### Asset Definitions (→ `assets/`)
- `partitioned_assets.py` → Migrated to `assets/raw.py`
- `ttb_parsing_assets.py` → Migrated to `assets/processed.py`
- `ttb_consolidated_assets.py` → Migrated to `assets/consolidated.py`
- `ttb_dimensional_assets.py` → Migrated to `assets/dimensional.py`
- `ttb_fact_assets.py` → Migrated to `assets/facts.py`

### Utility Functions (→ `utils/`)
- `ttb_utils.py` → Migrated to `utils/ttb_utils.py`
- `ttb_data_extraction.py` → Migrated to `utils/ttb_data_extraction.py`
- `ttb_transformations.py` → Migrated to `utils/ttb_transformations.py`
- `ttb_schema.py` → Migrated to `utils/ttb_schema.py`
- `ttb_consolidated_schema.py` → Migrated to `utils/ttb_consolidated_schema.py`

### Configuration (→ `config/`)
- `ttb_config.py` → Migrated to `config/ttb_config.py` (enhanced)
- `ttb_partitions.py` → Migrated to `config/ttb_partitions.py`

### Jobs & Schedules (→ `jobs/`)
- `ttb_consolidated_jobs.py` → Migrated to `jobs/ttb_jobs.py`
- `ttb_monitoring.py` → Migrated to `jobs/ttb_sensors.py`

### Asset Checks (→ `checks/`)
- `ttb_asset_checks.py` → Migrated to `checks/ttb_asset_checks.py`

### Resources (→ `resources/`)
- `resources.py` → Migrated to `resources/` module structure

## New Structure Location

The migrated functionality can now be found in:
```
src/ciq_test_2/
├── assets/        # Asset definitions organized by pipeline stage
├── config/        # Configuration management
├── utils/         # Utility functions and helpers
├── jobs/          # Job, schedule, and sensor definitions
├── checks/        # Asset checks for data quality
└── resources/     # Resource and IO manager definitions
```

## Safe to Delete?

✅ **Yes, these files are safe to delete** after confirming the new structure works correctly.

All functionality has been:
- Successfully migrated to the new organized structure
- Tested and validated
- Imports updated throughout the codebase
- Documentation updated

## Reference Information

- **Migration Plan**: See `/MIGRATION_PLAN.md` for detailed migration documentation
- **New Architecture**: See `/src/ciq_test_2/README.md` for the new structure overview
- **Original Location**: These files were originally in `/src/ciq_test_2/defs/`

## Restore Instructions

If you need to temporarily restore any of these files:
1. Copy the required file back to its original location in `/src/ciq_test_2/defs/`
2. Update imports if necessary
3. Test thoroughly before using in production

**Note**: The new organized structure is the recommended approach and should be used for all ongoing development.