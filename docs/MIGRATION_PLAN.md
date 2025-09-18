# TTB Pipeline Migration Plan

This document outlines the migration from the legacy `defs/` structure to the new organized architecture.

## ✅ Successfully Migrated

### Utilities (moved to `utils/`)
- ✅ `ttb_utils.py` → `utils/ttb_utils.py`
- ✅ `ttb_data_extraction.py` → `utils/ttb_data_extraction.py`
- ✅ `ttb_transformations.py` → `utils/ttb_transformations.py`
- ✅ `ttb_schema.py` → `utils/ttb_schema.py`
- ✅ `ttb_consolidated_schema.py` → `utils/ttb_consolidated_schema.py`

### Configuration (moved to `config/`)
- ✅ `ttb_partitions.py` → `config/ttb_partitions.py`
- ✅ Created new centralized config management in `config/`

### Assets (reorganized into `assets/`)
- ✅ `partitioned_assets.py` → `assets/raw.py` (reorganized)
- ✅ `ttb_parsing_assets.py` → `assets/processed.py` (reorganized)
- ✅ `ttb_consolidated_assets.py` → `assets/consolidated.py` (reorganized)
- ✅ `ttb_dimensional_assets.py` → `assets/dimensional.py` (reorganized)
- ✅ `ttb_fact_assets.py` → `assets/facts.py` (reorganized)

### Jobs & Schedules (moved to `jobs/`)
- ✅ `ttb_consolidated_jobs.py` → `jobs/ttb_jobs.py`
- ✅ `ttb_monitoring.py` → `jobs/ttb_sensors.py`
- ✅ Created `jobs/ttb_schedules.py` for schedule definitions

### Asset Checks (moved to `checks/`)
- ✅ `ttb_asset_checks.py` → `checks/ttb_asset_checks.py`

### Resources (moved to `resources/`)
- ✅ `resources.py` → Reorganized into `resources/` module with proper IO managers

## 📁 New Architecture Overview

```
src/ciq_test_2/
├── assets/              # 🆕 Organized asset modules
│   ├── raw.py          # Raw data extraction
│   ├── processed.py    # Data processing stages
│   ├── consolidated.py # Cross-dimensional consolidation
│   ├── dimensional.py  # Dimension tables
│   └── facts.py        # Fact tables
├── config/              # 🆕 Configuration management
│   ├── ttb_config.py   # Centralized config classes
│   ├── ttb_partitions.py # Partition definitions
│   └── environments.py # Environment-specific configs
├── resources/           # 🆕 Resource definitions
│   ├── s3_resources.py # S3 resource configurations
│   └── io_managers.py  # Custom IO managers
├── utils/               # 🆕 Utility modules
│   ├── ttb_utils.py    # TTB utilities
│   ├── ttb_data_extraction.py
│   ├── ttb_transformations.py
│   ├── ttb_schema.py
│   └── ttb_consolidated_schema.py
├── jobs/                # 🆕 Job definitions
│   ├── ttb_jobs.py     # Pipeline jobs
│   ├── ttb_schedules.py # Schedule definitions
│   └── ttb_sensors.py  # Monitoring sensors
├── checks/              # 🆕 Asset checks
│   └── ttb_asset_checks.py
└── definitions.py       # 🔄 Updated to use new structure
```

## 🗑️ Legacy Files Status

### Files that can be deprecated
- `defs/partitioned_assets.py` - ✅ Replaced by `assets/raw.py`
- `defs/ttb_parsing_assets.py` - ✅ Replaced by `assets/processed.py`
- `defs/ttb_consolidated_assets.py` - ✅ Replaced by `assets/consolidated.py`
- `defs/ttb_dimensional_assets.py` - ✅ Replaced by `assets/dimensional.py`
- `defs/ttb_fact_assets.py` - ✅ Replaced by `assets/facts.py`
- `defs/ttb_utils.py` - ✅ Moved to `utils/`
- `defs/ttb_data_extraction.py` - ✅ Moved to `utils/`
- `defs/ttb_transformations.py` - ✅ Moved to `utils/`
- `defs/ttb_schema.py` - ✅ Moved to `utils/`
- `defs/ttb_consolidated_schema.py` - ✅ Moved to `utils/`
- `defs/ttb_partitions.py` - ✅ Moved to `config/`
- `defs/ttb_consolidated_jobs.py` - ✅ Moved to `jobs/`
- `defs/ttb_monitoring.py` - ✅ Moved to `jobs/`
- `defs/ttb_asset_checks.py` - ✅ Moved to `checks/`
- `defs/resources.py` - ✅ Replaced by `resources/` module

### Files that can stay temporarily
- `defs/ttb_config.py` - Keep until fully migrated to `config/ttb_config.py`
- `defs/ttb_image_downloader.py` - Specialized functionality, evaluate if needed
- `defs/__init__.py` - Keep during transition period

## 🔧 Migration Benefits

### Before (Legacy Structure)
- All files mixed together in `defs/`
- No clear separation of concerns
- Difficult to find specific functionality
- Assets scattered across multiple files
- Configuration mixed with business logic

### After (Organized Structure)
- ✅ Clear separation by functional area
- ✅ Assets organized by pipeline stage
- ✅ Centralized configuration management
- ✅ Standardized resource definitions
- ✅ Proper import organization
- ✅ Environment-aware configuration
- ✅ Better maintainability and scalability

## 🚀 Next Steps

1. **Test the new structure** - Ensure all imports work correctly
2. **Update documentation** - Reflect new organization
3. **Gradual deprecation** - Remove legacy files after validation
4. **Update CI/CD** - Ensure deployment works with new structure
5. **Team training** - Onboard team on new architecture

## 🔄 Backward Compatibility

The new structure maintains backward compatibility during the transition:
- Legacy imports still work through the migration period
- Existing jobs and schedules continue to function
- Configuration can be gradually migrated
- Asset dependencies are preserved

## ⚠️ Breaking Changes

None at this time. The migration maintains full backward compatibility while providing the new organized structure.