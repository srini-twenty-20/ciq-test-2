# TTB Pipeline - Dagster Best Practices Implementation

This document describes the reorganized TTB pipeline following Dagster best practices for asset organization, resource management, and configuration.

## 📁 Project Structure

```
src/ciq_test_2/
├── assets/                    # Organized asset modules
│   ├── __init__.py           # Asset exports and documentation
│   ├── raw.py                # Raw data extraction assets
│   ├── processed.py          # Data processing and transformation
│   ├── consolidated.py       # Cross-dimensional consolidation
│   ├── dimensional.py        # Dimension tables for star schema
│   └── facts.py              # Fact tables for analytics
├── config/                   # Configuration management
│   ├── __init__.py          # Config exports
│   ├── ttb_config.py        # Centralized config classes
│   └── environments.py      # Environment-specific configs
├── resources/                # Resource definitions
│   ├── __init__.py          # Resource exports
│   ├── s3_resources.py      # S3 resource configurations
│   └── io_managers.py       # Custom IO managers
├── defs/                     # Legacy definitions (transitioning)
└── definitions.py            # Main Dagster definitions
```

## 🎯 Asset Organization

Assets are organized into logical groups by pipeline stage:

### Asset Groups

1. **ttb_raw** - Raw data extraction from external sources
   - `ttb_raw_data`: Extracts data from TTB website with rate limiting

2. **ttb_processing** - Data processing and transformation stages
   - `ttb_extracted_data`: Structured data extraction from HTML
   - `ttb_cleaned_data`: Data cleaning and validation
   - `ttb_structured_data`: Final schema transformation

3. **ttb_consolidated** - Cross-dimensional data consolidation
   - `ttb_consolidated_data`: Merges data across receipt methods and types

4. **ttb_dimensions** - Dimensional modeling for analytics
   - `dim_dates`: Date dimension with calendar attributes
   - `dim_companies`: Company dimension from TTB data
   - `dim_locations`: Geographic dimension
   - `dim_product_types`: Product classification dimension

5. **ttb_reference** - Reference and lookup data
   - `ttb_reference_data`: TTB lookup tables and codes

6. **ttb_facts** - Fact tables for star schema
   - `fact_products`: Product measures and metrics
   - `fact_certificates`: Certificate-specific measures

## ⚙️ Configuration Management

### Environment-Based Configuration

The pipeline supports three environments with optimized configurations:

- **Development**: Fast iteration, minimal data, local testing
- **Test**: Reliable testing, comprehensive validation, small datasets
- **Production**: Full scale, data quality focus, performance optimized

### Configuration Classes

- `TTBPipelineConfig`: Global pipeline settings
- `TTBExtractionConfig`: Raw data extraction configuration
- `TTBProcessingConfig`: Data processing settings
- `TTBConsolidationConfig`: Cross-partition consolidation settings
- `TTBAnalyticsConfig`: Analytics and dimensional modeling settings

### Environment Variables

Key environment variables for configuration:

```bash
# Environment
DAGSTER_ENVIRONMENT=development|test|production

# Partitioning (for testing vs production)
TTB_PARTITION_START_DATE=2024-01-01
TTB_PARTITION_END_DATE=2024-01-01
TTB_RECEIPT_METHODS=001
TTB_DATA_TYPES=cola-detail

# Extraction settings
TTB_MAX_SEQUENCE_PER_BATCH=1000
TTB_CONSECUTIVE_FAILURE_THRESHOLD=10
TTB_REQUEST_DELAY_SECONDS=0.5

# AWS settings
AWS_REGION=us-east-1
TTB_S3_BUCKET=ciq-dagster
```

## 🔧 Resource Management

### Standardized Resources

1. **S3Resource**: Standard Dagster S3 resource with environment configuration
2. **TTBStorageResource**: TTB-specific storage with path helpers
3. **TTBS3IOManager**: Custom IO manager for TTB data formats

### Resource Configuration

Resources are automatically configured based on the current environment:

```python
# Automatic environment detection
environment = EnvVar("DAGSTER_ENVIRONMENT").get_value("development")
resources = get_resources_for_environment(environment)
```

## 📊 Data Flow

The pipeline follows a clear data flow pattern:

```
Raw Data → Processing → Consolidation → Analytics
   ↓           ↓            ↓           ↓
ttb_raw → ttb_processed → ttb_consolidated → ttb_facts/dims
```

### Partitioning Strategy

- **Raw/Processing**: Multi-dimensional partitions by `date|method_type`
- **Consolidation**: Daily partitions (consolidates across method_types)
- **Analytics**: Daily partitions with dimension tables (non-partitioned)

## 🎮 Usage Examples

### Running Different Environments

```bash
# Development (default)
dagster dev

# Test environment
DAGSTER_ENVIRONMENT=test dagster dev

# Production environment
DAGSTER_ENVIRONMENT=production dagster dev
```

### Partition Configuration

```bash
# Testing with minimal partitions (default)
TTB_PARTITION_START_DATE=2024-01-01 \
TTB_PARTITION_END_DATE=2024-01-01 \
TTB_RECEIPT_METHODS=001 \
TTB_DATA_TYPES=cola-detail \
dagster dev

# Production with full partitions
TTB_PARTITION_START_DATE=2015-01-01 \
TTB_PARTITION_END_DATE=2025-12-31 \
TTB_RECEIPT_METHODS=001,002,003,000 \
TTB_DATA_TYPES=cola-detail,certificate \
dagster dev
```

### Asset Materialization

```bash
# Single asset
dagster asset materialize --select ttb_raw_data --partition "2024-01-01|001-cola-detail"

# Full pipeline for one day
dagster job execute --job ttb_raw_pipeline --partition-key "2024-01-01"

# Analytics pipeline
dagster job execute --job ttb_analytics_pipeline --partition-key "2024-01-01"
```

## 🏗️ Asset Dependencies

Clear dependency chain ensures proper execution order:

```
ttb_raw_data
    ↓
ttb_extracted_data
    ↓
ttb_cleaned_data
    ↓
ttb_structured_data
    ↓
ttb_consolidated_data
    ↓
[dim_dates, dim_companies, dim_locations, dim_product_types, ttb_reference_data]
    ↓
[fact_products, fact_certificates]
```

## 📈 Benefits of This Organization

1. **Clear Separation of Concerns**: Each module has a single responsibility
2. **Environment Flexibility**: Easy switching between dev/test/prod configurations
3. **Scalable Architecture**: Asset groups allow for independent scaling
4. **Maintainable Code**: Logical organization makes code easier to understand
5. **Testable Components**: Isolated modules enable focused testing
6. **Resource Efficiency**: Proper resource management and IO optimization
7. **Configuration Management**: Centralized, environment-aware configuration

## 🔄 Migration from Legacy Structure

The pipeline maintains backward compatibility while transitioning to the new structure:

- Legacy `defs/` modules still work but are being phased out
- New `assets/` modules follow Dagster best practices
- Configuration is centralized and environment-aware
- Resources are standardized and reusable

## 📋 Asset Metadata

Each asset includes comprehensive metadata:

- **Group assignment** for logical organization
- **Data type and format** information
- **Pipeline stage** identification
- **Dependencies** clearly specified
- **Comprehensive descriptions** for documentation