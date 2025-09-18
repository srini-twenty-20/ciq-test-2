# TTB Pipeline Configuration Guide

## Overview

The TTB pipeline uses a layered configuration approach that supports environment variables, YAML files, and code-based defaults for flexible deployment across development, staging, and production environments.

## Configuration Architecture

### 1. **Centralized Configuration Module**
- **Location**: `src/ciq_test_2/defs/ttb_config.py`
- **Purpose**: Provides unified configuration classes with environment variable support
- **Key Classes**:
  - `TTBGlobalConfig`: Base configuration for all TTB assets
  - `TTBExtractionConfig`: Configuration specific to data extraction
  - `TTBProcessingConfig`: Configuration for data processing assets
  - `TTBDimensionalConfig`: Configuration for dimensional modeling

### 2. **Environment Variable Configuration**
Environment variables provide runtime configuration overrides:

```bash
# S3 Configuration
TTB_S3_BUCKET=ciq-dagster                     # Primary S3 bucket
TTB_RAW_PREFIX=1-ttb-raw-data                 # Raw data prefix
TTB_PROCESSED_PREFIX=2-ttb-processed-data     # Processed data prefix
TTB_DIMENSIONAL_PREFIX=3-ttb-dimensional-data # Dimensional data prefix

# Processing Configuration
TTB_MAX_SEQUENCE=100                          # Max sequences per batch
TTB_REQUEST_DELAY=0.5                         # Delay between TTB requests
TTB_MAX_RETRIES=3                             # Max retries for failed requests
TTB_SSL_VERIFY=false                          # TTB has SSL cert issues

# Data Quality Settings
TTB_ENABLE_QUALITY_CHECKS=true                # Enable data validation
TTB_ENABLE_DEDUP=true                         # Enable deduplication
TTB_CREATE_PARTITIONED=true                   # Create partitioned outputs

# Dimensional Modeling
TTB_DATE_START_YEAR=2015                      # Date dimension start year
TTB_DATE_END_YEAR=2030                        # Date dimension end year
TTB_SURROGATE_KEYS=true                       # Use surrogate keys

# Deployment Environment
DAGSTER_DEPLOYMENT=dev                        # Environment: dev/staging/prod
```

### 3. **YAML Configuration Files**
Environment-specific YAML configurations in `config/` directory:

- **`config/dev.yaml`**: Development environment settings
  - Small batch sizes for testing
  - Debug logging enabled
  - Conservative rate limiting

- **`config/prod.yaml`**: Production environment settings
  - Large batch sizes for efficiency
  - Production bucket configurations
  - Monitoring and alerting enabled

### 4. **Asset Configuration Migration**

**Before** (Multiple inconsistent Config classes):
```python
# In ttb_fact_assets.py
class FactConfig(Config):
    s3_bucket: str = "ttb-structured-data"  # ❌ Wrong bucket
    input_prefix: str = "2-ttb-processed-data"
    output_prefix: str = "3-ttb-dimensional-data"

# In ttb_dimensional_assets.py
class DimensionalConfig(Config):
    s3_bucket: str = "ttb-structured-data"  # ❌ Wrong bucket
```

**After** (Centralized configuration):
```python
# All assets now use TTBGlobalConfig
from .ttb_config import TTBGlobalConfig

@asset(partitions_def=daily_partitions)
def fact_products(
    context: AssetExecutionContext,
    config: TTBGlobalConfig,  # ✅ Centralized config
    s3_resource: S3Resource
) -> Dict[str, Any]:
    # Access configuration through environment variables
    bucket = config.s3_bucket                    # From TTB_S3_BUCKET
    input_path = config.processed_data_prefix    # From TTB_PROCESSED_PREFIX
    output_path = config.dimensional_data_prefix # From TTB_DIMENSIONAL_PREFIX
```

## Usage Examples

### 1. **Development Environment**
```bash
# Set environment variables
export DAGSTER_DEPLOYMENT=dev
export TTB_S3_BUCKET=ciq-dagster
export TTB_MAX_SEQUENCE=100

# Run pipeline
PYTHONPATH=src dagster asset materialize --select fact_products --partition "2024-01-01"
```

### 2. **Production Environment**
```bash
# Set production environment variables
export DAGSTER_DEPLOYMENT=prod
export TTB_S3_BUCKET=ttb-production-data
export TTB_MAX_SEQUENCE=10000
export TTB_REQUEST_DELAY=1.0

# Run with production settings
PYTHONPATH=src dagster asset materialize --select fact_products --partition "2024-01-01"
```

### 3. **Job Configuration with YAML**
```bash
# Use job-specific configuration
PYTHONPATH=src dagster job execute --job ttb_full_day_assets --config-file config/dev.yaml
```

## Configuration Hierarchy

Configuration values are resolved in this order (highest priority first):

1. **Environment Variables** (Runtime overrides)
2. **YAML Configuration Files** (Environment-specific)
3. **Code Defaults** (Development defaults)

## Best Practices

### 1. **Environment Variables for Infrastructure**
Use environment variables for infrastructure settings that change between deployments:
- S3 bucket names
- AWS regions
- Processing limits
- Rate limiting settings

### 2. **YAML for Environment-Specific Settings**
Use YAML files for complex environment-specific configurations:
- Multiple related settings
- Deployment-specific feature flags
- Environment-specific processing parameters

### 3. **Code Defaults for Development**
Provide sensible defaults in code for local development:
- Development bucket names
- Conservative processing limits
- Debug settings enabled

### 4. **Deployment Environment Detection**
Use `DAGSTER_DEPLOYMENT` environment variable to automatically configure environment-specific behavior:

```python
from .ttb_config import DeploymentConfig

if DeploymentConfig.is_production():
    # Production-specific logic
    enable_monitoring = True
    batch_size = 10000
elif DeploymentConfig.is_development():
    # Development-specific logic
    enable_debug = True
    batch_size = 100
```

## Migration Benefits

1. **Consistency**: Single source of truth for configuration
2. **Flexibility**: Environment variable overrides for any deployment
3. **Security**: Sensitive values externalized from code
4. **Maintainability**: Centralized configuration reduces duplication
5. **Deployment**: Easy configuration per environment without code changes

## Configuration Validation

The TTB configuration system includes built-in validation:
- Type checking for all configuration values
- Environment variable parsing with defaults
- Deployment environment validation
- S3 bucket and prefix validation

This ensures configuration errors are caught early and provide clear error messages for debugging.