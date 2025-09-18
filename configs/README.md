# TTB Pipeline Configurations

This directory contains production and development configuration files for the TTB pipeline.

## 📁 Configuration Files

### Environment Configurations
- **dev.yaml** - Development environment configuration
- **prod.yaml** - Production environment configuration

### Pipeline Configurations
- **complete_pipeline_config.yaml** - Complete pipeline configuration with all stages
- **full_day_config.yaml** - Configuration for full day processing
- **max_batch_config.yaml** - Configuration with maximum batch settings
- **partition_config.yaml** - Partition-specific configuration settings

## 🚀 Usage

Use these configurations with Dagster commands:

```bash
# Environment-specific runs
dagster asset materialize --config configs/dev.yaml
dagster asset materialize --config configs/prod.yaml

# Complete pipeline run
dagster asset materialize --config configs/complete_pipeline_config.yaml

# Full day processing
dagster asset materialize --config configs/full_day_config.yaml

# Maximum batch processing
dagster asset materialize --config configs/max_batch_config.yaml

# Partition-specific run
dagster asset materialize --config configs/partition_config.yaml
```

## ⚙️ Configuration Categories

### Pipeline Stage Configurations
- **Raw Data Extraction**: TTB website scraping settings
- **Data Processing**: Parsing and transformation settings
- **Consolidation**: Cross-partition data merging settings
- **Analytics**: Dimensional modeling and fact table settings

### Environment Settings
- **S3 Configuration**: Bucket names, prefixes, regions
- **Rate Limiting**: Request delays and retry settings
- **Batch Processing**: Sequence limits and failure thresholds

## 🔧 Configuration Management

For environment-specific configurations, see:
- **Development**: Use programmatic configs in `src/ciq_test_2/config/environments.py`
- **Testing**: Use test configs in `tests/configs/`
- **Production**: Use these configuration files

## 📋 Configuration Schema

All configurations support:
- **ops**: Operation-specific settings for each asset
- **resources**: Resource configuration (S3, IO managers)
- **execution**: Execution environment settings

Refer to the pipeline code for the complete schema and available options.