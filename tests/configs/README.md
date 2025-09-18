# Test Configurations

Configuration files for testing different TTB pipeline scenarios.

## Files

- `full_day_test_config.yaml` - Configuration for full day processing tests
- `test_small_config.yaml` - Minimal configuration for quick testing

## Usage

Use these configurations with Dagster commands:

```bash
# Using minimal test config
dagster asset materialize --config tests/configs/test_small_config.yaml

# Using full day test config
dagster asset materialize --config tests/configs/full_day_test_config.yaml
```

## Purpose

- **Quick Testing**: Use small config for rapid iteration
- **Full Testing**: Use full day config for comprehensive testing
- **Development**: Reference configurations for different test scenarios