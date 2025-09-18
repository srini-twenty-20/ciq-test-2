# TTB Pipeline Tests

This directory contains all tests and development scripts for the TTB pipeline, organized by purpose and scope.

## 📁 Directory Structure

```
tests/
├── unit/                    # Unit tests for individual components
├── integration/            # Integration tests for pipeline stages
├── development_scripts/    # Development and debugging scripts
├── configs/               # Test configuration files
└── README.md             # This file
```

## 🧪 Test Categories

### Unit Tests (`unit/`)
Tests for individual functions and utilities:
- `test_asset_checks.py` - Asset check functionality tests
- `test_ttb_parsing.py` - HTML parsing utility tests
- `test_reference_data.py` - Reference data loading tests
- `test_reference_validation.py` - Reference data validation tests
- `test_beer_parsing.py` - Beer-specific parsing tests

### Integration Tests (`integration/`)
Tests for pipeline components and workflows:
- `test_backward_backfill.py` - Backfill functionality tests
- `test_enhanced_partitioning.py` - Partition behavior tests
- `test_single_partition.py` - Single partition processing tests
- `test_certificate.py` - Certificate data processing tests

### Development Scripts (`development_scripts/`)
Scripts used during development and debugging:
- `test_image_downloads.py` - Image downloading functionality
- `test_session_direct.py` - Direct session testing
- `test_session_images.py` - Image session testing

### Test Configurations (`configs/`)
Configuration files for testing:
- `full_day_test_config.yaml` - Full day processing configuration
- `test_small_config.yaml` - Minimal test configuration

## 🚀 Running Tests

### Unit Tests
```bash
# Run individual unit tests
python tests/unit/test_ttb_parsing.py
python tests/unit/test_asset_checks.py

# Run all unit tests (if using pytest)
pytest tests/unit/
```

### Integration Tests
```bash
# Run individual integration tests
python tests/integration/test_single_partition.py
python tests/integration/test_enhanced_partitioning.py

# Run all integration tests (if using pytest)
pytest tests/integration/
```

### Development Scripts
```bash
# Run development scripts for debugging
python tests/development_scripts/test_session_direct.py
python tests/development_scripts/test_image_downloads.py
```

## 🔧 Test Configuration

Use test configuration files for different scenarios:

```bash
# Using test configs with Dagster
dagster asset materialize --config tests/configs/test_small_config.yaml
dagster asset materialize --config tests/configs/full_day_test_config.yaml
```

## 📋 Test Guidelines

### Adding New Tests

1. **Unit Tests**: Place in `unit/` for testing individual functions
2. **Integration Tests**: Place in `integration/` for testing pipeline stages
3. **Development Scripts**: Place in `development_scripts/` for debugging tools

### Test Naming Convention

- Use descriptive names: `test_[component]_[functionality].py`
- Follow the existing pattern in each directory
- Include docstrings explaining test purpose

### Dependencies

Tests may require:
- Local TTB pipeline code (`src/ciq_test_2/`)
- Environment variables for S3 access
- Sample data files for parsing tests
- Network access for reference data tests

## 🏗️ Migration Notes

These test files were originally scattered in the root directory and have been organized into this proper structure for better maintainability and clarity.

### Legacy Locations
- All `test_*.py` files moved from project root
- All `*test*.yaml` config files moved from project root
- Organized by functionality and test scope

## 🔄 Future Improvements

Consider implementing:
- Proper pytest framework with fixtures
- Test data management and mocking
- CI/CD integration for automated testing
- Coverage reporting
- Performance benchmarking tests