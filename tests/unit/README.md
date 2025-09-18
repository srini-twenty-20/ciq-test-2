# Unit Tests

Unit tests for individual TTB pipeline components and utilities.

## Files

- `test_asset_checks.py` - Tests for asset check functionality and validation logic
- `test_ttb_parsing.py` - Tests for HTML parsing utilities and data extraction
- `test_reference_data.py` - Tests for TTB reference data loading and caching
- `test_reference_validation.py` - Tests for reference data validation logic
- `test_beer_parsing.py` - Tests for beer-specific parsing functionality

## Running Unit Tests

```bash
# From project root
python tests/unit/test_ttb_parsing.py
python tests/unit/test_asset_checks.py

# Or with pytest (if configured)
pytest tests/unit/
```

## Note

These tests may require network access to TTB website for reference data testing.