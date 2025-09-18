# Integration Tests

Integration tests for TTB pipeline stages and end-to-end workflows.

## Files

- `test_backward_backfill.py` - Tests for backfill functionality and historical data processing
- `test_enhanced_partitioning.py` - Tests for multi-dimensional partitioning behavior
- `test_single_partition.py` - Tests for single partition processing workflows
- `test_certificate.py` - Tests for certificate data processing pipeline

## Running Integration Tests

```bash
# From project root
python tests/integration/test_single_partition.py
python tests/integration/test_enhanced_partitioning.py

# Or with pytest (if configured)
pytest tests/integration/
```

## Requirements

These tests may require:
- S3 access for testing data storage
- TTB pipeline assets to be available
- Specific partition configurations