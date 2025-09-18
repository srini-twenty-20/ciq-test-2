# Development Scripts

Scripts used during development, debugging, and exploratory testing of TTB pipeline functionality.

## Files

- `test_image_downloads.py` - Development script for testing image downloading functionality
- `test_session_direct.py` - Direct session testing and debugging script
- `test_session_images.py` - Image session testing and validation script

## Usage

These scripts are intended for development and debugging purposes:

```bash
# Run development scripts for debugging
python tests/development_scripts/test_session_direct.py
python tests/development_scripts/test_image_downloads.py
```

## Purpose

- **Debugging**: Test specific functionality during development
- **Exploration**: Investigate TTB website behavior and responses
- **Validation**: Verify assumptions about data formats and structures

## Note

These scripts may:
- Make direct HTTP requests to TTB website
- Download test data for analysis
- Require network access and may be rate-limited