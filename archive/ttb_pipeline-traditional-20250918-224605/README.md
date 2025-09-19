# TTB COLA Pipeline v2.0

A clean, focused Dagster pipeline for extracting and processing TTB COLA certificate data.

## Overview

This pipeline extracts complete TTB COLA certificate entities by fetching both the detail view and certificate view for each TTB ID, preserving raw HTML content and creating consolidated structured data.

## Architecture

### Multi-Asset Design
- **Single multi-asset** handles complete COLA entity extraction
- **Three outputs**: detail HTML, certificate HTML, and consolidated data
- **Atomic operations**: success/failure per complete COLA entity
- **Data integrity**: ensures both views are fetched together

### Key Features
- ✅ **HTML Preservation**: Raw HTML stored for audit and reprocessing
- ✅ **Consolidated Data**: Structured data combining both views
- ✅ **Data Validation**: Cross-view consistency checking
- ✅ **Configurable Partitions**: Flexible date and receipt method partitions
- ✅ **Error Handling**: TTB error page detection and consecutive failure stopping
- ✅ **Rate Limiting**: Respectful request throttling

## Configuration

### Environment Variables

#### Partition Configuration
```bash
# Testing (minimal partitions)
TTB_START_DATE=2024-01-01
TTB_END_DATE=2024-01-01
TTB_RECEIPT_METHODS=001

# Production (full partitions)
TTB_START_DATE=2015-01-01
TTB_END_DATE=2025-12-31
TTB_RECEIPT_METHODS=001,002,003,000
```

#### Extraction Configuration
```bash
TTB_S3_BUCKET=ciq-dagster
TTB_MAX_SEQUENCE_PER_BATCH=1000
TTB_CONSECUTIVE_FAILURE_THRESHOLD=10
TTB_REQUEST_DELAY_SECONDS=0.5
```

#### AWS Configuration
```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key_id
AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Usage

### Running the Pipeline

```bash
# Single partition
dagster asset materialize --select ttb_cola_extraction --partition "2024-01-01|001"

# Multiple partitions
dagster asset materialize --select ttb_cola_extraction --partition-def date_range=2024-01-01:2024-01-03
```

### Development Mode
```bash
# Start Dagster UI
dagster dev -m ttb_pipeline.definitions

# Access at http://localhost:3000
```

## Data Outputs

### 1. TTB COLA Detail HTML (`ttb_cola_detail_html`)
Raw HTML content from the detail view containing product information, ingredients, etc.

### 2. TTB COLA Certificate HTML (`ttb_cola_certificate_html`)
Raw HTML content from the certificate view containing approval status, dates, etc.

### 3. TTB COLA Consolidated Data (`ttb_cola_consolidated_data`)
Structured data combining both views with:
- Basic entity fields (product name, applicant, approval date)
- Data consistency validation results
- Source tracking (content hashes, timestamps)
- Processing metadata

## File Structure

```
src/ttb_pipeline/
├── __init__.py
├── definitions.py          # Main Dagster definitions
├── README.md
├── assets/
│   ├── __init__.py
│   └── cola_extraction.py  # Main extraction multi-asset
├── config/
│   ├── __init__.py
│   ├── partitions.py       # Partition definitions
│   └── settings.py         # Configuration classes
├── resources/
│   ├── __init__.py
│   └── io_managers.py      # S3 storage managers
└── utils/
    ├── __init__.py
    └── ttb_utils.py         # TTB-specific utilities
```

## Next Steps

1. **HTML Parsing**: Implement actual parsing logic in `parse_cola_detail_view()` and `parse_cola_certificate_view()`
2. **Asset Checks**: Add data quality validation assets
3. **Downstream Processing**: Create assets for further data transformation
4. **Monitoring**: Add comprehensive logging and alerting
5. **Testing**: Create unit and integration tests