# TTB COLA Data Extraction System

## 🎯 Overview

This system implements a comprehensive, production-ready TTB (Alcohol and Tobacco Tax and Trade Bureau) COLA data extraction pipeline using modern Dagster concepts. It efficiently handles historical backfills and daily incremental updates while respecting TTB's rate limiting requirements.

## 🏗️ System Architecture

### Core Components

1. **TTB ID Structure Parser** (`ttb_utils.py`)
   - Parses 14-digit TTB IDs: `YYJJJRRRSSSSS`
   - Handles year/Julian day/receipt method/sequence extraction
   - Provides rate limiting and sequence detection utilities

2. **Partitioned Assets** (`partitioned_assets.py`)
   - Multi-dimensional partitioning: Date × Receipt Method
   - Intelligent sequence detection (stops after 5 consecutive failures)
   - Built-in 0.5-second rate limiting
   - Comprehensive S3 storage with organized keys

3. **Job Definitions** (`ttb_jobs.py`)
   - Historical backfill job (2015-present)
   - Daily forward-fill job (previous day)
   - Test jobs for single partitions

4. **Schedules**
   - Daily 10am ET execution
   - Weekend catch-up processing
   - Manual backfill controls

## 🚀 Usage Guide

### Starting the System

```bash
# Start Dagster development server
PYTHONPATH=src dagster dev -m ciq_test_2.definitions

# Access UI at: http://127.0.0.1:55775
```

### Available Assets & Jobs

#### Assets
- **`ttb_cola_partitioned`**: Main partitioned asset for TTB data extraction
- **`ttb_cola_summary`**: Downstream summary statistics asset
- **`url_to_s3_asset`**: Original single-URL asset (legacy)

#### Jobs
- **`ttb_historical_backfill_job`**: Historical data backfill (2015-present)
- **`ttb_daily_forward_fill_job`**: Daily incremental updates
- **`ttb_test_single_partition`**: Single partition testing
- **`materialize_url_to_s3_job`**: Legacy single-URL job

#### Schedules
- **`ttb_daily_schedule`**: 10am ET daily execution (STOPPED by default)
- **`ttb_weekend_catchup_schedule`**: Monday weekend catch-up (STOPPED by default)
- **`ttb_backfill_schedule`**: Manual backfill control (STOPPED by default)

### TTB ID Structure

**Format**: `YYJJJRRRSSSSS` (14 digits)

- **YY**: Calendar year (last 2 digits)
- **JJJ**: Julian date (001-366)
- **RRR**: Receipt method
  - `001`: e-filed
  - `002`: mailed
  - `003`: overnight
  - `000`: hand delivered
- **SSSSSS**: Sequential number (000001-999999, resets daily per method)

**Example**: `24001001000001`
- Year: 2024
- Julian Day: 1 (January 1st)
- Receipt Method: 001 (e-filed)
- Sequence: 000001 (first of the day)

## 📊 S3 Storage Structure

Files are organized hierarchically in S3:

```
s3://ciq-dagster/ttb-cola-data/
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   ├── receipt_method=001/
│   │   │   │   ├── 24001001000001.html
│   │   │   │   ├── 24001001000002.html
│   │   │   │   └── ...
│   │   │   ├── receipt_method=002/
│   │   │   └── receipt_method=003/
│   │   ├── day=02/
│   │   └── ...
│   ├── month=02/
│   └── ...
└── year=2025/
```

## ⚙️ Configuration

### Asset Configuration

```yaml
# Default configuration for TTB partitioned assets
bucket_name: "ciq-dagster"
max_sequence_per_batch: 100  # Limit for testing
receipt_methods: [1, 2, 3]   # Exclude hand delivered (0)
```

### Rate Limiting

- **0.5 seconds** between requests (built-in)
- **Intelligent stopping**: After 5 consecutive failures
- **Respectful to TTB servers**

### Partition Management

**Multi-dimensional partitions**:
- **Date dimension**: Daily from 2015-01-01 to present
- **Receipt method dimension**: 001, 002, 003

**Partition keys format**: `YYYY-MM-DD|RRR`
Example: `2024-01-01|001`

## 🔄 Operational Workflows

### 1. Historical Backfill (2015-Present)

```python
# Option A: Use backfill job in UI
# 1. Go to Jobs → ttb_historical_backfill_job
# 2. Click "Launch Backfill"
# 3. Select date range and receipt methods
# 4. Configure batch size (recommended: 30 days)

# Option B: Programmatic backfill
from datetime import date
from ciq_test_2.defs.ttb_utils import TTBIDUtils

# Generate partition keys for January 2024
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 31)
partition_keys = generate_partition_keys_for_date_range(start_date, end_date)
```

### 2. Daily Forward-Fill

```python
# Automatic via schedule (10am ET daily)
# Manual trigger:
# 1. Go to Jobs → ttb_daily_forward_fill_job
# 2. Select yesterday's partition keys
# 3. Launch run

# Schedule activation:
# 1. Go to Schedules → ttb_daily_schedule
# 2. Click "Start Schedule"
```

### 3. Testing Single Partitions

```python
# Use test job for development
# 1. Go to Jobs → ttb_test_single_partition
# 2. Select specific partition (e.g., "2024-01-01|001")
# 3. Monitor logs for rate limiting and sequence detection
```

## 📋 Best Practices

### 1. Rate Limiting Compliance
- **Built-in delays**: 0.5s between requests
- **Batch processing**: Limit concurrent partitions
- **Monitoring**: Watch for HTTP errors and adjust

### 2. Error Handling
- **Graceful failures**: System continues after individual TTB ID failures
- **Comprehensive logging**: All requests and responses logged
- **Retry logic**: Automatic retries for transient failures

### 3. Data Quality
- **Sequence validation**: Ensures no missed TTB IDs
- **Metadata tracking**: Success rates, processing times
- **Comprehensive statistics**: Available in asset metadata

### 4. Monitoring & Alerting
- **Asset metadata**: Success rates, failure counts
- **Schedule monitoring**: Daily execution health
- **S3 storage tracking**: File counts and sizes

## 🛠️ Development & Testing

### Testing TTB ID Utilities

```python
from ciq_test_2.defs.ttb_utils import TTBIDUtils

# Parse TTB ID
components = TTBIDUtils.parse_ttb_id("24001001000001")
print(f"Year: {components.year}")  # 2024
print(f"Julian Day: {components.julian_day}")  # 1
print(f"Receipt Method: {components.receipt_method}")  # 1
print(f"Sequence: {components.sequence}")  # 1

# Generate TTB IDs for a date
from datetime import date
for ttb_id in TTBIDUtils.generate_ttb_ids_for_date(
    date(2024, 1, 1),
    receipt_methods=[1, 2, 3],
    max_sequence=10
):
    print(ttb_id)
```

### Local Development

```bash
# Start with module approach for stability
PYTHONPATH=src dagster dev -m ciq_test_2.definitions

# Validate definitions
dg check defs

# Test specific partitions
# Use ttb_test_single_partition job in UI
```

## 🔐 Security & Compliance

### Environment Variables
```bash
# Required for S3 access
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# Optional for LocalStack
LOCALSTACK_ENDPOINT_URL=http://localhost:4566
```

### SSL Handling
- **Disabled verification** for TTB endpoints (certificate issues)
- **Warning suppression** for clean logs
- **Production consideration**: Monitor for SSL improvements

## 📈 Scaling Considerations

### Performance Optimization
- **Partition-level parallelism**: Process multiple dates simultaneously
- **Receipt method parallelism**: Run different methods concurrently
- **Sequence batching**: Group sequences for efficiency

### Resource Management
- **Memory usage**: Temporary files for large responses
- **Network throttling**: Respect TTB rate limits
- **S3 costs**: Organized storage reduces listing costs

## 🚨 Troubleshooting

### Common Issues

1. **Package Installation Errors**
   ```bash
   # Solution: Use module approach
   PYTHONPATH=src dagster dev -m ciq_test_2.definitions
   ```

2. **Rate Limiting Violations**
   ```python
   # Check TTBSequenceTracker logs
   # Adjust max_sequence_per_batch configuration
   ```

3. **S3 Upload Failures**
   ```bash
   # Verify AWS credentials
   # Check bucket permissions
   # Monitor S3 service status
   ```

4. **Schedule Not Running**
   ```python
   # Verify schedule status (default: STOPPED)
   # Check timezone configuration (America/New_York)
   # Review daemon logs
   ```

## 📚 Advanced Usage

### Custom Partition Selection

```python
# Select specific receipt methods
partition_keys = [
    "2024-01-01|001",  # e-filed only
    "2024-01-01|002",  # mailed only
]

# Date range with specific methods
from datetime import date, timedelta
start_date = date(2024, 1, 1)
end_date = start_date + timedelta(days=7)  # One week
keys = generate_partition_keys_for_date_range(
    start_date, end_date, receipt_methods=["001", "003"]
)
```

### Backfill Strategies

```python
# Strategy 1: Full historical (careful with rate limits)
# Use ttb_backfill_schedule manually

# Strategy 2: Incremental historical
# Process one month at a time
# Monitor success rates and adjust

# Strategy 3: Priority-based
# Focus on e-filed (001) first, then others
# Higher success rate, faster results
```

---

## ✅ System Status

**Implementation Complete**: All components implemented and tested
**Rate Limiting**: ✅ 0.5s between requests
**Intelligent Detection**: ✅ Stop after 5 consecutive failures
**Partitioning**: ✅ Multi-dimensional (Date × Receipt Method)
**Scheduling**: ✅ Daily 10am ET with timezone handling
**S3 Storage**: ✅ Organized hierarchical structure
**Error Handling**: ✅ Comprehensive logging and recovery
**Testing**: ✅ Single partition test jobs available

**Ready for Production**: Yes, with proper monitoring and gradual rollout