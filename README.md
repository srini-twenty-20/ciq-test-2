# TTB Pipeline - CIQ Test 2

A modern Dagster pipeline for extracting, processing, and analyzing TTB (Tax and Trade Bureau) COLA data following best practices for asset organization, configuration management, and data quality.

## 🏗️ Project Structure

```
├── src/ciq_test_2/        # Main pipeline code
│   ├── assets/            # Organized asset modules
│   ├── config/            # Configuration management
│   ├── resources/         # Resource definitions
│   ├── utils/             # Utility functions
│   ├── jobs/              # Job and schedule definitions
│   └── checks/            # Data quality checks
├── tests/                 # Organized test structure
├── configs/               # Production configurations
├── docs/                  # Comprehensive documentation
└── archive/               # Legacy files (safely archived)
```

## 🚀 Quick Start

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then active the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## 📋 Pipeline Usage

### Running with Configurations

```bash
# Complete pipeline with all stages
dagster asset materialize --config configs/complete_pipeline_config.yaml

# Full day processing
dagster asset materialize --config configs/full_day_config.yaml

# Single partition for testing
dagster asset materialize --select ttb_raw_data --partition "2024-01-01|001-cola-detail"
```

### Environment Configuration

```bash
# Development (default)
DAGSTER_ENVIRONMENT=development dg dev

# Production with full partitions
TTB_PARTITION_START_DATE=2015-01-01 \
TTB_PARTITION_END_DATE=2025-12-31 \
TTB_RECEIPT_METHODS=001,002,003,000 \
TTB_DATA_TYPES=cola-detail,certificate \
dg dev
```

## 📚 Documentation

- **System Guide**: `docs/TTB_SYSTEM_GUIDE.md` - Complete system overview
- **Code Organization**: `src/ciq_test_2/README.md` - Architecture details
- **Migration Details**: `docs/MIGRATION_PLAN.md` - Legacy to modern migration
- **Configuration**: `docs/CONFIGURATION.md` - Configuration management
- **Testing**: `tests/README.md` - Test structure and usage

## 🎯 Key Features

- **Modern Architecture**: Organized by Dagster best practices
- **Environment Flexibility**: Easy dev/test/prod configuration switching
- **Data Quality**: Comprehensive asset checks and validation
- **Scalable Design**: Asset groups for independent scaling
- **Full Documentation**: Comprehensive guides and examples

## Learn more

To learn more about Dagster:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
