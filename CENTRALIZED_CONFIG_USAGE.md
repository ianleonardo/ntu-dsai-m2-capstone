# Centralized Configuration Usage Guide

## Overview

The SEC pipeline now uses centralized configuration to eliminate duplicate parameter definitions across assets and jobs. **Year and Dataset** are the two core parameters that control all pipeline components.

## 🎯 Benefits

- **Single Source of Truth**: Year and Dataset defined once
- **Consistency**: All assets use same configuration
- **Easy Updates**: Change year/dataset in one place
- **Override Support**: Per-run overrides when needed

## 📁 Configuration Files

### 1. Central Configuration Module
**File**: `dataprocessing/dagster_orchestration/config/pipeline_config.py`

This module provides:
- `SECPipelineConfig` class with all parameters
- `get_pipeline_config()` function for centralized access
- Environment variable integration
- Validation and helper methods

### 2. Environment Configuration
**File**: `.env.centralized`

**Core Parameters** (control all assets):
```bash
SEC_YEAR=2024                        # Controls all assets
BIGQUERY_DATASET=insider_transactions    # Controls all assets
SEC_LOAD_QUARTER=                      # Optional: q1, q2, q3, q4
SEC_BATCH_SIZE=1000                      # BigQuery batch size
GOOGLE_PROJECT_ID=ntu-dsai-488112
DRY_RUN=false
```

## 🚀 Usage Examples

### 1. Default Configuration (Recommended)
All assets automatically use centralized configuration:

```bash
# Set year and dataset in .env.centralized
SEC_YEAR=2024
BIGQUERY_DATASET=insider_transactions

# Run any asset/job - all use 2024 and insider_transactions
dagster asset materialize sec_direct_ingestion
dagster job launch -j sec_pipeline_direct_complete_job
```

### 2. Override for Specific Run
Override configuration for individual runs:

```bash
# Override year and dataset for one execution
dagster asset materialize sec_direct_ingestion --config '{
    "year": 2023,
    "dataset": "insider_transactions_test"
}'

# Override multiple parameters
dagster job launch -j sec_pipeline_direct_complete_job --config '{
    "year": 2023,
    "dataset": "insider_transactions_dev",
    "quarters": ["q1", "q2"],
    "batch_size": 500
}'
```

### 3. Programmatic Usage
In Python code:

```python
from dataprocessing.dagster_orchestration.config.pipeline_config import get_pipeline_config

# Get default configuration
config = get_pipeline_config()
print(f"Year: {config.year}, Dataset: {config.dataset}")

# Override specific parameters
config = get_pipeline_config(year=2023, dataset="test_dataset")
print(f"Year: {config.year}, Dataset: {config.dataset}")
```

## 🔄 Asset Integration

### How Assets Use Centralized Config

1. **Import**: `from ..config.pipeline_config import get_pipeline_config`
2. **Get Config**: `pipeline_cfg = get_pipeline_config(**overrides)`
3. **Use Values**: `pipeline_cfg.year`, `pipeline_cfg.dataset`, etc.
4. **Validation**: `pipeline_cfg.validate_quarters()`

### Example Asset Implementation

```python
@asset
def my_asset(context: AssetExecutionContext, config: MyAssetConfig):
    # Get centralized configuration with optional overrides
    pipeline_cfg = get_pipeline_config(
        year=config.year,
        dataset=config.dataset,
    )
    
    # Use configuration values
    context.log.info(f"Processing year {pipeline_cfg.year} in dataset {pipeline_cfg.dataset}")
    quarters = pipeline_cfg.validate_quarters()
    
    # Asset logic here...
```

## 📋 Configuration Parameters

| Parameter | Description | Default | Environment Variable |
|------------|-------------|-----------|-------------------|
| `year` | SEC data year (controls all assets) | Current year | `SEC_YEAR` |
| `dataset` | BigQuery dataset (controls all assets) | `insider_transactions` | `BIGQUERY_DATASET` |
| `quarters` | Quarters to process | `["q1","q2","q3","q4"]` | `SEC_LOAD_QUARTER` |
| `project_id` | GCP project ID | `ntu-dsai-488112` | `GOOGLE_PROJECT_ID` |
| `batch_size` | BigQuery batch size | `1000` | `SEC_BATCH_SIZE` |
| `dry_run` | Dry run mode | `False` | `DRY_RUN` |

## 🎛️ Setup Instructions

### 1. Configure Environment
```bash
# Copy centralized environment template
cp .env.centralized .env

# Edit parameters as needed
nano .env
```

### 2. Update Core Parameters (Most Common Changes)
```bash
# Set year and dataset for all pipeline operations
export SEC_YEAR=2024
export BIGQUERY_DATASET=insider_transactions

# Or edit .env file to persist
```

### 3. Run Pipeline
```bash
# All assets and jobs will use the centralized year and dataset
dagster asset materialize sec_direct_ingestion
dagster job launch -j sec_pipeline_direct_complete_job
```

## 🔧 Advanced Usage

### Multiple Environments
Create different environment files:

```bash
# Development
.env.dev      -> SEC_YEAR=2023, BIGQUERY_DATASET=insider_dev

# Production  
.env.prod     -> SEC_YEAR=2024, BIGQUERY_DATASET=insider_transactions

# Testing
.env.test     -> SEC_YEAR=2024, BIGQUERY_DATASET=insider_test, DRY_RUN=true
```

### Dynamic Configuration
Programmatic configuration changes:

```python
# Configuration for different datasets
datasets = ["insider_transactions", "insider_test", "insider_dev"]
for dataset in datasets:
    config = get_pipeline_config(year=2024, dataset=dataset)
    # Process each dataset...
```

## 🎉 Benefits Achieved

### Before Centralization
```python
# Each asset defined its own config
class AssetConfig(Config):
    year: int = 2024  # Duplicated
    dataset: str = "insider_transactions"  # Duplicated
    quarter: str = "q1"  # Duplicated
```

### After Centralization
```python
# Single source of truth for core parameters
pipeline_cfg = get_pipeline_config()
# year and dataset controlled centrally
```

### Results
- ✅ **DRY Principle**: Don't Repeat Yourself
- ✅ **Maintainability**: Change year/dataset in one place
- ✅ **Consistency**: All assets use same values
- ✅ **Flexibility**: Override when needed
- ✅ **Validation**: Built-in parameter validation

## 🚨 Migration Notes

When migrating existing assets:

1. **Remove duplicate config classes**
2. **Import centralized config**: `from ..config.pipeline_config import get_pipeline_config`
3. **Update asset signatures**: Use optional overrides for year/dataset
4. **Replace parameter access**: Use `pipeline_cfg.year` and `pipeline_cfg.dataset`
5. **Test**: Verify behavior matches previous implementation

## 📞 Support

For configuration issues:

1. Check `.env` file has correct `SEC_YEAR` and `BIGQUERY_DATASET`
2. Verify environment variables are set
3. Use `get_pipeline_config()` for debugging
4. Check validation errors in asset logs

The centralized configuration ensures all pipeline components work with consistent **year and dataset** parameters while maintaining the flexibility to override when needed.
