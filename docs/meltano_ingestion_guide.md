# Meltano Ingestion Guide

This comprehensive guide covers data ingestion using Meltano for both SEC insider data and market data.

## Overview

The ingestion layer uses Meltano to extract data from local files and load it into Google BigQuery. Meltano provides a unified interface for running extractors (taps) and loaders (targets).

### Data Sources

1. **SEC Insider Data**: Form 3/4/5 insider transactions from SEC EDGAR
2. **S&P 500 Companies**: Company list and metadata  
3. **Market Data**: Daily stock price data from Yahoo Finance
4. **SEC Company Tickers**: Company reference data from SEC

### Data Flow

```
Source Data → Local Staging → Meltano (Extract/Load) → BigQuery Tables
```

## Prerequisites

Before running ingestion, ensure you have:

1. **Complete Setup**: Follow the [Pre-setup Guideline](setup.md)
2. **Python Environment**: Use `uv` for dependency management
3. **GCP Credentials**: Service account with BigQuery and Storage access
4. **Staging Data**: Data files in `dataprocessing/meltano_ingestion/staging/`

## Step-by-Step Installation

### 1. Navigate to Meltano Project

```bash
cd dataprocessing/meltano_ingestion
```

### 2. Install Meltano Dependencies

```bash
# Install all required plugins and dependencies
uv run --project .. meltano install
```

**Note**: If you encounter "Extractor/loader is not known to Meltano", run:
```bash
uv run --project .. meltano lock
uv run --project .. meltano install
```

### 3. Configure Environment Variables

Ensure your `.env` file (in repo root) contains:

```env
# Required for all ingestion
GOOGLE_PROJECT_ID=ntu-dsai-488112
TARGET_BIGQUERY_CREDENTIALS_PATH=/absolute/path/to/your/gcp-service-account-key.json
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/your/gcp-service-account-key.json

# Optional overrides
BIGQUERY_DATASET=insider_transactions
```

### 4. Verify Plugin Installation

Check that all required plugins are installed:

```bash
uv run --project .. meltano list plugins
```

Expected plugins:
- `tap-csv` (for SEC data)
- `tap-jsonl` (for company tickers)
- `target-bigquery` (for all data loading)

## Data Ingestion Jobs

### SEC Insider Data Ingestion

#### What It Does
- Downloads SEC Form 3/4/5 data from GCS bucket
- Processes quarterly TSV files into staging CSVs
- Loads 6 tables: SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS, etc.
- Applies BigQuery partitioning by year
- Uses upsert logic to handle duplicate keys

#### How to Run

**Option A: Full Pipeline (Recommended)**
```bash
# From project root
cd dataprocessing/meltano_ingestion
uv run --project .. bash run_load_sec_insider.sh

# With specific year/quarter
uv run --project .. bash run_load_sec_insider.sh --year 2025 --quarter q1
```

**Option B: Manual Steps**
```bash
# Step 1: Sync from GCS to staging
uv run --project .. python ../scripts/sync_sec_from_gcs.py

# Step 2: Load into BigQuery
uv run --project .. meltano run tap-csv target-bigquery
```

#### Tables Loaded

| Table | Primary Key(s) | Description |
|--------|------------------|-------------|
| SEC_SUBMISSION | ACCESSION_NUMBER | XML submissions, filer/issuer |
| SEC_REPORTINGOWNER | ACCESSION_NUMBER, RPTOWNERCIK | Reporting owner details |
| SEC_NONDERIV_TRANS | ACCESSION_NUMBER, NONDERIV_TRANS_SK | Non-derivative transactions |

### S&P 500 Companies Ingestion

#### What It Does
- Downloads latest S&P 500 constituent list
- Processes company metadata and sector information
- Loads to `SP500_COMPANIES` table

#### How to Run

```bash
uv run meltano run load-sp500-companies
```

Or use the helper script:
```bash
chmod +x run_load_sp500_companies.sh
./run_load_sp500_companies.sh
```

### Market Data Ingestion

#### What It Does
- Downloads daily OHLCV data for S&P 500 stocks
- Processes price and volume information
- Creates clustered table for efficient queries

#### How to Run

```bash
uv run meltano run load-sp500-stock-daily
```

### SEC Company Tickers Ingestion

#### What It Does
- Downloads SEC company_tickers.json reference file
- Processes ticker-to-CIK mappings
- Loads to `SEC_COMPANY_TICKERS` table

#### How to Run

```bash
uv run meltano run load-sec-tickers
```

## BigQuery Table Optimization

### Clustering Configuration

The `SP500_STOCK_DAILY` table uses BigQuery clustering for performance:

```yaml
# In meltano.yml
target-bigquery:
  cluster_on_key_properties: true
```

**Manual Clustering Setup** (if needed):
```bash
bq update --clustering_fields=symbol,date 'ntu-dsai-488112:insider_transactions.SP500_STOCK_DAILY'
```

### Partitioning

Tables are automatically partitioned by year for efficient time-series queries:
- SEC tables: Yearly partitioning on `year` column
- Market data: Optimized for date-based queries

## Running Multiple Jobs

### Individual Jobs

```bash
# Run specific jobs
uv run meltano run load-sec-insider
uv run meltano run load-sp500-companies
uv run meltano run load-sp500-stock-daily
uv run meltano run load-sec-tickers
```

### All Jobs

```bash
# Run all configured jobs
uv run meltano run job:all
```

## Helper Scripts

The project includes shell scripts for common tasks:

```bash
# Make scripts executable
chmod +x *.sh

# Available scripts
./run_load_sec_insider.sh      # SEC data ingestion
./run_load_sp500_companies.sh  # S&P 500 companies
./run_load_sp500_stock_daily.sh # Market data
./run_load_sec_tickers.sh       # SEC tickers
```

## Troubleshooting

### Common Issues

**Credentials Error**
```bash
# Verify credentials path
ls -la "$TARGET_BIGQUERY_CREDENTIALS_PATH"

# Test BigQuery connection
bq ls
```

**Missing Files**
```bash
# Check staging directory
ls -la staging/

# Verify expected files exist
find staging/ -name "*.csv"
```

**Plugin Installation Issues**

*Lockfile exists error*:
```bash
# This is not fatal - just run install
uv run --project .. meltano install
```

*ModuleNotFoundError: No module named 'pkg_resources'*:
```bash
# Install setuptools in Meltano venv
./.meltano/loaders/target-bigquery/venv/bin/pip install setuptools
```

*Pendulum build error on Python 3.12*:
```bash
# Use Python 3.11
uv python pin 3.11
uv sync --group meltano
uv run --project .. meltano install
```

**BigQuery Permission Errors**
- Ensure service account has BigQuery Data Editor and BigQuery Job User roles
- Verify project ID matches: `ntu-dsai-488112`

**Data Not Found**
- Check GCS bucket structure: `gs://bucket/sec-data/year/yearq1/`
- Ensure service account has Storage Object Viewer permission

**Duplicate Key Errors**
- This is normal with `upsert: true` - existing rows are updated
- Tables maintain uniqueness on primary keys

## Advanced Configuration

### Environment Variables

Fine-tune ingestion behavior:

```env
# SEC Data Configuration
SEC_LOAD_YEAR=2025
SEC_LOAD_QUARTER=q1
GCS_BUCKET=dsai-m2-bucket
GCS_SEC_PREFIX=sec-data
STAGING_DIR=staging

# BigQuery Configuration
BIGQUERY_DATASET=insider_transactions
```

### Accumulating Multiple Years

With `upsert: true`, you can safely load multiple years:

```bash
# Load 2024 data
SEC_LOAD_YEAR=2024 uv run --project .. bash run_load_sec_insider.sh

# Load 2025 data (updates existing rows)
SEC_LOAD_YEAR=2025 uv run --project .. bash run_load_sec_insider.sh
```

## Monitoring and Validation

### Check Job Status

```bash
# View Meltano logs
uv run --project .. meltano run tap-csv target-bigquery --log-level=debug

# Check BigQuery tables
bq query 'SELECT COUNT(*) FROM insider_transactions.SEC_SUBMISSION'
```

### Data Validation

After ingestion, verify data quality:

```sql
-- Check row counts
SELECT 
  table_name,
  row_count,
  last_updated
FROM (
  SELECT 'SEC_SUBMISSION' as table_name, COUNT(*) as row_count, MAX(_PARTITIONTIME) as last_updated
  FROM insider_transactions.SEC_SUBMISSION
  UNION ALL
  SELECT 'SEC_REPORTINGOWNER' as table_name, COUNT(*) as row_count, MAX(_PARTITIONTIME) as last_updated
  FROM insider_transactions.SEC_REPORTINGOWNER
)
```

## References

- **SEC Data**: [Insider Transactions Data Sets](https://www.sec.gov/developer)
- **Meltano**: [Getting Started Guide](https://docs.meltano.com/getting-started)
- **BigQuery**: [Clustered Tables Documentation](https://cloud.google.com/bigquery/docs/creating-clustered-tables)
- **Project Scripts**: `scripts/` directory for data processing utilities

## Next Steps

After successful ingestion:

1. **Run Transformations**: Follow [dbt Transformation Guide](dbt.md)
2. **Start Orchestration**: Follow [Orchestration Guide](orchestration.md)  
3. **Launch Dashboard**: Follow [Dashboard Setup Guide](dashboard_setup.md)
