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
- Reads **local tab-delimited** SEC extract files from `dataprocessing/meltano_ingestion/staging/`
- Runs Meltano job **`load-sec-insider`**: `tap-csv` → `target-bigquery`
- Loads **three** streams into BigQuery (see `meltano.yml`): `SEC_SUBMISSION`, `SEC_REPORTINGOWNER`, `SEC_NONDERIV_TRANS`
- Loader uses **`upsert: true`** on the tap’s key columns so re-runs merge instead of blindly duplicating

This repo does **not** ship `run_load_sec_insider.sh`. Use the Meltano job below, or use **Dagster** `sec_direct_ingestion` to load SEC data straight from the SEC website into BigQuery (see [Orchestration Guide](orchestration.md)).

#### Staging files (required before `load-sec-insider`)

Place these files under `dataprocessing/meltano_ingestion/staging/` (paths match `meltano.yml`):

| File | Meltano entity | Keys |
|------|----------------|------|
| `SUBMISSION.csv` | `SEC_SUBMISSION` | `ACCESSION_NUMBER` |
| `REPORTINGOWNER.csv` | `SEC_REPORTINGOWNER` | `ACCESSION_NUMBER`, `RPTOWNERCIK` |
| `NONDERIV_TRANS.csv` | `SEC_NONDERIV_TRANS` | `ACCESSION_NUMBER`, `NONDERIV_TRANS_SK` |

Delimiter in config is **tab** (`\t`). How you obtain these files (SEC bulk download, your own ETL, etc.) is outside this wrapper; the Meltano step only loads what is already in `staging/`.

#### How to Run

**Option A: Meltano job (recommended for CSV staging)**
```bash
cd dataprocessing/meltano_ingestion
uv run --project .. meltano install    # first time / after meltano.yml changes
uv run --project .. meltano run load-sec-insider
```

**Option B: Explicit tap → target**
```bash
cd dataprocessing/meltano_ingestion
uv run --project .. meltano run tap-csv target-bigquery
```

**Option C: Direct SEC → BigQuery (no Meltano for these tables)**  
Use Dagster asset **`sec_direct_ingestion`** (see [Orchestration Guide](orchestration.md)).

#### Tables Loaded

| Table | Primary Key(s) | Description |
|--------|------------------|-------------|
| SEC_SUBMISSION | ACCESSION_NUMBER | XML submissions, filer/issuer |
| SEC_REPORTINGOWNER | ACCESSION_NUMBER, RPTOWNERCIK | Reporting owner details |
| SEC_NONDERIV_TRANS | ACCESSION_NUMBER, NONDERIV_TRANS_SK | Non-derivative transactions |

#### Form 4 monthly files for current year (no BigQuery upload)

When SEC quarterly bulk ZIPs are not yet published (for example, early in a new year), use:
`scripts/download_sec_form4_monthly.py`.

This utility:
- Downloads exact **Form `4`** filings from EDGAR daily indexes (not `4/A`)
- Parses ownership XML and extracts only `SUBMISSION`, `REPORTINGOWNER`, `NONDERIV_TRANS`
- Writes monthly TSV files and local run state/failure logs
- Can optionally upload generated monthly TSVs to BigQuery tables (`sec_submission`, `sec_reportingowner`, `sec_nonderiv_trans`)

```bash
cd /path/to/repo
uv run --project . python scripts/download_sec_form4_monthly.py \
  --start-date 2026-01-01 \
  --end-date "$(date +%F)" \
  --user-agent "Your Name your_email@example.com" \
  --output-dir "dataprocessing/meltano_ingestion/staging/sec_form4_2026_monthly" \
  --resume
```

Expected outputs by month (`YYYY-MM`):
- `SUBMISSION_YYYY-MM.tsv`
- `REPORTINGOWNER_YYYY-MM.tsv`
- `NONDERIV_TRANS_YYYY-MM.tsv`
- `state_YYYY-MM.json` (resume state)
- `failures_YYYY-MM.tsv` (per-filing errors)
- `run_summary.json` (run totals)

Optional upload step:

```bash
uv run --project . python scripts/download_sec_form4_monthly.py \
  --start-date 2026-01-01 \
  --end-date "$(date +%F)" \
  --user-agent "Your Name your_email@example.com" \
  --output-dir "dataprocessing/meltano_ingestion/staging/sec_form4_2026_monthly" \
  --resume \
  --upload-bigquery \
  --bq-project-id "ntu-dsai-488112" \
  --bq-dataset "insider_transactions"
```

Dagster job:
- Job name: `sec_form4_monthly_pipeline_job`
- Config supports date range explicitly:

```yaml
from_date: "2026-01-01"
to_date: "2026-01-31"
user_agent: "Your Name your_email@example.com"
upload_bigquery: true
bq_project_id: "ntu-dsai-488112"
bq_dataset: "insider_transactions"
resume: true
```

Post-load validation summary in Dagster:
- Job name: `sec_form4_monthly_summary_job`
- Reads row counts from BigQuery for the same date range and reports:
  - `sec_submission_rows`
  - `sec_reportingowner_rows`
  - `sec_nonderiv_trans_rows`
  - `distinct_accession_numbers`

```yaml
from_date: "2026-01-01"
to_date: "2026-01-31"
bq_project_id: "ntu-dsai-488112"   # optional if GOOGLE_PROJECT_ID is set
bq_dataset: "insider_transactions"   # optional if BIGQUERY_DATASET is set
```

Single combined Dagster run (recommended):
- Job name: `sec_form4_monthly_combined_job`
- Executes:
  1. `sec_form4_monthly_ingestion` (download monthly files + optional BQ upload)
  2. `dbt_sp500_insider_transactions_form4` (materialize `sp500_insider_transactions`)
  3. `sec_form4_monthly_bigquery_summary` (BQ row-count summary for same range)

```yaml
from_date: "2026-01-01"
to_date: "2026-01-31"
user_agent: "Your Name your_email@example.com"
upload_bigquery: true
bq_project_id: "ntu-dsai-488112"
bq_dataset: "insider_transactions"
resume: true
max_requests_per_second: 5.0
```

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

There is **no** `job:all` in `meltano.yml`. Run each job when needed, for example:

```bash
cd dataprocessing/meltano_ingestion
uv run --project .. meltano run load-sec-insider
uv run --project .. meltano run load-sec-tickers
uv run --project .. meltano run load-sp500-companies
uv run --project .. meltano run load-sp500-stock-daily
```

## Helper Scripts

Shell wrappers live in `dataprocessing/meltano_ingestion/` (SEC insider CSV load has **no** wrapper — use `meltano run load-sec-insider`):

```bash
cd dataprocessing/meltano_ingestion
chmod +x *.sh   # optional

# Available scripts
./run_load_sp500_companies.sh   # S&P 500 companies → JSONL → Meltano
./run_load_sp500_stock_daily.sh # yfinance → JSONL → Meltano
./run_load_sec_tickers.sh      # SEC company_tickers.json → JSONL → Meltano
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
- For **Meltano SEC insider**: confirm `staging/SUBMISSION.csv`, `REPORTINGOWNER.csv`, `NONDERIV_TRANS.csv` exist and are tab-delimited as configured.

**Duplicate Key Errors**
- This is normal with `upsert: true` - existing rows are updated
- Tables maintain uniqueness on primary keys

## Advanced Configuration

### Environment Variables

Fine-tune ingestion behavior:

```env
# BigQuery / GCP (Meltano target-bigquery)
GOOGLE_PROJECT_ID=ntu-dsai-488112
GOOGLE_CLOUD_PROJECT=ntu-dsai-488112
BIGQUERY_DATASET=insider_transactions
TARGET_BIGQUERY_CREDENTIALS_PATH=/absolute/path/to/service-account.json

# (No GCS setup in this project)
```

Variables such as **`SEC_YEAR`**, **`SEC_LOAD_QUARTER`**, and related fields are used by **Dagster / `scripts/download_sec_to_bigquery.py`** (direct SEC pipeline), **not** by `tap-csv` in Meltano. For year/quarter loads via Dagster, see [Orchestration Guide](orchestration.md).

### Accumulating Multiple Years / Quarters (Meltano CSV path)

With **`upsert: true`**, you can run `load-sec-insider` again after replacing or appending extracts in `staging/` (same key columns merge in BigQuery). There is no wrapper script; repeat:

```bash
cd dataprocessing/meltano_ingestion
# Update staging/*.csv for the next period, then:
uv run --project .. meltano run load-sec-insider
```

For automated year/quarter pulls without maintaining CSV staging, prefer **Dagster `sec_direct_ingestion`**.

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
