# Orchestration Guideline (Dagster)

This document provides instructions on how to orchestrate the entire data pipeline using Dagster, including integration with the dashboard UI, data ingestion, and dbt transformations.

## Overview

Dagster serves as the orchestration layer, connecting ingestion (Meltano/Python) and transformation (dbt). It manages dependencies, scheduling, and error handling while providing visibility into pipeline execution, data freshness, and materialization tracking.

## Architecture

The Dagster integration follows a layered asset-based architecture seamlessly linking the raw data sources to the Dashboard API.

```text
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Ingestion     │───▶│   BigQuery      │
│   (SEC / Yahoo) │    │(Meltano/Python) │    │   Staging       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Dashboard UI  │◀───│   Dagster       │◀───│   dbt Models    │
│   (FastAPI)     │    │   Orchestration │    │   (Transform)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Project Structure

```text
dataprocessing/
├── dagster_orchestration/
│   ├── __init__.py
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── sec_download.py          # SEC data download and GCS upload
│   │   └── meltano_integration.py   # Meltano integration and BigQuery load
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── sec_pipeline.py          # Pipeline orchestration jobs
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── sec_schedules.py         # Automated execution schedules
│   └── repository.py                # Dagster repository definition
└── meltano_ingestion/               # Meltano project
```

## Environment Setup

Required environment variables (set in `.env` at the project root):

```env
# Google Cloud Configuration
GOOGLE_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCS_BUCKET_NAME=dsai-m2-bucket

# BigQuery Credentials (for Meltano)
TARGET_BIGQUERY_CREDENTIALS_PATH=/path/to/service-account.json

# API Configuration (optional)
FINNHUB_API_KEY=your-finnhub-api-key
VANTAGE_API_KEY=your-alpha-vantage-api-key
```

## Key Components & Asset Architecture

### 1. Data Ingestion Assets
- `sec_raw_data` / `sec_gcs_data`: Downloads SEC data and uploads to Google Cloud Storage.
- `meltano_staging_data` / `bigquery_sec_data`: Prepares Meltano staging area and loads GCS data into BigQuery.
- `sp500_stock_daily_staging_data`: Market data ingestion from Yahoo Finance.

### 2. Transformation Assets
- `dbt_insider_transformation`: Full dbt model execution with testing to create dimensional models.
- `bigquery_sp500_stock_daily_data`: Market data processing and modeling.

### 3. Summary Assets
- `sec_pipeline_summary`: Generates pipeline execution metadata and triggers dashboard cache invalidation.

### Asset Dependencies
```text
SEC Assets ───▶ dbt_insider_transformation ───▶ Dashboard Cache Nullification
Market Data Assets ───▶ bigquery_sp500_stock_daily_data
```

## Running Dagster

### Starting the Development Server
**IMPORTANT**: Run the following **from the project root** (the directory containing `pyproject.toml`), not from `dataprocessing/dagster_orchestration/`.

```bash
uv run --with dagster dagster dev --port 3001
```
*(If port 3001 is taken, use a different port, e.g., 3000).*

Access the Dagster web UI at `http://127.0.0.1:3001`.

### Available Jobs

- **`sec_pipeline_job`** / **`sec_pipeline_direct_complete_job`**: The full end-to-end SEC pipeline (Download → GCS → BigQuery → dbt → Summary).
- **`sec_download_job`**: Download and GCS upload only.
- **`sp500_stock_daily_pipeline_job`**: Complete market data pipeline for S&P 500 prices.

### Launching Jobs via Web UI (Configuring execution)

For parameterized jobs (like SEC pipelines requiring a specific year and quarter), you must provide run configuration using the "Launchpad" in the UI. 

1. Go to **Jobs** and select e.g., **sec_pipeline_job**.
2. Click **Launchpad** / **Launch Run**.
3. In the Config tab, paste the appropriate YAML configuration:

**Single Quarter Full Pipeline:**
```yaml
ops:
  sec_raw_data:
    config:
      year: 2023
      quarters: ["q1"]
  sec_gcs_data:
    config:
      year: 2023
      quarters: ["q1"]
      bucket_name: "dsai-m2-bucket"
      keep_local: false
  meltano_staging_data:
    config:
      year: 2023
      quarters: ["q1"]
      bucket_name: "dsai-m2-bucket"
  bigquery_sec_data:
    config:
      year: 2023
      quarters: ["q1"]
  sec_pipeline_summary:
    config:
      year: 2023
      quarters: ["q1"]
```

*Note: For a full year, set `quarters: ["q1", "q2", "q3", "q4"]` or omit the quarters parameter depending on your asset schema.*

### Launching Jobs via CLI

```bash
# Run download only using inline JSON config
uv run --with dagster dagster job execute --job sec_download_job --config '{"ops": {"sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}}}}'

# Run specific assets
uv run --with dagster dagster asset materialize --select sec_direct_ingestion
```

## Automated Scheduling

Dagster allows configuring automated schedules:

- **quarterly_sec_schedule**: Triggers at the start of each quarter for the previous quarter's complete SEC refresh.
- **monthly_validation_schedule**: Runs on the 1st of each month to perform data quality checks.
- **weekly_health_check_schedule**: Triggers every Sunday to monitor pipeline performance and connectivity.
- **year_end_schedule**: Executes full year processing and archival.

## Integration with Dashboard UI

The FastAPI backend automatically synchronizes with Dagster updates via cache invalidation.

```python
# When summary assets materialize, relevant caches are cleared
@asset
def sec_pipeline_summary():
    result = process_pipeline_data()
    # Trigger dashboard cache refresh
    invalidate_dashboard_cache("insider_transactions")
    return result
```
The Dashboard UI polls `/api/pipeline/status` and `/api/data/freshness` to display the latest execution metrics.

## Troubleshooting

### Common Issues

**Missing required config entry 'ops'**
If you try to run a job and get this error, it means the job requires execution parameters (like year/quarters). Use the "Scaffold all default config" button in the Launchpad or paste a YAML snippet as shown above.

**Module Not Found / Import Errors**
Ensure you are running `dagster dev` from the **project root folder**, not from inside the `dagster_orchestration` folder.
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

**Meltano Extraction Issues**
If the ingestion steps fail, ensure Meltano plugins are successfully installed:
```bash
cd dataprocessing/meltano_ingestion
uv run meltano install
```

**BigQuery / GCP Auth Errors**
Ensure `GOOGLE_APPLICATION_CREDENTIALS` is pointing to an active service account with BigQuery Admin / Storage Admin roles.

### Debugging Assets
Use the UI's **Asset Graph** to track materialization logs ("Events" tab) and visualize specific failure points. Use incremental materialization when debugging large datasets to save BigQuery costs.
