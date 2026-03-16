# Dagster Integration Guide

## Overview

This guide documents the Dagster integration for the NTU DSAI Capstone Project SEC data pipeline. The integration provides orchestration, scheduling, and monitoring capabilities for the existing Meltano-based pipeline.

## Architecture

The Dagster integration follows a layered architecture:

```
┌─────────────────────────────────────────┐
│           Dagster Orchestration         │
├─────────────────────────────────────────┤
│  Jobs    │  Assets   │  Schedules       │
├─────────────────────────────────────────┤
│     SEC Download │ GCS Upload ──────────┤
│     Meltano Integration │ BigQuery Load │
├─────────────────────────────────────────┤
│           Existing Scripts              │
│  download_sec_to_bucket.py │ Meltano    │
└─────────────────────────────────────────┘
```

## Project Structure

```
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
└── meltano_ingestion/               # Meltano project (SEC insider forms + company tickers → BigQuery)
```

## Key Components

### Assets

#### SEC Download Assets (`sec_download.py`)
- `sec_raw_data`: Downloads SEC data for specified year/quarters
- `sec_gcs_data`: Uploads downloaded data to Google Cloud Storage

#### Meltano Integration Assets (`meltano_integration.py`)
- `meltano_staging_data`: Prepares Meltano staging area with GCS data
- `bigquery_sec_data`: Loads data to BigQuery using Meltano
- `sec_pipeline_summary`: Provides execution summary and metadata

### Jobs

#### Complete Pipeline Jobs
- `sec_pipeline_job`: Full download → GCS → BigQuery pipeline
- `sec_download_job`: Download and GCS upload only
- `sec_bigquery_load_job`: GCS → BigQuery loading only
- `sec_backfill_job`: Historical data backfill

### Schedules

#### Automated Execution
- `quarterly_sec_schedule`: Quarterly automated loading
- `monthly_validation_schedule`: Monthly validation checks
- `weekly_health_check_schedule`: Weekly pipeline health checks
- `year_end_schedule`: Year-end complete load

## Usage

### Starting Dagster Development Server

Run the following **from the project root** (the directory that contains `pyproject.toml`), not from `dataprocessing/dagster_orchestration/`. The CLI looks for `[tool.dagster]` in `pyproject.toml` in the current working directory.

```bash
# From project root: start the Dagster web UI
uv run --with dagster dagster dev --port 3001

# Or pass the code location explicitly (also from project root)
uv run --with dagster dagster dev -m dataprocessing.dagster_orchestration.repository --port 3001

# Access the web UI at http://127.0.0.1:3001
```

Alternatively, use the recommended CLI (from project root): `uv run --with dagster dg dev --port 3001`.

### Running Pipeline Jobs

#### Via Web UI (year and quarter)

Each job has default run config (year 2023, quarters `["q1"]`) so the Launchpad can show prefilled values. If the Config tab is empty, click **Scaffold all default config** (or paste one of the YAML samples below) so you do not get "Missing required config entry 'ops'".

1. Open the Dagster UI (e.g. http://127.0.0.1:3001).
2. Go to **Jobs** and select the job (e.g. **sec_pipeline_job** or **sec_download_job**).
3. Click **Launch Run**.
4. In the launch dialog, open the **Config** tab and paste the appropriate YAML sample:

   **Full pipeline (sec_pipeline_job) – copy this into Config and launch:**
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

   **Download only (sec_download_job):**
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
   ```

   **Multiple quarters (e.g. 2023 Q1 and Q2):**
   ```yaml
   ops:
     sec_raw_data:
       config:
         year: 2023
         quarters: ["q1", "q2"]
     sec_gcs_data:
       config:
         year: 2023
         quarters: ["q1", "q2"]
         bucket_name: "dsai-m2-bucket"
         keep_local: false
   ```

   **Full year (all four quarters):** omit `quarters` or set to `["q1", "q2", "q3", "q4"]`.
6. Click **Launch Run** to start the job with that config.

For **sec_bigquery_load_job** you do not need run config (it uses data already in GCS from a previous run).

#### Via CLI
```bash
# Run complete pipeline (include config for all five assets; see Configuration Examples below)
uv run --with dagster dagster job execute --job sec_pipeline_job --config '{"ops": {"sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}}, "sec_gcs_data": {"config": {"year": 2023, "quarters": ["q1"], "bucket_name": "dsai-m2-bucket", "keep_local": false}}, "meltano_staging_data": {"config": {"year": 2023, "quarters": ["q1"], "bucket_name": "dsai-m2-bucket"}}, "bigquery_sec_data": {"config": {"year": 2023, "quarters": ["q1"]}}, "sec_pipeline_summary": {"config": {"year": 2023, "quarters": ["q1"]}}}}'

# Run download only
uv run --with dagster dagster job execute --job sec_download_job --config '{"ops": {"sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}}}}'
```

### Configuration Examples

Run config is passed under `ops.<asset_key>.config`. All assets that accept config need it in the same run (no data is passed between steps via the I/O manager).

**sec_download_job** (download + GCS only): provide `sec_raw_data` and optionally `sec_gcs_data` config.

**sec_pipeline_job** (full pipeline): provide config for all five assets (`sec_raw_data`, `sec_gcs_data`, `meltano_staging_data`, `bigquery_sec_data`, `sec_pipeline_summary`) with the same year/quarters.

#### Single Quarter Load (sec_download_job or minimal sec_pipeline_job)
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
```

#### Full Pipeline (sec_pipeline_job) – single quarter
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

#### Full Year Load
Use `quarters: ["q1", "q2", "q3", "q4"]` (or omit quarters) for each asset in the full-pipeline example above.

## Environment Variables

Required environment variables (set in `.env`):

```env
# Google Cloud Configuration
GOOGLE_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=dsai-m2-bucket

# BigQuery Credentials (for Meltano)
TARGET_BIGQUERY_CREDENTIALS_PATH=/path/to/service-account.json

# SEC API Configuration (optional)
FINNHUB_API_KEY=your-finnhub-api-key
VANTAGE_API_KEY=your-alpha-vantage-api-key
```

## Deployment

### Local Development
1. Install dependencies: `uv add --optional dagster`
2. Start development server: `uv run --with dagster dagster dev`
3. Access web UI for monitoring and execution

### Production Deployment (GCP)

#### Option 1: Dagster Cloud
1. Sign up for Dagster Cloud
2. Connect your repository
3. Configure GCP integration
4. Deploy schedules and jobs

#### Option 2: Self-Hosted on GCP
1. Use Google Cloud Run or Compute Engine
2. Configure PostgreSQL for storage
3. Set up GCP service accounts
4. Deploy with Docker

### Docker Deployment
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install uv
RUN uv pip install --system -e .[dagster]

EXPOSE 3000
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000"]
```

## Monitoring and Observability

### Web UI Features
- **Asset Lineage**: Visualize data dependencies
- **Run History**: Track execution history and performance
- **Materialization Logs**: Detailed execution logs
- **Schedule Status**: Monitor automated runs
- **Asset Health**: Data quality and freshness metrics

### Key Metrics
- Pipeline execution time
- Data volume processed
- Success/failure rates
- Data freshness
- Error tracking

## Troubleshooting

### Common Issues

#### Import Errors
```bash
# If you get import errors, ensure the dagster_orchestration directory is in Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

#### GCP Authentication
```bash
# Set up GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

#### Meltano Integration
```bash
# Ensure Meltano plugins are installed
cd dataprocessing/meltano_ingestion
meltano install
```

### Debug Mode
```bash
# Run with verbose logging
uv run --with dagster dagster dev --port 3001 --verbose
```

## Next Steps

1. **Configure Schedules**: Enable automated quarterly loading
2. **Set Up Monitoring**: Configure alerts for pipeline failures
3. **Add Data Quality Checks**: Implement validation assets
4. **Scale to Production**: Deploy to GCP with proper storage
5. **Add Visualizations**: Create dashboards for pipeline metrics

## Integration with Existing Workflow

The Dagster integration is designed to work alongside your existing Meltano workflow:

- **Non-disruptive**: Existing scripts continue to work
- **Incremental**: Can adopt Dagster features gradually
- **Compatible**: Uses same data formats and destinations
- **Enhanced**: Adds scheduling, monitoring, and orchestration

You can continue using your existing manual workflow while gradually adopting Dagster features for automation and monitoring.
