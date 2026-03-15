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
dagster_pipeline/
├── __init__.py
├── assets/
│   ├── __init__.py
│   ├── sec_download.py          # SEC data download and GCS upload
│   └── meltano_integration.py   # Meltano integration and BigQuery load
├── jobs/
│   ├── __init__.py
│   └── sec_pipeline.py          # Pipeline orchestration jobs
├── schedules/
│   ├── __init__.py
│   └── sec_schedules.py        # Automated execution schedules
└── repository.py                # Dagster repository definition
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

```bash
# Start the Dagster web UI
uv run --with dagster dagster dev --port 3001

# Access the web UI at http://127.0.0.1:3001
```

### Running Pipeline Jobs

#### Via Web UI
1. Navigate to http://127.0.0.1:3001
2. Select the desired job (e.g., `sec_pipeline_job`)
3. Configure parameters (year, quarters, bucket name)
4. Click "Launch Run"

#### Via CLI
```bash
# Run complete pipeline for specific year
uv run --with dagster dagster job execute --job sec_pipeline_job --config '{"ops": {"sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}}}}'

# Run download only
uv run --with dagster dagster job execute --job sec_download_job --config '{"ops": {"sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}}}}'
```

### Configuration Examples

#### Single Quarter Load
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

#### Full Year Load
```yaml
ops:
  sec_raw_data:
    config:
      year: 2023
      quarters: ["q1", "q2", "q3", "q4"]
  sec_gcs_data:
    config:
      year: 2023
      quarters: ["q1", "q2", "q3", "q4"]
      bucket_name: "dsai-m2-bucket"
      keep_local: false
```

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
# If you get import errors, ensure the dagster_pipeline directory is in Python path
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
cd meltano-ingestion
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
