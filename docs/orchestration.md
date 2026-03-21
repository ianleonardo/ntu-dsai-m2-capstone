# Orchestration Guideline (Dagster)

This document provides instructions on how to orchestrate the entire data pipeline using Dagster.

## Overview

Dagster serves as the orchestration layer, connecting ingestion (Meltano) and transformation (dbt). It manages dependencies, scheduling, and error handling.

## Configuration

The Dagster project is located in `dataprocessing/dagster_orchestration`.

- `repository.py`: Main entry point defining assets, jobs, and schedules.
- `assets/`: Definitions for individual data assets (Meltano jobs, dbt models, custom logic).

## Running Dagster

### 1. Using Dagster UI (Highly Recommended)

To launch the web-based environment for development and monitoring:

```bash
uv run dagster dev
```

The UI will typically be available at `http://localhost:3000`.

From the UI, you can:
- Visualize the asset graph.
- Launch manual runs of specific jobs or the entire pipeline.
- Inspect logs and task status in real-time.
- Manage schedules and sensors.

### 2. Using Dagster CLI

To list assets in the repository:
```bash
uv run dagster asset list -m dataprocessing.dagster_orchestration.repository
```

To execute a specific job:
```bash
uv run dagster job execute -m dataprocessing.dagster_orchestration.repository -j sec_pipeline_direct_complete_job
```

## Available Jobs

- `sec_pipeline_direct_complete_job`: The full end-to-end pipeline (Ingestion + dbt Transformation).
- `sec_direct_ingestion_job`: Only the ingestion phase.
- `dbt_transformation_job_direct`: Only the dbt transformation phase.
- `sp500_stock_daily_pipeline_job`: Pipeline for S&P 500 daily stock data.

## Schedules

Schedules are defined in `dataprocessing/dagster_orchestration/schedules/`. Check the Dagster UI's "Schedules" tab to enable automated runs for:
- Quarterly SEC data ingestion.
- Monthly validation.
- Weekly health checks.

## Troubleshooting

- **Port Conflict**: If port 3000 is taken, use `uv run dagster dev -p [PORT]`.
- **Module Not Found**: Ensure you are running commands from the root directory and have run `uv sync`.
