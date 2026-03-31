# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Instructions
* written by Zac on 14 March 2026

Always run in venv:
Mac
```bash
source .venv/bin/activate
```
Windows
```bash
.venv\Scripts\activate
```

If necessary, setup .venv using:
```bash
uv venv .venv --python 3.12
```

# Project Overview

Stock Analytics Data Pipeline with Insider Trading Dashboard — an ELT pipeline ingesting SEC Form 4 insider trading data, S&P 500 company metadata, and market price data into Google BigQuery, with a FastAPI backend and Next.js frontend dashboard.

Live deployment:
- Frontend: https://insideralpha.theluwak.com/
- API: https://insider-backend-1091217007062.asia-southeast1.run.app/docs

# Architecture

```
SEC EDGAR / Yahoo Finance / DataHub
        ↓
Ingestion (Meltano + Python scripts in /scripts/)
        ↓
BigQuery Staging Tables
        ↓
dbt Transformation (/dataprocessing/dbt_insider_transactions/)
        ↓
BigQuery Marts (star schema: dim_* + fct_* + materialized views)
        ↓
FastAPI Backend (/visualisation/backend/)
        ↓
Next.js Frontend (/visualisation/frontend/)
```

Orchestration via Dagster (`/dataprocessing/dagster_orchestration/`) coordinates the entire pipeline with 5 jobs and 4 schedules.

# Key Commands

## Python / Data Pipeline

```bash
# Install Python dependencies
uv pip install -r requirements.txt

# Run SEC ingestion directly to BigQuery
python scripts/download_sec_to_bigquery.py

# Run market data ingestion
python scripts/fetch_sp500_stock_daily_yfinance_to_jsonl.py

# Run dbt transformations
cd dataprocessing/dbt_insider_transactions
dbt run
dbt test

# Run a single dbt model
dbt run --select sp500_insider_transactions

# Start Dagster UI (orchestration)
cd dataprocessing/dagster_orchestration
dagster dev
```

## Backend (FastAPI)

```bash
cd visualisation/backend
uvicorn main:app --reload
# API docs: http://localhost:8000/docs
```

## Frontend (Next.js)

```bash
cd visualisation/frontend
npm install
npm run dev
# App: http://localhost:3000

npm run build
npm run lint
```

## Meltano

```bash
cd dataprocessing/meltano_ingestion
meltano run tap-csv target-bigquery
meltano run tap-jsonl target-bigquery
```

# Data Warehouse Design

BigQuery star schema with three layers:
- **Staging**: raw tables (`sec_submission`, `sec_reportingowner`, `sec_nonderiv_trans`, `sp500_companies`, `sp500_stock_daily`)
- **Dimensions**: `dim_sec_submission`, `dim_sec_reporting_owner`, `dim_sp500_company`, `dim_sp500_reporting_owner`
- **Facts/Marts**: `fct_sec_nonderiv_line`, `fct_insider_transactions`, materialized views `sp500_insider_transactions` (partitioned yearly, clustered on symbol+date) and `sp500_stock_daily`

Custom dbt macros in `/dataprocessing/dbt_insider_transactions/macros/`:
- `parse_sec_date`: handles multiple SEC date formats (DD-MON-YYYY, YYYY-MM-DD, YYYYMMDD, numeric)
- `transaction_code_type_label`: maps SEC codes to business labels

# Environment Variables

Copy `.env.example` to `.env`. Key variables:
- `GOOGLE_PROJECT_ID` — GCP project
- `GOOGLE_APPLICATION_CREDENTIALS` — service account key path
- `BIGQUERY_DATASET` — target dataset name
- `SEC_YEAR` — year to ingest (default: 2024)
- `SEC_LOAD_QUARTER` — optional quarter filter
- `DRY_RUN` — test mode flag

# Dagster Assets & Jobs

Assets (in `/dataprocessing/dagster_orchestration/assets/`):
- `sec_direct_ingestion` → `dbt_insider_transformation` (main pipeline)
- `sp500_stock_daily_staging_data` → `bigquery_sp500_stock_daily_data` (market data)

Jobs: `sec_pipeline_direct_complete_job` (full end-to-end), plus individual jobs per stage.

Schedules: quarterly SEC refresh, monthly validation, weekly health check, year-end processing.

# FastAPI Backend

`/visualisation/backend/core/`:
- `bigquery.py` — BigQuery client wrapper
- `cache.py` — in-memory TTL caching (search directories warmed on startup)
- `config.py` — pydantic-settings config

`/visualisation/backend/api/endpoints.py` — all REST routes (stock/insider search, transactions, sector analytics).
