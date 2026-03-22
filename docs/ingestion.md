# Ingestion Guideline (Meltano)

This document provides instructions on how to run data ingestion using Meltano.

## Overview

The ingestion layer uses Meltano to extract data from local CSV/JSONL files and load it into Google BigQuery. The configurations are located in `dataprocessing/meltano_ingestion`.

## Prerequisites

- Complete the [Pre-setup Guideline](setup.md).
- Ensure your staging data is present in `dataprocessing/meltano_ingestion/staging/`.

## Running Ingestion

Navigate to the Meltano project directory:

```bash
cd dataprocessing/meltano_ingestion
```

### 1. Install Plugins

Install the required Meltano extractors and loaders:

```bash
uv run meltano install
```

### 2. Run Ingestion Jobs

You can run individual ingestion jobs using `meltano run`. Available jobs are defined in `meltano.yml`.

#### SEC Insider Data
```bash
uv run meltano run load-sec-insider
```

#### SEC Tickers
```bash
uv run meltano run load-sec-tickers
```

#### S&P 500 Companies
```bash
uv run meltano run load-sp500-companies
```

#### S&P 500 Daily Stock Data
```bash
uv run meltano run load-sp500-stock-daily
```

`SP500_STOCK_DAILY` is created with **BigQuery clustering on the tap’s key columns** (`symbol`, then `date`) via `cluster_on_key_properties: true` on `target-bigquery` in `meltano.yml`. That helps queries scoped by symbol (e.g. latest close per ticker).

**Already have an unclustered table?** Clustering is applied when the target **first creates** the table. BigQuery does **not** support `clustering_fields` inside `ALTER TABLE ... SET OPTIONS` (that only covers options like `description`, `expiration_timestamp`, partition settings, etc.).

To add clustering on an existing table, use the **`bq` CLI** (comma‑separated fields, **no spaces**). Adjust project, dataset, table id, and column names to match your table schema:

```bash
bq update --clustering_fields=symbol,date 'YOUR_PROJECT:insider_transactions.SP500_STOCK_DAILY'
```

If you do not use `bq`, you can **recreate** the table with `CREATE TABLE ... CLUSTER BY ... AS SELECT * FROM ...` (or copy + swap) — see [Google’s clustered tables docs](https://cloud.google.com/bigquery/docs/creating-clustered-tables).

## Using Helper Scripts

We provide shell scripts to automate common ingestion tasks:

- `run_load_sec_tickers.sh`: Runs the ticker ingestion.
- `run_load_sp500_companies.sh`: Runs the S&P 500 company list ingestion.
- `run_load_sp500_stock_daily.sh`: Runs the S&P 500 daily stock data ingestion.

To run a script:

```bash
chmod +x *.sh
./run_load_sp500_companies.sh
```

## Troubleshooting

- **Credentials Error**: Ensure `TARGET_BIGQUERY_CREDENTIALS_PATH` in your `.env` is correct and absolute.
- **Missing Files**: Check that the files in `staging/` match the names expected in `meltano.yml`.
