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
