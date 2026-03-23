# dbt Transformation Guideline

This document provides instructions on how to run data transformations using dbt.

## Overview

We use dbt (data build tool) to transform raw data loaded into BigQuery into analytics-ready models. Sources are declared in `models/_sources.yml`; transforms live under `models/marts/` (dimensions + facts + S&P mart).

## Configuration

The dbt project is located in `dataprocessing/dbt_insider_transactions`.

- `dbt_project.yml`: Project-level configuration.
- `profiles.yml`: Connection settings for BigQuery (using `oauth` by default).

## Running dbt

Navigate to the dbt project directory:

```bash
cd dataprocessing/dbt_insider_transactions
```

### 1. Build Models

To run all models and tests:

```bash
uv run dbt build
```

This command will:
1.  Run the staging models.
2.  Test the staging models.
3.  Run the marts models (dimensions and facts).
4.  Test the marts models.

### 2. Specific Commands

- `uv run dbt run`: Run models without testing.
- `uv run dbt test`: Run only the tests.
- `uv run dbt docs generate`: Generate documentation for the models.
- `uv run dbt docs serve`: Serve the documentation locally.

## Model Structure

- **`models/_sources.yml`**: Registered raw tables in BigQuery.
- **`models/marts`**: `dim_*` (submissions, owners, S&P companies), `fct_sec_nonderiv_line`, `fct_insider_transactions`, `sp500_insider_transactions`.

## Testing and Quality Assurance

We have implemented several schema tests to ensure data integrity across the pipeline:

- **`unique`**: Ensures that the specified column (e.g., `ACCESSION_NUMBER`) contains only unique values.
- **`not_null`**: Ensures that the specified column does not contain any null values.

### Current Test Coverage:
- `dim_sec_submission`: `unique`, `not_null` on `ACCESSION_NUMBER`.
- `dim_sec_reporting_owner`: `not_null` on `RPTOWNERCIK`, `RPTOWNERNAME`, and `RPTOWNER_RELATIONSHIP`.
- `fct_sec_nonderiv_line`: `not_null` on `ACCESSION_NUMBER` and `NONDERIV_TRANS_SK`.
- `fct_insider_transactions`: `unique`, `not_null` on `ACCESSION_NUMBER`.

These tests are executed automatically during `dbt build`.

## Troubleshooting

- **Auth Error**: Ensure you have run `gcloud auth application-default login` OR that your environment variables are correctly set for dbt.
- **Dataset Not Found**: Ensure the dataset exists in BigQuery as specified in `profiles.yml`.
