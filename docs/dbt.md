# dbt Transformation Guideline

This document provides instructions on how to run data transformations using dbt.

## Overview

We use dbt (data build tool) to transform raw data loaded into BigQuery into analytics-ready models. The models follow a staging and marts structure.

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

- **Staging (`models/staging`)**: Initial cleaning and renaming of raw tables.
- **Marts (`models/marts`)**: Final business-level models (dimensions and facts) ready for visualization.

## Testing and Quality Assurance

We have implemented several schema tests to ensure data integrity across the pipeline:

- **`unique`**: Ensures that the specified column (e.g., `ACCESSION_NUMBER`, `date_key`) contains only unique values.
- **`not_null`**: Ensures that the specified column does not contain any null values.

### Current Test Coverage:
- `stg_sec_submission`: `unique`, `not_null` on `ACCESSION_NUMBER`.
- `dim_reporting_owner`: `not_null` on `reporting_owner_cik`, `reporting_owner_name`, and `RPTOWNER_RELATIONSHIP`.
- `dim_date`: `unique`, `not_null` on `date_key`.
- `dim_transaction_type`: `not_null` on `transaction_type_key`.
- `fct_insider_transactions`: `unique`, `not_null` on `ACCESSION_NUMBER`.

These tests are executed automatically during `dbt build`.

## Troubleshooting

- **Auth Error**: Ensure you have run `gcloud auth application-default login` OR that your environment variables are correctly set for dbt.
- **Dataset Not Found**: Ensure the dataset exists in BigQuery as specified in `profiles.yml`.
