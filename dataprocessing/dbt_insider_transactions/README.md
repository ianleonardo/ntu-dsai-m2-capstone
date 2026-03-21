# dbt Insider Transactions

This dbt project transforms SEC insider transactions data from BigQuery into a star schema for analytics.

## Project Structure

```
models/
├── staging/           # Raw data staging models
├── intermediate/      # Intermediate transformations (currently empty)
└── marts/            # Final dimensional models
    ├── dim_reporting_owner.sql
    ├── dim_security.sql
    ├── dim_date.sql
    ├── dim_transaction_type.sql
    └── fct_insider_transactions.sql
```

## Star Schema Design

### Fact Table
- **fct_insider_transactions**: Central fact table with ACCESSION_NUMBER as primary key

### Dimension Tables
- **dim_reporting_owner**: Information about reporting insiders with role classifications
- **dim_security**: Security/instrument information including derivatives
- **dim_date**: Comprehensive date dimension for temporal analysis
- **dim_transaction_type**: Transaction coding reference data

## Data Source

Transforms data from the `insider_transactions` dataset in BigQuery:
- SEC_SUBMISSION (fact table source)
- SEC_REPORTINGOWNER
- SEC_NONDERIV_TRANS
- SEC_NONDERIV_HOLDING
- SEC_DERIV_TRANS
- SEC_DERIV_HOLDING

## Usage

### Run all models
```bash
cd dataprocessing/dbt_insider_transactions
uv run dbt run --profiles-dir .
```

### Run specific models
```bash
uv run dbt run --models +fct_insider_transactions --profiles-dir .
```

### Test data quality
```bash
uv run dbt test --profiles-dir .
```

### Generate documentation
```bash
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .
```

## Integration with Dagster

The dbt transformations are integrated into the Dagster orchestration pipeline:
- `dbt_insider_transformation`: Asset that runs `dbt run` + `dbt test`
- `sec_pipeline_direct_complete_job`: SEC direct ingestion → dbt → summary
- `dbt_transformation_job_direct`: dbt-only job
- `sec_dedupe_only_job`: BigQuery dedupe on SEC raw tables only (no download)
