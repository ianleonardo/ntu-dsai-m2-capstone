# dbt Insider Transactions

This dbt project transforms SEC insider transactions data from BigQuery into analytics-ready models (dims + facts + S&P mart).

## Project Structure

```
models/
├── _sources.yml       # Raw BigQuery tables (Meltano / SEC loads)
├── intermediate/      # Optional (currently unused)
└── marts/
    ├── dim_sec_submission.sql
    ├── dim_sec_reporting_owner.sql
    ├── dim_sp500_company.sql
    ├── fct_sec_nonderiv_line.sql
    ├── fct_insider_transactions.sql
    └── sp500_insider_transactions.sql
```

## Design

### Facts
- **fct_sec_nonderiv_line**: Line-level non-derivative SEC rows (grain: `ACCESSION_NUMBER` + `NONDERIV_TRANS_SK`).
- **fct_insider_transactions**: Filing-grain fact (view); primary key `ACCESSION_NUMBER`.
- **sp500_insider_transactions**: Same grain as filing fact, filtered to S&P 500 issuers (materialized table).

### Dimensions
- **dim_sec_submission**: Deduped filings, parsed dates.
- **dim_sec_reporting_owner**: Reporting-owner rows (SEC `RPTOWNER*` columns + `role_type`, `is_insider`). Used by the API cluster breakdown.
- **dim_sp500_company**: Normalized S&P 500 constituent list.

Transaction coding labels (**Purchase** / **Sale** from SEC P/S codes) are computed in **`fct_insider_transactions`** via the `transaction_code_type_label` macro (`transaction_type_from_code` on the mart).

## Data Source

Transforms data from the `insider_transactions` dataset in BigQuery:
- `sec_submission`, `sec_reportingowner`, `sec_nonderiv_trans`, `sp500_companies` (see `_sources.yml`).

## Usage

```bash
cd dataprocessing/dbt_insider_transactions
uv run dbt build --profiles-dir .
```

```bash
uv run dbt run --models +fct_insider_transactions --profiles-dir .
uv run dbt docs generate --profiles-dir .
```

## Dagster

- `dbt_insider_transformation`, `sec_pipeline_direct_complete_job`, `dbt_transformation_job_direct`, `sec_dedupe_only_job` (see repo orchestration docs).
