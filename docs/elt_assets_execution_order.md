# ELT Assets by Execution Order

## Stage 1 — Ingestion (Meltano / Scripts / Dagster)

### SEC + Static Reference Loads
1. `run_load_sec_tickers.sh`
   - Runs `scripts/sync_sec_company_tickers.py`
   - Then `meltano run tap-jsonl target-bigquery`
2. `meltano run load-sec-insider`
   - `tap-csv -> target-bigquery`
   - Streams: `SEC_SUBMISSION`, `SEC_REPORTINGOWNER`, `SEC_NONDERIV_TRANS`
3. `run_load_sp500_companies.sh`
   - Runs `scripts/download_sync_sp500_companies.py`
   - Then `meltano run tap-jsonl-sp500 target-bigquery`

### Market Time-Series Load
4. `run_load_sp500_stock_daily.sh`
   - Runs `scripts/fetch_sp500_stock_daily_yfinance_to_jsonl.py`
   - Then `meltano run tap-jsonl-sp500-stock-daily target-bigquery`

### Orchestrated equivalents in Dagster
- `sec_direct_ingestion` (direct SEC website -> BigQuery)
- `sp500_stock_daily_staging_data` -> `bigquery_sp500_stock_daily_data`

## Stage 2 — dbt Modeling Order

Recommended build order from dependencies:

1. **Dimensions from sources**
   - `dim_sec_submission`
   - `dim_sec_reporting_owner`
   - `dim_sp500_company`
2. **Line fact**
   - `fct_sec_nonderiv_line`
3. **Filing fact**
   - `fct_insider_transactions` (depends on 1 + 2)
4. **S&P500 mart**
   - `sp500_insider_transactions` (depends on filing fact + S&P dim)
5. **S&P owner subset dim**
   - `dim_sp500_reporting_owner` (depends on S&P mart + owner dim)

## Stage 3 — Serving Layer (FastAPI)

### Primary serving tables
- `sp500_insider_transactions`:
  - `/transactions`
  - `/clusters`
  - `/summary` and most aggregate APIs
- `dim_sec_reporting_owner`:
  - `/clusters/breakdown` owner-level expansion
- `dim_sp500_reporting_owner` + `sp500_insider_transactions`:
  - `/search-directory/insiders` (S&P scoped insider directory)

### Supporting serving tables
- `dim_sp500_company` + `sp500_stock_daily`:
  - `/search-directory/stocks`
  - latest close enrichment

## Stage 4 — Frontend Use

1. **Overview**
   - summary/top-transactions endpoints
2. **Clusters**
   - `/clusters`
   - row drill-down `/clusters/breakdown`
3. **Detailed Transactions**
   - `/transactions` with paged filtering
4. **Shared search directory cache**
   - stocks + insiders loaded and cached in session storage

## Dagster Job Execution Order (Practical)

### Full SEC pipeline
`sec_pipeline_direct_complete_job`
1. `sec_direct_ingestion`
2. `dbt_insider_transformation` (`dbt run` + `dbt test`)
3. `sec_direct_pipeline_summary`

### SP500 daily stock pipeline
`sp500_stock_daily_pipeline_job`
1. `sp500_stock_daily_staging_data`
2. `bigquery_sp500_stock_daily_data`
3. `sp500_stock_daily_pipeline_summary`

## Schedule-to-Job Mapping
- `quarterly_sec_schedule` -> `sec_pipeline_direct_complete_job`
- `monthly_validation_schedule` -> `dbt_transformation_job_direct`
- `weekly_health_check_schedule` -> `sec_pipeline_direct_complete_job`
- `year_end_schedule` -> `sec_pipeline_direct_complete_job`

## Operational Checklist (Execution-Order View)

1. Run ingestion (or Dagster ingestion job).
2. Confirm raw tables updated in `insider_transactions`.
3. Run dbt build (`uv run dbt build --profiles-dir .`).
4. Confirm key marts exist:
   - `sp500_insider_transactions`
   - `dim_sec_reporting_owner`
   - `dim_sp500_reporting_owner`
5. Start/restart API and verify endpoints:
   - `/api/transactions`
   - `/api/clusters`
   - `/api/search-directory/stocks`
   - `/api/search-directory/insiders`
