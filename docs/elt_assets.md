# ELT Asset Inventory

Generated from current repository configuration and code.

## 1) Meltano Inventory

### 1.1 Meltano Project
- **Project file**: `dataprocessing/meltano_ingestion/meltano.yml`
- **Environment**: `dev`
- **Target dataset**: `ntu-dsai-488112.insider_transactions`
- **Loader**: `target-bigquery` (`z3z1ma` variant)
- **Loader config highlights**:
  - `denormalized: true`
  - `method: batch_job`
  - `upsert: true`
  - `partition_granularity: year`
  - `cluster_on_key_properties: true`

### 1.2 Extractors / Streams / Keys
| Extractor | Stream / Entity | Source file | Primary/merge keys |
|---|---|---|---|
| `tap-csv` | `SEC_SUBMISSION` | `staging/SUBMISSION.csv` | `ACCESSION_NUMBER` |
| `tap-csv` | `SEC_REPORTINGOWNER` | `staging/REPORTINGOWNER.csv` | `ACCESSION_NUMBER`, `RPTOWNERCIK` |
| `tap-csv` | `SEC_NONDERIV_TRANS` | `staging/NONDERIV_TRANS.csv` | `ACCESSION_NUMBER`, `NONDERIV_TRANS_SK` |
| `tap-jsonl` | `SEC_COMPANY_TICKERS` | `staging/company_tickers.jsonl` | `ticker` |
| `tap-jsonl-sp500` | `SP500_COMPANIES` | `staging/sp500_companies.jsonl` | `symbol` |
| `tap-jsonl-sp500-stock-daily` | `SP500_STOCK_DAILY` | `staging/sp500_stock_daily.jsonl` | `symbol`, `date` |

### 1.3 Meltano Jobs
| Job | Task chain |
|---|---|
| `load-sec-insider` | `tap-csv -> target-bigquery` |
| `load-sec-tickers` | `tap-jsonl -> target-bigquery` |
| `load-sp500-companies` | `tap-jsonl-sp500 -> target-bigquery` |
| `load-sp500-stock-daily` | `tap-jsonl-sp500-stock-daily -> target-bigquery` |

### 1.4 Wrapper Scripts (Meltano-related)
| Script | Purpose | Calls |
|---|---|---|
| `dataprocessing/meltano_ingestion/run_load_sec_tickers.sh` | Sync SEC ticker JSON -> JSONL then load | `scripts/sync_sec_company_tickers.py`, `meltano run tap-jsonl target-bigquery` |
| `dataprocessing/meltano_ingestion/run_load_sp500_companies.sh` | Download S&P500 constituents -> JSONL then load | `scripts/download_sync_sp500_companies.py`, `meltano run tap-jsonl-sp500 target-bigquery` |
| `dataprocessing/meltano_ingestion/run_load_sp500_stock_daily.sh` | Fetch yfinance OHLCV -> JSONL then load | `scripts/fetch_sp500_stock_daily_yfinance_to_jsonl.py`, `meltano run tap-jsonl-sp500-stock-daily target-bigquery` |

---

## 2) dbt Inventory (Models, Structures, Tests, Dependencies)

### 2.1 dbt Project Settings
- **Project**: `dataprocessing/dbt_insider_transactions`
- **Config** (`dbt_project.yml`):
  - default materialization: `view`
  - `marts` folder: `view` (except model-level overrides)
- **Var**: `sp500_companies_identifier` (default `sp500_companies`)

### 2.2 Sources (`models/_sources.yml`)
Source: `insider_transactions` (DB: `ntu-dsai-488112`, schema: `insider_transactions`)
- `sec_submission`
- `sec_reportingowner`
- `sec_nonderiv_trans`
- `sp500_companies` (identifier configurable via var)

Source-level tests:
- `sec_submission.ACCESSION_NUMBER`: `not_null`
- `sec_reportingowner.RPTOWNERCIK`: `not_null`
- `sec_nonderiv_trans.NONDERIV_TRANS_SK`: `not_null`
- `sec_nonderiv_trans.TRANS_SHARES`: `non_negative_or_null`
- `sec_nonderiv_trans.TRANS_PRICEPERSHARE`: `non_negative_or_null`

### 2.3 Custom Macros / Tests
- `macros/non_negative_or_null.sql`
  - Custom test: fails rows where `SAFE_CAST(column AS FLOAT64) < 0`
- `macros/parse_sec_date.sql`
  - Robust SEC date parser handling DATE/string/int/float/date formats
- `macros/transaction_code_type_label.sql`
  - Maps code `P -> Purchase`, `S -> Sale`

### 2.4 Model Dependency Graph (current)
- `dim_sec_submission` <- `source.sec_submission`
- `dim_sec_reporting_owner` <- `source.sec_reportingowner`
- `dim_sp500_company` <- `source.sp500_companies`
- `fct_sec_nonderiv_line` <- `source.sec_nonderiv_trans`
- `fct_insider_transactions` <- `dim_sec_submission`, `fct_sec_nonderiv_line`, `dim_sec_reporting_owner`
- `sp500_insider_transactions` <- `fct_insider_transactions`, `dim_sp500_company`
- `dim_sp500_reporting_owner` <- `dim_sec_reporting_owner`, `sp500_insider_transactions`

### 2.5 Model Structures and Tests

#### `dim_sec_submission` (view)
- **Grain**: one row per `ACCESSION_NUMBER`
- **Logic**:
  - dedupe by accession using latest `year`
  - parses date fields with `parse_sec_date`
- **Selected columns**:
  - `ACCESSION_NUMBER`, `FILING_DATE`, `PERIOD_OF_REPORT`, `DATE_OF_ORIGINAL_SUBMISSION`
  - `NO_SECURITIES_OWNED`, `NOT_SUBJECT_SEC16`, `FORM3_HOLDINGS_REPORTED`, `FORM4_TRANS_REPORTED`
  - `DOCUMENT_TYPE`, `ISSUERCIK`, `ISSUERNAME`, `ISSUERTRADINGSYMBOL`
- **Tests**:
  - `ACCESSION_NUMBER`: `unique`, `not_null`

#### `dim_sec_reporting_owner` (view)
- **Grain**: owner-row bridge at accession + owner line level
- **Columns**:
  - SEC owner fields: `ACCESSION_NUMBER`, `RPTOWNERCIK`, `RPTOWNERNAME`, `RPTOWNER_RELATIONSHIP`, `RPTOWNER_TITLE`, etc.
  - derived: `role_type`, `is_insider`
- **Tests**:
  - `RPTOWNERCIK`, `RPTOWNERNAME`, `RPTOWNER_RELATIONSHIP`: `not_null`

#### `dim_sp500_company` (view)
- **Grain**: `symbol_norm`
- **Columns**:
  - `symbol_norm`, `cik_int`, `sp500_security_name`, `gics_sector`, `gics_sub_industry`
- **Tests**: none defined in schema YAML

#### `fct_sec_nonderiv_line` (view)
- **Grain**: non-derivative line (`ACCESSION_NUMBER` + `NONDERIV_TRANS_SK`)
- **Columns**:
  - transaction line normalization: dates, codes, shares, price/share, ownership fields
- **Tests**:
  - `ACCESSION_NUMBER`: `not_null`
  - `NONDERIV_TRANS_SK`: `not_null`

#### `fct_insider_transactions` (view)
- **Grain**: filing-level (`ACCESSION_NUMBER`)
- **Core outputs**:
  - date keys: `filing_date_key`, `period_report_date_key`
  - transaction aggregates: shares/value acquire/dispose, estimated values
  - owner aggregates: owner count, names, role types, titles
  - derived `transaction_type_from_code`
- **Dependencies inside model**:
  - line aggregates from `fct_sec_nonderiv_line`
  - owner rollups from `dim_sec_reporting_owner`
- **Tests**:
  - `ACCESSION_NUMBER`: `unique`, `not_null`

#### `sp500_insider_transactions` (table)
- **Grain**: same as `fct_insider_transactions` (S&P500 filtered)
- **Materialization config**:
  - `materialized='table'`
  - partition: `TRANS_DATE` by `year`
  - clustering: `TRANS_DATE`, `symbol_norm`
- **Columns added**:
  - `symbol_norm`, `issuer_gics_sector`
- **Join logic**:
  - symbol match (`ISSUERTRADINGSYMBOL` vs `symbol_norm`) or CIK fallback
  - `QUALIFY ROW_NUMBER()` to keep best match per accession
- **Tests**: none defined in schema YAML

#### `dim_sp500_reporting_owner` (view)
- **Grain**: accession × owner subset
- **Purpose**:
  - filtered owner dimension restricted to accessions in `sp500_insider_transactions`
- **Logic**:
  - `SELECT d.* FROM dim_sec_reporting_owner d INNER JOIN distinct sp500 accessions`
- **Tests** (from schema):
  - `ACCESSION_NUMBER`: `not_null`
  - `RPTOWNERCIK`: `not_null`
  - `RPTOWNERNAME`: `not_null`

---

## 3) Dagster Inventory (Assets, Jobs, Schedules)

### 3.1 Repository / Definitions
- **Main file**: `dataprocessing/dagster_orchestration/repository.py`
- Exposes:
  - assets
  - jobs
  - schedules
- Also defines top-level `definitions = Definitions(...)` for Dagster CLI.

### 3.2 Assets

#### SEC Direct Pipeline Assets
1. `sec_direct_ingestion` (`assets/sec_direct_ingestion.py`)
   - **Group**: `ingestion`
   - **Purpose**: direct SEC download -> BigQuery load
   - **Config**: `year` OR `from_year`/`to_year`, `quarters`, `dataset`, `batch_size`, `dry_run`, `skip_dedupe`
   - **Core dependencies**: uses script module `scripts/download_sec_to_bigquery.py` (`download_sec_data`, `SECBigQueryLoader`)
   - **Metadata emitted**: years processed, quarters, dataset, row counts, tables loaded

2. `sec_bigquery_dedupe_only` (`assets/sec_bigquery_dedupe.py`)
   - **Group**: `maintenance`
   - **Purpose**: run BigQuery dedupe only (`dedupe_all_configured_tables`)
   - **Config**: optional `dataset`

3. `dbt_insider_transformation` (`assets/dbt_integration.py`)
   - **Group**: `dbt_transformation`
   - **Deps**: `sec_direct_ingestion`
   - **Purpose**: runs `dbt run` then `dbt test`
   - **Artifacts**: reads `run_results.json` and publishes test summary metadata

4. `sec_direct_pipeline_summary` (`assets/sec_direct_pipeline_summary.py`)
   - **Group**: `summary`
   - **Deps**: `sec_direct_ingestion`
   - **Purpose**: centralized summary metadata for SEC run

#### SP500 Daily Stock Assets
5. `sp500_stock_daily_staging_data` (`assets/sp500_stock_daily_integration.py`)
   - **Purpose**: run `fetch_sp500_stock_daily_yfinance_to_jsonl.py`
   - **Output**: `staging/sp500_stock_daily.jsonl`

6. `bigquery_sp500_stock_daily_data`
   - **Deps**: `sp500_stock_daily_staging_data`
   - **Purpose**: `meltano run tap-jsonl-sp500-stock-daily target-bigquery`

7. `sp500_stock_daily_pipeline_summary`
   - **Deps**: `bigquery_sp500_stock_daily_data`
   - **Purpose**: markdown summary metadata

### 3.3 Jobs

1. `sec_direct_ingestion_job`
   - **Selection**: `sec_direct_ingestion`

2. `dbt_transformation_job_direct`
   - **Selection**: `dbt_insider_transformation`

3. `sec_pipeline_direct_complete_job`
   - **Selection**: `sec_direct_ingestion`, `dbt_insider_transformation`, `sec_direct_pipeline_summary`
   - **ConfigMapping**: `_sec_direct_pipeline_config_fn`
   - **Launchpad schema**:
     - `year` or (`from_year`, `to_year`)
     - `quarters`, `dataset`, `batch_size`, `dry_run`, `skip_dedupe`

4. `sec_dedupe_only_job`
   - **Selection**: `sec_bigquery_dedupe_only`
   - **ConfigMapping**: optional `dataset`

5. `sp500_stock_daily_pipeline_job`
   - **Selection**: `sp500_stock_daily_staging_data`, `bigquery_sp500_stock_daily_data`, `sp500_stock_daily_pipeline_summary`
   - **ConfigMapping**: `_sp500_stock_daily_config_fn`
   - **Launchpad schema**: `start`, `end`, optional `chunk_size`
   - **Executor**: `in_process_executor`

### 3.4 Schedules
(Defined in `schedules/sec_schedules.py` and registered in `repository.py`)

1. `quarterly_sec_schedule`
   - **Cron**: `0 2 1 */3 *` (UTC)
   - **Job**: `sec_pipeline_direct_complete_job`
   - **Default**: `RUNNING`
   - **Intent**: previous quarter full run

2. `monthly_validation_schedule`
   - **Cron**: `0 8 1 * *` (UTC)
   - **Job**: `dbt_transformation_job_direct`
   - **Default**: `STOPPED`

3. `weekly_health_check_schedule`
   - **Cron**: `0 7 * * 1` (UTC)
   - **Job**: `sec_pipeline_direct_complete_job`
   - **Default**: `STOPPED`

4. `year_end_schedule`
   - **Cron**: `0 6 1 1 *` (UTC)
   - **Job**: `sec_pipeline_direct_complete_job`
   - **Default**: `STOPPED`

Additional schedule factories (not auto-registered by default):
- `create_custom_schedule(...)`
- `create_quarterly_schedule_for_year(year)`
- `create_backfill_schedule(years)`

### 3.5 Shared Configuration Module
- `config/pipeline_config.py`
  - `SECPipelineConfig` provides centralized values for `year`, `dataset`, `quarters`, `project_id`, `batch_size`, `dry_run`, `skip_dedupe`.
  - `get_pipeline_config(**overrides)` is used by SEC ingestion and summary assets.

---

## 4) Quick Cross-layer Dependency Summary
- **Ingestion (Meltano / SEC direct)** populates raw tables in `insider_transactions` dataset.
- **dbt** builds dimensions/facts from those sources; `sp500_insider_transactions` is the API-facing mart.
- **Dagster** orchestrates both SEC ingestion and dbt transformation; separate SP500-daily pipeline also uses Meltano.
