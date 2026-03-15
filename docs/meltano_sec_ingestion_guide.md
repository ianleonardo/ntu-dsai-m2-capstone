# Ingesting SEC Insider Transactions: GCS to BigQuery with Meltano

This guide describes loading SEC Form 3/4/5 insider transaction data from a GCP bucket into BigQuery using Meltano.

## Overview

- **Source**: GCS bucket `gs://dsai-m2-bucket/sec-data/` with folder structure `{year}/{year}q1`, `{year}/{year}q2`, etc. (e.g. `2025/2025q1`). Each quarter folder contains TSV files produced by the SEC (see [Insider Transactions Data Sets](https://www.sec.gov/developer) and `docs/insider_transactions_readme.pdf`).
- **Destination**: BigQuery project `ntu-dsai-488112`, dataset `insider_transactions`. Tables use the `SEC_` prefix (e.g. `SEC_SUBMISSION`, `SEC_REPORTINGOWNER`) and are partitioned by year.
- **Pipeline**: Because standard Meltano taps read local files only, the flow is **sync from GCS → local staging** then **tap-csv (tab-delimited) → target-bigquery**.

## Tables Loaded

| Table             | Primary key(s)                         | Description                    |
|------------------|----------------------------------------|--------------------------------|
| SUBMISSION       | ACCESSION_NUMBER                       | XML submissions, filer/issuer  |
| REPORTINGOWNER   | ACCESSION_NUMBER, RPTOWNERCIK          | Reporting owner details        |
| NONDERIV_TRANS   | ACCESSION_NUMBER, NONDERIV_TRANS_SK    | Non-derivative transactions    |
| NONDERIV_HOLDING | ACCESSION_NUMBER, NONDERIV_HOLDING_SK  | Non-derivative holdings        |
| DERIV_TRANS      | ACCESSION_NUMBER, DERIV_TRANS_SK       | Derivative transactions        |
| DERIV_HOLDING    | ACCESSION_NUMBER, DERIV_HOLDING_SK     | Derivative holdings             |

Table structures follow the SEC readme (`docs/insider_transactions_readme.pdf`). A `year` column is added during sync for BigQuery partitioning.

## Prerequisites

- Python 3.10+
- Meltano (via project: `uv sync --group meltano`; or [install Meltano](https://docs.meltano.com/getting-started/installation) globally)
- GCP service account with:
  - **BigQuery Data Editor** and **BigQuery Job User** (for target-bigquery)
  - **Storage Object Viewer** on the bucket (for the GCS sync script)

## Environment Variables

Copy `.env.example` to `.env` in the **repo root** and set:

```env
GOOGLE_PROJECT_ID=ntu-dsai-488112
# Path to service account JSON (used by sync script and target-bigquery)
TARGET_BIGQUERY_CREDENTIALS_PATH=/absolute/path/to/your/gcp-service-account-key.json
# Or use Application Default Credentials:
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

Optional overrides for the GCS sync step (defaults are fine for the standard setup):

- `SEC_LOAD_YEAR` – Year to sync (default: `2025`)
- `SEC_LOAD_QUARTER` – Optional: `q1`, `q2`, `q3`, or `q4`; if unset, all quarters are synced
- `GCS_BUCKET` – Bucket name (default: `dsai-m2-bucket`)
- `GCS_SEC_PREFIX` – Prefix under bucket (default: `sec-data`)
- `STAGING_DIR` – Local staging directory relative to `meltano-ingestion` (default: `staging`)

## Setup

1. **Install Meltano** (if using uv):

   ```bash
   uv sync --group meltano
   ```

2. **Install Meltano plugins** (run from `meltano-ingestion`; use `uv run` so the project venv is used):

   ```bash
   cd meltano-ingestion
   uv run --project .. meltano install
   ```

   If you see "Extractor/loader is not known to Meltano", run `uv run --project .. meltano lock` once to create lockfiles, then run `meltano install` again. If you see "Lockfile exists", lockfiles are already present—run only `meltano install` (no need to run `lock` again).  
   **Note:** If `target-bigquery` fails to install (e.g. `pendulum` build error on Python 3.12), use Python 3.11 for the project: from the repo root run `uv python pin 3.11`, then `uv sync --group meltano`, then retry the `meltano install` above.

3. **Configure credentials**  
   Ensure `.env` in the repo root sets `TARGET_BIGQUERY_CREDENTIALS_PATH` (or `GOOGLE_APPLICATION_CREDENTIALS`). Meltano and the sync script both read from the project root `.env` when you run from `meltano-ingestion`.

## Running the Pipeline

**Option A – Full pipeline (sync + load)**  
Use the wrapper script to sync from GCS then run the Meltano job:

```bash
cd meltano-ingestion
uv run --project .. bash run_load_sec_insider.sh
# With options (e.g. year and quarter):
uv run --project .. bash run_load_sec_insider.sh --year 2025 --quarter q1
```

**Option B – Steps separately**

```bash
cd meltano-ingestion
# 1. Sync from GCS to staging (writes tab-delimited .csv files)
uv run --project .. python ../scripts/sync_sec_from_gcs.py
# 2. Load into BigQuery
uv run --project .. meltano run tap-csv target-bigquery
```

Run the sync script first so `staging/` contains the tab-delimited `.csv` files. If Meltano is on your PATH, you can omit `uv run --project ..` where appropriate.

**Option C – Run sync script directly** (e.g. for debugging):

```bash
cd meltano-ingestion
# All quarters for 2025 (default)
uv run --project .. python ../scripts/sync_sec_from_gcs.py
# Single quarter via env or CLI
SEC_LOAD_YEAR=2025 SEC_LOAD_QUARTER=q1 uv run --project .. python ../scripts/sync_sec_from_gcs.py
uv run --project .. python ../scripts/sync_sec_from_gcs.py --year 2025 --quarter q2
```

After a successful run, BigQuery will have dataset `insider_transactions` with tables partitioned by year (e.g. `insider_transactions.SEC_SUBMISSION`, `insider_transactions.SEC_REPORTINGOWNER`, etc.).

## How It Works

1. **Sync (`scripts/sync_sec_from_gcs.py`)**
   - For the given year, downloads TSVs from `gs://dsai-m2-bucket/sec-data/{year}/{year}q1/` … `{year}q4/` for each of the 6 tables.
   - Merges the four quarters per table (single header, data rows from all quarters) and adds a `year` column.
   - Writes tab-delimited CSV (`.csv`) into `meltano-ingestion/staging/` for tap-csv.

2. **Extract (tap-csv)**
   - Reads the 6 CSV files from `staging/` with **tab** delimiter and the primary keys above.
   - Emits Singer messages to stdout.

3. **Load (target-bigquery)**
   - Writes to project `ntu-dsai-488112`, dataset `insider_transactions`, with `partition_granularity: year` and `denormalized: true`. **Upsert**: `upsert: true` is set so loads merge by primary key (e.g. ACCESSION_NUMBER, NONDERIV_TRANS_SK). You can accumulate multiple runs (e.g. different years or quarters) without duplicate keys; re-running the same data updates existing rows instead of inserting duplicates.

## Troubleshooting

- **"Lockfile exists for extractor/loader"**  
  This appears when running `meltano lock` and lockfiles are already present. It is not fatal. Run `meltano install` only; the existing lockfiles are used and plugins will install.

- **"Block job not found"**  
  Some Meltano versions do not resolve the `job` definition from `meltano.yml`. Run the pipeline directly: `meltano run tap-csv target-bigquery`. The wrapper script `run_load_sec_insider.sh` uses the direct form.

- **target-bigquery: ModuleNotFoundError: No module named 'pkg_resources'**  
  The loader’s dependency (PyFilesystem) expects `setuptools` in its venv. From `meltano-ingestion` run:  
  `./.meltano/loaders/target-bigquery/venv/bin/pip install setuptools`  
  Then re-run the pipeline.

- **target-bigquery install fails (pendulum / distutils / ModuleNotFoundError)**  
  The loader’s dependency `pendulum` does not build on Python 3.12 (distutils was removed). Use Python 3.11 for the project: from the repo root run `uv python pin 3.11`, then `uv sync --group meltano`, then from `meltano-ingestion` run `uv run --project .. meltano install` again.

- **“No data found for …”**  
  Check that the bucket has data under `sec-data/{year}/{year}q1/` … `q4/` and that the service account has Storage Object Viewer.

- **BigQuery permission errors**  
  Ensure the service account has BigQuery Data Editor and BigQuery Job User in project `ntu-dsai-488112`.

- **tap-csv not finding files**
  Run the sync step first so that `meltano-ingestion/staging/` contains the 6 tab-delimited `.csv` files.

- **Accumulating multiple years**
  With `upsert: true`, run the pipeline for different years (e.g. `SEC_LOAD_YEAR=2024 bash run_load_sec_insider.sh` then `SEC_LOAD_YEAR=2025 ...`). Rows are merged by primary key; duplicate keys update existing rows. Tables must remain unique on the configured keys (as per the SEC readme).

## References

- SEC insider data: [Insider Transactions Data Sets](https://www.sec.gov/developer) and `docs/insider_transactions_readme.pdf`
- Project script to upload data into the bucket: `scripts/download_sec_to_bucket.py`
- Meltano: [Getting Started](https://docs.meltano.com/getting-started/part1), [tap-csv](https://hub.meltano.com/extractors/tap-csv/), [target-bigquery](https://hub.meltano.com/loaders/target-bigquery/)
