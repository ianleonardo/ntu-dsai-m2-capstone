#!/usr/bin/env bash
# Sync company_tickers.json to staging CSV, then load SEC_COMPANY_TICKERS into BigQuery.
# Usage (from meltano_ingestion):
#   uv run --project .. bash run_load_sec_tickers.sh
# Or with venv active: bash run_load_sec_tickers.sh
set -e
cd "$(dirname "$0")"
# Wrapper lives in dataprocessing/meltano_ingestion/, so repo root is ../..
REPO_ROOT="$(cd ../.. && pwd)"

# Load .env and set GOOGLE_CLOUD_PROJECT from GOOGLE_PROJECT_ID for target-bigquery
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck source=../.env
  source "$REPO_ROOT/.env"
  set +a
fi
export GOOGLE_CLOUD_PROJECT="${GOOGLE_PROJECT_ID:-$GOOGLE_CLOUD_PROJECT}"

# 1. Download JSON and write staging/company_tickers.jsonl
echo "=== Syncing SEC company_tickers.json to staging/ (JSONL) ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" python "$REPO_ROOT/scripts/sync_sec_company_tickers.py" --staging-dir "$(pwd)/staging"
else
  python "$REPO_ROOT/scripts/sync_sec_company_tickers.py" --staging-dir "$(pwd)/staging"
fi

# 2. Load SEC_COMPANY_TICKERS into BigQuery via tap-jsonl
echo "=== Running tap-jsonl target-bigquery ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" meltano run tap-jsonl target-bigquery
else
  meltano run tap-jsonl target-bigquery
fi
