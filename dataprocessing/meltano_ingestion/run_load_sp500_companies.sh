#!/usr/bin/env bash
# Download S&P 500 constituents CSV -> JSONL staging -> load BigQuery.
#
# Usage:
#   uv run --project .. bash run_load_sp500_companies.sh
# Or with venv active:
#   bash run_load_sp500_companies.sh
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

echo "=== Downloading + staging S&P 500 constituents (JSONL) ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" python "$REPO_ROOT/scripts/download_sync_sp500_companies.py" --staging-dir "$(pwd)/staging"
else
  python "$REPO_ROOT/scripts/download_sync_sp500_companies.py" --staging-dir "$(pwd)/staging"
fi

echo "=== Running tap-jsonl-sp500 target-bigquery ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" meltano run tap-jsonl-sp500 target-bigquery
else
  meltano run tap-jsonl-sp500 target-bigquery
fi

