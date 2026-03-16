#!/usr/bin/env bash
# Run sync from GCS then load into BigQuery.
# Usage (from meltano_ingestion):
#   uv run --project .. bash run_load_sec_insider.sh
#   uv run --project .. bash run_load_sec_insider.sh --year 2025 --quarter q1
# Or with venv active: bash run_load_sec_insider.sh
set -e
cd "$(dirname "$0")"
REPO_ROOT="$(cd .. && pwd)"

# Load .env and set GOOGLE_CLOUD_PROJECT from GOOGLE_PROJECT_ID for target-bigquery
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck source=../.env
  source "$REPO_ROOT/.env"
  set +a
fi
export GOOGLE_CLOUD_PROJECT="${GOOGLE_PROJECT_ID:-$GOOGLE_CLOUD_PROJECT}"

# 1. Sync TSVs from GCS to staging/ (pass through --year, --quarter, etc.)
echo "=== Syncing from GCS to staging/ ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" python "$REPO_ROOT/scripts/sync_sec_from_gcs.py" "$@"
else
  python "$REPO_ROOT/scripts/sync_sec_from_gcs.py" "$@"
fi

# 2. Load into BigQuery (tap-csv reads tab-delimited staging/*.csv)
echo "=== Running tap-csv target-bigquery ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" meltano run tap-csv target-bigquery
else
  meltano run tap-csv target-bigquery
fi
