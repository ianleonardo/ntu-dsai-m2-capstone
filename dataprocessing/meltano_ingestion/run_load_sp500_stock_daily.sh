#!/usr/bin/env bash
# Fetch S&P 500 constituents -> yfinance daily OHLCV -> load to BigQuery via Meltano.
#
# Usage:
#   uv run --project . bash dataprocessing/meltano_ingestion/run_load_sp500_stock_daily.sh --start 2023-01-01 --end 2023-12-31
#
# Notes:
# - interval is fixed to 1d in the fetch script.
# - Output JSONL: dataprocessing/meltano_ingestion/staging/sp500_stock_daily.jsonl
set -e

cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck source=../.env
  source "$REPO_ROOT/.env"
  set +a
fi
export GOOGLE_CLOUD_PROJECT="${GOOGLE_PROJECT_ID:-$GOOGLE_CLOUD_PROJECT}"

echo "=== Fetching S&P 500 daily stocks -> staging JSONL ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" python "$REPO_ROOT/scripts/fetch_sp500_stock_daily_yfinance_to_jsonl.py" "$@"
else
  python "$REPO_ROOT/scripts/fetch_sp500_stock_daily_yfinance_to_jsonl.py" "$@"
fi

echo "=== Running tap-jsonl-sp500-stock-daily target-bigquery (pinned catalog) ==="
if command -v uv &>/dev/null; then
  uv run --project "$REPO_ROOT" meltano el tap-jsonl-sp500-stock-daily target-bigquery \
    --catalog catalogs/sp500_stock_daily.catalog.json \
    --full-refresh
else
  meltano el tap-jsonl-sp500-stock-daily target-bigquery \
    --catalog catalogs/sp500_stock_daily.catalog.json \
    --full-refresh
fi

