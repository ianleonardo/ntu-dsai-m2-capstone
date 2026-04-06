"""
Dagster job for fetching SP500 daily stocks (yfinance) and loading to BigQuery.
"""

import os
import warnings
from datetime import date, timedelta
from pathlib import Path

from dotenv import dotenv_values
from dagster import (
    AssetSelection,
    Field,
    ConfigMapping,
    define_asset_job,
    in_process_executor,
)

from ..assets.sp500_stock_daily_integration import (
    sp500_stock_daily_staging_data,
    bigquery_sp500_stock_daily_data,
    sp500_stock_daily_pipeline_summary,
)


def _blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _max_date_sp500_stock_daily() -> str | None:
    """Latest `date` in BigQuery `sp500_stock_daily`, or None if missing/empty/error."""
    try:
        from google.cloud import bigquery
    except ImportError:
        warnings.warn(
            "google-cloud-bigquery not available; cannot default start from sp500_stock_daily MAX(date).",
            stacklevel=2,
        )
        return None

    root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    dotenv_path = root / ".env"
    if dotenv_path.is_file():
        for k, v in dotenv_values(dotenv_path).items():
            if v is not None and str(v).strip() != "":
                env[str(k)] = str(v)

    project_id = (env.get("GOOGLE_CLOUD_PROJECT") or env.get("GOOGLE_PROJECT_ID") or "").strip()
    dataset = (env.get("BIGQUERY_DATASET") or "insider_transactions").strip()
    if not project_id:
        warnings.warn(
            "GOOGLE_PROJECT_ID / GOOGLE_CLOUD_PROJECT not set; cannot default start from BigQuery.",
            stacklevel=2,
        )
        return None

    fqtn = f"`{project_id}.{dataset}.sp500_stock_daily`"
    sql = f"SELECT MAX(`date`) AS max_d FROM {fqtn}"
    try:
        client = bigquery.Client(project=project_id)
        rows = list(client.query(sql).result())
    except Exception as e:  # noqa: BLE001
        warnings.warn(f"BigQuery MAX(date) on sp500_stock_daily failed: {e}", stacklevel=2)
        return None
    if not rows or rows[0].max_d is None:
        return None
    d = rows[0].max_d
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)[:10]


def _sp500_stock_daily_config_fn(job_config: dict) -> dict:
    """Map Launchpad-level params into per-op configs (no root keys)."""
    raw_start = job_config.get("start")
    raw_end = job_config.get("end")

    if _blank(raw_start):
        resolved_start = _max_date_sp500_stock_daily() or "2023-01-01"
    else:
        resolved_start = str(raw_start).strip()

    if _blank(raw_end):
        resolved_end = (date.today() - timedelta(days=1)).isoformat()
    else:
        resolved_end = str(raw_end).strip()

    chunk_size = job_config.get("chunk_size")
    staging_config: dict = {"start": resolved_start, "end": resolved_end}
    if chunk_size not in (None, 0, "0", ""):
        staging_config["chunk_size"] = int(chunk_size)

    return {
        "ops": {
            "sp500_stock_daily_staging_data": {"config": staging_config},
            "bigquery_sp500_stock_daily_data": {
                "config": {
                    "start": resolved_start,
                    "end": resolved_end,
                }
            },
            "sp500_stock_daily_pipeline_summary": {
                "config": {
                    "start": resolved_start,
                    "end": resolved_end,
                }
            },
        }
    }


sp500_stock_daily_pipeline_job = define_asset_job(
    name="sp500_stock_daily_pipeline_job",
    description=(
        "SP500 daily stocks pipeline: yfinance -> JSONL -> BigQuery (via Meltano). "
        "Leave start blank to use MAX(date) from sp500_stock_daily; leave end blank for yesterday."
    ),
    selection=AssetSelection.assets(
        sp500_stock_daily_staging_data,
        bigquery_sp500_stock_daily_data,
        sp500_stock_daily_pipeline_summary,
    ),
    # Launchpad config should request `start`/`end` once, and reuse those values
    # for all ops in this job.
    config=ConfigMapping(
        config_fn=_sp500_stock_daily_config_fn,
        # Launchpad-level config: only ask for start/end once,
        # then map into all ops configs.
        config_schema={
            "start": Field(
                str,
                is_required=False,
                default_value="",
                description="YYYY-MM-DD. Empty = MAX(date) in BigQuery sp500_stock_daily (fallback 2023-01-01 if empty table).",
            ),
            "end": Field(
                str,
                is_required=False,
                default_value="",
                description="YYYY-MM-DD. Empty = calendar yesterday (today minus 1 day).",
            ),
            # Keep optional/blank in Launchpad; default to 50 in config_fn mapping.
            "chunk_size": Field(int, is_required=False),
        },
    ),
    executor_def=in_process_executor,
)


__all__ = ["sp500_stock_daily_pipeline_job"]

