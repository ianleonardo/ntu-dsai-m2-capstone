"""
Dagster job for fetching SP500 daily stocks (yfinance) and loading to BigQuery.
"""

from datetime import date

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


def _sp500_stock_daily_config_fn(job_config: dict) -> dict:
    """Map Launchpad-level params into per-op configs (no root keys)."""
    resolved_start = job_config.get("start") or "2023-01-01"
    resolved_end = job_config.get("end") or date.today().isoformat()

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
    description="SP500 daily stocks pipeline: yfinance -> JSONL -> BigQuery (via Meltano).",
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
            # Prefill start so Launchpad doesn't require it.
            "start": Field(str, default_value="2023-01-01", is_required=False),
            "end": Field(str, is_required=False),
            # Keep optional/blank in Launchpad; default to 50 in config_fn mapping.
            "chunk_size": Field(int, is_required=False),
        },
    ),
    executor_def=in_process_executor,
)


__all__ = ["sp500_stock_daily_pipeline_job"]

