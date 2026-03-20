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
        config_fn=lambda job_config: {
            # If `end` is omitted/blank, default to today.
            **(
                {
                    "end": (job_config.get("end") or date.today().isoformat()),
                }
            ),
            "ops": {
                "sp500_stock_daily_staging_data": {
                    "config": {
                        "start": job_config.get("start") or "2023-01-01",
                        "end": (job_config.get("end") or date.today().isoformat()),
                        # Optional in Launchpad; default to 50 if unset.
                        "chunk_size": job_config.get("chunk_size") or 50,
                    }
                },
                "bigquery_sp500_stock_daily_data": {
                    "config": {
                        "start": job_config.get("start") or "2023-01-01",
                        "end": (job_config.get("end") or date.today().isoformat()),
                    }
                },
                "sp500_stock_daily_pipeline_summary": {
                    "config": {
                        "start": job_config.get("start") or "2023-01-01",
                        "end": (job_config.get("end") or date.today().isoformat()),
                    }
                },
            }
        },
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

