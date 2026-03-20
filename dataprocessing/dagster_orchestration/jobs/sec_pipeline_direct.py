"""
Simplified SEC Pipeline with Direct Ingestion

This module defines Dagster jobs for the simplified SEC data pipeline
that uses direct download from SEC website to BigQuery.
All jobs use centralized configuration for consistency.
"""

from dagster import define_asset_job, AssetSelection, ConfigMapping, Field

from ..assets.sec_direct_ingestion import sec_direct_ingestion
from ..assets.sec_direct_pipeline_summary import sec_direct_pipeline_summary
from ..assets.dbt_integration import dbt_insider_transformation

# Define asset selections
sec_ingestion_assets = AssetSelection.assets("sec_direct_ingestion")
dbt_assets = AssetSelection.assets("dbt_insider_transformation")
summary_assets = AssetSelection.assets("sec_direct_pipeline_summary")


def _sec_direct_pipeline_config_fn(job_config: dict) -> dict:
    """Map job-level year-range params into op configs (no root keys)."""
    ingestion_cfg: dict = {}
    summary_cfg: dict = {}

    # Ingestion op supports year/from_year/to_year, quarters, dataset, batch_size, dry_run.
    for key in ("year", "from_year", "to_year", "dataset", "batch_size", "dry_run"):
        v = job_config.get(key)
        if v not in (None, "", []):
            ingestion_cfg[key] = v

    # Summary op supports year/from_year/to_year, quarters, dataset.
    for key in ("year", "from_year", "to_year", "dataset"):
        v = job_config.get(key)
        if v not in (None, "", []):
            summary_cfg[key] = v

    return {
        "ops": {
            "sec_direct_ingestion": {"config": ingestion_cfg},
            "sec_direct_pipeline_summary": {"config": summary_cfg},
        }
    }


_SEC_DIRECT_PIPELINE_CONFIG_SCHEMA = {
    "year": Field(int, is_required=False),
    "from_year": Field(int, is_required=False),
    "to_year": Field(int, is_required=False),
    "dataset": Field(str, is_required=False),
    "batch_size": Field(int, is_required=False),
    "dry_run": Field(bool, is_required=False),
}


# Job for SEC direct ingestion only
sec_direct_ingestion_job = define_asset_job(
    name="sec_direct_ingestion_job",
    selection=sec_ingestion_assets,
    description="Load SEC data directly from SEC website to BigQuery",
)

sec_direct_pipeline_job = define_asset_job(
    name="sec_direct_pipeline_job",
    selection=AssetSelection.assets(sec_direct_ingestion, sec_direct_pipeline_summary),
    description="Complete SEC pipeline (no GCS): direct ingestion + summary",
    config=ConfigMapping(
        config_fn=_sec_direct_pipeline_config_fn,
        config_schema=_SEC_DIRECT_PIPELINE_CONFIG_SCHEMA,
    ),
)

# Job for dbt transformations only
dbt_transformation_job_direct = define_asset_job(
    name="dbt_transformation_job_direct",
    selection=dbt_assets,
    description="Run dbt transformations on SEC data (direct pipeline variant)",
)

# Complete simplified pipeline: SEC direct ingestion + dbt transformations + summary
sec_pipeline_direct_complete_job = define_asset_job(
    name="sec_pipeline_direct_complete_job",
    selection=AssetSelection.assets(
        sec_direct_ingestion,
        dbt_insider_transformation,
        sec_direct_pipeline_summary,
    ),
    description="Complete SEC pipeline (no GCS): direct ingestion + dbt + summary",
    config=ConfigMapping(
        config_fn=_sec_direct_pipeline_config_fn,
        config_schema=_SEC_DIRECT_PIPELINE_CONFIG_SCHEMA,
    ),
)
