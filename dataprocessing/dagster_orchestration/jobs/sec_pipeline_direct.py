"""
Simplified SEC Pipeline with Direct Ingestion

This module defines Dagster jobs for the simplified SEC data pipeline
that uses direct download from SEC website to BigQuery.
All jobs use centralized configuration for consistency.
"""

from dagster import Array, ConfigMapping, Field, define_asset_job, AssetSelection

from ..assets.sec_bigquery_dedupe import sec_bigquery_dedupe_only
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

    # Ingestion op supports year/from_year/to_year, quarters, dataset, batch_size, dry_run, skip_dedupe.
    for key in (
        "year",
        "from_year",
        "to_year",
        "quarters",
        "dataset",
        "batch_size",
        "dry_run",
        "skip_dedupe",
    ):
        v = job_config.get(key)
        if v not in (None, "", []):
            ingestion_cfg[key] = v

    # Summary op supports year/from_year/to_year, quarters, dataset.
    for key in ("year", "from_year", "to_year", "quarters", "dataset"):
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
    "year": Field(
        int,
        is_required=False,
        description="Single calendar year to load (use this OR from_year + to_year, not both).",
    ),
    "from_year": Field(
        int,
        is_required=False,
        description="Start year for a multi-year backfill (must be set together with to_year).",
    ),
    "to_year": Field(
        int,
        is_required=False,
        description="End year for a multi-year backfill (must be set together with from_year).",
    ),
    "quarters": Field(
        Array(str),
        is_required=False,
        description="Quarters to load per year, e.g. [\"q1\", \"q2\"]. Omit for all quarters (q1–q4).",
    ),
    "dataset": Field(
        str,
        is_required=False,
        description="BigQuery dataset id; defaults from BIGQUERY_DATASET env if omitted.",
    ),
    "batch_size": Field(
        int,
        is_required=False,
        description="Rows per BigQuery insert batch; defaults from SEC_BATCH_SIZE env if omitted.",
    ),
    "dry_run": Field(
        bool,
        is_required=False,
        description="If true, ingestion skips loading data (validation / dry run only).",
    ),
    "skip_dedupe": Field(
        bool,
        is_required=False,
        description="If true, skip BigQuery dedupe after each table load (duplicates may remain).",
    ),
}


# Job for SEC direct ingestion only
sec_direct_ingestion_job = define_asset_job(
    name="sec_direct_ingestion_job",
    selection=sec_ingestion_assets,
    description="Load SEC data directly from SEC website to BigQuery",
)

# Job for dbt transformations only
dbt_transformation_job_direct = define_asset_job(
    name="dbt_transformation_job_direct",
    selection=dbt_assets,
    description="dbt run + dbt test on SEC data (direct pipeline variant)",
)

# Complete simplified pipeline: SEC direct ingestion + dbt transformations + summary
sec_pipeline_direct_complete_job = define_asset_job(
    name="sec_pipeline_direct_complete_job",
    selection=AssetSelection.assets(
        sec_direct_ingestion,
        dbt_insider_transformation,
        sec_direct_pipeline_summary,
    ),
    description="Complete SEC pipeline: direct ingestion + dbt + summary",
    config=ConfigMapping(
        config_fn=_sec_direct_pipeline_config_fn,
        config_schema=_SEC_DIRECT_PIPELINE_CONFIG_SCHEMA,
    ),
)

def _dedupe_only_job_config_fn(job_config: dict) -> dict:
    op_cfg: dict = {}
    ds = job_config.get("dataset")
    if ds not in (None, ""):
        op_cfg["dataset"] = ds
    return {"ops": {"sec_bigquery_dedupe_only": {"config": op_cfg}}}


# Catalog: BigQuery dedupe only (no SEC download)
sec_dedupe_only_job = define_asset_job(
    name="sec_dedupe_only_job",
    selection=AssetSelection.assets(sec_bigquery_dedupe_only),
    description="Dedupe SEC raw tables in BigQuery only (no SEC download; optional dataset override)",
    config=ConfigMapping(
        config_fn=_dedupe_only_job_config_fn,
        config_schema={
            "dataset": Field(
                str,
                is_required=False,
                description="BigQuery dataset id; defaults from BIGQUERY_DATASET / pipeline config if omitted.",
            ),
        },
    ),
)
