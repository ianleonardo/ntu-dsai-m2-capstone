"""
Simplified SEC Pipeline with Direct Ingestion

This module defines Dagster jobs for the simplified SEC data pipeline
that uses direct download from SEC website to BigQuery.
All jobs use centralized configuration for consistency.
"""

from dagster import define_asset_job, AssetSelection

from ..assets.sec_direct_ingestion import sec_direct_ingestion
from ..assets.sec_direct_pipeline_summary import sec_direct_pipeline_summary
from ..assets.dbt_integration import dbt_insider_transformation
from ..config.pipeline_config import get_pipeline_config

# Define asset selections
sec_ingestion_assets = AssetSelection.assets("sec_direct_ingestion")
dbt_assets = AssetSelection.assets("dbt_insider_transformation")
summary_assets = AssetSelection.assets("sec_direct_pipeline_summary")

# Job for SEC direct ingestion only
sec_direct_ingestion_job = define_asset_job(
    name="sec_direct_ingestion_job",
    selection=sec_ingestion_assets,
    description="Load SEC data directly from SEC website to BigQuery",
    config=get_pipeline_config(),
)

sec_direct_pipeline_job = define_asset_job(
    name="sec_direct_pipeline_job",
    selection=AssetSelection.assets(sec_direct_ingestion, sec_direct_pipeline_summary),
    description="Complete SEC pipeline (no GCS): direct ingestion + summary",
    config=get_pipeline_config(),
)

# Job for dbt transformations only
dbt_transformation_job_direct = define_asset_job(
    name="dbt_transformation_job_direct",
    selection=dbt_assets,
    description="Run dbt transformations on SEC data (direct pipeline variant)",
    config=get_pipeline_config(),
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
)
