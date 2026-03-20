"""
SEC dbt transformations job.

Note: The legacy GCS-based SEC pipeline assets/jobs were removed from the active
Dagster repository. This file now only defines the dbt-only job.
"""

from dagster import AssetSelection, define_asset_job

from ..assets.dbt_integration import dbt_insider_transformation


dbt_transformation_job = define_asset_job(
    name="dbt_transformation_job",
    selection=AssetSelection.assets(dbt_insider_transformation),
    description="Run only dbt transformations (useful for incremental updates)",
)
