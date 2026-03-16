"""
SEC pipeline job that includes dbt transformations.
This job runs the complete pipeline: download -> GCS -> BigQuery -> dbt transformations.
"""

from dagster import AssetSelection, define_asset_job

# Import assets
from ..assets.sec_download import sec_raw_data, sec_gcs_data
from ..assets.meltano_integration import meltano_staging_data, bigquery_sec_data, sec_pipeline_summary
from ..assets.dbt_integration import dbt_insider_transformation

# Define the complete SEC pipeline with dbt transformations
sec_pipeline_with_dbt_job = define_asset_job(
    name="sec_pipeline_with_dbt_job",
    selection=AssetSelection.assets(
        sec_raw_data,
        sec_gcs_data,
        meltano_staging_data,
        bigquery_sec_data,
        dbt_insider_transformation,
        sec_pipeline_summary,
    ),
    description="Complete SEC pipeline including dbt transformations",
)

# Define a job that only runs dbt transformations (for incremental updates)
dbt_transformation_job = define_asset_job(
    name="dbt_transformation_job",
    selection=AssetSelection.assets(dbt_insider_transformation),
    description="Run only dbt transformations (useful for incremental updates)",
)
