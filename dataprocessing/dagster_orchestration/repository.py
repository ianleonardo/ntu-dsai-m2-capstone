"""
Dagster repository for NTU DSAI Capstone Project SEC data pipeline.

This repository defines all assets, jobs, and schedules for orchestrating
the SEC data pipeline with Dagster.
"""

from dagster import Definitions, ScheduleDefinition, repository

# Import assets (relative imports inside package)
from .assets.sec_download import sec_raw_data, sec_gcs_data
from .assets.meltano_integration import (
    meltano_staging_data,
    bigquery_sec_data,
    sec_pipeline_summary,
)
from .assets.dbt_integration import dbt_insider_transformation

# Import jobs
from .jobs.sec_pipeline import (
    sec_pipeline_job,
    sec_download_job,
    sec_bigquery_load_job,
)
from .jobs.sec_pipeline_with_dbt import (
    sec_pipeline_with_dbt_job,
    dbt_transformation_job,
)

# Import schedules
from .schedules.sec_schedules import (
    quarterly_sec_schedule,
    monthly_validation_schedule,
    weekly_health_check_schedule,
    year_end_schedule,
)


@repository
def sec_data_repository():
    """
    Dagster repository for SEC data pipeline.
    
    This repository includes:
    - Assets for downloading, uploading, and loading SEC data
    - Jobs for different pipeline execution scenarios
    - Schedules for automated execution
    """
    return [
        # Assets
        sec_raw_data,
        sec_gcs_data,
        meltano_staging_data,
        bigquery_sec_data,
        sec_pipeline_summary,
        dbt_insider_transformation,
        
        # Jobs
        sec_pipeline_job,
        sec_download_job,
        sec_bigquery_load_job,
        sec_pipeline_with_dbt_job,
        dbt_transformation_job,
        
        # Schedules
        quarterly_sec_schedule,
        monthly_validation_schedule,
        weekly_health_check_schedule,
        year_end_schedule,
    ]


# Create a simple repository definition for testing
def create_simple_repository():
    """Create a simple repository for initial testing."""
    return Definitions(
        assets=[
            sec_raw_data,
            sec_gcs_data,
            meltano_staging_data,
            bigquery_sec_data,
            sec_pipeline_summary,
            dbt_insider_transformation,
        ],
        jobs=[
            sec_pipeline_job,
            sec_download_job,
            sec_bigquery_load_job,
            sec_pipeline_with_dbt_job,
            dbt_transformation_job,
        ],
    )


# Top-level definitions for Dagster CLI (loads when using [tool.dagster] module_name)
definitions = Definitions(
    assets=[
        sec_raw_data,
        sec_gcs_data,
        meltano_staging_data,
        bigquery_sec_data,
        sec_pipeline_summary,
        dbt_insider_transformation,
    ],
    jobs=[
        sec_pipeline_job,
        sec_download_job,
        sec_bigquery_load_job,
        sec_pipeline_with_dbt_job,
        dbt_transformation_job,
    ],
    schedules=[
        quarterly_sec_schedule,
        monthly_validation_schedule,
        weekly_health_check_schedule,
        year_end_schedule,
    ],
)


# Export the main repository
__all__ = ["sec_data_repository", "create_simple_repository", "definitions"]
