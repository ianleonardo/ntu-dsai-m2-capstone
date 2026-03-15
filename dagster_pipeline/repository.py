"""
Dagster repository for NTU DSAI Capstone Project SEC data pipeline.

This repository defines all assets, jobs, and schedules for orchestrating
the SEC data pipeline with Dagster.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    repository,
)

# Import assets
from dagster_pipeline.assets.sec_download import sec_raw_data, sec_gcs_data
from dagster_pipeline.assets.meltano_integration import (
    meltano_staging_data,
    bigquery_sec_data,
    sec_pipeline_summary,
)

# Import jobs
from dagster_pipeline.jobs.sec_pipeline import (
    sec_pipeline_job,
    sec_download_job,
    sec_bigquery_load_job,
)

# Import schedules
from dagster_pipeline.schedules.sec_schedules import (
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
        
        # Jobs
        sec_pipeline_job,
        sec_download_job,
        sec_bigquery_load_job,
        
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
        ],
        jobs=[
            SEC_PIPELINE_JOB,
        ],
    )


# Export the main repository
__all__ = ["sec_data_repository", "create_simple_repository"]
