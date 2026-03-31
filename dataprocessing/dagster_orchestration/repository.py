"""
Dagster repository for NTU DSAI Capstone Project SEC data pipeline.

This repository defines all assets, jobs, and schedules for orchestrating
the SEC data pipeline with Dagster.
"""

from dagster import Definitions, repository

# Import assets (relative imports inside package)
from .assets.dbt_integration import dbt_insider_transformation, dbt_sp500_insider_transactions_form4
from .assets.sec_bigquery_dedupe import sec_bigquery_dedupe_only

# Import SEC direct ingestion assets
from .assets.sec_direct_ingestion import sec_direct_ingestion
from .assets.sec_direct_pipeline_summary import sec_direct_pipeline_summary
from .assets.sec_form4_monthly_ingestion import sec_form4_monthly_ingestion
from .assets.sec_form4_monthly_bigquery_summary import sec_form4_monthly_bigquery_summary

# Import assets for SP500 daily stock pipeline
from .assets.sp500_stock_daily_integration import (
    sp500_stock_daily_staging_data,
    bigquery_sp500_stock_daily_data,
    sp500_stock_daily_pipeline_summary,
)

# Import simplified SEC direct ingestion jobs
from .jobs.sec_pipeline_direct import (
    dbt_transformation_job_direct,
    sec_dedupe_only_job,
    sec_direct_ingestion_job,
    sec_pipeline_direct_complete_job,
)

# Import jobs for SP500 daily stock pipeline
from .jobs.sp500_stock_daily_pipeline import sp500_stock_daily_pipeline_job
from .jobs.sec_form4_monthly_pipeline import sec_form4_monthly_pipeline_job
from .jobs.sec_form4_monthly_summary_job import sec_form4_monthly_summary_job
from .jobs.sec_form4_monthly_combined_job import sec_form4_monthly_combined_job

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
        dbt_insider_transformation,
        sec_bigquery_dedupe_only,

        # SEC direct ingestion assets
        sec_direct_ingestion,
        sec_direct_pipeline_summary,
        sec_form4_monthly_ingestion,
        sec_form4_monthly_bigquery_summary,
        dbt_sp500_insider_transactions_form4,

        sp500_stock_daily_staging_data,
        bigquery_sp500_stock_daily_data,
        sp500_stock_daily_pipeline_summary,
        
        # Jobs
        sec_direct_ingestion_job,
        dbt_transformation_job_direct,
        sec_pipeline_direct_complete_job,
        sec_dedupe_only_job,
        sec_form4_monthly_pipeline_job,
        sec_form4_monthly_summary_job,
        sec_form4_monthly_combined_job,

        sp500_stock_daily_pipeline_job,
        
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
            dbt_insider_transformation,
            sec_bigquery_dedupe_only,
            sec_direct_ingestion,
            sec_direct_pipeline_summary,
            sec_form4_monthly_ingestion,
            sec_form4_monthly_bigquery_summary,
            dbt_sp500_insider_transactions_form4,
        ],
        jobs=[
            sec_direct_ingestion_job,
            dbt_transformation_job_direct,
            sec_pipeline_direct_complete_job,
            sec_dedupe_only_job,
            sec_form4_monthly_pipeline_job,
            sec_form4_monthly_summary_job,
            sec_form4_monthly_combined_job,
        ],
    )


# Top-level definitions for Dagster CLI (loads when using [tool.dagster] module_name)
definitions = Definitions(
    assets=[
        dbt_insider_transformation,
        sec_bigquery_dedupe_only,
        sec_direct_ingestion,
        sec_direct_pipeline_summary,
        sec_form4_monthly_ingestion,
        sec_form4_monthly_bigquery_summary,
        dbt_sp500_insider_transactions_form4,

        sp500_stock_daily_staging_data,
        bigquery_sp500_stock_daily_data,
        sp500_stock_daily_pipeline_summary,
    ],
    jobs=[
        sec_direct_ingestion_job,
        dbt_transformation_job_direct,
        sec_pipeline_direct_complete_job,
        sec_dedupe_only_job,
        sec_form4_monthly_pipeline_job,
        sec_form4_monthly_summary_job,
        sec_form4_monthly_combined_job,

        sp500_stock_daily_pipeline_job,
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
