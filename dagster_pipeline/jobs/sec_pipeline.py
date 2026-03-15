"""
Dagster jobs for SEC data pipeline orchestration.
"""

from typing import List, Optional

from dagster import (
    AssetSelection,
    Definitions,
    JobDefinition,
    RunRequest,
    job,
    op,
    In,
    Out,
    graph,
    Config,
)

from dagster_pipeline.assets.sec_download import sec_raw_data, sec_gcs_data
from dagster_pipeline.assets.meltano_integration import (
    meltano_staging_data,
    bigquery_sec_data,
    sec_pipeline_summary,
)


@job(
    description="Complete SEC data pipeline: Download -> GCS -> BigQuery",
)
def sec_pipeline_job():
    """
    Complete SEC data pipeline job that orchestrates all assets.
    
    This job runs the full pipeline:
    1. Download SEC data for specified year/quarters
    2. Upload to Google Cloud Storage
    3. Stage for Meltano
    4. Load to BigQuery
    5. Generate execution summary
    """
    sec_pipeline_summary()


@job(
    description="Download and upload SEC data to GCS only",
)
def sec_download_job():
    """
    SEC download job that only handles download and GCS upload.
    
    This job runs:
    1. Download SEC data for specified year/quarters
    2. Upload to Google Cloud Storage
    """
    sec_gcs_data()


@job(
    description="Load existing SEC data from GCS to BigQuery using Meltano",
)
def sec_bigquery_load_job():
    """
    BigQuery load job that assumes data is already in GCS.
    
    This job runs:
    1. Stage data from GCS for Meltano
    2. Load to BigQuery using Meltano
    3. Generate execution summary
    """
    sec_pipeline_summary()


@job(
    description="Backfill historical SEC data for multiple years",
)
def sec_backfill_job():
    """
    Backfill job for loading historical SEC data.
    
    This job is designed to be run with multiple run requests
    for different years and quarters.
    """
    sec_pipeline_summary()


@op(
    description="Generate run requests for backfill",
    out=Out(List[RunRequest]),
)
def generate_backfill_requests(context) -> List[RunRequest]:
    """
    Generate run requests for backfilling historical data.
    
    This op can be configured with year ranges and quarters to backfill.
    """
    # Default configuration - can be overridden via run config
    start_year = 2020
    end_year = 2023
    quarters = ["q1", "q2", "q3", "q4"]
    
    run_requests = []
    
    for year in range(start_year, end_year + 1):
        for quarter in quarters:
            run_requests.append(
                RunRequest(
                    run_key=f"backfill_{year}_{quarter}",
                    run_config={
                        "ops": {
                            "sec_raw_data": {
                                "config": {
                                    "year": year,
                                    "quarters": [quarter],
                                }
                            }
                        }
                    },
                )
            )
    
    return run_requests


@graph
def sec_backfill_graph():
    """
    Graph for backfilling SEC data with multiple execution requests.
    """
    requests = generate_backfill_requests()
    # Note: In a real implementation, you'd use a sensor or schedule
    # to trigger these run requests. This is a simplified example.


# Create job definitions
SEC_PIPELINE_JOB = sec_pipeline_job
SEC_DOWNLOAD_JOB = sec_download_job
SEC_BIGQUERY_LOAD_JOB = sec_bigquery_load_job
SEC_BACKFILL_JOB = sec_backfill_job


# Asset-based job definitions for more granular control
SEC_ALL_ASSETS_JOB = Definitions(
    assets=[
        sec_raw_data,
        sec_gcs_data,
        meltano_staging_data,
        bigquery_sec_data,
        sec_pipeline_summary,
    ],
    jobs=[
        sec_pipeline_job,
        sec_download_job,
        sec_bigquery_load_job,
    ],
).get_job_def("sec_pipeline_job")


# Configuration schemas for different job types
class SecPipelineConfig(Config):
    """Configuration for SEC pipeline job."""
    
    year: int
    quarters: Optional[List[str]] = None
    bucket_name: str = "dsai-m2-bucket"
    keep_local: bool = False


class SecBackfillConfig(Config):
    """Configuration for SEC backfill job."""
    
    start_year: int
    end_year: int
    quarters: Optional[List[str]] = None
    bucket_name: str = "dsai-m2-bucket"
    keep_local: bool = False
