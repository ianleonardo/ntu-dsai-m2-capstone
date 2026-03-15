"""
Dagster jobs for SEC data pipeline orchestration.
"""

from typing import List, Optional, Tuple

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


@graph(
    description="Complete SEC data pipeline: Download -> GCS -> BigQuery",
)
def sec_pipeline_graph(year: int, quarters: List[str], bucket_name: Optional[str] = None, keep_local: Optional[bool] = None):
    """
    Complete SEC data pipeline graph that orchestrates all assets.
    
    This graph runs the full pipeline:
    1. Download SEC data for specified year/quarters
    2. Upload to Google Cloud Storage
    3. Stage for Meltano
    4. Load to BigQuery
    5. Generate execution summary
    """
    # Create dependency chain: download -> gcs -> staging -> bigquery -> summary
    gcs_data = sec_gcs_data(sec_raw_data.alias(year=year, quarters=quarters), bucket_name=bucket_name, keep_local=keep_local)
    staging_data = meltano_staging_data(gcs_data)
    bigquery_data = bigquery_sec_data(staging_data)
    return sec_pipeline_summary(bigquery_data)


@graph(
    description="Download and upload SEC data to GCS only",
)
def sec_download_graph(year: int, quarters: List[str]):
    """
    SEC download graph that only handles download and GCS upload.
    
    This graph runs:
    1. Download SEC data for specified year/quarters
    2. Upload to Google Cloud Storage
    """
    return sec_gcs_data(sec_raw_data.alias(year=year, quarters=quarters))


@graph(
    description="Load existing SEC data from GCS to BigQuery using Meltano",
)
def sec_bigquery_load_graph():
    """
    BigQuery load graph that assumes data is already in GCS.
    
    This graph runs:
    1. Stage data from GCS for Meltano
    2. Load to BigQuery using Meltano
    3. Generate execution summary
    """
    # Create dependency chain: staging -> bigquery -> summary
    bigquery_data = bigquery_sec_data(meltano_staging_data())
    return sec_pipeline_summary(bigquery_data)


# Convert graphs to jobs
sec_pipeline_job = sec_pipeline_graph.to_job()
sec_download_job = sec_download_graph.to_job()
sec_bigquery_load_job = sec_bigquery_load_graph.to_job()


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


# Job factory functions for dynamic configuration
def create_sec_pipeline_job(config: SecPipelineConfig) -> JobDefinition:
    """
    Create a SEC pipeline job with specific configuration.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Configured job definition
    """
    return sec_pipeline_job.to_job(
        name=f"sec_pipeline_{config.year}",
        config={
            "ops": {
                "sec_raw_data": {
                    "config": {
                        "year": config.year,
                        "quarters": config.quarters,
                    }
                },
                "sec_gcs_data": {
                    "config": {
                        "year": config.year,
                        "quarters": config.quarters,
                        "bucket_name": config.bucket_name,
                        "keep_local": config.keep_local,
                    }
                },
                "meltano_staging_data": {
                    "config": {
                        "year": config.year,
                        "quarters": config.quarters,
                    }
                },
                "bigquery_sec_data": {
                    "config": {
                        "year": config.year,
                        "quarters": config.quarters,
                    }
                },
                "sec_pipeline_summary": {
                    "config": {
                        "year": config.year,
                        "quarters": config.quarters,
                    }
                },
            }
        }
    )


def create_sec_backfill_job(config: SecBackfillConfig) -> List[JobDefinition]:
    """
    Create multiple SEC pipeline jobs for backfill.
    
    Args:
        config: Backfill configuration
        
    Returns:
        List of configured job definitions
    """
    jobs = []
    
    for year in range(config.start_year, config.end_year + 1):
        job_config = SecPipelineConfig(
            year=year,
            quarters=config.quarters,
            bucket_name=config.bucket_name,
            keep_local=config.keep_local,
        )
        jobs.append(create_sec_pipeline_job(job_config))
    
    return jobs
