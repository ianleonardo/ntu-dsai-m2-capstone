"""
Dagster jobs for SEC data pipeline orchestration.

Jobs are defined as asset jobs. Config (year, quarters, bucket_name, keep_local)
is passed at run time via run_config when launching a run.
"""

from typing import List, Optional

from dagster import AssetSelection, Config, JobDefinition, RunConfig, define_asset_job, in_process_executor

from ..assets.sec_download import sec_raw_data, sec_gcs_data
from ..assets.meltano_integration import (
    meltano_staging_data,
    bigquery_sec_data,
    sec_pipeline_summary,
)


# Default run config: prefills Launchpad so "Missing required config" does not show.
# In the UI, use "Scaffold all default config" if the config editor is empty to load these.
DEFAULT_CONFIG_FULL = {
    "ops": {
        "sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}},
        "sec_gcs_data": {
            "config": {
                "year": 2023,
                "quarters": ["q1"],
                "bucket_name": "dsai-m2-bucket",
                "keep_local": False,
            }
        },
        "meltano_staging_data": {
            "config": {"year": 2023, "quarters": ["q1"], "bucket_name": "dsai-m2-bucket"}
        },
        "bigquery_sec_data": {"config": {"year": 2023, "quarters": ["q1"]}},
        "sec_pipeline_summary": {"config": {"year": 2023, "quarters": ["q1"]}},
    }
}

DEFAULT_CONFIG_DOWNLOAD = {
    "ops": {
        "sec_raw_data": {"config": {"year": 2023, "quarters": ["q1"]}},
        "sec_gcs_data": {
            "config": {
                "year": 2023,
                "quarters": ["q1"],
                "bucket_name": "dsai-m2-bucket",
                "keep_local": False,
            }
        },
    }
}

DEFAULT_CONFIG_BIGQUERY_LOAD = {
    "ops": {
        "meltano_staging_data": {
            "config": {"year": 2023, "quarters": ["q1"], "bucket_name": "dsai-m2-bucket"}
        },
        "bigquery_sec_data": {"config": {"year": 2023, "quarters": ["q1"]}},
        "sec_pipeline_summary": {"config": {"year": 2023, "quarters": ["q1"]}},
    }
}

# Use in-process executor so asset outputs (e.g. MaterializeResult) are passed in memory
# between steps; avoids FileNotFoundError when multiprocess executor serializes to temp storage.
sec_pipeline_job = define_asset_job(
    name="sec_pipeline_job",
    description="Complete SEC data pipeline: Download -> GCS -> BigQuery",
    selection=AssetSelection.assets(
        sec_raw_data,
        sec_gcs_data,
        meltano_staging_data,
        bigquery_sec_data,
        sec_pipeline_summary,
    ),
    config=RunConfig(ops=DEFAULT_CONFIG_FULL["ops"]),
    executor_def=in_process_executor,
)

sec_download_job = define_asset_job(
    name="sec_download_job",
    description="Download and upload SEC data to GCS only",
    selection=AssetSelection.assets(sec_raw_data, sec_gcs_data),
    config=RunConfig(ops=DEFAULT_CONFIG_DOWNLOAD["ops"]),
    executor_def=in_process_executor,
)

sec_bigquery_load_job = define_asset_job(
    name="sec_bigquery_load_job",
    description="Load existing SEC data from GCS to BigQuery using Meltano",
    selection=AssetSelection.assets(
        meltano_staging_data,
        bigquery_sec_data,
        sec_pipeline_summary,
    ),
    config=RunConfig(ops=DEFAULT_CONFIG_BIGQUERY_LOAD["ops"]),
    executor_def=in_process_executor,
)


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


# Run config helper: use when launching sec_pipeline_job to pass year/quarters/bucket
def get_sec_pipeline_run_config(config: SecPipelineConfig) -> dict:
    """Return run_config for sec_pipeline_job with the given config."""
    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
    return {
        "ops": {
            "sec_raw_data": {"config": {"year": config.year, "quarters": config.quarters}},
            "sec_gcs_data": {
                "config": {
                    "year": config.year,
                    "quarters": config.quarters,
                    "bucket_name": config.bucket_name,
                    "keep_local": config.keep_local,
                }
            },
            "meltano_staging_data": {
                "config": {"year": config.year, "quarters": quarters, "bucket_name": config.bucket_name}
            },
            "bigquery_sec_data": {"config": {"year": config.year, "quarters": quarters}},
            "sec_pipeline_summary": {"config": {"year": config.year, "quarters": quarters}},
        }
    }


def create_sec_pipeline_job(config: SecPipelineConfig) -> JobDefinition:
    """
    Return the SEC pipeline job. Config is passed at run time via run_config.
    Use get_sec_pipeline_run_config(config) when launching the job.
    """
    return sec_pipeline_job


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
