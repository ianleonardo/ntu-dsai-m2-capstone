"""
Dagster job for SEC Form 4 monthly ingestion with from/to date config.
"""

from dagster import ConfigMapping, Field, define_asset_job, AssetSelection


def _sec_form4_monthly_config_fn(job_config: dict) -> dict:
    cfg: dict = {}
    for key in (
        "from_date",
        "to_date",
        "user_agent",
        "output_dir",
        "max_requests_per_second",
        "sleep_seconds",
        "resume",
        "upload_bigquery",
        "bq_project_id",
        "bq_dataset",
    ):
        v = job_config.get(key)
        if v not in (None, "", []):
            cfg[key] = v
    return {"ops": {"sec_form4_monthly_ingestion": {"config": cfg}}}


SEC_FORM4_MONTHLY_SCHEMA = {
    "from_date": Field(str, is_required=True, description="Inclusive start date YYYY-MM-DD."),
    "to_date": Field(str, is_required=True, description="Inclusive end date YYYY-MM-DD."),
    "user_agent": Field(
        str,
        is_required=False,
        description="SEC-compliant user agent string.",
    ),
    "output_dir": Field(
        str,
        is_required=False,
        description="Output directory for monthly TSV/state files.",
    ),
    "max_requests_per_second": Field(
        float,
        is_required=False,
        description="Request throttle (must be <= 10).",
    ),
    "sleep_seconds": Field(
        float,
        is_required=False,
        description="Additional sleep after each request.",
    ),
    "resume": Field(bool, is_required=False, description="Skip accessions already in state files."),
    "upload_bigquery": Field(bool, is_required=False, description="Upload generated monthly TSVs to BigQuery."),
    "bq_project_id": Field(
        str,
        is_required=False,
        description="BigQuery project id override (optional).",
    ),
    "bq_dataset": Field(
        str,
        is_required=False,
        description="BigQuery dataset override (optional).",
    ),
}


sec_form4_monthly_pipeline_job = define_asset_job(
    name="sec_form4_monthly_pipeline_job",
    selection=AssetSelection.assets("sec_form4_monthly_ingestion"),
    description="SEC Form 4 date-range pipeline: monthly files + optional BigQuery upload.",
    config=ConfigMapping(
        config_fn=_sec_form4_monthly_config_fn,
        config_schema=SEC_FORM4_MONTHLY_SCHEMA,
    ),
)

