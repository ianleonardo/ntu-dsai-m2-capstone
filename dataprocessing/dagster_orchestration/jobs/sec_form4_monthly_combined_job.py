"""
Combined Dagster job: Form4 ingestion/upload + BigQuery summary.
"""

from dagster import ConfigMapping, Field, define_asset_job, AssetSelection


SEC_FORM4_COMBINED_SCHEMA = {
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
    "upload_bigquery": Field(
        bool,
        is_required=False,
        description="When true, upload monthly TSVs to BigQuery before summary runs.",
    ),
    "bq_project_id": Field(
        str,
        is_required=False,
        description="BigQuery project id override.",
    ),
    "bq_dataset": Field(
        str,
        is_required=False,
        description="BigQuery dataset override.",
    ),
}


def _sec_form4_combined_config_fn(job_config: dict) -> dict:
    ingest_cfg: dict = {}
    summary_cfg: dict = {}
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
            ingest_cfg[key] = v
    for key in ("from_date", "to_date", "bq_project_id", "bq_dataset"):
        v = job_config.get(key)
        if v not in (None, "", []):
            summary_cfg[key] = v
    return {
        "ops": {
            "sec_form4_monthly_ingestion": {"config": ingest_cfg},
            "sec_form4_monthly_bigquery_summary": {"config": summary_cfg},
        }
    }


sec_form4_monthly_combined_job = define_asset_job(
    name="sec_form4_monthly_combined_job",
    selection=AssetSelection.assets(
        "sec_form4_monthly_ingestion",
        "dbt_sp500_insider_transactions_form4",
        "sec_form4_monthly_bigquery_summary",
    ),
    description="Combined job: Form4 date-range ingestion/upload -> dbt sp500_insider_transactions -> BigQuery summary.",
    config=ConfigMapping(
        config_fn=_sec_form4_combined_config_fn,
        config_schema=SEC_FORM4_COMBINED_SCHEMA,
    ),
)

