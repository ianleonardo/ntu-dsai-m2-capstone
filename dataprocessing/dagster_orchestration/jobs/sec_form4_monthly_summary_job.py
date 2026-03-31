"""
Dagster job for Form4 BigQuery summary asset.
"""

from dagster import ConfigMapping, Field, define_asset_job, AssetSelection


def _sec_form4_summary_config_fn(job_config: dict) -> dict:
    cfg: dict = {}
    for key in ("from_date", "to_date", "bq_project_id", "bq_dataset"):
        v = job_config.get(key)
        if v not in (None, "", []):
            cfg[key] = v
    return {"ops": {"sec_form4_monthly_bigquery_summary": {"config": cfg}}}


SEC_FORM4_SUMMARY_SCHEMA = {
    "from_date": Field(str, is_required=True, description="Inclusive start date YYYY-MM-DD."),
    "to_date": Field(str, is_required=True, description="Inclusive end date YYYY-MM-DD."),
    "bq_project_id": Field(str, is_required=False, description="BigQuery project id override."),
    "bq_dataset": Field(str, is_required=False, description="BigQuery dataset override."),
}


sec_form4_monthly_summary_job = define_asset_job(
    name="sec_form4_monthly_summary_job",
    selection=AssetSelection.assets("sec_form4_monthly_bigquery_summary"),
    description="Summarize Form4 BigQuery rows in date range.",
    config=ConfigMapping(
        config_fn=_sec_form4_summary_config_fn,
        config_schema=SEC_FORM4_SUMMARY_SCHEMA,
    ),
)

