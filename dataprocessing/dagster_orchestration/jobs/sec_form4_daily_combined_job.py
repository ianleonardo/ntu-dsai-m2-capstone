"""
Combined Dagster job: Form4 daily-index ingestion/upload + BigQuery summary.
"""

import os
import warnings
from datetime import date, timedelta
from pathlib import Path

from dotenv import dotenv_values
from dagster import AssetSelection, ConfigMapping, Field, define_asset_job


def _blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _bq_project_and_dataset(job_config: dict) -> tuple[str, str]:
    """Resolve BigQuery project and dataset from job launch config and .env."""
    root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    dotenv_path = root / ".env"
    if dotenv_path.is_file():
        for k, v in dotenv_values(dotenv_path).items():
            if v is not None and str(v).strip() != "":
                env[str(k)] = str(v)

    project_id = (
        (job_config.get("bq_project_id") or "").strip()
        or env.get("GOOGLE_CLOUD_PROJECT", "").strip()
        or env.get("GOOGLE_PROJECT_ID", "").strip()
    )
    dataset = (
        (job_config.get("bq_dataset") or "").strip()
        or env.get("BIGQUERY_DATASET", "insider_transactions").strip()
    )
    return project_id, dataset


def _max_filing_date_dim_sec_submission(job_config: dict) -> str | None:
    """Latest FILING_DATE in dbt dim_sec_submission (DATE), or None if unavailable/empty."""
    try:
        from google.cloud import bigquery
    except ImportError:
        warnings.warn(
            "google-cloud-bigquery not available; cannot default from_date from dim_sec_submission.",
            stacklevel=2,
        )
        return None

    project_id, dataset = _bq_project_and_dataset(job_config)
    if not project_id:
        warnings.warn(
            "GOOGLE_PROJECT_ID / bq_project_id not set; cannot default from_date from BigQuery.",
            stacklevel=2,
        )
        return None

    fqtn = f"`{project_id}.{dataset}.dim_sec_submission`"
    sql = f"SELECT MAX(FILING_DATE) AS max_fd FROM {fqtn}"
    try:
        client = bigquery.Client(project=project_id)
        rows = list(client.query(sql).result())
    except Exception as e:  # noqa: BLE001
        warnings.warn(
            f"BigQuery MAX(FILING_DATE) on dim_sec_submission failed: {e}",
            stacklevel=2,
        )
        return None
    if not rows or rows[0].max_fd is None:
        return None
    d = rows[0].max_fd
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)[:10]


SEC_FORM4_COMBINED_SCHEMA = {
    "from_date": Field(
        str,
        is_required=False,
        default_value="",
        description="Inclusive start YYYY-MM-DD. Empty = MAX(FILING_DATE) on dim_sec_submission (fallback 2026-01-01).",
    ),
    "to_date": Field(
        str,
        is_required=False,
        default_value="",
        description="Inclusive end YYYY-MM-DD. Empty = yesterday.",
    ),
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
    raw_from = job_config.get("from_date")
    raw_to = job_config.get("to_date")

    if _blank(raw_from):
        resolved_from = _max_filing_date_dim_sec_submission(job_config) or "2026-01-01"
    else:
        resolved_from = str(raw_from).strip()

    if _blank(raw_to):
        resolved_to = (date.today() - timedelta(days=1)).isoformat()
    else:
        resolved_to = str(raw_to).strip()

    ingest_cfg: dict = {
        "from_date": resolved_from,
        "to_date": resolved_to,
    }
    summary_cfg: dict = {
        "from_date": resolved_from,
        "to_date": resolved_to,
    }

    for key in (
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
    for key in ("bq_project_id", "bq_dataset"):
        v = job_config.get(key)
        if v not in (None, "", []):
            summary_cfg[key] = v

    return {
        "ops": {
            "sec_form4_daily_ingestion": {"config": ingest_cfg},
            "sec_form4_daily_bigquery_summary": {"config": summary_cfg},
        }
    }


sec_form4_daily_combined_job = define_asset_job(
    name="sec_form4_daily_combined_job",
    selection=AssetSelection.assets(
        "sec_form4_daily_ingestion",
        "dbt_sp500_insider_transactions_form4",
        "sec_form4_daily_bigquery_summary",
    ),
    description=(
        "Combined job: Form4 date-range ingestion/upload -> dbt sp500_insider_transactions -> BigQuery summary. "
        "Leave from_date empty for MAX(FILING_DATE) on dim_sec_submission; leave to_date empty for yesterday."
    ),
    config=ConfigMapping(
        config_fn=_sec_form4_combined_config_fn,
        config_schema=SEC_FORM4_COMBINED_SCHEMA,
    ),
)


__all__ = ["sec_form4_daily_combined_job"]
