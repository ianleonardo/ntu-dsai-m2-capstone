"""
Dagster asset: summarize Form 4 monthly BigQuery loads for a date range.
"""

import os
from datetime import date as date_type

from dagster import AssetExecutionContext, Config, MaterializeResult, asset
from google.cloud import bigquery


def _filing_date_as_date_sql(col: str) -> str:
    """Parse SEC raw FILING_DATE (DD-MON-YYYY from Form 4 / bulk); plain CAST is often NULL."""
    return f"""
COALESCE(
  SAFE.PARSE_DATE(
    '%d-%b-%Y',
    NULLIF(TRIM(REGEXP_REPLACE(CAST({col} AS STRING), r'\\.0+$', '')), '')
  ),
  SAFE.PARSE_DATE(
    '%Y-%m-%d',
    NULLIF(TRIM(REGEXP_REPLACE(CAST({col} AS STRING), r'\\.0+$', '')), '')
  ),
  SAFE_CAST({col} AS DATE)
)"""


class SecForm4MonthlyBigQuerySummaryConfig(Config):
    from_date: str
    to_date: str
    bq_project_id: str = ""
    bq_dataset: str = ""


@asset(
    key="sec_form4_monthly_bigquery_summary",
    description="BigQuery row-count summary for Form4 tables in a date range.",
    deps=["sec_form4_monthly_ingestion"],
)
def sec_form4_monthly_bigquery_summary(
    context: AssetExecutionContext, config: SecForm4MonthlyBigQuerySummaryConfig
) -> MaterializeResult:
    project_id = (config.bq_project_id or os.getenv("GOOGLE_PROJECT_ID", "")).strip()
    dataset = (config.bq_dataset or os.getenv("BIGQUERY_DATASET", "insider_transactions")).strip()
    if not project_id:
        raise ValueError("Missing project id. Set bq_project_id or GOOGLE_PROJECT_ID.")

    client = bigquery.Client(project=project_id)

    # Match dbt `parse_sec_date` / Form 4 TSV: DD-MON-YYYY primary (not plain CAST).
    fd = _filing_date_as_date_sql("FILING_DATE")
    fds = _filing_date_as_date_sql("s.FILING_DATE")
    q_submission = f"""
        SELECT COUNT(*) AS c
        FROM `{project_id}.{dataset}.sec_submission`
        WHERE {fd} BETWEEN @from_date AND @to_date
    """
    q_reporting_owner = f"""
        SELECT COUNT(*) AS c
        FROM `{project_id}.{dataset}.sec_reportingowner` r
        JOIN `{project_id}.{dataset}.sec_submission` s
          ON r.ACCESSION_NUMBER = s.ACCESSION_NUMBER
        WHERE {fds} BETWEEN @from_date AND @to_date
    """
    q_nonderiv = f"""
        SELECT COUNT(*) AS c
        FROM `{project_id}.{dataset}.sec_nonderiv_trans` n
        JOIN `{project_id}.{dataset}.sec_submission` s
          ON n.ACCESSION_NUMBER = s.ACCESSION_NUMBER
        WHERE {fds} BETWEEN @from_date AND @to_date
    """
    q_distinct_accessions = f"""
        SELECT COUNT(DISTINCT ACCESSION_NUMBER) AS c
        FROM `{project_id}.{dataset}.sec_submission`
        WHERE {fd} BETWEEN @from_date AND @to_date
    """
    d_from = date_type.fromisoformat(str(config.from_date).strip())
    d_to = date_type.fromisoformat(str(config.to_date).strip())
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("from_date", "DATE", d_from),
            bigquery.ScalarQueryParameter("to_date", "DATE", d_to),
        ]
    )

    def _count(sql: str) -> int:
        return int(next(client.query(sql, job_config=cfg).result()).c)

    submission_count = _count(q_submission)
    reporting_owner_count = _count(q_reporting_owner)
    nonderiv_count = _count(q_nonderiv)
    distinct_accessions = _count(q_distinct_accessions)

    context.log.info(
        "Form4 BQ summary %s..%s -> submission=%s reportingowner=%s nonderiv=%s distinct_accessions=%s",
        config.from_date,
        config.to_date,
        submission_count,
        reporting_owner_count,
        nonderiv_count,
        distinct_accessions,
    )

    return MaterializeResult(
        metadata={
            "from_date": config.from_date,
            "to_date": config.to_date,
            "project_id": project_id,
            "dataset": dataset,
            "sec_submission_rows": submission_count,
            "sec_reportingowner_rows": reporting_owner_count,
            "sec_nonderiv_trans_rows": nonderiv_count,
            "distinct_accession_numbers": distinct_accessions,
        }
    )

