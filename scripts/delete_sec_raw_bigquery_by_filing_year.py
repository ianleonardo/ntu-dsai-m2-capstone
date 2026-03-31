#!/usr/bin/env python3
"""
Delete rows in raw SEC BigQuery tables for a given filing-year (default 2026).

Targets:
  - insider_transactions.sec_submission       (filter: parsed FILING_DATE year)
  - insider_transactions.sec_reportingowner   (delete by ACCESSION_NUMBER in that cohort)
  - insider_transactions.sec_nonderiv_trans   (delete by ACCESSION_NUMBER in that cohort)

Deletes children first, then submission. Uses the same filing-date parsing fallbacks as dbt
(DD-MON-YYYY, ISO, native DATE).

Usage (dry-run — counts only):
  uv run python scripts/delete_sec_raw_bigquery_by_filing_year.py

Execute deletes:
  uv run python scripts/delete_sec_raw_bigquery_by_filing_year.py --execute

Environment:
  GOOGLE_PROJECT_ID or --project-id
  BIGQUERY_DATASET or --dataset (default insider_transactions)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_repo_root = Path(__file__).resolve().parent.parent
_dotenv = _repo_root / ".env"
if _dotenv.exists():
    load_dotenv(_dotenv)

try:
    from google.cloud import bigquery
except ImportError:
    print("Error: pip install google-cloud-bigquery", file=sys.stderr)
    sys.exit(1)

# Parsed FILING_DATE for WHERE year = :year (matches common SEC + Form 4 loads)
FILING_DATE_SQL = """
COALESCE(
  SAFE.PARSE_DATE(
    '%d-%b-%Y',
    NULLIF(TRIM(REGEXP_REPLACE(CAST(FILING_DATE AS STRING), r'\\.0+$', '')), '')
  ),
  SAFE.PARSE_DATE(
    '%Y-%m-%d',
    NULLIF(TRIM(REGEXP_REPLACE(CAST(FILING_DATE AS STRING), r'\\.0+$', '')), '')
  ),
  SAFE_CAST(FILING_DATE AS DATE)
)
"""


def _fq(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


def _accession_in_filing_year_predicate(project: str, dataset: str, year: int) -> str:
    sub = _fq(project, dataset, "sec_submission")
    return f"""
ACCESSION_NUMBER IN (
  SELECT ACCESSION_NUMBER
  FROM {sub}
  WHERE EXTRACT(YEAR FROM {FILING_DATE_SQL}) = {year}
)
"""


def run_counts(client: bigquery.Client, project: str, dataset: str, year: int) -> None:
    sub = _fq(project, dataset, "sec_submission")
    ro = _fq(project, dataset, "sec_reportingowner")
    nd = _fq(project, dataset, "sec_nonderiv_trans")

    q_sub = f"""
SELECT COUNT(*) AS n FROM {sub}
WHERE EXTRACT(YEAR FROM {FILING_DATE_SQL}) = {year}
"""
    q_ro = f"""
SELECT COUNT(*) AS n FROM {ro}
WHERE {_accession_in_filing_year_predicate(project, dataset, year)}
"""
    q_nd = f"""
SELECT COUNT(*) AS n FROM {nd}
WHERE {_accession_in_filing_year_predicate(project, dataset, year)}
"""
    for label, q in (
        ("sec_submission (by FILING_DATE year)", q_sub),
        ("sec_reportingowner (by submission cohort)", q_ro),
        ("sec_nonderiv_trans (by submission cohort)", q_nd),
    ):
        n = list(client.query(q).result())[0]["n"]
        print(f"  {label}: {n}")


def run_deletes(client: bigquery.Client, project: str, dataset: str, year: int) -> None:
    sub = _fq(project, dataset, "sec_submission")
    ro = _fq(project, dataset, "sec_reportingowner")
    nd = _fq(project, dataset, "sec_nonderiv_trans")

    pred_nd = _accession_in_filing_year_predicate(project, dataset, year)
    pred_ro = _accession_in_filing_year_predicate(project, dataset, year)
    pred_sub = f"EXTRACT(YEAR FROM {FILING_DATE_SQL}) = {year}"

    # Order: nonderiv -> reportingowner -> submission
    statements = [
        (f"DELETE FROM {nd} WHERE {pred_nd}", "sec_nonderiv_trans"),
        (f"DELETE FROM {ro} WHERE {pred_ro}", "sec_reportingowner"),
        (f"DELETE FROM {sub} WHERE {pred_sub}", "sec_submission"),
    ]

    for sql, name in statements:
        job = client.query(sql)
        job.result()
        n = getattr(job, "num_dml_affected_rows", None)
        if n is not None:
            print(f"  {name}: deleted {n} row(s)")
        else:
            print(f"  {name}: delete job finished (affected rows not reported by client)")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Delete SEC raw BigQuery rows for a filing year (default 2026)."
    )
    p.add_argument("--project-id", default=os.getenv("GOOGLE_PROJECT_ID", "").strip())
    p.add_argument(
        "--dataset",
        default=os.getenv("BIGQUERY_DATASET", "insider_transactions").strip(),
    )
    p.add_argument("--year", type=int, default=2026)
    p.add_argument(
        "--execute",
        action="store_true",
        help="Run DELETE statements. Without this flag, only prints row counts (dry-run).",
    )
    args = p.parse_args()

    if not args.project_id:
        print("Error: set GOOGLE_PROJECT_ID or pass --project-id", file=sys.stderr)
        return 2

    client = bigquery.Client(project=args.project_id)

    print(f"Project={args.project_id} dataset={args.dataset} filing_year={args.year}")
    print("Rows that would be affected (or current counts):")
    run_counts(client, args.project_id, args.dataset, args.year)

    if not args.execute:
        print("\nDry-run only. Re-run with --execute to delete.")
        return 0

    print("\nDeleting...")
    run_deletes(client, args.project_id, args.dataset, args.year)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
