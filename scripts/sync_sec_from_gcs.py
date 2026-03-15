#!/usr/bin/env python3
"""
Sync SEC insider transaction TSVs from GCS to local staging for Meltano.

Reads from gs://{bucket}/{prefix}/{year}/{year}q1|q2|q3|q4/*.tsv, merges
quarters per table (with a year column for partitioning), and writes
tab-delimited CSV (.csv) for tap-csv.

Environment variables:
  SEC_LOAD_YEAR     Year to sync (default: 2025)
  SEC_LOAD_QUARTER  Optional: q1, q2, q3, or q4; if unset, all quarters are synced
  GCS_BUCKET        Bucket name (default: dsai-m2-bucket)
  GCS_SEC_PREFIX    Prefix under bucket (default: sec-data)
  STAGING_DIR       Local directory to write CSV files (default: staging)
  GOOGLE_PROJECT_ID or GOOGLE_APPLICATION_CREDENTIALS for GCS auth.
"""

import argparse
import os
import sys
from pathlib import Path

VALID_QUARTERS = ("q1", "q2", "q3", "q4")
ALL_QUARTERS = list(VALID_QUARTERS)

# Load .env from repo root when run from meltano-ingestion
_repo_root = Path(__file__).resolve().parent.parent
_dotenv = _repo_root / ".env"
if _dotenv.exists():
    from dotenv import load_dotenv
    load_dotenv(_dotenv)

try:
    from google.cloud import storage
except ImportError:
    print("Error: google-cloud-storage is required. Install with: pip install google-cloud-storage", file=sys.stderr)
    sys.exit(1)


REQUIRED_TABLES = [
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
]


def get_client_and_bucket(bucket_name: str):
    project_id = os.getenv("GOOGLE_PROJECT_ID")
    client = storage.Client(project=project_id) if project_id else storage.Client()
    return client, client.bucket(bucket_name)


def download_blob_content(bucket, prefix: str, path: str):
    """Return blob bytes or None if not found."""
    blob_path = f"{prefix.rstrip('/')}/{path}".replace("//", "/")
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return None
    return blob.download_as_bytes()


def sync_year_to_staging(
    year: int,
    bucket_name: str,
    prefix: str,
    staging_dir: str | Path,
    quarters: list[str] | None = None,
) -> bool:
    if quarters is None:
        quarters = ALL_QUARTERS
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    client, bucket = get_client_and_bucket(bucket_name)
    base_prefix = f"{prefix.rstrip('/')}/{year}"

    for table in REQUIRED_TABLES:
        gcs_filename = f"{table}.tsv"  # SEC data on GCS is .tsv
        out_filename = f"{table}.csv"  # tap-csv requires .csv extension; content is tab-delimited
        rows_with_year = []
        header_row = None

        for q in quarters:
            quarter_prefix = f"{base_prefix}/{year}{q}"
            content = download_blob_content(bucket, quarter_prefix, gcs_filename)
            if content is None:
                continue
            text = content.decode("utf-8", errors="replace")
            lines = [ln for ln in text.splitlines() if ln.strip()]

            for i, line in enumerate(lines):
                if i == 0:
                    if header_row is None:
                        header_row = "year\t" + line
                        rows_with_year.append(header_row)
                    continue
                rows_with_year.append(f"{year}\t{line}")

        if header_row is None:
            print(f"Warning: No data found for {table}", file=sys.stderr)
            continue

        out_path = staging_dir / out_filename
        out_path.write_text("\n".join(rows_with_year) + "\n", encoding="utf-8")
        print(f"Wrote {out_path} ({len(rows_with_year) - 1} rows)")

    return True


def parse_quarters(value: str | None) -> list[str]:
    """Return [q1, q2, q3, q4] if value is None/empty, else [value] after validation."""
    if not (value or "").strip():
        return ALL_QUARTERS
    q = value.strip().lower()
    if q not in VALID_QUARTERS:
        raise ValueError(f"Quarter must be one of {VALID_QUARTERS}, got: {value!r}")
    return [q]


def main():
    parser = argparse.ArgumentParser(
        description="Sync SEC insider TSVs from GCS to local staging for Meltano.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to sync (default: SEC_LOAD_YEAR env or 2025)",
    )
    parser.add_argument(
        "--quarter",
        type=str,
        default=None,
        metavar="Q",
        help="Quarter to sync: q1, q2, q3, or q4. If omitted, all quarters are synced.",
    )
    args = parser.parse_args()

    year = args.year if args.year is not None else int(os.getenv("SEC_LOAD_YEAR", "2025"))
    quarters = parse_quarters(args.quarter or os.getenv("SEC_LOAD_QUARTER"))
    bucket_name = os.getenv("GCS_BUCKET", "dsai-m2-bucket")
    prefix = os.getenv("GCS_SEC_PREFIX", "sec-data")
    staging_dir = os.getenv("STAGING_DIR", "staging")

    # Staging dir relative to cwd (meltano-ingestion when run via Meltano)
    staging_path = Path(staging_dir).resolve()
    quarters_desc = ", ".join(quarters) if len(quarters) == 4 else quarters[0]
    print(f"Syncing year {year} quarters [{quarters_desc}] from gs://{bucket_name}/{prefix}/ to {staging_path}")

    try:
        sync_year_to_staging(year, bucket_name, prefix, staging_path, quarters=quarters)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
