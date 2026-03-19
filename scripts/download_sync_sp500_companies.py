#!/usr/bin/env python3
"""
Download S&P 500 constituents CSV, upload to GCS, and write JSONL for Meltano.

Source:
  https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv

Output:
  - Writes JSONL to <staging-dir>/sp500_companies.jsonl for tap-jsonl.
  - Uploads the raw CSV to gs://<bucket>/<gcs_blob_path> (default: sp500-data/constituents.csv).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.api_core import retry
from google.cloud import storage


SP500_CONSTITUENTS_URL = (
    "https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv"
)
DEFAULT_GCS_BLOB_PATH = "sp500-data/constituents.csv"

# DataHub is public; no special headers required, but keep it polite.
DEFAULT_USER_AGENT = "ntu-dsai-m2-capstone/1.0 (data engineering)"


def _normalize_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _csv_row_to_record(row: dict[str, str]) -> dict[str, str]:
    # DataHub headers (with consistent capitalization/spaces):
    # Symbol,Security,GICS Sector,GICS Sub-Industry,Headquarters Location,Date added,CIK,Founded
    return {
        "symbol": _normalize_value(row.get("Symbol", "")),
        "security": _normalize_value(row.get("Security", "")),
        "gics_sector": _normalize_value(row.get("GICS Sector", "")),
        "gics_sub_industry": _normalize_value(row.get("GICS Sub-Industry", "")),
        "headquarters_location": _normalize_value(row.get("Headquarters Location", "")),
        "date_added": _normalize_value(row.get("Date added", "")),
        "cik": _normalize_value(row.get("CIK", "")),
        "founded": _normalize_value(row.get("Founded", "")),
    }


def download_csv(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to download constituents.csv: HTTP {resp.status_code}")
    # DataHub returns text/csv.
    return resp.text


def write_jsonl(records: list[dict[str, str]], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for record in records:
            # JSONL: one JSON object per line
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def upload_to_gcs(
    bucket_name: str,
    blob_path: str,
    local_path: Path,
) -> None:
    load_dotenv()
    project_id = os.getenv("GOOGLE_PROJECT_ID")
    if not project_id:
        raise RuntimeError(
            "Missing GOOGLE_PROJECT_ID in env (required for GCS uploads)."
        )

    # If only the BigQuery credentials path is configured, reuse it for GCS.
    # google-cloud-storage reads credentials via GOOGLE_APPLICATION_CREDENTIALS.
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        target_creds = os.getenv("TARGET_BIGQUERY_CREDENTIALS_PATH")
        if target_creds:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = target_creds

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(
        str(local_path),
        timeout=300,
        retry=retry.Retry(
            initial=1.0,
            maximum=60.0,
            multiplier=2.0,
            deadline=300.0,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download S&P 500 constituents.csv, upload to GCS, and write JSONL for Meltano."
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "dataprocessing"
        / "meltano_ingestion"
        / "staging",
        help="Directory to write sp500_companies.jsonl (default: meltano_ingestion/staging).",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET_NAME", "dsai-m2-bucket"),
        help="GCS bucket name (default: GCS_BUCKET_NAME or dsai-m2-bucket).",
    )
    parser.add_argument(
        "--gcs-blob-path",
        default=os.getenv("SP500_GCS_BLOB_PATH", DEFAULT_GCS_BLOB_PATH),
        help=f"GCS blob path (default: {DEFAULT_GCS_BLOB_PATH}).",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=None,
        help="Optional JSONL output path. Defaults to <staging-dir>/sp500_companies.jsonl.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("SP500_CONSTITUENTS_URL", SP500_CONSTITUENTS_URL),
        help="Override data source URL (primarily for testing).",
    )
    args = parser.parse_args()

    staging_dir: Path = args.staging_dir
    out_jsonl: Path = args.output_jsonl or (staging_dir / "sp500_companies.jsonl")

    print(f"Downloading S&P 500 constituents from: {args.url}")
    csv_text = download_csv(args.url)

    # Parse CSV -> JSONL records
    reader = csv.DictReader(io.StringIO(csv_text))
    records: list[dict[str, str]] = []
    for row in reader:
        record = _csv_row_to_record(row)
        if not record["symbol"]:
            continue
        records.append(record)

    if not records:
        raise RuntimeError("No S&P 500 records parsed; aborting.")

    # Write JSONL for Meltano tap-jsonl
    jsonl_count = write_jsonl(records, out_jsonl)
    print(f"Wrote {jsonl_count} JSONL records to {out_jsonl}")

    # Upload raw CSV to GCS (store exactly what we downloaded)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp.write(csv_text)

    try:
        print(f"Uploading raw CSV to gs://{args.bucket}/{args.gcs_blob_path}")
        upload_to_gcs(
            bucket_name=args.bucket,
            blob_path=args.gcs_blob_path,
            local_path=tmp_path,
        )
        print("GCS upload complete.")
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            # Not critical; just avoid failing the pipeline on cleanup.
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

