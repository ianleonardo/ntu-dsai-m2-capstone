#!/usr/bin/env python3
"""
Download S&P 500 constituents CSV and write JSONL for Meltano.

Source:
  https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv

Output:
  - Writes JSONL to <staging-dir>/sp500_companies.jsonl for tap-jsonl.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


SP500_CONSTITUENTS_URL = (
    "https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv"
)

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download S&P 500 constituents.csv and write JSONL for Meltano."
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


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

