#!/usr/bin/env python3
"""
Simplified SEC Data Pipeline: Direct download from SEC website to BigQuery.

This script streams SEC data directly to BigQuery, reducing complexity, cost,
and processing time.

Usage:
    python scripts/download_sec_to_bigquery.py 2024
    python scripts/download_sec_to_bigquery.py 2024 --quarter q1
    python scripts/download_sec_to_bigquery.py 2024 --dry-run

Environment variables:
    GOOGLE_PROJECT_ID: GCP project ID (required)
    GOOGLE_APPLICATION_CREDENTIALS: Path to service account key (optional)
    BIGQUERY_DATASET: BigQuery dataset name (default: insider_transactions)
    SEC_BATCH_SIZE: Batch size for BigQuery inserts (default: 1000)
    SEC_SKIP_DEDUPE: If 1/true/yes, skip post-load BigQuery dedupe (default: dedupe on)

Deduplication:
    Loads use streaming inserts, so re-running the pipeline can append duplicate primary keys.
    After each successful table load, we run a BigQuery query with WRITE_TRUNCATE into the
    same table (one row per primary key; prefer highest pipeline `year` when present).
    This preserves partitioning/clustering; CREATE OR REPLACE is not used. Use `--dedupe-only`
    to run cleanup without downloading SEC data.
"""

import argparse
import sys
import requests
import zipfile
import io
import os
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from repo root
_repo_root = Path(__file__).resolve().parent.parent
_dotenv = _repo_root / ".env"
if _dotenv.exists():
    load_dotenv(_dotenv)

try:
    from google.cloud import bigquery
    from google.api_core import retry
    from google.api_core.exceptions import GoogleAPICallError
except ImportError:
    print("Error: google-cloud-bigquery is required. Install with: pip install google-cloud-bigquery", file=sys.stderr)
    sys.exit(1)

VALID_QUARTERS = ("q1", "q2", "q3", "q4")
ALL_QUARTERS = list(VALID_QUARTERS)

# SEC tables to process
SEC_TABLES = [
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
]

# BigQuery table schema mappings (simplified - BigQuery will infer schema)
TABLE_CONFIGS = {
    "SUBMISSION": {
        "table_id": "sec_submission",
        "primary_keys": ["ACCESSION_NUMBER"],
    },
    "REPORTINGOWNER": {
        "table_id": "sec_reportingowner", 
        "primary_keys": ["ACCESSION_NUMBER", "RPTOWNERCIK"],
    },
    "NONDERIV_TRANS": {
        "table_id": "sec_nonderiv_trans",
        "primary_keys": ["ACCESSION_NUMBER", "NONDERIV_TRANS_SK"],
    },
}


def primary_keys_for_table_id(table_id: str) -> Optional[List[str]]:
    """Return SEC natural keys for a BigQuery table id (e.g. sec_submission)."""
    for cfg in TABLE_CONFIGS.values():
        if cfg.get("table_id") == table_id:
            return list(cfg["primary_keys"])
    return None


class SECBigQueryLoader:
    """Handles direct SEC data loading to BigQuery."""
    
    def __init__(self, project_id: str, dataset: str = "insider_transactions"):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.batch_size = int(os.getenv("SEC_BATCH_SIZE", "1000"))
        env_skip = os.getenv("SEC_SKIP_DEDUPE", "").strip().lower()
        self.skip_dedupe = env_skip in ("1", "true", "yes")
        
    def ensure_dataset_exists(self):
        """Ensure the BigQuery dataset exists."""
        dataset_ref = self.client.dataset(self.dataset)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.client.create_dataset(dataset)
            print(f"Created BigQuery dataset: {self.dataset}")
    
    def stream_to_bigquery(self, table_name: str, rows: List[Dict], year: int) -> bool:
        """Stream rows to BigQuery table with error handling and retries."""
        if not rows:
            return True
            
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        # Add year column to all rows for partitioning
        for row in rows:
            row['year'] = year
        
        # Configure retry strategy
        retry_strategy = retry.Retry(
            initial=1.0,
            maximum=60.0,
            multiplier=2.0,
            deadline=300.0,
        )

        def _insert_batch(batch: List[Dict]) -> bool:
            """Insert batch and recursively split on oversized requests (HTTP 413)."""
            if not batch:
                return True
            try:
                errors = self.client.insert_rows_json(table_id, batch, retry=retry_strategy)
            except GoogleAPICallError as e:
                # BigQuery insertAll can fail with 413 if payload is too large.
                msg = str(e)
                if "413" in msg and len(batch) > 1:
                    mid = len(batch) // 2
                    return _insert_batch(batch[:mid]) and _insert_batch(batch[mid:])
                print(f"BigQuery API error for {table_name}: {e}")
                return False
            except Exception as e:
                msg = str(e)
                if "413" in msg and len(batch) > 1:
                    mid = len(batch) // 2
                    return _insert_batch(batch[:mid]) and _insert_batch(batch[mid:])
                print(f"Unexpected error inserting to {table_name}: {e}")
                return False

            if errors:
                print(f"Errors inserting batch to {table_name}: {errors}")
                return False
            return True

        # Build conservative request-sized chunks (both row-count and JSON bytes).
        # Keeping this below the API limit prevents most 413 failures.
        max_request_bytes = 8 * 1024 * 1024  # 8 MB safety cap
        start = 0
        while start < len(rows):
            end = min(start + self.batch_size, len(rows))
            payload_bytes = 0
            batch = []
            i = start
            while i < end:
                row = rows[i]
                row_size = len(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
                # keep at least 1 row in batch
                if batch and payload_bytes + row_size > max_request_bytes:
                    break
                batch.append(row)
                payload_bytes += row_size
                i += 1

            if not _insert_batch(batch):
                return False
            start = i

        print(f"Successfully inserted {len(rows)} rows to {table_id}")
        return True

    def dedupe_table(self, table_name: str, primary_keys: Sequence[str]) -> bool:
        """
        Truncate and reload deduplicated rows (same table): one row per primary_keys composite.
        Prefers max(`year`) when a `year` column exists; ties broken arbitrarily.
        """
        if not primary_keys:
            print(f"Dedupe skipped for {table_name}: no primary_keys", file=sys.stderr)
            return True

        fq_ref = f"{self.project_id}.{self.dataset}.{table_name}"
        fqtn = f"`{self.project_id}.{self.dataset}.{table_name}`"

        try:
            table = self.client.get_table(fq_ref)
        except Exception as e:
            print(f"Dedupe failed: cannot read table {fq_ref}: {e}", file=sys.stderr)
            return False

        col_names = {f.name for f in table.schema}
        pk_quoted = ", ".join(f"`{k}`" for k in primary_keys)
        for pk in primary_keys:
            if pk not in col_names:
                print(
                    f"Dedupe failed: primary key column {pk!r} missing on {fq_ref}",
                    file=sys.stderr,
                )
                return False

        has_year = "year" in col_names
        order_clause = (
            " ORDER BY COALESCE(SAFE_CAST(`year` AS INT64), -1) DESC"
            if has_year
            else ""
        )

        # Truncate-and-reload via query job keeps partitioning / clustering (e.g. Meltano _sdc_batched_at).
        # CREATE OR REPLACE TABLE ... AS would drop the partition spec and fail on those tables.
        sql = f"""
        SELECT * EXCEPT(rn)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {pk_quoted}{order_clause}) AS rn
            FROM {fqtn}
        )
        WHERE rn = 1
        """

        try:
            job_config = bigquery.QueryJobConfig(
                destination=fq_ref,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = self.client.query(sql, job_config=job_config)
            job.result()
            print(f"Deduped {fq_ref} on ({', '.join(primary_keys)})")
        except Exception as e:
            print(f"Dedupe query failed for {fq_ref}: {e}", file=sys.stderr)
            return False
        return True

    def dedupe_all_configured_tables(self) -> bool:
        """Run dedupe for every SEC table in TABLE_CONFIGS (e.g. --dedupe-only)."""
        ok = True
        for table in SEC_TABLES:
            cfg = TABLE_CONFIGS.get(table)
            if not cfg:
                continue
            tid = cfg["table_id"]
            pks = cfg["primary_keys"]
            if not self.dedupe_table(tid, pks):
                ok = False
        return ok
    
    def process_table_data(self, table_name: str, tsv_content: str, year: int) -> bool:
        """Process TSV content and stream to BigQuery."""
        lines = tsv_content.strip().split('\n')
        if not lines:
            return True
            
        # Parse header
        headers = lines[0].split('\t')
        rows = []
        
        # Parse data rows
        for line in lines[1:]:
            if line.strip():
                values = line.split('\t')
                if len(values) == len(headers):
                    row_dict = dict(zip(headers, values))
                    rows.append(row_dict)
                else:
                    print(f"Warning: Skipping malformed row in {table_name}: {line[:100]}...")
        
        if not self.stream_to_bigquery(table_name, rows, year):
            return False

        if self.skip_dedupe:
            return True

        pks = primary_keys_for_table_id(table_name)
        if not pks:
            print(f"Warning: No TABLE_CONFIGS entry for {table_name}; skipping dedupe", file=sys.stderr)
            return True

        return self.dedupe_table(table_name, pks)


def download_sec_data(year: int, quarters: List[str]) -> Dict[str, str]:
    """Download SEC data for specified year and quarters."""
    quarter_ids = [f"{year}{q}" for q in quarters]
    base_url = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/"
    
    headers = {
        "User-Agent": "NTU DSAI Capstone Project (contact@example.com)",
        "Accept": "application/zip"
    }
    
    downloaded_data = {}
    
    for quarter in quarter_ids:
        filename = f"{quarter}_form345.zip"
        url = base_url + filename
        
        print(f"Downloading {filename}...")
        
        try:
            response = requests.get(url, headers=headers, timeout=300)
            response.raise_for_status()
            
            # Extract zip content in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for table in SEC_TABLES:
                    tsv_filename = f"{table}.tsv"
                    if tsv_filename in z.namelist():
                        tsv_content = z.read(tsv_filename).decode('utf-8', errors='replace')
                        downloaded_data.setdefault(table, []).append(tsv_content)
                        print(f"Extracted {tsv_filename} from {filename}")
                    else:
                        print(f"Warning: {tsv_filename} not found in {filename}")
                        
        except requests.RequestException as e:
            print(f"Failed to download {filename}: {e}")
            return {}
        except zipfile.BadZipFile as e:
            print(f"Failed to extract {filename}: {e}")
            return {}
    
    return downloaded_data


def parse_quarters(value: Optional[str]) -> List[str]:
    """Return [q1, q2, q3, q4] if value is None/empty, else [value] after validation."""
    if not (value or "").strip():
        return ALL_QUARTERS
    q = value.strip().lower()
    if q not in VALID_QUARTERS:
        raise ValueError(f"Quarter must be one of {VALID_QUARTERS}, got: {value!r}")
    return [q]


def main():
    """Main function to handle command line arguments and execute the pipeline."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Download SEC data directly to BigQuery."
    )
    parser.add_argument(
        "year",
        nargs="?",
        type=int,
        help="Year for which to download SEC data (e.g., 2024). Optional if you use --from-year/--to-year.",
    )
    parser.add_argument(
        "--from-year",
        dest="from_year",
        type=int,
        default=None,
        help="Start year for backfill (inclusive).",
    )
    parser.add_argument(
        "--to-year",
        dest="to_year",
        type=int,
        default=None,
        help="End year for backfill (inclusive).",
    )
    parser.add_argument(
        "--quarter",
        type=str,
        default=None,
        metavar="Q",
        help="Quarter to download: q1, q2, q3, or q4. If omitted, all quarters are downloaded.",
    )
    parser.add_argument(
        "--dataset",
        default=os.getenv("BIGQUERY_DATASET", "insider_transactions"),
        help="BigQuery dataset name (default: insider_transactions)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and process data but don't insert into BigQuery",
    )
    parser.add_argument(
        "--skip-dedupe",
        action="store_true",
        help="After inserts, do not run BigQuery dedupe (duplicates may remain)",
    )
    parser.add_argument(
        "--dedupe-only",
        action="store_true",
        help="Only run BigQuery dedupe for all configured SEC tables; no SEC download",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("SEC_BATCH_SIZE", "1000")),
        help="Batch size for BigQuery inserts (default: 1000)",
    )
    args = parser.parse_args()

    if args.dedupe_only:
        project_id = os.getenv("GOOGLE_PROJECT_ID")
        if not project_id:
            print("Error: GOOGLE_PROJECT_ID environment variable not set", file=sys.stderr)
            sys.exit(1)
        loader = SECBigQueryLoader(project_id, args.dataset)
        loader.batch_size = args.batch_size
        if args.skip_dedupe:
            print("Error: --dedupe-only cannot be combined with --skip-dedupe", file=sys.stderr)
            sys.exit(1)
        loader.skip_dedupe = False
        loader.ensure_dataset_exists()
        print(f"Dedupe-only: project={project_id} dataset={args.dataset}")
        if not loader.dedupe_all_configured_tables():
            sys.exit(1)
        print("Dedupe-only completed successfully")
        sys.exit(0)

    # Resolve year(s) to process
    current_year = datetime.now().year
    if args.from_year is not None and args.to_year is not None:
        if args.from_year > args.to_year:
            print("Error: --from-year must be <= --to-year", file=sys.stderr)
            sys.exit(1)
        years_to_process = list(range(args.from_year, args.to_year + 1))
    elif args.year is not None:
        years_to_process = [args.year]
    else:
        print("Error: Provide either positional `year` or both `--from-year` and `--to-year`.", file=sys.stderr)
        sys.exit(1)

    for y in years_to_process:
        if y < 2006 or y > current_year:
            print(f"Error: Year must be between 2006 and {current_year}. Got: {y}", file=sys.stderr)
            sys.exit(1)

    # Validate quarter
    try:
        quarters = parse_quarters(args.quarter or os.getenv("SEC_LOAD_QUARTER"))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Get project ID
    project_id = os.getenv("GOOGLE_PROJECT_ID")
    if not project_id:
        print("Error: GOOGLE_PROJECT_ID environment variable not set")
        sys.exit(1)

    quarters_desc = ", ".join(quarters) if len(quarters) == 4 else quarters[0]
    print(f"SEC Direct Pipeline: Loading years {years_to_process[0]} -> {years_to_process[-1]} quarters [{quarters_desc}] to BigQuery")
    print(f"Project: {project_id}, Dataset: {args.dataset}")

    # Initialize BigQuery loader
    loader = SECBigQueryLoader(project_id, args.dataset)
    loader.batch_size = args.batch_size
    if args.skip_dedupe:
        loader.skip_dedupe = True

    # Ensure dataset exists
    if not args.dry_run:
        loader.ensure_dataset_exists()

    success = True

    for year in years_to_process:
        print(f"=== Downloading SEC data for year {year} ===")
        downloaded_data = download_sec_data(year, quarters)

        if not downloaded_data:
            print(f"Error: No data downloaded for year {year}", file=sys.stderr)
            sys.exit(1)

        print(f"=== Processing and loading data for year {year} ===")
        for table in SEC_TABLES:
            if table not in downloaded_data:
                print(f"Warning: No data for {table} (year {year})")
                continue
                
            combined_tsv = "\n".join(downloaded_data[table])

            table_config = TABLE_CONFIGS.get(table, {})
            table_id = table_config.get("table_id", table.lower())

            print(f"Processing {table} ({len(downloaded_data[table])} quarters, year {year})...")

            if args.dry_run:
                print(f"DRY RUN: Would load {combined_tsv.count(chr(10))} rows to {table_id}")
            else:
                if not loader.process_table_data(table_id, combined_tsv, year):
                    success = False
                    print(f"Failed to load {table} (year {year})", file=sys.stderr)
                else:
                    print(f"Successfully loaded {table} (year {year})")

    if success:
        print(f"✅ SEC data pipeline completed successfully for years {years_to_process[0]} -> {years_to_process[-1]}")
    else:
        print(f"❌ SEC data pipeline completed with errors for years {years_to_process[0]} -> {years_to_process[-1]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
