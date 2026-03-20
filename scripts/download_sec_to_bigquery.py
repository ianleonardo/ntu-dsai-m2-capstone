#!/usr/bin/env python3
"""
Simplified SEC Data Pipeline: Direct download from SEC website to BigQuery.

This script eliminates the GCS intermediate step and streams SEC data directly
to BigQuery, reducing complexity, cost, and processing time.

Usage:
    python scripts/download_sec_to_bigquery.py 2024
    python scripts/download_sec_to_bigquery.py 2024 --quarter q1
    python scripts/download_sec_to_bigquery.py 2024 --dry-run

Environment variables:
    GOOGLE_PROJECT_ID: GCP project ID (required)
    GOOGLE_APPLICATION_CREDENTIALS: Path to service account key (optional)
    BIGQUERY_DATASET: BigQuery dataset name (default: insider_transactions)
    SEC_BATCH_SIZE: Batch size for BigQuery inserts (default: 1000)
"""

import argparse
import sys
import requests
import zipfile
import io
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
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
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
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
    "NONDERIV_HOLDING": {
        "table_id": "sec_nonderiv_holding",
        "primary_keys": ["ACCESSION_NUMBER", "NONDERIV_HOLDING_SK"],
    },
    "DERIV_TRANS": {
        "table_id": "sec_deriv_trans",
        "primary_keys": ["ACCESSION_NUMBER", "DERIV_TRANS_SK"],
    },
    "DERIV_HOLDING": {
        "table_id": "sec_deriv_holding",
        "primary_keys": ["ACCESSION_NUMBER", "DERIV_HOLDING_SK"],
    },
}


class SECBigQueryLoader:
    """Handles direct SEC data loading to BigQuery."""
    
    def __init__(self, project_id: str, dataset: str = "insider_transactions"):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.batch_size = int(os.getenv("SEC_BATCH_SIZE", "1000"))
        
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
        
        try:
            # Configure retry strategy
            retry_strategy = retry.Retry(
                initial=1.0,
                maximum=60.0,
                multiplier=2.0,
                deadline=300.0
            )
            
            # Stream insert in batches
            for i in range(0, len(rows), self.batch_size):
                batch = rows[i:i + self.batch_size]
                errors = self.client.insert_rows_json(table_id, batch, retry=retry_strategy)
                
                if errors:
                    print(f"Errors inserting batch to {table_name}: {errors}")
                    return False
                    
            print(f"Successfully inserted {len(rows)} rows to {table_id}")
            return True
            
        except GoogleAPICallError as e:
            print(f"BigQuery API error for {table_name}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error inserting to {table_name}: {e}")
            return False
    
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
        
        return self.stream_to_bigquery(table_name, rows, year)


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
        description="Download SEC data directly to BigQuery (no GCS intermediate)."
    )
    parser.add_argument(
        "year",
        type=int,
        help="Year for which to download SEC data (e.g., 2024)",
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
        "--batch-size",
        type=int,
        default=int(os.getenv("SEC_BATCH_SIZE", "1000")),
        help="Batch size for BigQuery inserts (default: 1000)",
    )
    args = parser.parse_args()

    # Validate year
    current_year = datetime.now().year
    if args.year < 2006 or args.year > current_year:
        print(f"Error: Year must be between 2006 and {current_year}")
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
    print(f"SEC Direct Pipeline: Loading {args.year} quarters [{quarters_desc}] to BigQuery")
    print(f"Project: {project_id}, Dataset: {args.dataset}")

    # Initialize BigQuery loader
    loader = SECBigQueryLoader(project_id, args.dataset)
    loader.batch_size = args.batch_size

    # Ensure dataset exists
    if not args.dry_run:
        loader.ensure_dataset_exists()

    # Download SEC data
    print("=== Downloading SEC data ===")
    downloaded_data = download_sec_data(args.year, quarters)
    
    if not downloaded_data:
        print("Error: No data downloaded")
        sys.exit(1)

    # Process and load data
    print("=== Processing and loading data ===")
    success = True
    
    for table in SEC_TABLES:
        if table not in downloaded_data:
            print(f"Warning: No data for {table}")
            continue
            
        # Combine data from all quarters
        combined_tsv = "\n".join(downloaded_data[table])
        
        # Get table configuration
        table_config = TABLE_CONFIGS.get(table, {})
        table_id = table_config.get("table_id", table.lower())
        
        print(f"Processing {table} ({len(downloaded_data[table])} quarters)...")
        
        if args.dry_run:
            print(f"DRY RUN: Would load {combined_tsv.count(chr(10))} rows to {table_id}")
        else:
            if not loader.process_table_data(table_id, combined_tsv, args.year):
                success = False
                print(f"Failed to load {table}")
            else:
                print(f"Successfully loaded {table}")

    if success:
        print(f"✅ SEC data pipeline completed successfully for {args.year}")
    else:
        print(f"❌ SEC data pipeline completed with errors for {args.year}")
        sys.exit(1)


if __name__ == "__main__":
    main()
