#!/usr/bin/env python3
"""
Script to download SEC data to cloud bucket for a given year.

Optional quarter (q1, q2, q3, q4): if not specified, all quarters are downloaded.
"""

import argparse
import sys
import requests
import zipfile
import io
import os
import shutil
from pathlib import Path
from google.cloud import storage
from google.api_core import retry
from dotenv import load_dotenv

VALID_QUARTERS = ("q1", "q2", "q3", "q4")
ALL_QUARTERS = list(VALID_QUARTERS)


def parse_quarters(value):
    """Return [q1, q2, q3, q4] if value is None/empty, else [value] after validation."""
    if not (value or "").strip():
        return ALL_QUARTERS
    q = value.strip().lower()
    if q not in VALID_QUARTERS:
        raise ValueError(f"Quarter must be one of {VALID_QUARTERS}, got: {value!r}")
    return [q]


def download_sec_data(year, quarters=None):
    """Download SEC insider transaction data for specified year and quarters."""
    if quarters is None:
        quarters = ALL_QUARTERS
    quarter_ids = [f"{year}{q}" for q in quarters]
    base_url = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/"
    output_dir = f"../data/sec/{year}"
    os.makedirs(output_dir, exist_ok=True)

    headers = {"User-Agent": "myemail@example.com"}  # SEC requires a User-Agent

    for quarter in quarter_ids:
        filename = f"{quarter}_form345.zip"
        url = base_url + filename
        print(f"Downloading {filename}...")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(f"{output_dir}/{quarter}")
            print(f"Extracted {quarter} to {output_dir}/{quarter}")
        else:
            print(f"Failed to download {filename}: Status {response.status_code}")
            return False
    
    return True


def upload_to_bucket(year, local_path, bucket_name="dsai-m2-bucket", keep_local=False):
    """Upload downloaded data to Google Cloud Storage bucket."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get project ID from environment
        project_id = os.getenv('GOOGLE_PROJECT_ID')
        if not project_id:
            print("Error: GOOGLE_PROJECT_ID environment variable not set")
            return False
        
        # Initialize Google Cloud Storage client with project ID
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        print(f"Uploading data from {local_path} to GCS bucket {bucket_name} (project: {project_id})...")
        
        # Upload all files for the year
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                # Create blob path relative to the local_path
                blob_path = os.path.relpath(local_file_path, local_path)
                blob = bucket.blob(f"sec-data/{year}/{blob_path}")
                
                # Upload file with retry logic
                blob.upload_from_filename(
                    local_file_path,
                    timeout=300,
                    retry=retry.Retry(
                        initial=1.0,
                        maximum=60.0,
                        multiplier=2.0,
                        deadline=300.0
                    )
                )
                print(f"Uploaded {blob_path} to gs://{bucket_name}/sec-data/{year}/{blob_path}")
        
        print(f"Successfully uploaded all SEC data for {year} to GCS bucket")
        
        # Remove local data after successful upload (unless keep_local is True)
        if not keep_local:
            try:
                print(f"Removing local data from {local_path}...")
                shutil.rmtree(local_path)
                print(f"Successfully removed local data for {year}")
            except Exception as e:
                print(f"Warning: Failed to remove local data {local_path}: {str(e)}")
        else:
            print(f"Keeping local data at {local_path} (as requested)")
        
        return True
        
    except Exception as e:
        print(f"Error uploading to GCS bucket: {str(e)}")
        return False


def main():
    """Main function to handle command line arguments and download SEC data."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Download SEC data to cloud bucket for a specified year (and optional quarter)."
    )
    parser.add_argument(
        "year",
        type=int,
        help="Year for which to download SEC data (e.g., 2025)",
    )
    parser.add_argument(
        "--quarter",
        type=str,
        default=None,
        metavar="Q",
        help="Quarter to download: q1, q2, q3, or q4. If omitted, all quarters are downloaded.",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET_NAME", "dsai-m2-bucket"),
        help="Google Cloud Storage bucket name (default: dsai-m2-bucket or GCS_BUCKET_NAME env var)",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local data after upload (default: remove local data)",
    )
    args = parser.parse_args()

    # Validate year
    current_year = 2026
    if args.year < 2006 or args.year > current_year:
        print(f"Error: Year must be between 2006 and {current_year}")
        sys.exit(1)

    try:
        quarters = parse_quarters(args.quarter or os.getenv("SEC_LOAD_QUARTER"))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    quarters_desc = ", ".join(quarters) if len(quarters) == 4 else quarters[0]
    print(f"Downloading SEC data for year {args.year} quarters [{quarters_desc}]...")

    if download_sec_data(args.year, quarters=quarters):
        local_path = f"../data/sec/{args.year}"
        
        # Upload to cloud bucket
        if upload_to_bucket(args.year, local_path, args.bucket, args.keep_local):
            print(f"SEC data for {args.year} downloaded and uploaded to bucket successfully.")
        else:
            print(f"Failed to upload SEC data for {args.year} to bucket.")
            sys.exit(1)
    else:
        print(f"Failed to download SEC data for {args.year}")
        sys.exit(1)


if __name__ == "__main__":
    main()
