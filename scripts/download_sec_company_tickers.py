#!/usr/bin/env python3
"""
Download SEC company_tickers.json and upload to GCS bucket.

Downloads https://www.sec.gov/files/company_tickers.json and uploads to
gs://<bucket>/sec-data/company_tickers.json (same bucket/prefix as SEC insider data).
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.api_core import retry

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_USER_AGENT = "myemail@example.com"  # SEC requires a User-Agent


def download_company_tickers(dest_path: str | Path) -> bool:
    """Download company_tickers.json from SEC to dest_path."""
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": SEC_USER_AGENT}
    print(f"Downloading {COMPANY_TICKERS_URL} ...")
    response = requests.get(COMPANY_TICKERS_URL, headers=headers, timeout=60)
    if response.status_code != 200:
        print(f"Failed to download: HTTP {response.status_code}", file=sys.stderr)
        return False
    dest_path.write_bytes(response.content)
    print(f"Saved to {dest_path}")
    return True


def upload_to_bucket(
    local_path: str | Path,
    bucket_name: str,
    blob_path: str = "sec-data/company_tickers.json",
    keep_local: bool = False,
) -> bool:
    """Upload file to GCS bucket under sec-data/."""
    load_dotenv()
    project_id = os.getenv("GOOGLE_PROJECT_ID")
    if not project_id:
        print("Error: GOOGLE_PROJECT_ID environment variable not set", file=sys.stderr)
        return False

    local_path = Path(local_path)
    if not local_path.is_file():
        print(f"Error: file not found: {local_path}", file=sys.stderr)
        return False

    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        print(f"Uploading to gs://{bucket_name}/{blob_path} ...")
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
        print(f"Uploaded to gs://{bucket_name}/{blob_path}")
        if not keep_local and local_path.name.startswith("company_tickers"):
            try:
                local_path.unlink()
                print(f"Removed local file {local_path}")
            except OSError as e:
                print(f"Warning: could not remove {local_path}: {e}")
        return True
    except Exception as e:
        print(f"Error uploading to GCS: {e}", file=sys.stderr)
        return False


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Download SEC company_tickers.json and upload to GCS bucket (sec-data/)."
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET_NAME", "dsai-m2-bucket"),
        help="GCS bucket name (default: GCS_BUCKET_NAME or dsai-m2-bucket)",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local file after upload",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        metavar="PATH",
        help="Local path to save JSON (default: temp file)",
    )
    args = parser.parse_args()

    if args.output:
        local_path = Path(args.output)
    else:
        fd, local_path = tempfile.mkstemp(suffix=".json", prefix="company_tickers_")
        os.close(fd)
        local_path = Path(local_path)

    if not download_company_tickers(local_path):
        sys.exit(1)
    if not upload_to_bucket(
        local_path,
        args.bucket,
        keep_local=args.keep_local or bool(args.output),
    ):
        sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()
