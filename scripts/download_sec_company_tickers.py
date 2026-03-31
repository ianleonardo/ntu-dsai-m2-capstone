#!/usr/bin/env python3
"""
Download SEC company_tickers.json to a local file.

Source:
  https://www.sec.gov/files/company_tickers.json
"""

import argparse
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

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


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Download SEC company_tickers.json to a local file."
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        metavar="PATH",
        help="Local path to save JSON.",
    )
    args = parser.parse_args()

    local_path = Path(args.output)

    if not download_company_tickers(local_path):
        sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()
