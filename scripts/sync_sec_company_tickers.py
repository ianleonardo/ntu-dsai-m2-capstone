#!/usr/bin/env python3
"""
Download SEC company_tickers.json and write JSONL for Meltano tap-jsonl.

Downloads https://www.sec.gov/files/company_tickers.json and writes one JSON
object per line (JSONL) to staging/company_tickers.jsonl for tap-jsonl.
No CSV conversion; records are flat objects with cik_str, ticker, title.
"""

import argparse
import json
import sys
from pathlib import Path

import requests

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT = "myemail@example.com"


def main():
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Download SEC company_tickers.json to staging JSONL.")
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=repo_root / "meltano-ingestion" / "staging",
        help="Directory to write company_tickers.jsonl (default: meltano-ingestion/staging)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSONL path (default: <staging-dir>/company_tickers.jsonl)",
    )
    args = parser.parse_args()

    out_path = args.output or args.staging_dir / "company_tickers.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {COMPANY_TICKERS_URL} ...")
    resp = requests.get(COMPANY_TICKERS_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
    if resp.status_code != 200:
        print(f"Failed: HTTP {resp.status_code}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    if not isinstance(data, dict):
        print("Unexpected JSON shape: not an object", file=sys.stderr)
        sys.exit(1)

    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for _key, obj in data.items():
            if not isinstance(obj, dict):
                continue
            record = {
                "cik_str": obj.get("cik_str", ""),
                "ticker": obj.get("ticker", ""),
                "title": obj.get("title", ""),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    print(f"Wrote {count} records to {out_path}")


if __name__ == "__main__":
    main()
