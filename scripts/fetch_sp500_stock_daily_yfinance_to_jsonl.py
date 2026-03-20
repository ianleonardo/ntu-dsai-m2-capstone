#!/usr/bin/env python3
"""
Fetch daily stock OHLCV for all S&P 500 constituents and write JSONL for Meltano.

This script:
  1) Downloads S&P 500 constituents from DataHub.
  2) Converts tickers to Yahoo Finance format when needed.
  3) Uses yfinance to fetch 1d interval data between --start and --end.
  4) Writes one JSON object per (symbol, date) row to JSONL:
       {symbol, date, year, open, high, low, close, volume}

The resulting JSONL is intended for:
  tap-jsonl -> target-bigquery (upsert by primary key: symbol+date)
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from datetime import date
from pathlib import Path

import requests

SP500_CONSTITUENTS_URL = (
    "https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv"
)
DEFAULT_USER_AGENT = "ntu-dsai-m2-capstone/1.0 (yfinance sp500 pipeline)"


def _parse_date(value: str) -> str:
    # yfinance accepts ISO strings; validate format lightly.
    try:
        d = date.fromisoformat(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from e
    return d.isoformat()


def _download_sp500_symbols() -> list[str]:
    resp = requests.get(
        SP500_CONSTITUENTS_URL,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to download S&P 500 constituents: HTTP {resp.status_code}"
        )

    reader = csv.DictReader(io.StringIO(resp.text))
    symbols: list[str] = []
    for row in reader:
        symbol = (row.get("Symbol") or "").strip()
        if not symbol:
            continue
        symbols.append(symbol)

    # De-dup while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _to_yahoo_symbol(symbol: str) -> str:
    # Yahoo Finance uses '-' instead of '.' for many tickers (e.g., BRK.B -> BRK-B).
    return symbol.replace(".", "-")


def _safe_json_number(value) -> float | int | None:
    # Convert NaN/NaT to None, keep numbers as Python scalars.
    import pandas as pd

    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        # numpy scalar -> python scalar
        return value.item()
    except Exception:
        return float(value)


def _safe_json_int(value) -> int | None:
    """Convert numeric values to JSON integer scalars.

    yfinance sometimes returns volume as floats like 4960400.0; target-bigquery
    expects an integer for volume, so we coerce integral floats to int.
    """
    import pandas as pd

    if value is None:
        return None
    if pd.isna(value):
        return None
    try:
        # Handle numpy scalar -> python scalar
        if hasattr(value, "item"):
            value = value.item()
        # Common case: float ending in .0
        f = float(value)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return int(f)
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch S&P 500 daily stock data (1d) and write JSONL for Meltano."
    )
    parser.add_argument("--start", type=_parse_date, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=_parse_date, required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50,
        help="How many tickers to request per yfinance call (default: 50).",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=None,
        help="Output JSONL path (default: staging/sp500_stock_daily.jsonl).",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "dataprocessing"
        / "meltano_ingestion"
        / "staging",
        help="Local Meltano staging directory (default: meltano_ingestion/staging).",
    )
    args = parser.parse_args()

    try:
        import pandas as pd
        import yfinance as yf
    except ModuleNotFoundError as e:
        print(
            "Missing dependency. Install with:\n"
            "  uv add yfinance pandas\n"
            "  # (or pip install yfinance pandas)\n",
            file=sys.stderr,
        )
        raise e

    symbols = _download_sp500_symbols()
    if not symbols:
        raise SystemExit("No S&P 500 symbols parsed.")

    staging_dir: Path = args.staging_dir
    staging_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = args.output_jsonl or (staging_dir / "sp500_stock_daily.jsonl")

    chunk_size = max(1, int(args.chunk_size))
    start = args.start
    end = args.end

    # Stream write JSONL to avoid holding the whole dataset in memory.
    total_rows = 0
    with out_jsonl.open("w", encoding="utf-8") as f:
        for i in range(0, len(symbols), chunk_size):
            chunk_symbols = symbols[i : i + chunk_size]
            chunk_yf = [_to_yahoo_symbol(s) for s in chunk_symbols]
            yf_to_raw = {y: raw for y, raw in zip(chunk_yf, chunk_symbols)}

            print(
                f"Fetching {len(chunk_symbols)} tickers ({i+1}-{min(i+chunk_size, len(symbols))})..."
            )
            df = yf.download(
                chunk_yf,
                start=start,
                end=end,
                interval="1d",
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )

            if df is None or getattr(df, "empty", True):
                continue

            # Expected shape with group_by='ticker': columns MultiIndex: (ticker, field)
            if isinstance(df.columns, pd.MultiIndex):
                tickers_in_df = list(df.columns.get_level_values(0).unique())
            else:
                tickers_in_df = chunk_yf

            for yf_symbol in tickers_in_df:
                if yf_symbol not in yf_to_raw:
                    continue
                raw_symbol = yf_to_raw[yf_symbol]

                if isinstance(df.columns, pd.MultiIndex):
                    if yf_symbol not in df.columns.get_level_values(0):
                        continue
                    sub = df[yf_symbol].copy()
                else:
                    sub = df.copy()

                # yfinance returns DatetimeIndex; normalize to a 'date' field.
                sub = sub.reset_index()
                # Common date column name is 'Date'
                date_col = "Date" if "Date" in sub.columns else "Datetime" if "Datetime" in sub.columns else None
                if date_col is None:
                    raise RuntimeError(
                        f"Unexpected yfinance index column names: {list(sub.columns)}"
                    )
                sub = sub.rename(columns={date_col: "date"})

                # Standardize expected OHLCV column names.
                # With auto_adjust=True, yfinance typically uses Open/High/Low/Close/Volume.
                col_map = {
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
                for src, dst in col_map.items():
                    if src not in sub.columns:
                        sub[dst] = None
                    else:
                        sub[dst] = sub[src]

                # Drop rows without a date.
                sub = sub.dropna(subset=["date"]).copy()

                for _idx, row in sub.iterrows():
                    d = row["date"]
                    if pd.isna(d):
                        continue
                    # Convert pandas Timestamp -> ISO date string
                    iso_date = d.date().isoformat() if hasattr(d, "date") else str(d)
                    year = int(d.year) if hasattr(d, "year") else int(str(d)[:4])

                    record = {
                        "symbol": raw_symbol,
                        "date": iso_date,
                        "year": year,
                        "open": _safe_json_number(row.get("open")),
                        "high": _safe_json_number(row.get("high")),
                        "low": _safe_json_number(row.get("low")),
                        "close": _safe_json_number(row.get("close")),
                        # target-bigquery expects integer volume
                        "volume": _safe_json_int(row.get("volume")),
                    }
                    # Avoid writing completely empty rows.
                    if record["close"] is None and record["open"] is None:
                        continue
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    total_rows += 1

    print(f"Wrote {total_rows} JSONL rows to {out_jsonl}")


if __name__ == "__main__":
    main()

