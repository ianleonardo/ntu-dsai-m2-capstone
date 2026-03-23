#!/usr/bin/env python3
"""
Download historical OHLCV data from Yahoo Finance (yfinance) and save to CSV.

Required parameters:
  - list of stocks (symbols)
  - start date
  - end date
  - interval

Example:
  python scripts/get_stock_data_yfinance.py \
    --symbols AAPL,MSFT,TSLA \
    --start 2023-01-01 \
    --end 2023-12-31 \
    --interval 1d \
    --output-file data/yfinance/yf_1d_2023-01-01_2023-12-31.csv
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


def _parse_symbols(values: list[str]) -> list[str]:
    # Supports both "--symbols AAPL,MSFT" and "--symbols AAPL --symbols MSFT"
    out: list[str] = []
    for v in values:
        for part in v.split(","):
            s = part.strip().upper()
            if s:
                out.append(s)
    # De-dup while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


def _parse_date(value: str) -> str:
    # Keep as string because yfinance accepts ISO date strings.
    # Validate format lightly (YYYY-MM-DD).
    try:
        d = date.fromisoformat(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from e
    return d.isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Download stock data from yfinance.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Tickers list. Accepts comma-separated or repeated flags.",
    )
    parser.add_argument("--start", type=_parse_date, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=_parse_date, required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--interval",
        default="1d",
        help="yfinance interval (e.g. 1d, 1h, 15m). Default: 1d",
    )
    parser.add_argument(
        "--auto-adjust",
        action="store_true",
        default=True,
        help="Use auto_adjust=True (default on).",
    )
    parser.add_argument(
        "--no-auto-adjust",
        dest="auto_adjust",
        action="store_false",
        help="Disable auto_adjust.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Output CSV path. If omitted, writes to data/yfinance/yf_<interval>_<start>_<end>.csv",
    )
    args = parser.parse_args()

    try:
        import pandas as pd
        import yfinance as yf
    except ModuleNotFoundError as e:
        print(
            "Missing dependency. Install with:\n"
            "  uv add yfinance\n"
            "  # (or pip install yfinance)\n",
            file=sys.stderr,
        )
        raise e

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("No valid symbols provided.")

    if args.output_file is None:
        out_dir = Path("data/yfinance")
        out_dir.mkdir(parents=True, exist_ok=True)
        args.output_file = out_dir / f"yf_{args.interval}_{args.start}_{args.end}.csv"
    else:
        args.output_file.parent.mkdir(parents=True, exist_ok=True)

    all_frames: list[pd.DataFrame] = []

    for symbol in symbols:
        df = yf.download(
            symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            auto_adjust=args.auto_adjust,
            progress=False,
            threads=False,
        )

        if df is None or getattr(df, "empty", True):
            print(f"Warning: no data for {symbol}", file=sys.stderr)
            continue

        # yfinance can sometimes return MultiIndex columns; flatten to the symbol.
        if isinstance(df.columns, pd.MultiIndex):
            try:
                df = df.xs(symbol, axis=1, level=0)
            except KeyError:
                # Some yfinance versions use a different level ordering; fallback to last level.
                df = df.xs(symbol, axis=1, level=-1)

        # Standardize column names (matches your notebook style).
        df.columns = [str(c).title() for c in df.columns]
        df = df.dropna()

        # Move datetime index to a column; yfinance uses a DatetimeIndex.
        df = df.reset_index()
        # Name the datetime column consistently.
        if "Date" in df.columns and "Datetime" not in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns and "Datetime" not in df.columns:
            df = df.rename(columns={"index": "Datetime"})

        df["Symbol"] = symbol
        all_frames.append(df)

    if not all_frames:
        raise SystemExit("No frames collected; all symbols returned empty data.")

    out = pd.concat(all_frames, ignore_index=True)
    out.to_csv(args.output_file, index=False)
    print(f"Wrote {len(out)} rows to {args.output_file}")


if __name__ == "__main__":
    main()

