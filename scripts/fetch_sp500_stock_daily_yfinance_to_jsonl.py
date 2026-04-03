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
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import requests

SP500_CONSTITUENTS_URL = (
    "https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv"
)
DEFAULT_USER_AGENT = "ntu-dsai-m2-capstone/1.0 (yfinance sp500 pipeline)"
SMA_LEN = 200
# Calendar lookback for yfinance `start` (inclusive). 200 *daily trading* bars need ~290 calendar days
# (~252 trading days/year); smaller lookbacks leave SMA200 as null for every output row, and tap-jsonl
# then infers SMA200 as type ["null"] → BigQuery merge failures against FLOAT64.
TA_LOOKBACK_CALENDAR_DAYS = 420
BACKCANDLES_PREV = 3
HIST_THRESH = 4e-6
HIST_WINDOW = 3


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


def _json_float(value) -> float | None:
    """BigQuery FLOAT64 merge requires JSON numbers, not strings — coerce object/str/NA."""
    import pandas as pd

    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ("nan", "none", "null", "nat"):
            return None
        try:
            return float(s)
        except ValueError:
            return None
    n = _safe_json_number(value)
    if n is None:
        return None
    out = float(n)
    import math

    if math.isnan(out) or math.isinf(out):
        return None
    return out


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


def _sma_trend_signal(df, i: int, backcandles_prev: int, sma_col: str) -> int:
    """Trend signal from script/ta_sma_macd.py logic."""
    if i < backcandles_prev:
        return 0
    if df[sma_col].isna().iloc[i]:
        return 0
    start = i - backcandles_prev
    seg = df.iloc[start : i + 1]
    up = ((seg["open"] > seg[sma_col]) & (seg["close"] > seg[sma_col])).all()
    down = ((seg["open"] < seg[sma_col]) & (seg["close"] < seg[sma_col])).all()
    return 1 if up else (-1 if down else 0)


def _add_ta_columns(
    sub,
    sma_col: str = "SMA200",
    signal_col: str = "pre_signal",
    macd_col: str = "MACD_12_26_9",
    macds_col: str = "MACDs_12_26_9",
    macdh_col: str = "MACDh_12_26_9",
):
    """Add SMA/MACD/pre_signal columns using ta_sma_macd.py semantics."""
    out = sub.copy()
    out = out.sort_values("date").reset_index(drop=True)

    # Indicators
    out[sma_col] = out["close"].rolling(window=SMA_LEN, min_periods=SMA_LEN).mean()
    ema_fast = out["close"].ewm(span=12, adjust=False).mean()
    ema_slow = out["close"].ewm(span=26, adjust=False).mean()
    out[macd_col] = ema_fast - ema_slow
    out[macds_col] = out[macd_col].ewm(span=9, adjust=False).mean()
    out[macdh_col] = out[macd_col] - out[macds_col]

    # sma_signal
    out["sma_signal"] = [
        _sma_trend_signal(out, i, BACKCANDLES_PREV, sma_col) for i in range(len(out))
    ]

    # MACD cross logic with pullback confirmation via histogram
    macd_line = out[macd_col]
    macd_sig = out[macds_col]
    macd_hist = out[macdh_col]
    macd_line_prev = macd_line.shift(1)
    macd_sig_prev = macd_sig.shift(1)

    hist_below_win = (
        macd_hist.rolling(HIST_WINDOW, min_periods=HIST_WINDOW).min() < -HIST_THRESH
    )
    hist_above_win = (
        macd_hist.rolling(HIST_WINDOW, min_periods=HIST_WINDOW).max() > HIST_THRESH
    )

    bull_cross_below0 = (
        hist_below_win
        & (macd_line_prev <= macd_sig_prev)
        & (macd_line > macd_sig)
        & (macd_line < 0)
        & (macd_sig < 0)
    )
    bear_cross_above0 = (
        hist_above_win
        & (macd_line_prev >= macd_sig_prev)
        & (macd_line < macd_sig)
        & (macd_line > 0)
        & (macd_sig > 0)
    )

    out["MACD_signal"] = 0
    out.loc[bull_cross_below0, "MACD_signal"] = 1
    out.loc[bear_cross_above0, "MACD_signal"] = -1

    out[signal_col] = 0
    out.loc[(out["sma_signal"] == 1) & (out["MACD_signal"] == 1), signal_col] = 1
    out.loc[(out["sma_signal"] == -1) & (out["MACD_signal"] == -1), signal_col] = -1

    # Keep rows for loading; represent unavailable indicators as null in JSONL.
    return out


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
    out_start = date.fromisoformat(start)
    out_end = date.fromisoformat(end)
    # Download extra history so SMA200 / MACD are numeric on rows inside [start, end]. A short
    # window with only nulls makes tap-jsonl infer SMA200 as type ["null"] → BigQuery STRING,
    # which then fails merge into an existing FLOAT64 column.
    fetch_start = out_start - timedelta(days=TA_LOOKBACK_CALENDAR_DAYS)
    fetch_start_str = fetch_start.isoformat()
    print(
        f"Yahoo download range: {fetch_start_str} .. {end} (JSONL rows only {start} .. {end})",
        flush=True,
    )

    # Stream write JSONL to avoid holding the whole dataset in memory.
    total_rows = 0
    started_at = datetime.utcnow()
    chunks_done = 0
    total_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    with out_jsonl.open("w", encoding="utf-8") as f:
        for i in range(0, len(symbols), chunk_size):
            chunk_symbols = symbols[i : i + chunk_size]
            chunk_yf = [_to_yahoo_symbol(s) for s in chunk_symbols]
            yf_to_raw = {y: raw for y, raw in zip(chunk_yf, chunk_symbols)}

            print(
                f"[chunk {chunks_done + 1}/{total_chunks}] Fetching {len(chunk_symbols)} tickers ({i+1}-{min(i+chunk_size, len(symbols))})...",
                flush=True,
            )
            df = yf.download(
                chunk_yf,
                start=fetch_start_str,
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
                sub = _add_ta_columns(
                    sub,
                    sma_col="SMA200",
                    signal_col="pre_signal",
                    macd_col="MACD_12_26_9",
                    macds_col="MACDs_12_26_9",
                    macdh_col="MACDh_12_26_9",
                )

                ts = pd.to_datetime(sub["date"])
                in_window = (ts.dt.date >= out_start) & (ts.dt.date <= out_end)
                sub = sub.loc[in_window]

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
                        "open": _json_float(row.get("open")),
                        "high": _json_float(row.get("high")),
                        "low": _json_float(row.get("low")),
                        "close": _json_float(row.get("close")),
                        # target-bigquery expects integer volume
                        "volume": _safe_json_int(row.get("volume")),
                        "SMA200": _json_float(row.get("SMA200")),
                        "pre_signal": _safe_json_int(row.get("pre_signal")),
                        "MACD_12_26_9": _json_float(row.get("MACD_12_26_9")),
                        "MACDs_12_26_9": _json_float(row.get("MACDs_12_26_9")),
                        "MACDh_12_26_9": _json_float(row.get("MACDh_12_26_9")),
                    }
                    # Avoid writing completely empty rows.
                    if record["close"] is None and record["open"] is None:
                        continue
                    # Reject NaN/Inf so tap-jsonl + BQ never see ambiguous non-JSON literals.
                    f.write(
                        json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n"
                    )
                    total_rows += 1
            chunks_done += 1
            elapsed_sec = int((datetime.utcnow() - started_at).total_seconds())
            print(
                f"[heartbeat] chunks={chunks_done}/{total_chunks}, rows_written={total_rows}, elapsed={elapsed_sec}s",
                flush=True,
            )

    print(f"Wrote {total_rows} JSONL rows to {out_jsonl}", flush=True)


if __name__ == "__main__":
    main()

