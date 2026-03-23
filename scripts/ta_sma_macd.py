# Import Libraries
from datetime import datetime, timedelta
import pytz
import numpy as np
import pandas as pd
import pandas_ta as ta

# Fetch historical data using yfinance
def fetch_data(symbol: str, start: str, end: str, interval: str) -> pd.DataFrame:
    df = yf.download(symbol, start=start, end=end, interval=interval,
                     auto_adjust=True, progress=False, threads=False)

    if isinstance(df.columns, pd.MultiIndex):
        try:
            df = df.xs(symbol, axis=1, level=1)
        except KeyError:
            possible = [lev for lev in df.columns.levels[1]]
            raise KeyError(f"Symbol '{symbol}' not found in MultiIndex columns. "
                           f"Available: {possible}")
    else:
        pass

    # Ensure standardized column names
    df.columns = [c.title() for c in df.columns]
    return df.dropna()

    # SMA trend signal
def sma_trend_signal(df: pd.DataFrame, i: int, backcandles_prev: int) -> int:
    """
    Return:
      1  if ALL of [i-backcandles_prev .. i] have Open>SMA and Close>SMA  (uptrend)
     -1  if ALL of [i-backcandles_prev .. i] have Open<SMA and Close<SMA  (downtrend)
      0  otherwise
    """
    if i < backcandles_prev:
        return 0
    if np.isnan(df["SMA200"].iloc[i]):
        return 0

    start = i - backcandles_prev
    seg = df.iloc[start:i+1]
    up   = ((seg["Open"] > seg["SMA200"]) & (seg["Close"] > seg["SMA200"])).all()
    down = ((seg["Open"] < seg["SMA200"]) & (seg["Close"] < seg["SMA200"])).all()
    return 1 if up else (-1 if down else 0)

# Build features
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Indicators
    out["SMA200"] = ta.sma(out["Close"], length=SMA_LEN)
    macd = ta.macd(out["Close"], fast=12, slow=26, signal=9)
    out = out.join(macd)

    # sma_signal via the function specified
    out["sma_signal"] = 0
    out["sma_signal"] = [sma_trend_signal(out, i, BACKCANDLES_PREV) for i in range(len(out))]

    # MACD cross logic with pullback confirmation via histogram
    macd_line = out["MACD_12_26_9"]
    macd_sig  = out["MACDs_12_26_9"]
    macd_hist = out["MACDh_12_26_9"]
    macd_line_prev = macd_line.shift(1)
    macd_sig_prev  = macd_sig.shift(1)
    # macd_hist_prev = macd_hist.shift(1)

    # Params
    hist_thresh = 4e-6
    hist_window = 3 
    # "Any of last 3 bars" condition via rolling extremes
    hist_below_win = macd_hist.rolling(hist_window, min_periods=hist_window).min() < -hist_thresh
    hist_above_win = macd_hist.rolling(hist_window, min_periods=hist_window).max() >  hist_thresh

    # --- Entries ---
    # Long: MACD line crosses ABOVE signal line while both are BELOW zero (bullish resumption)
    bull_cross_below0 = (
        hist_below_win &                          # pullback was deep enough in last 3 bars
        (macd_line_prev <= macd_sig_prev) &       # was not above on prior bar
        (macd_line > macd_sig) &                  # crosses above now
        (macd_line < 0) & (macd_sig < 0)          # cross happens below zero
    )

    # Short: MACD line crosses BELOW signal line while both are ABOVE zero (bearish resumption)
    bear_cross_above0 = (
        hist_above_win &                          # pullback was deep enough in last 3 bars
        (macd_line_prev >= macd_sig_prev) &       # was not below on prior bar
        (macd_line < macd_sig) &                  # crosses below now
        (macd_line > 0) & (macd_sig > 0)          # cross happens above zero
    )

    out["MACD_signal"] = 0
    out.loc[bull_cross_below0, "MACD_signal"] = 1
    out.loc[bear_cross_above0, "MACD_signal"] = -1

    # Precomputed combined signal
    out["pre_signal"] = 0
    out.loc[(out["sma_signal"] == 1) & (out["MACD_signal"] == 1), "pre_signal"] = 1
    out.loc[(out["sma_signal"] == -1) & (out["MACD_signal"] == -1), "pre_signal"] = -1
    
    # Drop early NaNs from indicators
    out = out.dropna().copy()
    return out

