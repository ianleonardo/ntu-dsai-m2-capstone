from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from core.bigquery import query_bigquery
from core.config import settings
from core.cache import get_cached_item, set_cached_item
import pandas as pd

router = APIRouter()

# ─────────────────────────────────────────────
# Shared helper: cached max filing_date lookup
# ─────────────────────────────────────────────
TABLE = lambda: f"`{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`"

def _default_date_range() -> tuple[str, str]:
    """Returns (start_date, end_date) defaulting to 6 months before the latest filing_date.
    Result is cached for 10 minutes to avoid repeated BigQuery scans."""
    cache_key = "max_filing_date"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(f"SELECT MAX(filing_date) as max_date FROM {TABLE()}")
    if not df.empty and not pd.isna(df.iloc[0]["max_date"]):
        max_date = df.iloc[0]["max_date"]
    else:
        max_date = pd.Timestamp.now()

    end_date = max_date.strftime("%Y-%m-%d")
    start_date = (max_date - pd.Timedelta(days=180)).strftime("%Y-%m-%d")
    result = (start_date, end_date)
    set_cached_item(cache_key, result)
    return result


def _resolve_dates(start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    if not start_date:
        return _default_date_range()
    return start_date, end_date or pd.Timestamp.now().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────
# /summary
# ─────────────────────────────────────────────
@router.get("/summary")
async def get_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get combined market activity overview statistics."""
    start_date, end_date = _resolve_dates(start_date, end_date)

    query = f"""
        SELECT
            SUM(non_deriv_value_acquired)  AS purchase_value,
            COUNT(DISTINCT CASE WHEN non_deriv_value_acquired > 0 THEN ISSUERTRADINGSYMBOL END) AS purchase_company_count,
            SUM(non_deriv_value_disposed)  AS sales_value,
            COUNT(DISTINCT CASE WHEN non_deriv_value_disposed > 0 THEN ISSUERTRADINGSYMBOL END) AS sales_company_count
        FROM {TABLE()}
        WHERE filing_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """
    df = query_bigquery(query)
    if df.empty:
        return {"purchase_value_m": 0.0, "purchase_count": 0,
                "sales_value_m": 0.0, "sales_count": 0,
                "start_date": start_date, "end_date": end_date}

    row = df.iloc[0]
    return {
        "purchase_value_m": round(float(row["purchase_value"] or 0) / 1_000_000, 2),
        "purchase_count":   int(row["purchase_company_count"] or 0),
        "sales_value_m":    round(float(row["sales_value"] or 0) / 1_000_000, 2),
        "sales_count":      int(row["sales_company_count"] or 0),
        "start_date": start_date,
        "end_date":   end_date
    }


# ─────────────────────────────────────────────
# /top-transactions
# ─────────────────────────────────────────────
@router.get("/top-transactions")
async def get_top_transactions(
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    limit: int = 3
):
    """Get top buys and top sells by value within the given period."""
    start_date, end_date = _resolve_dates(start_date, end_date)
    date_filter = f"filing_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"

    # Single query: compute both buys and sells in one pass
    combined_query = f"""
        WITH base AS (
            SELECT ISSUERTRADINGSYMBOL AS ticker, ISSUERNAME AS company,
                   non_deriv_value_acquired, non_deriv_value_disposed
            FROM {TABLE()}
            WHERE {date_filter}
        ),
        top_buys AS (
            SELECT ticker, company, SUM(non_deriv_value_acquired) AS value, 'buy' AS side
            FROM base WHERE non_deriv_value_acquired > 0
            GROUP BY ticker, company
            ORDER BY value DESC LIMIT {limit}
        ),
        top_sells AS (
            SELECT ticker, company, SUM(non_deriv_value_disposed) AS value, 'sell' AS side
            FROM base WHERE non_deriv_value_disposed > 0
            GROUP BY ticker, company
            ORDER BY value DESC LIMIT {limit}
        )
        SELECT * FROM top_buys
        UNION ALL
        SELECT * FROM top_sells
    """
    df = query_bigquery(combined_query)

    def fmt(rows):
        return [{"ticker": r["ticker"], "company": r["company"],
                 "value_m": round(float(r["value"] or 0) / 1_000_000, 2)} for _, r in rows.iterrows()]

    buys_df  = df[df["side"] == "buy"]  if not df.empty else pd.DataFrame()
    sells_df = df[df["side"] == "sell"] if not df.empty else pd.DataFrame()

    return {
        "top_buys":   fmt(buys_df),
        "top_sells":  fmt(sells_df),
        "start_date": start_date,
        "end_date":   end_date
    }


# ─────────────────────────────────────────────
# /transactions
# ─────────────────────────────────────────────
@router.get("/transactions")
async def get_transactions(
    ticker:    Optional[str] = None,
    min_value: float = 0.0,
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    page:      int = 1,
    page_size: int = 25
):
    """Get paged and filtered insider transactions."""
    start_date, end_date = _resolve_dates(start_date, end_date)

    where_clauses = [f"filing_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"]
    if min_value > 0:
        where_clauses.append(f"total_non_deriv_value >= {min_value}")
    if ticker:
        where_clauses.append(f"ISSUERTRADINGSYMBOL = '{ticker.upper()}'")

    where_str = " AND ".join(where_clauses)
    offset    = (page - 1) * page_size

    # Combine data + count in a single query to halve round-trips
    query = f"""
        WITH filtered AS (
            SELECT * FROM {TABLE()}
            WHERE {where_str}
        )
        SELECT *, COUNT(*) OVER() AS _total_count
        FROM filtered
        ORDER BY filing_date DESC
        LIMIT {page_size} OFFSET {offset}
    """
    df = query_bigquery(query)

    total_count = int(df["_total_count"].iloc[0]) if not df.empty else 0
    df = df.drop(columns=["_total_count"], errors="ignore")

    return {
        "data":      df.to_dict(orient="records"),
        "total":     total_count,
        "page":      page,
        "page_size": page_size
    }


# ─────────────────────────────────────────────
# /tickers  (cached)
# ─────────────────────────────────────────────
@router.get("/tickers")
async def get_tickers():
    """Get list of available tickers (cached)."""
    cache_key = "tickers_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(
        f"SELECT DISTINCT ISSUERTRADINGSYMBOL AS ticker FROM {TABLE()} ORDER BY ticker"
    )
    tickers = df["ticker"].tolist()
    set_cached_item(cache_key, tickers)
    return tickers


# ─────────────────────────────────────────────
# /owners  (cached)
# ─────────────────────────────────────────────
@router.get("/owners")
async def get_owners():
    """Get list of reporting owners (cached)."""
    cache_key = "owners_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(
        f"SELECT DISTINCT reporting_owner_name FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.dim_reporting_owner` ORDER BY reporting_owner_name"
    )
    owners = df["reporting_owner_name"].tolist()
    set_cached_item(cache_key, owners)
    return owners
