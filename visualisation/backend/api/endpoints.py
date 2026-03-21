from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from core.bigquery import query_bigquery
from core.config import settings
from core.cache import get_cached_item, set_cached_item
import pandas as pd

router = APIRouter()

@router.get("/summary")
async def get_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get combined market activity overview statistics."""
    # Default to last 6 months if no date provided
    if not start_date:
        latest_date_query = f"SELECT MAX(FILING_DATE) as max_date FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`"
        latest_df = query_bigquery(latest_date_query)
        if not latest_df.empty and not pd.isna(latest_df.iloc[0]['max_date']):
            max_date = latest_df.iloc[0]['max_date']
            start_date = (max_date - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
            end_date = max_date.strftime('%Y-%m-%d')
        else:
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
            end_date = pd.Timestamp.now().strftime('%Y-%m-%d')

    query = f"""
        SELECT 
            SUM(non_deriv_value_acquired) as purchase_value,
            COUNT(DISTINCT CASE WHEN non_deriv_value_acquired > 0 THEN ISSUERTRADINGSYMBOL END) as purchase_company_count,
            SUM(non_deriv_value_disposed) as sales_value,
            COUNT(DISTINCT CASE WHEN non_deriv_value_disposed > 0 THEN ISSUERTRADINGSYMBOL END) as sales_company_count
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        WHERE FILING_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """
    df = query_bigquery(query)
    if df.empty:
        return {
            "purchase_value": 0, "purchase_count": 0,
            "sales_value": 0, "sales_count": 0,
            "start_date": start_date, "end_date": end_date
        }
    
    row = df.iloc[0]
    return {
        "purchase_value_m": round(float(row['purchase_value'] or 0) / 1_000_000, 2),
        "purchase_count": int(row['purchase_company_count'] or 0),
        "sales_value_m": round(float(row['sales_value'] or 0) / 1_000_000, 2),
        "sales_count": int(row['sales_company_count'] or 0),
        "start_date": start_date,
        "end_date": end_date
    }

@router.get("/top-transactions")
async def get_top_transactions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 3
):
    """Get top buys and top sells by value within the given period."""
    if not start_date:
        latest_date_query = f"SELECT MAX(FILING_DATE) as max_date FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`"
        latest_df = query_bigquery(latest_date_query)
        if not latest_df.empty and not pd.isna(latest_df.iloc[0]['max_date']):
            max_date = latest_df.iloc[0]['max_date']
            start_date = (max_date - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
            end_date = max_date.strftime('%Y-%m-%d')
        else:
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
            end_date = pd.Timestamp.now().strftime('%Y-%m-%d')

    date_filter = f"FILING_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')"

    top_buys_query = f"""
        SELECT ISSUERTRADINGSYMBOL as ticker, ISSUERNAME as company,
            SUM(non_deriv_value_acquired) as value
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        WHERE {date_filter} AND non_deriv_value_acquired > 0
        GROUP BY ticker, company
        ORDER BY value DESC
        LIMIT {limit}
    """

    top_sells_query = f"""
        SELECT ISSUERTRADINGSYMBOL as ticker, ISSUERNAME as company,
            SUM(non_deriv_value_disposed) as value
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        WHERE {date_filter} AND non_deriv_value_disposed > 0
        GROUP BY ticker, company
        ORDER BY value DESC
        LIMIT {limit}
    """

    buys_df = query_bigquery(top_buys_query)
    sells_df = query_bigquery(top_sells_query)

    def format_rows(df):
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "ticker": row["ticker"],
                "company": row["company"],
                "value_m": round(float(row["value"] or 0) / 1_000_000, 2)
            })
        return rows

    return {
        "top_buys": format_rows(buys_df),
        "top_sells": format_rows(sells_df),
        "start_date": start_date,
        "end_date": end_date
    }

@router.get("/transactions")
async def get_transactions(
    ticker: Optional[str] = None,
    role: Optional[str] = None,
    trade_type: Optional[str] = None,
    min_value: float = 0.0,  # Default to 0 as per user request (no filter for value)
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 25
):
    """Get paged and filtered insider transactions."""
    # Default to last 6 months if no date provided
    if not start_date:
        latest_date_query = f"SELECT MAX(FILING_DATE) as max_date FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`"
        latest_df = query_bigquery(latest_date_query)
        if not latest_df.empty and not pd.isna(latest_df.iloc[0]['max_date']):
            max_date = latest_df.iloc[0]['max_date']
            start_date = (max_date - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
            end_date = max_date.strftime('%Y-%m-%d')

    offset = (page - 1) * page_size
    where_clauses = []
    if min_value > 0:
        where_clauses.append(f"total_non_deriv_value >= {min_value}")
    if start_date and end_date:
        where_clauses.append(f"FILING_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')")
    
    if ticker:
        where_clauses.append(f"ISSUERTRADINGSYMBOL = '{ticker.upper()}'")
    # Note: trade_type and role filtering might need more complex joins or logic based on the schema
    # For now, we'll implement ticker and value filtering.
    
    where_str = " AND ".join(where_clauses)
    
    query = f"""
        SELECT *
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        WHERE {where_str}
        ORDER BY FILING_DATE DESC
        LIMIT {page_size} OFFSET {offset}
    """
    df = query_bigquery(query)
    
    # Get total count for pagination
    count_query = f"""
        SELECT COUNT(*) as total
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        WHERE {where_str}
    """
    count_df = query_bigquery(count_query)
    total_count = int(count_df.iloc[0]['total']) if not count_df.empty else 0
    
    return {
        "data": df.to_dict(orient="records"),
        "total": total_count,
        "page": page,
        "page_size": page_size
    }

@router.get("/tickers")
async def get_tickers():
    """Get list of available tickers (cached)."""
    cache_key = "tickers_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached
    
    query = f"""
        SELECT DISTINCT ISSUERTRADINGSYMBOL as ticker
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.sp500_insider_transactions`
        ORDER BY ticker
    """
    df = query_bigquery(query)
    tickers = df['ticker'].tolist()
    set_cached_item(cache_key, tickers)
    return tickers

@router.get("/owners")
async def get_owners():
    """Get list of reporting owners (cached)."""
    cache_key = "owners_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached
    
    query = f"""
        SELECT DISTINCT reporting_owner_name
        FROM `{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.dim_reporting_owner`
        ORDER BY reporting_owner_name
    """
    df = query_bigquery(query)
    owners = df['reporting_owner_name'].tolist()
    set_cached_item(cache_key, owners)
    return owners
