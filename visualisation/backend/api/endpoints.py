from fastapi import APIRouter, Query
from typing import Any, List, Literal, Optional
import json
import math
import re

import numpy as np
import pandas as pd

from core.bigquery import query_bigquery
from core.bq import fqtn, sp500_mart, sp500_stock_daily, stg_sec_reportingowner
from core.config import settings
from core.cache import (
    get_cached_item,
    set_cached_item,
    get_summary_cache,
    set_summary_cache,
    get_transactions_cache,
    set_transactions_cache,
    get_clusters_cache,
    set_clusters_cache,
    get_cluster_breakdown_cache,
    set_cluster_breakdown_cache,
)

router = APIRouter()

MART = sp500_mart

# BigQuery → pandas often lowercases column names; TransactionTable ColDefs use SEC-style uppercase
_TRANSACTION_COL_CANONICAL = {
    "accession_number": "ACCESSION_NUMBER",
    "filing_date": "FILING_DATE",
    "period_of_report": "PERIOD_OF_REPORT",
    "trans_date": "TRANS_DATE",
    "date_of_original_submission": "DATE_OF_ORIGINAL_SUBMISSION",
    "no_securities_owned": "NO_SECURITIES_OWNED",
    "issuercik": "ISSUERCIK",
    "issuername": "ISSUERNAME",
    "issuertradingsymbol": "ISSUERTRADINGSYMBOL",
    "filing_date_key": "filing_date_key",
    "period_report_date_key": "period_report_date_key",
    "non_deriv_transaction_count": "non_deriv_transaction_count",
    "total_transaction_count": "total_transaction_count",
    "non_deriv_shares_acquired": "non_deriv_shares_acquired",
    "non_deriv_shares_disposed": "non_deriv_shares_disposed",
    "total_shares_acquired": "total_shares_acquired",
    "total_shares_disposed": "total_shares_disposed",
    "non_deriv_value_acquired": "non_deriv_value_acquired",
    "non_deriv_value_disposed": "non_deriv_value_disposed",
    "total_non_deriv_value": "total_non_deriv_value",
    "total_non_deriv_shares_owned": "total_non_deriv_shares_owned",
    "reporting_owner_count": "reporting_owner_count",
    "reporting_owner_names": "reporting_owner_names",
    "reporting_owner_role_types": "reporting_owner_role_types",
    "reporting_owner_titles": "reporting_owner_titles",
    "transaction_type_from_code": "transaction_type_from_code",
    "issuer_gics_sector": "ISSUER_GICS_SECTOR",
    "gics_sector": "ISSUER_GICS_SECTOR",
    "symbol_norm": "symbol_norm",
    "est_acquire_value": "EST_ACQUIRE_VALUE",
    "est_dispose_value": "EST_DISPOSE_VALUE",
}

# Narrow projection for /transactions (avoids SELECT * over the mart).
_TRANSACTION_SELECT_SQL = """
f.ACCESSION_NUMBER,
f.filing_date_key,
f.period_report_date_key,
f.FILING_DATE,
f.PERIOD_OF_REPORT,
f.TRANS_DATE,
f.DATE_OF_ORIGINAL_SUBMISSION,
f.NO_SECURITIES_OWNED,
f.ISSUERCIK,
f.ISSUERNAME,
f.ISSUERTRADINGSYMBOL,
f.non_deriv_transaction_count,
f.total_transaction_count,
f.non_deriv_shares_acquired,
f.non_deriv_shares_disposed,
f.total_shares_acquired,
f.total_shares_disposed,
f.non_deriv_value_acquired,
f.non_deriv_value_disposed,
f.total_non_deriv_value,
f.total_non_deriv_shares_owned,
f.est_acquire_value AS EST_ACQUIRE_VALUE,
f.est_dispose_value AS EST_DISPOSE_VALUE,
f.reporting_owner_count,
f.reporting_owner_names,
f.reporting_owner_role_types,
f.reporting_owner_titles,
f.transaction_type_from_code,
f.symbol_norm,
f.issuer_gics_sector
"""


def _json_safe_cell(v: Any) -> Any:
    """BigQuery/pandas may yield NaN/Inf; JSON cannot represent those."""
    if v is None:
        return None
    if isinstance(v, (str, bool)):
        return v
    try:
        if pd.isna(v):
            return None
    except TypeError:
        pass
    if hasattr(v, "isoformat") and not isinstance(v, (str, bytes)):
        try:
            return v.isoformat()
        except Exception:
            pass
    if isinstance(v, (np.integer, int)) and not isinstance(v, bool):
        return int(v)
    try:
        xf = float(v)
    except (TypeError, ValueError):
        return v
    if math.isnan(xf) or math.isinf(xf):
        return None
    return xf


def _normalize_transaction_rows(records: List[dict]) -> List[dict]:
    if not records:
        return records
    out = []
    for row in records:
        fixed = {}
        for k, v in row.items():
            if k is None:
                continue
            lk = str(k).lower() if isinstance(k, str) else k
            name = _TRANSACTION_COL_CANONICAL.get(lk, k)
            fixed[name] = _json_safe_cell(v)
        out.append(fixed)
    return out


def _default_date_range() -> tuple[str, str]:
    """Default range: 6 months ending at latest transaction date in mart (cached)."""
    cache_key = "max_trans_date_v1"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(
        f"""
        SELECT
          COALESCE(
            (SELECT MAX(TRANS_DATE) FROM {MART()} WHERE TRANS_DATE IS NOT NULL),
            (SELECT MAX(FILING_DATE) FROM {MART()} WHERE FILING_DATE IS NOT NULL)
          ) AS max_date
        """
    )
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


def _safe_ticker(t: str) -> Optional[str]:
    t = (t or "").strip().upper()
    if not t or not re.match(r"^[A-Z0-9.\-]{1,20}$", t):
        return None
    return t


def _pd_float(v, default: float = 0.0) -> float:
    """Avoid `v or 0` on pandas/NA scalars (ambiguous truth value)."""
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _pd_int(v, default: int = 0) -> int:
    try:
        if v is None or pd.isna(v):
            return default
        return int(v)
    except (TypeError, ValueError):
        return default


def _safe_week_start_date(s: Optional[str]) -> Optional[str]:
    """YYYY-MM-DD for SQL DATE(...); cluster week_start from API."""
    if not s:
        return None
    head = str(s).strip()[:10]
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", head):
        return None
    return head


def _safe_search_fragment(q: Optional[str]) -> Optional[str]:
    """Single-quoted SQL literal body for LIKE (no % wildcards in user text)."""
    if not q or not (t := q.strip()):
        return None
    t = t[:120].replace("%", "").replace("_", "")
    if ";" in t or "--" in t:
        return None
    if not re.match(r"^[\w\s\-.,&'’]+$", t, flags=re.UNICODE):
        return None
    return t.replace("'", "''")


def _token_looks_like_ticker(part: str) -> bool:
    p = (part or "").strip()
    if not p or re.search(r"\s", p):
        return False
    return _safe_ticker(p) is not None


def _parse_mart_search_tokens(
    ticker: Optional[str], search: Optional[str],
) -> tuple[list[str], list[str]]:
    """Split `search` on comma/semicolon/newline; classify tokens as symbols vs free-text LIKE."""
    tickers: list[str] = []
    texts: list[str] = []
    if ticker:
        st = _safe_ticker(ticker)
        if st:
            tickers.append(st)
    if search:
        for part in re.split(r"[,;\n]+", search.strip()):
            p = part.strip()
            if not p:
                continue
            if _token_looks_like_ticker(p):
                st = _safe_ticker(p)
                if st:
                    tickers.append(st)
            else:
                lit = _safe_search_fragment(p)
                if lit:
                    texts.append(lit)
    seen: set[str] = set()
    uniq_t: list[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            uniq_t.append(t)
    return uniq_t, texts


def _mart_symbol_search_predicate_sql(
    ticker: Optional[str], search: Optional[str],
) -> tuple[str, str]:
    """
    Returns (cache_key_fragment, SQL boolean for mart alias `f`).
    Rows match if ANY ticker matches IN list OR ANY text token matches LIKE triple.
    """
    tiks, texts = _parse_mart_search_tokens(ticker, search)
    parts: list[str] = []
    ck: list[str] = []
    if tiks:
        ck.append("sym:" + ",".join(tiks))
        inn = ",".join(f"'{t}'" for t in tiks)
        parts.append(f"f.symbol_norm IN ({inn})")
    for lit in texts:
        ck.append(f"like:{lit[:48]}")
        parts.append(
            f"""(IFNULL(CAST(f.symbol_norm AS STRING), '') LIKE UPPER('%' || '{lit}' || '%')
            OR UPPER(CAST(f.ISSUERNAME AS STRING)) LIKE UPPER('%' || '{lit}' || '%')
            OR UPPER(CAST(IFNULL(f.reporting_owner_names, '') AS STRING)) LIKE UPPER('%' || '{lit}' || '%'))"""
        )
    if not parts:
        return ("", "TRUE")
    return ("::".join(ck), "(" + " OR ".join(parts) + ")")


# ─────────────────────────────────────────────
# /summary  (cached; uses shares×price when fact dollar fields are empty)
# ─────────────────────────────────────────────
@router.get("/summary")
async def get_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    start_date, end_date = _resolve_dates(start_date, end_date)
    cache_key = f"summary_txn_v2:{start_date}:{end_date}"
    hit = get_summary_cache(cache_key)
    if hit is not None:
        return hit

    query = f"""
        WITH enriched AS (
            SELECT
                f.ISSUERTRADINGSYMBOL,
                COALESCE(
                    NULLIF(f.est_acquire_value, 0),
                    IFNULL(f.non_deriv_value_acquired, 0),
                    0
                ) AS purchase_usd,
                COALESCE(
                    NULLIF(f.est_dispose_value, 0),
                    IFNULL(f.non_deriv_value_disposed, 0),
                    0
                ) AS sale_usd
            FROM {MART()} AS f
            WHERE f.TRANS_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        )
        SELECT
            SUM(purchase_usd) AS purchase_value,
            COUNT(DISTINCT CASE WHEN purchase_usd > 0 THEN ISSUERTRADINGSYMBOL END) AS purchase_company_count,
            SUM(sale_usd) AS sales_value,
            COUNT(DISTINCT CASE WHEN sale_usd > 0 THEN ISSUERTRADINGSYMBOL END) AS sales_company_count
        FROM enriched
    """
    df = query_bigquery(query)
    if df.empty:
        payload = {
            "purchase_value_m": 0.0,
            "purchase_count": 0,
            "sales_value_m": 0.0,
            "sales_count": 0,
            "start_date": start_date,
            "end_date": end_date,
        }
        set_summary_cache(cache_key, payload)
        return payload

    row = df.iloc[0]
    payload = {
        "purchase_value_m": round(_pd_float(row["purchase_value"]) / 1_000_000, 2),
        "purchase_count": _pd_int(row["purchase_company_count"]),
        "sales_value_m": round(_pd_float(row["sales_value"]) / 1_000_000, 2),
        "sales_count": _pd_int(row["sales_company_count"]),
        "start_date": start_date,
        "end_date": end_date,
    }
    set_summary_cache(cache_key, payload)
    return payload


# ─────────────────────────────────────────────
# /top-transactions  (cached)
# ─────────────────────────────────────────────
@router.get("/top-transactions")
async def get_top_transactions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 3,
):
    start_date, end_date = _resolve_dates(start_date, end_date)
    cache_key = f"toptx_txn_v2:{start_date}:{end_date}:{limit}"
    hit = get_summary_cache(cache_key)
    if hit is not None:
        return hit

    date_filter = f"f.TRANS_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
    combined_query = f"""
        WITH base AS (
            SELECT
                f.ISSUERTRADINGSYMBOL AS ticker,
                f.ISSUERNAME AS company,
                COALESCE(
                    NULLIF(f.est_acquire_value, 0),
                    IFNULL(f.non_deriv_value_acquired, 0),
                    0
                ) AS buy_val,
                COALESCE(
                    NULLIF(f.est_dispose_value, 0),
                    IFNULL(f.non_deriv_value_disposed, 0),
                    0
                ) AS sell_val
            FROM {MART()} AS f
            WHERE {date_filter}
        ),
        top_buys AS (
            SELECT ticker, company, SUM(buy_val) AS value, 'buy' AS side
            FROM base
            WHERE buy_val > 0
            GROUP BY ticker, company
            ORDER BY value DESC
            LIMIT {limit}
        ),
        top_sells AS (
            SELECT ticker, company, SUM(sell_val) AS value, 'sell' AS side
            FROM base
            WHERE sell_val > 0
            GROUP BY ticker, company
            ORDER BY value DESC
            LIMIT {limit}
        )
        SELECT * FROM top_buys
        UNION ALL
        SELECT * FROM top_sells
    """
    df = query_bigquery(combined_query)

    def fmt(rows):
        return [
            {
                "ticker": r["ticker"] if pd.notna(r["ticker"]) else "",
                "company": r["company"] if pd.notna(r["company"]) else "",
                "value_m": round(_pd_float(r["value"]) / 1_000_000, 2),
            }
            for _, r in rows.iterrows()
        ]

    buys_df = df[df["side"] == "buy"] if not df.empty else pd.DataFrame()
    sells_df = df[df["side"] == "sell"] if not df.empty else pd.DataFrame()

    payload = {
        "top_buys": fmt(buys_df),
        "top_sells": fmt(sells_df),
        "start_date": start_date,
        "end_date": end_date,
    }
    set_summary_cache(cache_key, payload)
    return payload


# ─────────────────────────────────────────────
# /transactions  (cached)
# ─────────────────────────────────────────────
def fetch_transactions_payload(
    ticker: Optional[str] = None,
    search: Optional[str] = None,
    min_value: float = 0.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> dict:
    """
    Build transactions JSON (used by /transactions and startup warm).
    Expects mart `sp500_insider_transactions` as a partitioned table with symbol_norm + issuer_gics_sector
    (see dbt model). No runtime join to sp500_companies.
    """
    start_date, end_date = _resolve_dates(start_date, end_date)
    ck_frag, pred_sql = _mart_symbol_search_predicate_sql(
        _safe_ticker(ticker) if ticker else None,
        search,
    )
    cache_key = f"tx_v10:{start_date}:{end_date}:{ck_frag}:{min_value}:{page}:{page_size}"
    hit = get_transactions_cache(cache_key)
    if hit is not None:
        return hit

    where_clauses = [
        f"f.TRANS_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')",
        "f.transaction_type_from_code IS NOT NULL "
        "AND TRIM(CAST(f.transaction_type_from_code AS STRING)) != ''",
    ]
    if min_value > 0:
        where_clauses.append(f"f.total_non_deriv_value >= {min_value}")
    if pred_sql != "TRUE":
        where_clauses.append(pred_sql)

    where_str = " AND ".join(where_clauses)
    offset = (page - 1) * page_size

    filtered_cte = f"""
        WITH filtered AS (
            SELECT
{_TRANSACTION_SELECT_SQL}
            FROM {MART()} AS f
            WHERE {where_str}
        )
    """
    count_query = f"""
        SELECT COUNT(*) AS _c
        FROM {MART()} AS f
        WHERE {where_str}
    """

    def _count_rows() -> int:
        cnt_df = query_bigquery(count_query)
        if cnt_df.empty:
            return 0
        return int(pd.to_numeric(cnt_df.iloc[0, 0], errors="coerce") or 0)

    if pred_sql != "TRUE":
        fetch_limit = page_size + 1
        page_query = f"""
            {filtered_cte}
            SELECT * FROM filtered
            ORDER BY TRANS_DATE ASC NULLS LAST, FILING_DATE ASC
            LIMIT {fetch_limit} OFFSET {offset}
        """
        df = query_bigquery(page_query)
        has_more = len(df) > page_size
        if has_more:
            df = df.iloc[:page_size]
        total_count = -1
    else:
        page_query = f"""
            {filtered_cte}
            SELECT * FROM filtered
            ORDER BY TRANS_DATE ASC NULLS LAST, FILING_DATE ASC
            LIMIT {page_size} OFFSET {offset}
        """
        df = query_bigquery(page_query)
        total_count = _count_rows()
        has_more = len(df) == page_size and (offset + page_size) < total_count

    raw_rows = df.to_dict(orient="records")
    payload = {
        "data": _normalize_transaction_rows(raw_rows),
        "total": total_count,
        "page": page,
        "page_size": page_size,
        "has_more": has_more,
    }
    set_transactions_cache(cache_key, payload)
    return payload


def warm_default_transactions_cache() -> None:
    """First page, default date window, no ticker/search — primes common /transactions cache."""
    fetch_transactions_payload(page=1, page_size=50)


@router.get("/transactions")
async def get_transactions(
    ticker: Optional[str] = None,
    search: Optional[str] = None,
    min_value: float = 0.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
):
    return fetch_transactions_payload(
        ticker=ticker,
        search=search,
        min_value=min_value,
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size,
    )


def _fetch_cluster_breakdown_payload(
    side: Literal["buy", "sell"],
    ticker: str,
    week_start: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> dict:
    start_date, end_date = _resolve_dates(start_date, end_date)
    safe_sym = _safe_ticker(ticker)
    ws = _safe_week_start_date(week_start)
    if not safe_sym or not ws:
        return {"data": [], "side": side, "ticker": ticker, "week_start": week_start}

    if side == "buy":
        side_pred = (
            "COALESCE(NULLIF(f.est_acquire_value, 0), IFNULL(f.non_deriv_value_acquired, 0), 0) > 0"
        )
        value_expr = "COALESCE(NULLIF(f.est_acquire_value, 0), IFNULL(f.non_deriv_value_acquired, 0), 0)"
    else:
        side_pred = (
            "COALESCE(NULLIF(f.est_dispose_value, 0), IFNULL(f.non_deriv_value_disposed, 0), 0) > 0"
        )
        value_expr = "COALESCE(NULLIF(f.est_dispose_value, 0), IFNULL(f.non_deriv_value_disposed, 0), 0)"

    ro = stg_sec_reportingowner()
    # One pass over the mart for accession + amount + date; avoids a second mart join.
    query = f"""
        WITH filing_slice AS (
            SELECT
                f.ACCESSION_NUMBER,
                f.TRANS_DATE AS trans_date,
                {value_expr} AS amount_usd
            FROM {MART()} AS f
            WHERE f.symbol_norm = '{safe_sym}'
              AND DATE_TRUNC(f.TRANS_DATE, WEEK(MONDAY)) = DATE('{ws}')
              AND f.TRANS_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              AND {side_pred}
        )
        SELECT
            TRIM(ro.RPTOWNERNAME) AS insider_name,
            TRIM(COALESCE(
                NULLIF(ro.RPTOWNER_TITLE, ''),
                CASE
                    WHEN LOWER(ro.RPTOWNER_RELATIONSHIP) LIKE '%director%'
                        AND LOWER(ro.RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Director & Officer'
                    WHEN LOWER(ro.RPTOWNER_RELATIONSHIP) LIKE '%director%' THEN 'Director'
                    WHEN LOWER(ro.RPTOWNER_RELATIONSHIP) LIKE '%officer%' THEN 'Officer'
                    WHEN LOWER(ro.RPTOWNER_RELATIONSHIP) LIKE '%10%%' THEN '10% Owner'
                    ELSE 'Other'
                END
            )) AS role,
            fs.trans_date AS trans_date,
            fs.amount_usd AS amount_usd
        FROM filing_slice AS fs
        INNER JOIN {ro} AS ro ON ro.ACCESSION_NUMBER = fs.ACCESSION_NUMBER
        ORDER BY fs.trans_date ASC NULLS LAST, insider_name ASC
    """
    cache_key = f"cluster_breakdown_v2:{side}:{safe_sym}:{ws}:{start_date}:{end_date}"
    hit = get_cluster_breakdown_cache(cache_key)
    if hit is not None:
        return hit

    df = query_bigquery(query)
    out = []
    if not df.empty:
        for _, r in df.iterrows():
            td = r["trans_date"]
            out.append(
                {
                    "insider_name": (r["insider_name"] if pd.notna(r["insider_name"]) else "") or "",
                    "role": (r["role"] if pd.notna(r["role"]) else "") or "",
                    "trans_date": td.isoformat() if hasattr(td, "isoformat") and pd.notna(td) else str(td),
                    "amount_usd": _pd_float(r["amount_usd"]),
                }
            )
    payload = {
        "data": out,
        "side": side,
        "ticker": safe_sym,
        "week_start": ws,
        "start_date": start_date,
        "end_date": end_date,
    }
    set_cluster_breakdown_cache(cache_key, payload)
    return payload


@router.get("/clusters/breakdown")
async def get_cluster_breakdown(
    side: Literal["buy", "sell"],
    ticker: str,
    week_start: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    return _fetch_cluster_breakdown_payload(side, ticker, week_start, start_date, end_date)


# ─────────────────────────────────────────────
# /clusters  (cached; weekly buckets, buy vs sell)
# ─────────────────────────────────────────────
@router.get("/clusters")
async def get_clusters(
    side: Literal["buy", "sell"] = "buy",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_filings: int = Query(2, ge=2, le=20),
    limit: int = Query(50, ge=1, le=200),
    ticker: Optional[str] = None,
    search: Optional[str] = None,
):
    start_date, end_date = _resolve_dates(start_date, end_date)
    ck_frag, ticker_search_sql = _mart_symbol_search_predicate_sql(
        _safe_ticker(ticker) if ticker else None,
        search,
    )
    cache_key = (
        f"clusters_txn_v7:{side}:{start_date}:{end_date}:{min_filings}:{limit}:{ck_frag}"
    )
    hit = get_clusters_cache(cache_key)
    if hit is not None:
        return hit

    if side == "buy":
        side_pred = (
            "COALESCE(NULLIF(f.est_acquire_value, 0), IFNULL(f.non_deriv_value_acquired, 0), 0) > 0"
        )
        value_expr = "COALESCE(NULLIF(f.est_acquire_value, 0), IFNULL(f.non_deriv_value_acquired, 0), 0)"
        shares_expr = "IFNULL(f.non_deriv_shares_acquired, 0)"
    else:
        side_pred = (
            "COALESCE(NULLIF(f.est_dispose_value, 0), IFNULL(f.non_deriv_value_disposed, 0), 0) > 0"
        )
        value_expr = "COALESCE(NULLIF(f.est_dispose_value, 0), IFNULL(f.non_deriv_value_disposed, 0), 0)"
        shares_expr = "IFNULL(f.non_deriv_shares_disposed, 0)"

    ro_tbl = stg_sec_reportingowner()
    query = f"""
        WITH filings AS (
            SELECT
                f.ACCESSION_NUMBER,
                f.ISSUERTRADINGSYMBOL AS ticker,
                f.ISSUERNAME AS company,
                f.TRANS_DATE AS txn_date,
                f.FILING_DATE AS filing_date,
                f.reporting_owner_titles,
                f.reporting_owner_role_types,
                {value_expr} AS cluster_line_value,
                {shares_expr} AS line_shares
            FROM {MART()} AS f
            WHERE f.TRANS_DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              AND {side_pred}
              AND ({ticker_search_sql})
        ),
        filing_week_keys AS (
            SELECT DISTINCT
                ACCESSION_NUMBER,
                ticker,
                DATE_TRUNC(txn_date, WEEK(MONDAY)) AS week_start
            FROM filings
        ),
        cik_counts AS (
            SELECT
                fw.ticker,
                fw.week_start,
                COUNT(DISTINCT ro.RPTOWNERCIK) AS distinct_insider_ciks
            FROM filing_week_keys AS fw
            INNER JOIN {ro_tbl} AS ro ON ro.ACCESSION_NUMBER = fw.ACCESSION_NUMBER
            WHERE ro.RPTOWNERCIK IS NOT NULL
              AND TRIM(CAST(ro.RPTOWNERCIK AS STRING)) != ''
            GROUP BY fw.ticker, fw.week_start
        ),
        agg AS (
            SELECT
                ticker,
                ANY_VALUE(company) AS company,
                DATE_TRUNC(txn_date, WEEK(MONDAY)) AS week_start,
                COUNT(DISTINCT ACCESSION_NUMBER) AS filing_count,
                MIN(txn_date) AS first_trans,
                MAX(txn_date) AS last_trans,
                MAX(filing_date) AS last_filing_date,
                SUM(cluster_line_value) AS cluster_value,
                SUM(line_shares) AS cluster_shares,
                STRING_AGG(DISTINCT NULLIF(reporting_owner_role_types, ''), ', ') AS roles,
                STRING_AGG(DISTINCT NULLIF(reporting_owner_titles, ''), ' | ') AS titles
            FROM filings
            GROUP BY ticker, week_start
            HAVING COUNT(DISTINCT ACCESSION_NUMBER) >= {min_filings}
        )
        SELECT
            a.ticker,
            a.company,
            a.week_start,
            a.filing_count,
            a.first_trans,
            a.last_trans,
            a.last_filing_date,
            a.cluster_value,
            a.cluster_shares,
            a.roles,
            a.titles,
            cc.distinct_insider_ciks AS insider_count
        FROM agg AS a
        INNER JOIN cik_counts AS cc
            ON a.ticker = cc.ticker AND a.week_start = cc.week_start
            AND cc.distinct_insider_ciks >= 2
        ORDER BY cluster_value DESC
        LIMIT {limit}
    """
    df = query_bigquery(query)
    rows = []
    if not df.empty:
        for _, r in df.iterrows():
            ws = r["week_start"]
            ft = r["first_trans"]
            lt = r["last_trans"]
            lfd = r["last_filing_date"]
            cv = _pd_float(r["cluster_value"])
            sh = _pd_float(r["cluster_shares"])
            ipp = (cv / sh) if sh > 1e-9 else None
            if ipp is not None and (not math.isfinite(ipp) or ipp <= 0):
                ipp = None
            rows.append(
                {
                    "ticker": r["ticker"] if pd.notna(r["ticker"]) else "",
                    "company": (r["company"] if pd.notna(r["company"]) else "") or "",
                    "filing_count": _pd_int(r["filing_count"]),
                    "insider_count": _pd_int(r["insider_count"]),
                    "week_start": ws.isoformat() if hasattr(ws, "isoformat") and pd.notna(ws) else str(ws),
                    "first_trans": ft.isoformat() if hasattr(ft, "isoformat") and pd.notna(ft) else str(ft),
                    "last_trans": lt.isoformat() if hasattr(lt, "isoformat") and pd.notna(lt) else str(lt),
                    "last_filing_date": lfd.isoformat()
                    if hasattr(lfd, "isoformat") and pd.notna(lfd)
                    else "",
                    "cluster_value": cv,
                    "cluster_shares": sh,
                    "implied_price_per_share": ipp,
                    "roles": (r["roles"] if pd.notna(r["roles"]) else "") or "",
                    "titles": (r["titles"] if pd.notna(r["titles"]) else "") or "",
                }
            )

    payload = {"data": rows, "side": side, "start_date": start_date, "end_date": end_date}
    set_clusters_cache(cache_key, payload)
    return payload


# ─────────────────────────────────────────────
# /tickers  (cached; tickers present in insider mart)
# ─────────────────────────────────────────────
@router.get("/tickers")
async def get_tickers():
    cache_key = "tickers_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(
        f"""
        SELECT DISTINCT symbol_norm AS ticker
        FROM {MART()}
        WHERE symbol_norm IS NOT NULL AND TRIM(CAST(symbol_norm AS STRING)) != ''
        ORDER BY ticker
        """
    )
    tickers = df["ticker"].dropna().astype(str).tolist()
    set_cached_item(cache_key, tickers)
    return tickers


# ─────────────────────────────────────────────
# /sp500-companies  (same payload as search-directory/stocks; includes last close)
# ─────────────────────────────────────────────
@router.get("/sp500-companies")
async def get_sp500_companies():
    """S&P 500 constituents plus latest `close` from `sp500_stock_daily` (per symbol, max date)."""
    return build_search_directory_stocks()


def _query_sp500_companies_with_last_close() -> pd.DataFrame:
    sp = fqtn("sp500_companies")
    sd = sp500_stock_daily()
    return query_bigquery(
        f"""
        WITH sp AS (
            SELECT DISTINCT
                UPPER(TRIM(CAST(symbol AS STRING))) AS ticker,
                TRIM(CAST(security AS STRING)) AS company,
                TRIM(CAST(gics_sector AS STRING)) AS sector
            FROM {sp}
            WHERE symbol IS NOT NULL AND TRIM(CAST(symbol AS STRING)) != ''
        ),
        ranked AS (
            SELECT
                UPPER(TRIM(CAST(symbol AS STRING))) AS ticker,
                CAST(`date` AS DATE) AS px_date,
                CAST(close AS FLOAT64) AS last_close,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(CAST(symbol AS STRING)))
                    ORDER BY CAST(`date` AS DATE) DESC
                ) AS rn
            FROM {sd}
            WHERE symbol IS NOT NULL AND close IS NOT NULL
        ),
        lx AS (
            SELECT ticker, last_close, px_date AS last_close_date
            FROM ranked
            WHERE rn = 1
        )
        SELECT
            sp.ticker,
            sp.company,
            sp.sector,
            lx.last_close,
            lx.last_close_date
        FROM sp
        LEFT JOIN lx ON sp.ticker = lx.ticker
        ORDER BY sp.ticker
        """
    )


# ─────────────────────────────────────────────
# /search-directory/*  (split payloads — single 40MB+ JSON breaks browsers & DevTools)
# ─────────────────────────────────────────────
SEARCH_DIRECTORY_STOCKS_KEY = "search_directory_stocks_v2"
SEARCH_DIRECTORY_INSIDERS_KEY = "search_directory_insiders_v2"
# Full distinct insider count is huge; cap keeps JSON parse + sessionStorage viable.
INSIDER_DIRECTORY_ROW_CAP = 18_000


def _records_json_safe(df: pd.DataFrame) -> List[dict]:
    """Round-trip via JSON so FastAPI emits plain lists/dicts (no numpy scalars / NaN edge cases)."""
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))


def build_search_directory_stocks() -> List[dict]:
    """S&P 500 rows for combobox + latest daily close (sp500_stock_daily); cached separately."""
    cached = get_cached_item(SEARCH_DIRECTORY_STOCKS_KEY)
    if isinstance(cached, list) and len(cached) > 0:
        return cached

    stocks_df = _query_sp500_companies_with_last_close()
    rows = _records_json_safe(stocks_df)
    if rows:
        set_cached_item(SEARCH_DIRECTORY_STOCKS_KEY, rows)
    return rows


def build_search_directory_insiders() -> List[dict]:
    """Reporting owners for combobox (capped); cached separately."""
    cached = get_cached_item(SEARCH_DIRECTORY_INSIDERS_KEY)
    if isinstance(cached, list) and len(cached) > 0:
        return cached

    dim = fqtn("dim_reporting_owner")
    # One row per SEC reporting-owner CIK. DISTINCT across (cik, name, role, title) repeated the
    # same person for every role/title variant in staging.
    insiders_df = query_bigquery(
        f"""
        SELECT
            CAST(reporting_owner_cik AS STRING) AS cik,
            MIN(TRIM(CAST(reporting_owner_name AS STRING))) AS name,
            ANY_VALUE(role_type) AS role_type,
            ANY_VALUE(TRIM(CAST(RPTOWNER_TITLE AS STRING))) AS title
        FROM {dim}
        WHERE reporting_owner_name IS NOT NULL
          AND TRIM(CAST(reporting_owner_name AS STRING)) != ''
          AND reporting_owner_cik IS NOT NULL
          AND TRIM(CAST(reporting_owner_cik AS STRING)) != ''
        GROUP BY reporting_owner_cik
        ORDER BY MIN(UPPER(TRIM(CAST(reporting_owner_name AS STRING))))
        LIMIT {INSIDER_DIRECTORY_ROW_CAP}
        """
    )
    rows = _records_json_safe(insiders_df)
    if rows:
        set_cached_item(SEARCH_DIRECTORY_INSIDERS_KEY, rows)
    return rows


@router.get("/search-directory/stocks")
async def get_search_directory_stocks():
    """S&P 500 constituents for client-side ticker search (~500 rows)."""
    return {"stocks": build_search_directory_stocks()}


@router.get("/search-directory/insiders")
async def get_search_directory_insiders():
    """Reporting owners for client-side name search (capped for payload size)."""
    rows = build_search_directory_insiders()
    return {
        "insiders": rows,
        "insiders_capped": True,
        "insiders_limit": INSIDER_DIRECTORY_ROW_CAP,
    }


@router.get("/search-directory")
async def get_search_directory_legacy():
    """
    Deprecated combined payload (multi‑MB). Use /search-directory/stocks and /search-directory/insiders.
    Returns a tiny object so old clients fail fast instead of freezing the browser.
    """
    return {
        "error": "use_split_endpoints",
        "stocks_path": "/api/search-directory/stocks",
        "insiders_path": "/api/search-directory/insiders",
    }


# ─────────────────────────────────────────────
# /owners  (cached)
# ─────────────────────────────────────────────
@router.get("/owners")
async def get_owners():
    cache_key = "owners_list"
    cached = get_cached_item(cache_key)
    if cached:
        return cached

    df = query_bigquery(
        f"SELECT DISTINCT reporting_owner_name FROM {fqtn('dim_reporting_owner')} ORDER BY reporting_owner_name"
    )
    owners = df["reporting_owner_name"].tolist()
    set_cached_item(cache_key, owners)
    return owners
