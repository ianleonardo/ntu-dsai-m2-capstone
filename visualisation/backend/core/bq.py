"""BigQuery fully-qualified table names."""

from .config import settings


def fqtn(table: str) -> str:
    return f"`{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table}`"


def sp500_mart() -> str:
    return fqtn("sp500_insider_transactions")


def sp500_stock_daily() -> str:
    """Daily OHLCV loaded by Meltano (stream SP500_STOCK_DAILY → typically this table name)."""
    return fqtn("sp500_stock_daily")


def stg_sec_reportingowner() -> str:
    """dbt `stg_sec_reportingowner` — required for /clusters/breakdown owner-level rows."""
    return fqtn("stg_sec_reportingowner")
