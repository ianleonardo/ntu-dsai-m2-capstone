"""BigQuery fully-qualified table names."""

from .config import settings


def fqtn(table: str) -> str:
    return f"`{settings.GOOGLE_PROJECT_ID}.{settings.BIGQUERY_DATASET}.{table}`"


def sp500_mart() -> str:
    return fqtn("sp500_insider_transactions")


def sp500_stock_daily() -> str:
    """Daily OHLCV loaded by Meltano (stream SP500_STOCK_DAILY → typically this table name)."""
    return fqtn("sp500_stock_daily")


def dim_sec_reporting_owner() -> str:
    """dbt `dim_sec_reporting_owner` — owner-level rows for /clusters/breakdown (RPTOWNER* columns)."""
    return fqtn("dim_sec_reporting_owner")

def dim_sp500_reporting_owner() -> str:
    """dbt `dim_sp500_reporting_owner` — S&P 500 restricted reporting owners."""
    return fqtn("dim_sp500_reporting_owner")
