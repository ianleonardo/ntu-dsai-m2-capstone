import asyncio
from core.bigquery import query_bigquery
from core.bq import sp500_stock_daily

def test():
    df = query_bigquery(f"SELECT * FROM {sp500_stock_daily()} LIMIT 1")
    print(df.columns.tolist())

if __name__ == "__main__":
    test()
