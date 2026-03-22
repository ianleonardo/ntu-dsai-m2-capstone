from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="../../.env", 
        extra="ignore"
    )
    
    GOOGLE_PROJECT_ID: str = os.getenv("GOOGLE_PROJECT_ID", "ntu-dsai-488112")
    BIGQUERY_DATASET: str = os.getenv("BIGQUERY_DATASET", "insider_transactions")
    CACHE_TTL_SECONDS: int = 3600  # 1 hour (tickers, owners, sp500 list)
    CACHE_SUMMARY_TTL_SECONDS: int = 120
    CACHE_TRANSACTIONS_TTL_SECONDS: int = 300
    CACHE_CLUSTERS_TTL_SECONDS: int = 180
    # Breakdown is per-cluster; longer TTL makes repeat expands feel instant.
    CACHE_CLUSTER_BREAKDOWN_TTL_SECONDS: int = 600

settings = Settings()
