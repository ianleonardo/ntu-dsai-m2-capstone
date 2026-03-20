"""
Dagster assets for SP500 daily stock pipeline:
  yfinance -> staging JSONL -> Meltano tap-jsonl -> BigQuery
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset


# Resolve project root (repo root) and add scripts to the Python path
# File path: .../dataprocessing/dagster_orchestration/assets/sp500_stock_daily_integration.py
# repo root is 3 levels up from this file (parents[3])
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "scripts"))


SP500_STAGING_DIR = project_root / "dataprocessing" / "meltano_ingestion" / "staging"
SP500_OUT_JSONL = SP500_STAGING_DIR / "sp500_stock_daily.jsonl"


class Sp500StockDailyFetchConfig(Config):
    start: str  # YYYY-MM-DD
    end: str  # YYYY-MM-DD
    chunk_size: int = 50


class Sp500StockDailyLoadConfig(Config):
    start: str  # YYYY-MM-DD (for metadata only)
    end: str  # YYYY-MM-DD (for metadata only)


class Sp500StockDailySummaryConfig(Config):
    start: str
    end: str


@asset(
    key="sp500_stock_daily_staging_data",
    description="Fetch SP500 daily OHLCV via yfinance and write staging JSONL for Meltano.",
    metadata={"tool": "yfinance", "stage": "staging"},
)
def sp500_stock_daily_staging_data(
    context: AssetExecutionContext,
    config: Sp500StockDailyFetchConfig,
) -> MaterializeResult:
    """
    Runs the fetch script which writes `staging/sp500_stock_daily.jsonl`.
    """
    SP500_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    context.log.info(f"Fetching SP500 daily stocks: {config.start} -> {config.end}")

    fetch_script = project_root / "scripts" / "fetch_sp500_stock_daily_yfinance_to_jsonl.py"
    if not fetch_script.exists():
        raise FileNotFoundError(f"Missing fetch script: {fetch_script}")

    cmd = [
        sys.executable,
        str(fetch_script),
        "--start",
        config.start,
        "--end",
        config.end,
        "--chunk-size",
        str(config.chunk_size),
        "--output-jsonl",
        str(SP500_OUT_JSONL),
        "--staging-dir",
        str(SP500_STAGING_DIR),
    ]

    # We use subprocess so Dagster works even when fetch script imports its own deps.
    proc = subprocess.run(cmd, capture_output=True, text=True)
    context.log.info(proc.stdout)
    if proc.returncode != 0:
        context.log.error(proc.stderr)
        raise RuntimeError(f"SP500 fetch failed with exit code {proc.returncode}")

    # Extract row count from fetch output (best-effort).
    m = re.search(r"Wrote\s+(\d+)\s+JSONL rows", proc.stdout)
    rows = int(m.group(1)) if m else None

    return MaterializeResult(
        metadata={
            "start": config.start,
            "end": config.end,
            "staging_jsonl": str(SP500_OUT_JSONL),
            "rows_written": rows if rows is not None else "Unknown",
            "staging_status": "success",
        }
    )


@asset(
    key="bigquery_sp500_stock_daily_data",
    description="Load SP500 daily stock JSONL into BigQuery via Meltano tap-jsonl.",
    metadata={
        "tool": "Meltano",
        "target": "BigQuery",
        "dataset": "insider_transactions",
    },
    deps=["sp500_stock_daily_staging_data"],
)
def bigquery_sp500_stock_daily_data(
    context: AssetExecutionContext,
    config: Sp500StockDailyLoadConfig,
) -> MaterializeResult:
    """
    Runs Meltano job:
      meltano run tap-jsonl-sp500-stock-daily target-bigquery
    """
    meltano_dir = project_root / "dataprocessing" / "meltano_ingestion"

    env = os.environ.copy()
    # Wrapper scripts typically load these from .env; we let users manage env/ADC.
    if "GOOGLE_CLOUD_PROJECT" not in env:
        env["GOOGLE_CLOUD_PROJECT"] = env.get("GOOGLE_PROJECT_ID", "")

    try:
        context.log.info("Installing Meltano plugins (best-effort).")
        subprocess.run(
            ["meltano", "install"],
            cwd=str(meltano_dir),
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        context.log.info("Running Meltano load job: tap-jsonl-sp500-stock-daily -> target-bigquery")
        run_cmd = [
            "meltano",
            "run",
            "tap-jsonl-sp500-stock-daily",
            "target-bigquery",
        ]
        run_proc = subprocess.run(
            run_cmd,
            cwd=str(meltano_dir),
            capture_output=True,
            text=True,
            env=env,
        )
        context.log.info(run_proc.stdout)
        if run_proc.returncode != 0:
            context.log.error(run_proc.stderr)
            raise RuntimeError(f"Meltano run failed: {run_proc.stderr}")

        # Best-effort parse.
        numbers = re.findall(r"\b(\d+)\b", run_proc.stdout)
        rows_loaded = int(numbers[0]) if numbers else "Unknown"

        return MaterializeResult(
            metadata={
                "start": config.start,
                "end": config.end,
                "meltano_job": "tap-jsonl-sp500-stock-daily target-bigquery",
                "bigquery_project": "ntu-dsai-488112",
                "bigquery_dataset": "insider_transactions",
                "rows_loaded": rows_loaded,
                "load_status": "success",
                "meltano_output_tail": MetadataValue.md(f"```\n{run_proc.stdout[-2000:]}\n```"),
            }
        )
    except Exception as e:
        raise RuntimeError(f"Failed to load SP500 daily stocks to BigQuery: {e}")


@asset(
    key="sp500_stock_daily_pipeline_summary",
    description="Summary of the complete SP500 daily stock pipeline execution.",
    metadata={
        "pipeline": "SP500 Daily Stock Pipeline",
        "scope": "Fetch -> Meltano -> BigQuery",
    },
    deps=["bigquery_sp500_stock_daily_data"],
)
def sp500_stock_daily_pipeline_summary(
    context: AssetExecutionContext,
    config: Sp500StockDailySummaryConfig,
) -> MaterializeResult:
    summary_md = f"""
**Pipeline:** SP500 Daily Stock Pipeline

**Start date:** {config.start}
**End date:** {config.end}

**Destination:** `ntu-dsai-488112.insider_transactions.SP500_STOCK_DAILY`
"""
    context.log.info("Generating SP500 pipeline summary.")
    return MaterializeResult(
        metadata={
            "pipeline_status": "COMPLETED",
            "pipeline_summary": MetadataValue.md(summary_md),
            "next_run_ready": True,
        }
    )

