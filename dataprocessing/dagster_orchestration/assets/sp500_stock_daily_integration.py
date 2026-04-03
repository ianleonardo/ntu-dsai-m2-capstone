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

from dotenv import dotenv_values
from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset

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
    # Stream script logs live so long fetch windows do not look hung.
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    ) as proc:
        assert proc.stdout is not None
        assert proc.stderr is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            stdout_lines.append(line)
            if line:
                context.log.info(line)
        stderr_text = proc.stderr.read()
        if stderr_text:
            stderr_lines = [ln for ln in stderr_text.splitlines() if ln.strip()]
            for ln in stderr_lines:
                context.log.error(ln)
        return_code = proc.wait()
    if return_code != 0:
        raise RuntimeError(f"SP500 fetch failed with exit code {return_code}")
    stdout_text = "\n".join(stdout_lines)

    # Extract row count from fetch output (best-effort).
    m = re.search(r"Wrote\s+(\d+)\s+JSONL rows", stdout_text)
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
    Loads staging JSONL via Meltano with a pinned Singer catalog so nullable numerics
    (e.g. SMA200) stay FLOAT64 in BigQuery instead of inferred STRING from type ["null"].
    """
    meltano_dir = project_root / "dataprocessing" / "meltano_ingestion"
    catalog_path = meltano_dir / "catalogs" / "sp500_stock_daily.catalog.json"
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Missing Meltano catalog: {catalog_path}")

    env = os.environ.copy()
    # Match shell wrappers: load repo `.env` so TARGET_BIGQUERY_CREDENTIALS_PATH / project IDs apply.
    _dotenv = project_root / ".env"
    if _dotenv.is_file():
        for _k, _v in dotenv_values(_dotenv).items():
            if _v is not None and str(_v).strip() != "":
                env[str(_k)] = str(_v)
    if "GOOGLE_CLOUD_PROJECT" not in env or not env.get("GOOGLE_CLOUD_PROJECT"):
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

        context.log.info(
            "Running Meltano el: tap-jsonl-sp500-stock-daily -> target-bigquery "
            f"(catalog={catalog_path.name})"
        )
        # Full refresh: tap-jsonl bookmarks by file mtime; without this, re-runs often skip the
        # file and load 0 rows while still exiting 0 — looks like "nothing in BigQuery".
        run_cmd = [
            "meltano",
            "el",
            "tap-jsonl-sp500-stock-daily",
            "target-bigquery",
            "--catalog",
            str(catalog_path.relative_to(meltano_dir)),
            "--full-refresh",
        ]
        run_proc = subprocess.run(
            run_cmd,
            cwd=str(meltano_dir),
            capture_output=True,
            text=True,
            env=env,
        )
        if run_proc.stdout.strip():
            context.log.info(run_proc.stdout)
        if run_proc.stderr.strip():
            for _ln in run_proc.stderr.strip().splitlines():
                context.log.warning(_ln)
        if run_proc.returncode != 0:
            context.log.error(run_proc.stderr)
            raise RuntimeError(f"Meltano el failed: {run_proc.stderr}")

        combined = f"{run_proc.stdout}\n{run_proc.stderr}"
        m = re.search(r'"metric"\s*:\s*"record_count"\s*,\s*"value"\s*:\s*(\d+)', combined)
        rows_loaded: int | str = int(m.group(1)) if m else "Unknown"
        if rows_loaded == "Unknown":
            m2 = re.search(r"Processed\s+(\d+)\s+rows\s+from", combined)
            rows_loaded = int(m2.group(1)) if m2 else "Unknown"

        return MaterializeResult(
            metadata={
                "start": config.start,
                "end": config.end,
                "meltano_job": "meltano el ... --catalog ... --full-refresh",
                "bigquery_project": "ntu-dsai-488112",
                "bigquery_dataset": "insider_transactions",
                "bigquery_table": "SP500_STOCK_DAILY",
                "tap_rows_emitted": rows_loaded,
                "load_status": "success",
                "meltano_output_tail": MetadataValue.md(
                    f"```\n{(combined.strip() or '(no stdout/stderr)')[-4000:]}\n```"
                ),
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

