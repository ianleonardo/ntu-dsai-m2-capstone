"""
Dagster asset: load S&P 500 constituents into BigQuery (sp500_companies).

Flow:
  DataHub CSV -> staging/sp500_companies.jsonl -> Meltano tap-jsonl-sp500 -> target-bigquery
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset
from dotenv import dotenv_values

from ..utils.meltano_cli import resolve_meltano_executable

# repo root is 3 levels up from this file (parents[3])
project_root = Path(__file__).resolve().parents[3]


class Sp500CompaniesLoadConfig(Config):
    """Optional overrides for staging / BigQuery target."""

    staging_dir: str = str(project_root / "dataprocessing" / "meltano_ingestion" / "staging")


@asset(
    key="sp500_companies_ingestion",
    description="Download S&P 500 constituents and load to BigQuery via Meltano (tap-jsonl-sp500).",
    metadata={"source": "datahub.io", "target": "BigQuery", "dataset": "insider_transactions"},
)
def sp500_companies_ingestion(
    context: AssetExecutionContext, config: Sp500CompaniesLoadConfig
) -> MaterializeResult:
    meltano_dir = project_root / "dataprocessing" / "meltano_ingestion"
    if not meltano_dir.is_dir():
        raise FileNotFoundError(f"Missing Meltano dir: {meltano_dir}")

    env = os.environ.copy()
    _dotenv = project_root / ".env"
    if _dotenv.is_file():
        for _k, _v in dotenv_values(_dotenv).items():
            if _v is not None and str(_v).strip() != "":
                env[str(_k)] = str(_v)
    if "GOOGLE_CLOUD_PROJECT" not in env or not env.get("GOOGLE_CLOUD_PROJECT"):
        env["GOOGLE_CLOUD_PROJECT"] = env.get("GOOGLE_PROJECT_ID", "")

    meltano_exe = resolve_meltano_executable(project_root)

    # 1) Download CSV -> write staging/sp500_companies.jsonl
    staging_dir = Path(config.staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    fetch_script = project_root / "scripts" / "download_sync_sp500_companies.py"
    cmd_fetch = [
        sys.executable,
        str(fetch_script),
        "--staging-dir",
        str(staging_dir),
    ]
    fetch_proc = subprocess.run(
        cmd_fetch,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        env=env,
    )
    if fetch_proc.returncode != 0:
        raise RuntimeError(
            f"sp500_companies_ingestion fetch failed: {fetch_proc.stderr or fetch_proc.stdout}"
        )

    # 2) Load JSONL to BigQuery via Meltano (same pattern as sp500_stock_daily_pipeline_job).
    context.log.info("Installing Meltano plugins (best-effort).")
    subprocess.run(
        [meltano_exe, "install"],
        cwd=str(meltano_dir),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    # Full refresh: tap-jsonl bookmarks by file mtime; without this, re-runs can skip loads.
    load_cmd = [
        meltano_exe,
        "el",
        "tap-jsonl-sp500",
        "target-bigquery",
        "--full-refresh",
    ]
    context.log.info("Running Meltano el: tap-jsonl-sp500 -> target-bigquery (--full-refresh)")
    load_proc = subprocess.run(
        load_cmd,
        cwd=str(meltano_dir),
        capture_output=True,
        text=True,
        env=env,
    )
    if load_proc.returncode != 0:
        raise RuntimeError(
            f"sp500_companies_ingestion load failed: {load_proc.stderr or load_proc.stdout}"
        )

    combined = "\n".join(
        [fetch_proc.stdout, fetch_proc.stderr, load_proc.stdout, load_proc.stderr]
    )
    m = re.search(r'"metric"\s*:\s*"record_count"\s*,\s*"value"\s*:\s*(\d+)', combined)
    rows_loaded: int | str = int(m.group(1)) if m else "Unknown"
    return MaterializeResult(
        metadata={
            "staging_dir": str(staging_dir),
            "staging_jsonl": str(staging_dir / "sp500_companies.jsonl"),
            "meltano_cmd": " ".join(load_cmd),
            "tap_rows_emitted": rows_loaded,
            "output_tail": MetadataValue.md(f"```\n{(combined.strip() or '')[-3000:]}\n```"),
        }
    )


__all__ = ["sp500_companies_ingestion"]

