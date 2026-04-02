"""
Dagster asset for SEC Form 4 daily-index ingestion (download + optional BigQuery load).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset

project_root = Path(__file__).resolve().parents[3]


class SecForm4DailyConfig(Config):
    from_date: str = "2026-01-01"
    to_date: str
    user_agent: str = "NTU DSAI Capstone ian@example.com"
    output_dir: str = str(
        project_root
        / "dataprocessing"
        / "meltano_ingestion"
        / "staging"
        / "sec_form4_2026_monthly"
    )
    max_requests_per_second: float = 5.0
    sleep_seconds: float = 0.0
    resume: bool = True
    upload_bigquery: bool = True
    bq_project_id: str = ""
    bq_dataset: str = ""


@asset(
    key="sec_form4_daily_ingestion",
    description="Download Form 4 filings (via daily indexes) by date range and load monthly files to BigQuery.",
    metadata={"tool": "SEC EDGAR + BigQuery", "scope": "Form 4 only"},
)
def sec_form4_daily_ingestion(
    context: AssetExecutionContext, config: SecForm4DailyConfig
) -> MaterializeResult:
    script = project_root / "scripts" / "download_sec_form4_daily.py"
    if not script.is_file():
        raise FileNotFoundError(f"Missing script: {script}")

    cmd = [
        sys.executable,
        str(script),
        "--start-date",
        config.from_date,
        "--end-date",
        config.to_date,
        "--user-agent",
        config.user_agent,
        "--output-dir",
        config.output_dir,
        "--max-requests-per-second",
        str(config.max_requests_per_second),
        "--sleep-seconds",
        str(config.sleep_seconds),
    ]
    if config.resume:
        cmd.append("--resume")
    if config.upload_bigquery:
        cmd.append("--upload-bigquery")
    if config.bq_project_id.strip():
        cmd.extend(["--bq-project-id", config.bq_project_id.strip()])
    if config.bq_dataset.strip():
        cmd.extend(["--bq-dataset", config.bq_dataset.strip()])

    env = os.environ.copy()
    context.log.info(
        "Running SEC Form4 daily-index ingestion: "
        f"{config.from_date} -> {config.to_date}, upload_bigquery={config.upload_bigquery}"
    )
    proc = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.stdout.strip():
        context.log.info(proc.stdout)
    if proc.stderr.strip():
        context.log.warning(proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"sec_form4_daily_ingestion failed: {proc.stderr or proc.stdout}")

    summary_path = Path(config.output_dir) / "run_summary.json"
    summary_obj = {}
    if summary_path.exists():
        try:
            summary_obj = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            summary_obj = {}

    return MaterializeResult(
        metadata={
            "from_date": config.from_date,
            "to_date": config.to_date,
            "output_dir": config.output_dir,
            "upload_bigquery": config.upload_bigquery,
            "summary": MetadataValue.json(summary_obj) if summary_obj else "{}",
            "stdout_tail": MetadataValue.md(f"```\n{proc.stdout[-2000:]}\n```"),
        }
    )


__all__ = ["sec_form4_daily_ingestion", "SecForm4DailyConfig"]

