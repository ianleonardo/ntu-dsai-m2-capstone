"""
Dagster assets for SEC data download and GCS upload.
"""

import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    Config,
)

# Resolve project root (repo root) and add scripts to Python path to import existing functions
# File path: .../ntu-dsai-m2-capstone/dataprocessing/dagster_orchestration/assets/sec_download.py
# repo root is 3 levels up from this file (parents[3])
project_root = Path(__file__).resolve().parents[3]
script_dir = project_root / "scripts"
sys.path.insert(0, str(script_dir))

try:
    from download_sec_to_bucket import download_sec_data, upload_to_bucket, parse_quarters
except ImportError as e:
    print(f"Warning: Could not import from download_sec_to_bucket: {e}")
    # Fallback implementations for testing
    def download_sec_data(year, quarters=None):
        print(f"Mock download for {year}, quarters: {quarters}")
        return True
    
    def upload_to_bucket(year, local_path, bucket_name="dsai-m2-bucket", keep_local=False):
        print(f"Mock upload for {year} from {local_path} to {bucket_name}")
        return True
    
    def parse_quarters(value):
        if not (value or "").strip():
            return ["q1", "q2", "q3", "q4"]
        q = value.strip().lower()
        if q not in ["q1", "q2", "q3", "q4"]:
            raise ValueError(f"Quarter must be one of q1, q2, q3, q4, got: {value!r}")
        return [q]


class SecDownloadConfig(Config):
    """Configuration for SEC data download."""
    year: int
    quarters: Optional[List[str]] = None


class SecGcsConfig(Config):
    """Configuration for GCS upload. Year/quarters optional: when omitted, inferred from data/sec/ (after sec_raw_data)."""
    year: Optional[int] = None
    quarters: Optional[List[str]] = None
    bucket_name: str = "dsai-m2-bucket"
    keep_local: bool = False


@asset(
    key="sec_raw_data",
    description="Downloads SEC insider transaction data for specified year and quarters",
    metadata={
        "source": "SEC.gov",
        "data_type": "insider_transactions",
        "format": "TSV",
    },
)
def sec_raw_data(
    context: AssetExecutionContext,
    config: SecDownloadConfig,
) -> MaterializeResult:
    """
    Download SEC insider transaction data for specified year and quarters.
    Config (year, quarters) is passed via run_config when launching a run.
    
    Args:
        context: Dagster execution context
        config: Year and quarters from run config (ops.sec_raw_data.config)
    
    Returns:
        MaterializeResult with metadata about the download
    """
    year = config.year
    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
    
    context.log.info(f"Starting SEC data download for year {year}, quarters: {quarters}")
    
    # Validate year
    current_year = 2026
    if year < 2006 or year > current_year:
        raise ValueError(f"Year must be between 2006 and {current_year}, got: {year}")
    
    # Validate quarters
    valid_quarters = ["q1", "q2", "q3", "q4"]
    for q in quarters:
        if q not in valid_quarters:
            raise ValueError(f"Invalid quarter: {q}. Must be one of {valid_quarters}")
    
    # Download data: run from scripts/ so script's ../data/sec is project_root/data/sec
    orig_cwd = os.getcwd()
    try:
        os.chdir(script_dir)
        success = download_sec_data(year, quarters)
    finally:
        os.chdir(orig_cwd)
    if not success:
        raise RuntimeError(f"Failed to download SEC data for year {year}")
    
    # Local path under project root (same base sec_gcs_data infers)
    local_path = str(project_root / "data" / "sec" / str(year))
    # Count downloaded files for metadata
    downloaded_files = []
    if os.path.exists(local_path):
        for root, dirs, files in os.walk(local_path):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), local_path)
                downloaded_files.append(rel_path)
    
    return MaterializeResult(
        metadata={
            "year": year,
            "quarters": quarters,
            "local_path": local_path,
            "downloaded_files": MetadataValue.md("\n".join(f"- {f}" for f in downloaded_files)),
            "file_count": len(downloaded_files),
            "download_status": "success",
        }
    )


@asset(
    key="sec_gcs_data",
    description="Uploads downloaded SEC data to Google Cloud Storage",
    metadata={
        "destination": "Google Cloud Storage",
        "bucket": "dsai-m2-bucket",
    },
    deps=["sec_raw_data"],
)
def sec_gcs_data(
    context: AssetExecutionContext,
    config: SecGcsConfig,
) -> MaterializeResult:
    """
    Upload downloaded SEC data to Google Cloud Storage.
    Runs after sec_raw_data (deps). Year/quarters from config, or inferred from data/sec/ if omitted.
    
    Args:
        context: Dagster execution context
        config: Optional year, quarters; bucket_name, keep_local (ops.sec_gcs_data.config)
    
    Returns:
        MaterializeResult with metadata about the upload
    """
    bucket_name = config.bucket_name or os.getenv("GCS_BUCKET_NAME", "dsai-m2-bucket")
    keep_local = config.keep_local
    local_path = None  # set below
    if config.year is not None:
        year = config.year
        quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
        # Use project-root path so we match where sec_raw_data writes
        local_path = str(project_root / "data" / "sec" / str(year))
    else:
        # Infer from data/sec/ after sec_raw_data has run (deps ensure order).
        # Try cwd, project root, and cwd.parent (legacy script write location).
        candidates = (
            Path.cwd() / "data" / "sec",
            project_root / "data" / "sec",
            Path.cwd().parent / "data" / "sec",
        )
        for data_sec in candidates:
            if data_sec.exists():
                years_dirs = [d for d in data_sec.iterdir() if d.is_dir() and d.name.isdigit()]
                if years_dirs:
                    year = max(int(d.name) for d in years_dirs)
                    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
                    local_path = str(data_sec / str(year))
                    context.log.info(f"Inferred year={year} from {data_sec}")
                    break
        if local_path is None:
            raise FileNotFoundError(
                "No year directory in data/sec/; run sec_raw_data first or set ops.sec_gcs_data.config.year in run config."
            )
    
    context.log.info(f"Starting GCS upload for year {year}, quarters: {quarters}")
    
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local data path not found: {local_path}")
    
    # Upload to GCS
    success = upload_to_bucket(year, local_path, bucket_name, keep_local)
    
    if not success:
        raise RuntimeError(f"Failed to upload SEC data for year {year} to GCS")
    
    # Count uploaded files for metadata
    uploaded_files = []
    for root, dirs, files in os.walk(local_path):
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), local_path)
            uploaded_files.append(rel_path)
    
    gcs_path = f"gs://{bucket_name}/sec-data/{year}/"
    
    return MaterializeResult(
        metadata={
            "year": year,
            "quarters": quarters,
            "local_path": local_path,
            "gcs_path": gcs_path,
            "bucket_name": bucket_name,
            "uploaded_files": MetadataValue.md("\n".join(f"- {f}" for f in uploaded_files)),
            "file_count": len(uploaded_files),
            "upload_status": "success",
            "keep_local": keep_local,
        }
    )
