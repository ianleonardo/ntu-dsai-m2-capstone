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
    configured,
)

# Add the scripts directory to the Python path to import existing functions
script_dir = Path(__file__).parent.parent.parent / "scripts"
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
    """Configuration for GCS upload."""
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
    
    Args:
        context: Dagster execution context
        config: Configuration containing year and quarters
    
    Returns:
        MaterializeResult with metadata about the download
    """
    year = config.year
    quarters = config.quarters or ["q1", "q2", "q3", "q4"]
    
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
    
    # Download data
    success = download_sec_data(year, quarters)
    
    if not success:
        raise RuntimeError(f"Failed to download SEC data for year {year}")
    
    # Get local data path for metadata
    local_path = f"data/sec/{year}"
    
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
    sec_raw_data: MaterializeResult,
) -> MaterializeResult:
    """
    Upload downloaded SEC data to Google Cloud Storage.
    
    Args:
        context: Dagster execution context
        config: Configuration for GCS upload
        sec_raw_data: Output from sec_raw_data asset
    
    Returns:
        MaterializeResult with metadata about the upload
    """
    # Extract year and quarters from the upstream asset metadata
    year = sec_raw_data.metadata.get("year")
    quarters = sec_raw_data.metadata.get("quarters", ["q1", "q2", "q3", "q4"])
    
    bucket_name = config.bucket_name
    keep_local = config.keep_local
    
    context.log.info(f"Starting GCS upload for year {year}, quarters: {quarters}")
    
    # Get local path from previous asset metadata
    local_path = f"data/sec/{year}"
    
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
