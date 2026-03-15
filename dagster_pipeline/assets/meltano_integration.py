"""
Dagster assets for Meltano integration and BigQuery loading.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    Config,
)

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


@asset(
    key="meltano_staging_data",
    description="Prepares Meltano staging area with SEC data from GCS",
    metadata={
        "tool": "Meltano",
        "stage": "staging",
    },
    deps=["sec_gcs_data"],
)
def meltano_staging_data(
    context: AssetExecutionContext,
    sec_gcs_data: MaterializeResult,
) -> MaterializeResult:
    """
    Prepares the Meltano staging area by downloading data from GCS to local staging.
    
    Args:
        context: Dagster execution context
        sec_gcs_data: Output from sec_gcs_data asset
    
    Returns:
        MaterializeResult with metadata about staging preparation
    """
    # Extract year and quarters from the upstream asset metadata
    year = sec_gcs_data.metadata.get("year")
    quarters = sec_gcs_data.metadata.get("quarters", ["q1", "q2", "q3", "q4"])
    
    context.log.info(f"Preparing Meltano staging area for year {year}, quarters: {quarters}")
    
    # Meltano project path
    meltano_dir = project_root / "meltano-ingestion"
    staging_dir = meltano_dir / "staging"
    
    # Ensure staging directory exists
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Get GCS path from previous asset metadata
    bucket_name = sec_gcs_data.metadata.get("bucket_name", "dsai-m2-bucket")
    gcs_prefix = f"sec-data/{year}/"
    
    # Use gsutil to sync data from GCS to staging
    try:
        # Clear existing staging data for this year
        for existing_file in staging_dir.glob("*"):
            if existing_file.is_file():
                existing_file.unlink()
        
        # Download data from GCS to staging
        gsutil_cmd = [
            "gsutil",
            "-m",
            "cp",
            f"gs://{bucket_name}/{gcs_prefix}**",
            str(staging_dir),
        ]
        
        context.log.info(f"Running: {' '.join(gsutil_cmd)}")
        result = subprocess.run(
            gsutil_cmd,
            capture_output=True,
            text=True,
            cwd=str(meltano_dir),
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"gsutil failed: {result.stderr}")
        
        # List staged files
        staged_files = []
        for file_path in staging_dir.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(staging_dir)
                staged_files.append(str(rel_path))
        
        return MaterializeResult(
            metadata={
                "year": year,
                "quarters": quarters,
                "staging_directory": str(staging_dir),
                "gcs_source": f"gs://{bucket_name}/{gcs_prefix}",
                "staged_files": MetadataValue.md("\n".join(f"- {f}" for f in staged_files)),
                "file_count": len(staged_files),
                "staging_status": "success",
            }
        )
        
    except Exception as e:
        raise RuntimeError(f"Failed to prepare Meltano staging area: {str(e)}")


@asset(
    key="bigquery_sec_data",
    description="Loads SEC data from staging to BigQuery using Meltano",
    metadata={
        "tool": "Meltano",
        "target": "BigQuery",
        "dataset": "insider_transactions",
    },
    deps=["meltano_staging_data"],
)
def bigquery_sec_data(
    context: AssetExecutionContext,
    meltano_staging_data: MaterializeResult,
) -> MaterializeResult:
    """
    Loads SEC data from staging to BigQuery using Meltano.
    
    Args:
        context: Dagster execution context
        meltano_staging_data: Output from meltano_staging_data asset
    
    Returns:
        MaterializeResult with metadata about BigQuery load
    """
    # Extract year and quarters from the upstream asset metadata
    year = meltano_staging_data.metadata.get("year")
    quarters = meltano_staging_data.metadata.get("quarters", ["q1", "q2", "q3", "q4"])
    
    context.log.info(f"Loading SEC data to BigQuery for year {year}, quarters: {quarters}")
    
    # Meltano project path
    meltano_dir = project_root / "meltano-ingestion"
    
    try:
        # Run Meltano install to ensure plugins are up to date
        install_cmd = ["meltano", "install"]
        context.log.info(f"Running Meltano install: {' '.join(install_cmd)}")
        
        install_result = subprocess.run(
            install_cmd,
            capture_output=True,
            text=True,
            cwd=str(meltano_dir),
        )
        
        if install_result.returncode != 0:
            context.log.warning(f"Meltano install warning: {install_result.stderr}")
        
        # Run Meltano job to load data to BigQuery
        run_cmd = ["meltano", "run", "tap-csv", "target-bigquery"]
        context.log.info(f"Running Meltano job: {' '.join(run_cmd)}")
        
        run_result = subprocess.run(
            run_cmd,
            capture_output=True,
            text=True,
            cwd=str(meltano_dir),
        )
        
        if run_result.returncode != 0:
            raise RuntimeError(f"Meltano run failed: {run_result.stderr}")
        
        # Parse Meltano output for load statistics
        output_lines = run_result.stdout.split('\n')
        load_stats = {}
        
        for line in output_lines:
            if 'rows' in line.lower() or 'records' in line.lower():
                # Try to extract numeric information
                import re
                numbers = re.findall(r'\d+', line)
                if numbers:
                    load_stats['rows_loaded'] = int(numbers[0])
        
        # Default stats if parsing failed
        if not load_stats:
            load_stats = {
                'rows_loaded': 'Unknown',
                'tables_updated': 'SEC_SUBMISSION, SEC_REPORTINGOWNER, SEC_NONDERIV_TRANS, SEC_NONDERIV_HOLDING, SEC_DERIV_TRANS, SEC_DERIV_HOLDING'
            }
        
        return MaterializeResult(
            metadata={
                "year": year,
                "quarters": quarters,
                "meltano_job": "tap-csv target-bigquery",
                "bigquery_project": "ntu-dsai-488112",
                "bigquery_dataset": "insider_transactions",
                "load_statistics": MetadataValue.md("\n".join(f"- {k}: {v}" for k, v in load_stats.items())),
                "meltano_output": MetadataValue.md(f"```\n{run_result.stdout[-1000:]}\n```"),
                "load_status": "success",
            }
        )
        
    except Exception as e:
        raise RuntimeError(f"Failed to load data to BigQuery: {str(e)}")


@asset(
    key="sec_pipeline_summary",
    description="Summary of the complete SEC data pipeline execution",
    metadata={
        "pipeline": "SEC Data Pipeline",
        "scope": "Download -> GCS -> BigQuery",
    },
    deps=["bigquery_sec_data"],
)
def sec_pipeline_summary(
    context: AssetExecutionContext,
    bigquery_sec_data: MaterializeResult,
) -> MaterializeResult:
    """
    Provides a summary of the complete SEC data pipeline execution.
    
    Args:
        context: Dagster execution context
        bigquery_sec_data: Output from bigquery_sec_data asset
    
    Returns:
        MaterializeResult with pipeline summary
    """
    # Extract year and quarters from the upstream asset metadata
    year = bigquery_sec_data.metadata.get("year")
    quarters = bigquery_sec_data.metadata.get("quarters", ["q1", "q2", "q3", "q4"])
    
    context.log.info(f"Generating pipeline summary for year {year}, quarters: {quarters}")
    
    # Collect metadata from all upstream assets
    summary = {
        "pipeline_name": "SEC Data Pipeline",
        "execution_year": year,
        "quarters_processed": quarters,
        "stages_completed": [
            "✅ SEC Data Download",
            "✅ GCS Upload", 
            "✅ Meltano Staging",
            "✅ BigQuery Load"
        ],
        "bigquery_destination": {
            "project": "ntu-dsai-488112",
            "dataset": "insider_transactions",
            "tables": [
                "SEC_SUBMISSION",
                "SEC_REPORTINGOWNER", 
                "SEC_NONDERIV_TRANS",
                "SEC_NONDERIV_HOLDING",
                "SEC_DERIV_TRANS",
                "SEC_DERIV_HOLDING"
            ]
        },
        "data_freshness": f"{year}",
        "pipeline_status": "COMPLETED"
    }
    
    return MaterializeResult(
        metadata={
            "pipeline_summary": MetadataValue.md("\n".join(f"- {k}: {v}" for k, v in summary.items())),
            "execution_details": MetadataValue.md(f"""
## Pipeline Execution Summary

**Year:** {year}
**Quarters:** {', '.join(quarters)}

### Completed Stages:
1. ✅ SEC Data Download - Downloaded from SEC.gov
2. ✅ GCS Upload - Uploaded to dsai-m2-bucket
3. ✅ Meltano Staging - Prepared for loading
4. ✅ BigQuery Load - Loaded to insider_transactions dataset

### Destination:
- **Project:** ntu-dsai-488112
- **Dataset:** insider_transactions
- **Tables:** 6 tables loaded (SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS, NONDERIV_HOLDING, DERIV_TRANS, DERIV_HOLDING)

### Data Access:
```sql
-- Example query for BigQuery
SELECT * FROM `ntu-dsai-488112.insider_transactions.SEC_SUBMISSION`
WHERE EXTRACT(YEAR FROM filing_date) = {year};
```
"""),
            "pipeline_status": "COMPLETED",
            "next_run_ready": True,
        }
    )
