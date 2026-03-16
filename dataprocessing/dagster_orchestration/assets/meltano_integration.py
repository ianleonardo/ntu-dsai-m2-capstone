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

# Resolve project root (repo root) and add scripts to the Python path
# File path: .../ntu-dsai-m2-capstone/dataprocessing/dagster_orchestration/assets/meltano_integration.py
# repo root is 3 levels up from this file (parents[3])
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "scripts"))

try:
    from sync_sec_from_gcs import sync_year_to_staging
except ImportError:
    sync_year_to_staging = None


class MeltanoStagingConfig(Config):
    """Config for Meltano staging (avoids I/O manager handoff from sec_gcs_data)."""
    year: int
    quarters: Optional[List[str]] = None
    bucket_name: str = "dsai-m2-bucket"


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
    config: MeltanoStagingConfig,
) -> MaterializeResult:
    """
    Prepares the Meltano staging area by downloading data from GCS to local staging.
    Runs after sec_gcs_data (deps); year/quarters/bucket from config.
    """
    year = config.year
    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
    bucket_name = config.bucket_name or os.getenv("GCS_BUCKET_NAME", "dsai-m2-bucket")

    context.log.info(f"Preparing Meltano staging area for year {year}, quarters: {quarters}")

    meltano_dir = project_root / "dataprocessing" / "meltano_ingestion"
    staging_dir = meltano_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    if sync_year_to_staging is None:
        raise RuntimeError("sync_sec_from_gcs.sync_year_to_staging not available; check scripts/sync_sec_from_gcs.py")

    try:
        # Use existing sync script: downloads from GCS sec-data/{year}/{year}q1|q2|.../*.tsv,
        # merges per table, writes staging/SUBMISSION.csv, DERIV_HOLDING.csv, etc. for tap-csv
        sync_year_to_staging(
            year=year,
            bucket_name=bucket_name,
            prefix="sec-data",
            staging_dir=staging_dir,
            quarters=quarters,
        )
        staged_files = [f.name for f in staging_dir.glob("*.csv")]
        if not staged_files:
            raise RuntimeError(
                f"No CSV files written under {staging_dir}; check GCS gs://{bucket_name}/sec-data/{year}/"
            )
        context.log.info(f"Staged {len(staged_files)} CSV files for Meltano: {staged_files}")

        return MaterializeResult(
            metadata={
                "year": year,
                "quarters": quarters,
                "staging_directory": str(staging_dir),
                "gcs_source": f"gs://{bucket_name}/sec-data/{year}/",
                "staged_files": MetadataValue.md("\n".join(f"- {f}" for f in staged_files)),
                "file_count": len(staged_files),
                "staging_status": "success",
            }
        )
    except Exception as e:
        raise RuntimeError(f"Failed to prepare Meltano staging area: {str(e)}")


class BigQuerySecConfig(Config):
    """Config for BigQuery load (avoids I/O manager handoff from meltano_staging_data)."""
    year: int
    quarters: Optional[List[str]] = None


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
    config: BigQuerySecConfig,
) -> MaterializeResult:
    """
    Loads SEC data from staging to BigQuery using Meltano.
    Runs after meltano_staging_data (deps); year/quarters from config.
    """
    year = config.year
    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
    
    context.log.info(f"Loading SEC data to BigQuery for year {year}, quarters: {quarters}")
    
    # Meltano project path
    meltano_dir = project_root / "dataprocessing" / "meltano_ingestion"
    
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


class SecPipelineSummaryConfig(Config):
    """Config for pipeline summary (avoids I/O manager handoff from bigquery_sec_data)."""
    year: int
    quarters: Optional[List[str]] = None


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
    config: SecPipelineSummaryConfig,
) -> MaterializeResult:
    """
    Provides a summary of the complete SEC data pipeline execution.
    Runs after bigquery_sec_data (deps); year/quarters from config.
    """
    year = config.year
    quarters = config.quarters if config.quarters is not None else ["q1", "q2", "q3", "q4"]
    
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
