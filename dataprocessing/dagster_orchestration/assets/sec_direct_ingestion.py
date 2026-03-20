"""
Simplified SEC Data Ingestion Assets for Dagster

This module provides Dagster assets for the simplified SEC data pipeline
that downloads data directly from SEC website to BigQuery, eliminating
the GCS intermediate step.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    Config,
    Definitions,
)

# Add repo root to Python path for imports
repo_root = Path(__file__).resolve().parent.parent.parent
if str(repo_root) not in os.sys.path:
    os.sys.path.insert(0, str(repo_root))

from scripts.download_sec_to_bigquery import SECBigQueryLoader, download_sec_data, parse_quarters
from ..config.pipeline_config import get_pipeline_config


class SECIngestionConfig(Config):
    """Configuration for SEC data ingestion (inherits from centralized config)."""
    
    # Allow overrides for specific runs
    year: Optional[int] = None
    quarters: Optional[List[str]] = None
    dataset: Optional[str] = None
    batch_size: Optional[int] = None
    dry_run: Optional[bool] = None


@asset(
    name="sec_direct_ingestion",
    description="SEC insider transaction data loaded directly from SEC website to BigQuery",
    group_name="ingestion",
)
def sec_direct_ingestion(
    context: AssetExecutionContext,
    config: SECIngestionConfig,
) -> MaterializeResult:
    """
    Download SEC data directly from SEC website and load to BigQuery.
    
    This asset eliminates the GCS intermediate step and streams data directly
    to BigQuery, reducing complexity and processing time.
    """
    # Get centralized configuration with overrides
    pipeline_cfg = get_pipeline_config(
        year=config.year,
        quarters=config.quarters,
        dataset=config.dataset,
        batch_size=config.batch_size,
        dry_run=config.dry_run,
    )
    
    # Validate configuration
    current_year = 2026
    if pipeline_cfg.year < 2006 or pipeline_cfg.year > current_year:
        raise ValueError(f"Year must be between 2006 and {current_year}")
    
    # Get validated quarters
    quarters = pipeline_cfg.validate_quarters()
    
    quarters_desc = pipeline_cfg.get_quarters_description()
    context.log.info(f"SEC Direct Pipeline: Loading {pipeline_cfg.year} [{quarters_desc}] to BigQuery")
    context.log.info(f"Project: {pipeline_cfg.project_id}, Dataset: {pipeline_cfg.dataset}")
    
    # Initialize BigQuery loader
    loader = SECBigQueryLoader(pipeline_cfg.project_id, pipeline_cfg.dataset)
    loader.batch_size = pipeline_cfg.batch_size
    
    # Ensure dataset exists
    if not pipeline_cfg.dry_run:
        loader.ensure_dataset_exists()
    
    # Download SEC data
    context.log.info("=== Downloading SEC data ===")
    downloaded_data = download_sec_data(pipeline_cfg.year, quarters)
    
    if not downloaded_data:
        raise ValueError("No data downloaded from SEC website")
    
    # Process and load data
    context.log.info("=== Processing and loading data ===")
    
    # Import SEC tables and configs
    from scripts.download_sec_to_bigquery import SEC_TABLES, TABLE_CONFIGS
    
    total_rows = 0
    success = True
    
    for table in SEC_TABLES:
        if table not in downloaded_data:
            context.log.warning(f"No data for {table}")
            continue
        
        # Combine data from all quarters
        combined_tsv = "\n".join(downloaded_data[table])
        row_count = combined_tsv.count('\n') - 1  # Subtract header row
        total_rows += row_count
        
        # Get table configuration
        table_config = TABLE_CONFIGS.get(table, {})
        table_id = table_config.get("table_id", table.lower())
        
        context.log.info(f"Processing {table} ({len(downloaded_data[table])} quarters, {row_count} rows)...")
        
        if pipeline_cfg.dry_run:
            context.log.info(f"DRY RUN: Would load {row_count} rows to {table_id}")
        else:
            if not loader.process_table_data(table_id, combined_tsv, pipeline_cfg.year):
                success = False
                context.log.error(f"Failed to load {table}")
            else:
                context.log.info(f"Successfully loaded {table}")
    
    if not success:
        raise ValueError("SEC data pipeline completed with errors")
    
    return MaterializeResult(
        metadata={
            "year": pipeline_cfg.year,
            "quarters": quarters,
            "dataset": pipeline_cfg.dataset,
            "total_rows": total_rows,
            "tables_loaded": len(downloaded_data),
            "dry_run": pipeline_cfg.dry_run,
        }
    )


# Define the assets for the simplified pipeline
defs = Definitions(
    assets=[sec_direct_ingestion],
)
