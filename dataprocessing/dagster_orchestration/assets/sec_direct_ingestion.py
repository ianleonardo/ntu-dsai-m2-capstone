"""
Simplified SEC Data Ingestion Assets for Dagster

This module provides Dagster assets for the simplified SEC data pipeline
that downloads data directly from SEC website to BigQuery, eliminating
any intermediate storage layer.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

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
    # Accept either:
    # - `year` (single-year run), OR
    # - `from_year` + `to_year` (multi-year backfill run)
    year: Optional[int] = None
    from_year: Optional[int] = None
    to_year: Optional[int] = None
    quarters: Optional[List[str]] = None
    dataset: Optional[str] = None
    batch_size: Optional[int] = None
    dry_run: Optional[bool] = None
    skip_dedupe: Optional[bool] = None


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
    
    Streams data directly to BigQuery, reducing complexity and processing time.
    """
    current_year = datetime.now().year

    # Resolve the set of years to process for this run.
    if config.from_year is not None and config.to_year is not None:
        if config.from_year > config.to_year:
            raise ValueError("from_year must be <= to_year")
        years_to_process = list(range(config.from_year, config.to_year + 1))
    elif config.year is not None:
        years_to_process = [config.year]
    else:
        raise ValueError("Provide either `year` or (`from_year` and `to_year`).")

    for y in years_to_process:
        if y < 2006 or y > current_year:
            raise ValueError(f"Year must be between 2006 and {current_year}. Got: {y}")

    # Get centralized config with overrides for "static" settings.
    # Important: only pass overrides when they are not None; otherwise we'd set
    # fields like `dataset: str` to None and pydantic will fail validation.
    pipeline_overrides = {
        "year": years_to_process[0],
        "quarters": config.quarters,
    }
    if config.dataset is not None:
        pipeline_overrides["dataset"] = config.dataset
    if config.batch_size is not None:
        pipeline_overrides["batch_size"] = config.batch_size
    if config.dry_run is not None:
        pipeline_overrides["dry_run"] = config.dry_run
    if config.skip_dedupe is not None:
        pipeline_overrides["skip_dedupe"] = config.skip_dedupe

    base_cfg = get_pipeline_config(**pipeline_overrides)

    quarters = base_cfg.validate_quarters()
    quarters_desc = base_cfg.get_quarters_description()
    context.log.info(
        f"SEC Direct Pipeline: Loading years {years_to_process[0]} -> {years_to_process[-1]} [{quarters_desc}] to BigQuery"
    )
    context.log.info(f"Project: {base_cfg.project_id}, Dataset: {base_cfg.dataset}")

    # Initialize BigQuery loader once (dataset settings are stable across years).
    loader = SECBigQueryLoader(base_cfg.project_id, base_cfg.dataset)
    loader.batch_size = base_cfg.batch_size
    loader.skip_dedupe = base_cfg.skip_dedupe

    if not base_cfg.dry_run:
        loader.ensure_dataset_exists()

    # Process and load data for each year.
    from scripts.download_sec_to_bigquery import SEC_TABLES, TABLE_CONFIGS

    total_rows = 0
    success = True
    tables_loaded = set()

    for year in years_to_process:
        context.log.info(f"=== Downloading SEC data for year {year} ===")
        downloaded_data = download_sec_data(year, quarters)
        if not downloaded_data:
            raise ValueError(f"No data downloaded from SEC website for year {year}")

        context.log.info("=== Processing and loading data ===")

        for table in SEC_TABLES:
            if table not in downloaded_data:
                context.log.warning(f"No data for {table} (year {year})")
                continue

            # Combine data from all quarters for this year/table.
            combined_tsv = "\n".join(downloaded_data[table])
            row_count = combined_tsv.count("\n") - 1  # best-effort metadata
            total_rows += row_count
            tables_loaded.add(table)

            table_config = TABLE_CONFIGS.get(table, {})
            table_id = table_config.get("table_id", table.lower())

            context.log.info(
                f"Processing {table} (year {year}, {len(downloaded_data[table])} quarters, {row_count} rows)..."
            )

            if base_cfg.dry_run:
                context.log.info(f"DRY RUN: Would load {row_count} rows to {table_id}")
            else:
                if not loader.process_table_data(table_id, combined_tsv, year):
                    success = False
                    context.log.error(f"Failed to load {table} (year {year})")
                else:
                    context.log.info(f"Successfully loaded {table} (year {year})")

    if not success:
        raise ValueError("SEC data pipeline completed with errors")

    return MaterializeResult(
        metadata={
            "from_year": years_to_process[0],
            "to_year": years_to_process[-1],
            "years_processed": years_to_process,
            "quarters": quarters,
            "dataset": base_cfg.dataset,
            "total_rows": total_rows,
            "tables_loaded": len(tables_loaded),
            "dry_run": base_cfg.dry_run,
            "skip_dedupe": base_cfg.skip_dedupe,
        }
    )


# Define the assets for the simplified pipeline
defs = Definitions(
    assets=[sec_direct_ingestion],
)
