"""
SEC Direct Pipeline Summary Asset

This asset provides a summary of the SEC direct pipeline execution,
using centralized configuration for consistency across all assets.
"""

import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    Config,
)

# Add repo root to Python path for imports
repo_root = Path(__file__).resolve().parent.parent.parent
if str(repo_root) not in os.sys.path:
    os.sys.path.insert(0, str(repo_root))

from ..config.pipeline_config import get_pipeline_config


class SecDirectPipelineSummaryConfig(Config):
    """Configuration for SEC pipeline summary (inherits from centralized config)."""
    
    # Allow overrides for specific runs
    year: Optional[int] = None
    from_year: Optional[int] = None
    to_year: Optional[int] = None
    quarters: Optional[List[str]] = None
    dataset: Optional[str] = None


@asset(
    name="sec_direct_pipeline_summary",
    description="Summary of SEC direct pipeline execution with centralized configuration",
    group_name="summary",
    deps=["sec_direct_ingestion"],
)
def sec_direct_pipeline_summary(
    context: AssetExecutionContext,
    config: SecDirectPipelineSummaryConfig,
) -> MaterializeResult:
    """
    Generate a summary of SEC direct pipeline execution.
    
    This asset uses centralized configuration to ensure consistency
    across all pipeline components.
    """
    current_year = datetime.now().year

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

    # Get centralized configuration with overrides for stable settings.
    # Only pass overrides when they are not None so we don't set `dataset: None`.
    pipeline_overrides = {
        "year": years_to_process[0],
        "quarters": config.quarters,
    }
    if config.dataset is not None:
        pipeline_overrides["dataset"] = config.dataset

    pipeline_cfg = get_pipeline_config(**pipeline_overrides)

    # Get validated quarters (independent of year).
    quarters = pipeline_cfg.validate_quarters()

    year_range_label = (
        f"{years_to_process[0]}..{years_to_process[-1]}"
        if len(years_to_process) > 1
        else str(years_to_process[0])
    )

    # Generate summary metadata
    summary_data = {
        "pipeline_type": "Process data directly from SEC website to BigQuery",
        "from_year": years_to_process[0],
        "to_year": years_to_process[-1],
        "years_processed": len(years_to_process),
        "year_range": year_range_label,
        "quarters": quarters,
        "quarters_description": pipeline_cfg.get_quarters_description(),
        "project_id": pipeline_cfg.project_id,
        "dataset": pipeline_cfg.dataset,
        "batch_size": pipeline_cfg.batch_size,
        "configuration_source": "centralized_config",
        "environment_variables": {
            "SEC_YEAR": os.getenv("SEC_YEAR"),
            "SEC_LOAD_QUARTER": os.getenv("SEC_LOAD_QUARTER"),
            "BIGQUERY_DATASET": os.getenv("BIGQUERY_DATASET"),
            "GOOGLE_PROJECT_ID": os.getenv("GOOGLE_PROJECT_ID"),
            "SEC_BATCH_SIZE": os.getenv("SEC_BATCH_SIZE"),
        },
    }
    
    # Log summary information
    context.log.info(f"SEC Direct Pipeline Summary for {year_range_label}")
    context.log.info(f"Quarters: {pipeline_cfg.get_quarters_description()}")
    context.log.info(f"Target: {pipeline_cfg.project_id}.{pipeline_cfg.dataset}")
    context.log.info(f"Configuration: Centralized (single source of truth)")
    
    return MaterializeResult(
        metadata=summary_data
    )

