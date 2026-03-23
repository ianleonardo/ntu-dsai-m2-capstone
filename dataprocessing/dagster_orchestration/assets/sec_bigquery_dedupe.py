"""
BigQuery maintenance: dedupe SEC raw tables without downloading from SEC.
"""

import sys
from pathlib import Path
from typing import Optional

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

repo_root = Path(__file__).resolve().parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.download_sec_to_bigquery import SECBigQueryLoader

from ..config.pipeline_config import get_pipeline_config


class DedupeOnlyConfig(Config):
    """Optional overrides for dedupe-only runs."""

    dataset: Optional[str] = None


@asset(
    name="sec_bigquery_dedupe_only",
    group_name="maintenance",
    description=(
        "Runs BigQuery dedupe (WRITE_TRUNCATE) on all configured SEC raw tables. "
        "No SEC download; use after re-loads or from the Launchpad catalog."
    ),
)
def sec_bigquery_dedupe_only(
    context: AssetExecutionContext,
    config: DedupeOnlyConfig,
) -> MaterializeResult:
    overrides = {}
    if config.dataset is not None:
        overrides["dataset"] = config.dataset
    pcfg = get_pipeline_config(**overrides)

    loader = SECBigQueryLoader(pcfg.project_id, pcfg.dataset)
    loader.skip_dedupe = False
    loader.ensure_dataset_exists()

    fq = f"{pcfg.project_id}.{pcfg.dataset}"
    context.log.info(f"Dedupe-only starting for dataset {fq}")

    if not loader.dedupe_all_configured_tables():
        raise RuntimeError("BigQuery dedupe failed for one or more SEC tables")

    context.log.info(f"Dedupe-only completed for {fq}")

    return MaterializeResult(
        metadata={
            "project_id": MetadataValue.text(pcfg.project_id),
            "dataset": MetadataValue.text(pcfg.dataset),
            "operation": MetadataValue.text("dedupe_all_configured_tables"),
        }
    )
