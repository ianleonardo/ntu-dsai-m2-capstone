"""
Dagster assets for dbt integration with SEC insider transactions transformation.
"""

import os
from pathlib import Path
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource

# Resolve repo root and dbt project directory
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = REPO_ROOT / "dataprocessing" / "dbt_insider_transactions"

@asset(
    group_name="dbt_transformation",
    description="Runs dbt transformations for SEC insider transactions",
    deps=["sec_direct_ingestion"],
)
def dbt_insider_transformation(context: AssetExecutionContext) -> None:
    """
    Asset that runs dbt transformations for SEC insider transactions.
    This asset depends on the BigQuery data being loaded by Meltano.
    """
    dbt_cli = DbtCliResource(
        project_dir=os.fspath(DBT_PROJECT_DIR),
        profiles_dir=os.fspath(DBT_PROJECT_DIR),
    )
    
    # Run dbt run to execute all models
    invocation = dbt_cli.cli(["run"]).wait()
    
    if not invocation.is_successful():
        err = invocation.get_error()
        context.log.error(f"dbt run failed: {err}")
        raise RuntimeError(f"dbt run failed: {err}")
    
    context.log.info("dbt transformations completed successfully")


# If you later want fine-grained dbt asset integration (one asset per dbt model),
# you can reintroduce dbt_dbt's `@dbt_assets` integration here using the
# signature that matches your installed dagster-dbt version.
