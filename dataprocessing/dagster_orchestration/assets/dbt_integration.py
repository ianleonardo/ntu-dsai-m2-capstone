"""
Dagster assets for dbt integration with SEC insider transactions transformation.
"""

import os
from pathlib import Path
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

# Get the dbt project directory
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dataprocessing" / "dbt_insider_transactions"

@asset(
    group_key="dbt_transformation",
    description="Runs dbt transformations for SEC insider transactions"
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
    result = dbt_cli.cli(["run"]).wait()
    
    if result.exit_code != 0:
        context.log.error(f"dbt run failed with exit code {result.exit_code}")
        context.log.error(f"stdout: {result.stdout}")
        context.log.error(f"stderr: {result.stderr}")
        raise Exception("dbt run failed")
    
    context.log.info("dbt transformations completed successfully")
    context.log.info(f"Processed {result.num_success} models successfully")


@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
)
def dbt_insider_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt assets for SEC insider transactions transformation.
    This creates individual assets for each dbt model.
    """
    yield from dbt.cli(["run"]).stream()
