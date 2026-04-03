"""
Dagster job for loading S&P 500 constituents into BigQuery.
"""

from dagster import AssetSelection, ConfigMapping, Field, define_asset_job


def _sp500_companies_config_fn(job_config: dict) -> dict:
    cfg: dict = {}
    v = job_config.get("staging_dir")
    if v not in (None, "", []):
        cfg["staging_dir"] = v
    return {"ops": {"sp500_companies_ingestion": {"config": cfg}}}


sp500_companies_ingestion_job = define_asset_job(
    name="sp500_companies_ingestion_job",
    description="Load S&P 500 constituents: CSV -> JSONL -> BigQuery (via Meltano).",
    selection=AssetSelection.assets("sp500_companies_ingestion"),
    config=ConfigMapping(
        config_fn=_sp500_companies_config_fn,
        config_schema={
            "staging_dir": Field(
                str,
                is_required=False,
                description="Override Meltano staging dir (default: dataprocessing/meltano_ingestion/staging).",
            )
        },
    ),
)


__all__ = ["sp500_companies_ingestion_job"]

