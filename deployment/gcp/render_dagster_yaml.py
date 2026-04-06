"""Write $DAGSTER_HOME/dagster.yaml for Postgres (production) or SQLite (fallback)."""

from __future__ import annotations

import os
from pathlib import Path

import yaml


def main() -> None:
    home = Path(os.environ.get("DAGSTER_HOME", "/opt/dagster/dagster_home"))
    home.mkdir(parents=True, exist_ok=True)
    out = home / "dagster.yaml"

    pg_host = os.environ.get("DAGSTER_POSTGRES_HOST", "").strip()
    if pg_host:
        cfg = {
            "storage": {
                "postgres": {
                    "postgres_db": {
                        "username": os.environ.get("DAGSTER_POSTGRES_USER", "dagster"),
                        "password": os.environ["DAGSTER_POSTGRES_PASSWORD"],
                        "hostname": pg_host,
                        "db_name": os.environ.get("DAGSTER_POSTGRES_DB", "dagster"),
                        "port": int(os.environ.get("DAGSTER_POSTGRES_PORT", "5432")),
                    }
                }
            }
        }
    else:
        # Ephemeral / dev: SQLite under DAGSTER_HOME (not for multi-replica production).
        storage_dir = home / "storage"
        storage_dir.mkdir(parents=True, exist_ok=True)
        cfg = {"storage": {"sqlite": {"base_dir": str(storage_dir)}}}

    out.write_text(yaml.safe_dump(cfg, default_flow_style=False, sort_keys=False), encoding="utf-8")


if __name__ == "__main__":
    main()
