#!/bin/sh
set -e
mkdir -p "${DAGSTER_HOME:-/opt/dagster/dagster_home}"
python /app/deployment/gcp/render_dagster_yaml.py
exec "$@"
