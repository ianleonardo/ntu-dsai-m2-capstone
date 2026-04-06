# Dagster + Meltano + dbt (BigQuery) — deploy to GKE / Cloud Run / local.
# Build: docker build -t dagster-capstone:latest .

FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "uv>=0.4,<0.7"

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY . .

RUN uv sync --frozen --extra gcp

ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:${PATH}"
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Meltano plugins — requires network at image build
RUN cd dataprocessing/meltano_ingestion && meltano install

# dbt packages (dbt_utils, etc.)
RUN cd dataprocessing/dbt_insider_transactions && dbt deps --no-version-check

RUN mkdir -p /opt/dagster/dagster_home /mnt/form4_staging \
    && chmod +x /app/deployment/gcp/docker-entrypoint.sh

ENTRYPOINT ["/app/deployment/gcp/docker-entrypoint.sh"]
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dataprocessing.dagster_orchestration.repository", "-a", "definitions"]
