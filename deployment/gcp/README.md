# Dagster on GCP

This folder contains artifacts to run the Dagster OSS stack (user code gRPC + webserver + daemon) on Google Cloud with **Cloud SQL Postgres** for instance storage and optional **persistent volume** for Form 4 staging.

## What is in the image

The repo [`Dockerfile`](../../Dockerfile) installs:

- `uv sync --extra gcp` (includes `dagster`, `dagster-postgres`, `dagster-gcp`, `meltano`, `dbt-bigquery`, … — see [`pyproject.toml`](../../pyproject.toml))
- `meltano install` under `dataprocessing/meltano_ingestion`
- `dbt deps` under `dataprocessing/dbt_insider_transactions`

Runtime layout: `WORKDIR /app`, `PYTHONPATH=/app`, `PATH` includes `/app/.venv/bin` so **Meltano is on PATH** (no `.venv` path required).

## `DAGSTER_HOME` and `dagster.yaml`

- **`DAGSTER_HOME`** defaults to `/opt/dagster/dagster_home` in the image.
- On container start, [`docker-entrypoint.sh`](docker-entrypoint.sh) runs [`render_dagster_yaml.py`](render_dagster_yaml.py), which writes `$DAGSTER_HOME/dagster.yaml`:
  - If **`DAGSTER_POSTGRES_HOST`** is set: unified **Postgres** storage (`DagsterPostgresStorage`).
  - If not: **SQLite** under `$DAGSTER_HOME/storage` (dev only; not for multi-replica production).

Set these when using Cloud SQL (see `k8s/secret-dagster-postgres.example.yaml`):

| Variable | Purpose |
|----------|---------|
| `DAGSTER_POSTGRES_HOST` | Cloud SQL IP / private IP / proxy |
| `DAGSTER_POSTGRES_USER` | DB user |
| `DAGSTER_POSTGRES_PASSWORD` | DB password |
| `DAGSTER_POSTGRES_DB` | Database name (default `dagster`) |
| `DAGSTER_POSTGRES_PORT` | Port (default `5432`) |

## BigQuery and dbt

- Prefer **Workload Identity** (GKE) or the **Cloud Run service account** with BigQuery roles; avoid long‑lived JSON keys.
- [`profiles.yml`](../../dataprocessing/dbt_insider_transactions/profiles.yml) uses `env_var('GOOGLE_PROJECT_ID')` and `env_var('BIGQUERY_DATASET')`. Set `GOOGLE_PROJECT_ID` and `BIGQUERY_DATASET` in the deployment env.

## Logs

- Container **stdout/stderr** go to **Cloud Logging** automatically on GKE / Cloud Run.
- Optional: add a GCS compute log manager later if you need Dagster UI step logs after pod eviction (requires extra `dagster-gcp` wiring beyond the current stub package layout).

## Build and push the image

**Cloud Build** (Artifact Registry):

```bash
gcloud builds submit --config=cloudbuild.yaml --substitutions=_REGION=asia-southeast1 .
```

**Local**:

```bash
docker build -t capstone-dagster:local .
```

## Docker Compose (Single VM)

For a simpler deployment without Kubernetes, you can run the exact same OSS topology on a single Google Compute Engine (GCE) VM using Docker Compose.

Unlike Kubernetes (which expects an external Cloud SQL instance), this **Docker Compose stack includes its own Postgres database container** specifically for Dagster's metadata. 

### Step-by-Step Deployment Guide

1. **Deploy a GCE Instance**:
   - Provision a VM via the GCP Console (e.g., `e2-standard-2`).
   - Choose a Linux image (e.g., Debian or Ubuntu) and ensure you allow HTTP traffic (to expose Dagster UI port `3000`).
   - SSH into the VM and install Docker and Git:
     ```bash
     sudo apt-get update && sudo apt-get install -y docker.io docker-compose git
     ```

2. **Clone and Configure**:
   - Clone your repository onto the VM.
   - Navigate to the deployment folder: `cd ntu-dsai-m2-capstone/deployment/gcp`
   - Copy the `.env.docker.example` file to create your local environment:
     ```bash
     cp .env.docker.example .env
     ```
   
3. **Database and GCP Credentials (`.env`)**:
   - Open your `.env` file (`nano .env`).
   - Modify the `DAGSTER_POSTGRES_PASSWORD` to a secure password. *(Note: Because Docker Compose launches both the Postgres DB and Dagster, setting this variable in `.env` automatically configures the database and tells Dagster how to connect to it!)*
   - Place your Google Service Account JSON key somewhere on the VM (e.g., `/home/user/sa-key.json`).
   - Update `GOOGLE_APPLICATION_CREDENTIALS=/home/user/sa-key.json` inside the `.env` file.

4. **Launch the Stack**:
   - From inside the `deployment/gcp` folder, pull the latest image and start the cluster:
     ```bash
     sudo docker-compose pull
     sudo docker-compose up -d
     ```
   - *Note: `docker-compose up -d` runs the daemon in the background. You can check logs using `sudo docker-compose logs -f`.*

5. **Access the UI**:
   - Go to `http://<YOUR_VM_EXTERNAL_IP>:3000`. You may need to create a VPC Firewall Rule in GCP to explicitly allow TCP port `3000`.

*Persistence:* This topology uses Docker volumes for the Postgres database and the `SEC_FORM4_OUTPUT_DIR` to ensure downloads and run history survive container restarts without needing external GCS blob storage.

## Kubernetes (GKE)

Example manifests live under [`k8s/`](k8s/):

1. Create namespace: `kubectl apply -f k8s/namespace.yaml`
2. Create a **Google service account** for workloads (BigQuery, optional GCS) and bind **Workload Identity** to `k8s/serviceaccount.yaml` (replace `PROJECT_ID` and the GSA email).
3. Create Postgres secret from `secret-dagster-postgres.example.yaml` (use real values; do not commit secrets).
4. Replace **`REGION-docker.pkg.dev/PROJECT_ID/REPO/capstone-dagster:latest`** in deployments with your image URI.
5. Apply: `kubectl apply -f k8s/`

Topology:

- **`dagster-user-code`**: gRPC code server (`dagster api grpc` … `-m dataprocessing.dagster_orchestration.repository -a definitions`).
- **`dagster-webserver`**: UI (`dagster-webserver -w /app/deployment/gcp/workspace.yaml`).
- **`dagster-daemon`**: schedules/sensors (`dagster-daemon run`).

[`workspace.yaml`](workspace.yaml) points the UI at the in-cluster service `dagster-user-code:4000`.

## Form 4 staging and PVC

- **Ephemeral** (default in `deployment-user-code`): `SEC_FORM4_OUTPUT_DIR=/tmp/sec_form4_staging` — resume state is lost when the pod restarts.
- **Persistent (recommended for resume)**: apply [`k8s/pvc-form4.yaml`](k8s/pvc-form4.yaml), mount the volume at e.g. `/mnt/form4_staging`, and set `SEC_FORM4_OUTPUT_DIR=/mnt/form4_staging` on the **user-code** (and any job that runs Form 4) container. The asset [`sec_form4_daily_ingestion`](../../dataprocessing/dagster_orchestration/assets/sec_form4_daily_ingestion.py) reads this env var when set.

A full GCS-backed state store would require code changes in `scripts/download_sec_form4_daily.py`; the PVC approach avoids that.

## GitHub Actions

[`.github/workflows/dagster-docker.yml`](../../.github/workflows/dagster-docker.yml) builds the image on each push to `main` (when paths change). Configure repository **Variables** / **Secrets** for Workload Identity Federation to push to Artifact Registry if desired.
