# Pre-setup Guideline

This document outlines the necessary steps to set up your environment before running the data pipeline.

## 1. Install `uv`

We use `uv` for fast and reliable Python package management. If you haven't installed it yet, run:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

For more installation options, see the [uv documentation](https://github.com/astral-sh/uv).

## 2. Install Dependencies

Once `uv` is installed, sync the project dependencies from the root directory:

```bash
uv sync
```

This will create a virtual environment (`.venv`) and install all required packages as defined in `pyproject.toml`.

## 3. Google Cloud Platform (GCP) Setup

The pipeline loads data into Google BigQuery. Ensure you have the following:

- **GCP Project**: A project with the BigQuery API enabled.
- **BigQuery Dataset**: Create a dataset (e.g., `insider_transactions`) in your preferred location (e.g., `asia-southeast1`).
- **Service Account**: Create a service account with `BigQuery Admin` and `Storage Admin` roles.
- **Credentials Key**: Download the JSON key file for your service account and store it securely.

## 4. Environment Variables

Create a `.env` file in the root directory by copying `.env.example`:

```bash
cp .env.example .env
```

Fill in the following variables:

- `TARGET_BIGQUERY_CREDENTIALS_PATH`: Absolute path to your GCP service account JSON key.
- `GOOGLE_APPLICATION_CREDENTIALS`: Same as above (used by some Google libraries).

## 5. Verify Installation

You can verify that the core tools are available by running:

```bash
uv run meltano --version
uv run dbt --version
uv run dagster --version
```
