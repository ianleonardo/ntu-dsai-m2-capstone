# Pre-setup Guideline

This document outlines the necessary steps to set up your environment before running the data pipeline and dashboard UI.

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

This will create a virtual environment (`.venv`) and install all required packages as defined in `pyproject.toml`, including:
- Dagster (orchestration)
- Meltano (ELT)
- dbt (transformation)
- FastAPI (dashboard backend)
- BigQuery client libraries

## 3. Node.js and Frontend Dependencies

For the dashboard UI, you'll need Node.js 18+:

```bash
# Verify Node.js installation
node --version  # Should be 18+

# Install frontend dependencies (from visualisation/frontend/)
cd visualisation/frontend
npm install
cd ../..
```

## 4. Google Cloud Platform (GCP) Setup

The pipeline loads data into Google BigQuery and the dashboard queries from it. Ensure you have the following:

### 4.1 GCP Project Configuration
- **GCP Project**: A project with the BigQuery API enabled.
- **BigQuery Dataset**: Create a dataset (e.g., `insider_transactions`) in your preferred location (e.g., `asia-southeast1`).
- **Service Account**: Create a service account with `BigQuery Admin` and `Storage Admin` roles.
- **Credentials Key**: Download the JSON key file for your service account and store it securely.

### 4.2 BigQuery Table Setup

The pipeline will automatically create the necessary tables, but you can pre-create them for better control:

```sql
-- Create dataset
CREATE SCHEMA `insider_transactions`;

-- Example table creation (pipeline handles this automatically)
CREATE TABLE `insider_transactions.sec_submission` (
  ACCESSION_NUMBER STRING,
  FILING_DATE DATE,
  PERIOD_OF_REPORT DATE,
  -- Other columns will be added automatically
);
```

## 5. Environment Variables

Create a `.env` file in the root directory by copying `.env.example`:

```bash
cp .env.example .env
```

Fill in the following variables:

```bash
# BigQuery Configuration
TARGET_BIGQUERY_CREDENTIALS_PATH=/path/to/your/service-account-key.json
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json

# GCP Project Settings
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=insider_transactions

# Dashboard Configuration (optional)
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
```

### 5.1 Authentication Setup

Choose one of the following authentication methods:

**Option 1: Service Account Key (Recommended for development)**
```bash
# Set the environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/key.json"

# Test the connection
gcloud auth activate-service-account --key-file="/path/to/your/key.json"
```

**Option 2: User Account Credentials**
```bash
# Login with your Google account
gcloud auth application-default login

# Test the connection
gcloud auth list
```

## 6. Verify Installation

You can verify that the core tools are available by running:

```bash
# Check data engineering tools
uv run meltano --version
uv run dbt --version
uv run dagster --version

# Check BigQuery connection
uv run python -c "
from google.cloud import bigquery
client = bigquery.Client()
print('Connected to BigQuery project:', client.project)
"

# Check frontend tools
cd visualisation/frontend
npm --version
node --version
```

## 7. Optional Development Tools

### 7.1 Docker (for containerization)
```bash
# Install Docker for containerized deployment
# Visit: https://docs.docker.com/get-docker/
```

### 7.2 Git Hooks (for code quality)
```bash
# Install pre-commit hooks (if configured)
pip install pre-commit
pre-commit install
```

## 8. Project Structure Verification

Ensure your project structure looks correct:

```bash
# Verify key directories exist
ls -la dataprocessing/
ls -la visualisation/
ls -la scripts/
ls -la docs/

# Check configuration files
ls -la .env* pyproject.toml
```

## 9. Initial Data Setup (Optional)

To test the pipeline with sample data:

```bash
# Download sample SEC data (small dataset)
python scripts/download_sec.py
# Enter year: 2024
# This will download Q1 2024 data for testing

# Or use existing sample data if provided
ls -la data/sec/
```

## 10. Next Steps

After completing the setup:

1. **Test Data Ingestion**: Follow the [Ingestion Guideline](ingestion.md)
2. **Run Transformations**: Follow the [dbt Transformation Guideline](dbt.md)
3. **Start Orchestration**: Follow the [Orchestration Guideline](orchestration.md)
4. **Launch Dashboard**: Follow the [Dashboard Setup Guide](dashboard_setup.md)

## Troubleshooting

### Common Issues

**uv Installation Issues**:
```bash
# If curl fails, try alternative installation
pip install uv
```

**BigQuery Authentication Issues**:
```bash
# Verify credentials path
ls -la "$GOOGLE_APPLICATION_CREDENTIALS"

# Test connection manually
bq ls
```

**Node.js Version Issues**:
```bash
# Install correct Node.js version using nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
nvm install 18
nvm use 18
```

**Permission Issues**:
```bash
# Make scripts executable
chmod +x scripts/*.py
chmod +x visualisation/frontend/package.json
```

### Environment-Specific Setup

**Windows Users**:
```bash
# Use Windows Subsystem for Linux (WSL) for best compatibility
# Or use Git Bash for Unix-like commands
```

**macOS Users**:
```bash
# Install Xcode command line tools if needed
xcode-select --install
```

**Linux Users**:
```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install python3-dev build-essential  # Ubuntu/Debian
```

## Performance Considerations

For optimal performance:

1. **BigQuery Location**: Choose a region close to your users
2. **Network**: Ensure stable internet connection for data downloads
3. **Storage**: Ensure sufficient disk space for raw data (~10GB for full year)
4. **Memory**: 8GB+ RAM recommended for transformation jobs

## Security Notes

- Never commit service account keys to version control
- Use environment variables for sensitive configuration
- Regularly rotate service account keys
- Implement least-privilege access for service accounts
