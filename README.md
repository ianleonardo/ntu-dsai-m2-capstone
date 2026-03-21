# NTU DSAI Capstone Project (Module 2: Data Engineering)
## Stock Analytics Data Pipeline

### Project Overview
This project is a robust Stock Analytics Data Pipeline designed to ingest, process, and store financial market data and SEC insider trading information. Built as part of the NTU DSAI Module 2 Capstone, it automates the end-to-end flow from raw data extraction to analytics-ready models in Google BigQuery.

### Data Processing Flow

The pipeline follows a modern ELT (Extract, Load, Transform) architecture, orchestrated by Dagster:

1.  **Ingestion (Extract & Load)**: We use **Meltano** to extract raw data from local files (CSV/JSONL) and load it into Google BigQuery staging tables.
2.  **Transformation (Transform)**: **dbt (data build tool)** handles the data cleaning, deduplication, and modeling within BigQuery, transforming raw staging data into structured dimension and fact tables (Marts).
3.  **Orchestration**: **Dagster** coordinates the entire process, managing dependencies between Meltano ingestion and dbt transformations, and providing a monitoring UI.

### Data Sources
1.  **SEC Form 4 Insider Trading Data**: Quarterly TSV datasets containing insider submissions, transactions, and signatures.
2.  **S&P 500 Market Data**: Comprehensive ticker and company profile information for S&P 500 constituents.

### Project Structure
```text
ntu-dsai-m2-capstone/
├── docs/                  # Project documentation and detailed guidelines
├── dataprocessing/
│   ├── meltano_ingestion/     # Ingestion logic (Meltano)
│   ├── dbt_insider_transactions/ # Transformation logic (dbt)
│   └── dagster_orchestration/   # Orchestration logic (Dagster)
├── data/                  # Local storage for raw data files
├── notebooks/             # Data exploration notebooks
├── scripts/               # Helper scripts for data management
└── README.md              # Project entry point
```

### Detailed Guidelines

For detailed instructions on setup and operation, refer to the following documents:

1.  **[Pre-setup Guideline](docs/setup.md)**: Environment setup, `uv` installation, GCP/BigQuery configuration, and environment variables.
2.  **[Ingestion Guideline](docs/ingestion.md)**: How to run data ingestion jobs using Meltano and provided helper scripts.
3.  **[dbt Transformation Guideline](docs/dbt.md)**: Running data transformations, understanding the model structure, and overview of data quality tests.
4.  **[Orchestration Guideline](docs/orchestration.md)**: Orchestrating the entire pipeline with Dagster (CLI and UI).

### Setup Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Configure environment
cp .env.example .env
# Edit .env with your GCP credentials path

# 3. Launch orchestration UI
uv run dagster dev
```

### Future Work
- Expand data sources to include real-time market data.
- Develop a centralized visualization dashboard for insider trading signals.
- Integrate AI/LLM tools for automated narrative generation on market trends.