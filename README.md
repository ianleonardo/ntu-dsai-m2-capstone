# NTU DSAI Capstone Project (Module 2: Data Engineering)
## Stock Analytics Data Pipeline

### Project Overview
This project is a bespoke Stock Price Monitor designed to ingest, process, and store financial market data and insider trading information. Built as part of the NTU DSAI Module 2 Capstone, it automates the extraction of stock data and SEC insider transactions, loading them into Google BigQuery for downstream technical analysis and visualization.

### Data Sources
1. **Finnhub Stock API**: Real-time stock data, company fundamentals, and historical OHLC data.
2. **Alpha Vantage API**: Supplementary market data and foreign exchange information.
3. **SEC Form 4 Insider Trading Data**: Quarterly TSV datasets containing insider submissions, transactions, and signatures.

### Project Structure
```text
ntu-dsai-m2-capstone/
├── data/                  # Local storage for extracted raw data (Finnhub, SEC)
├── docs/                  # Project documentation and ingestion guides
├── meltano-ingestion/            # Meltano: SEC insider forms + company tickers → BigQuery
├── notebooks/             # Jupyter notebooks for data exploration and initial extraction
├── scripts/               # Python scripts for automated data downloads
└── README.md              # This project overview
```

### Architecture & Pipeline (Current Stage)
The project currently implements the **Extract** and **Load** phases of the ELT pipeline:
- **Data Extraction**: Custom Python scripts (`scripts/download_sec.py`) and Jupyter notebooks (`notebooks/finnhub_downloader.ipynb`) handle downloading data from REST APIs and ZIP archives.
- **Data Loading (Meltano)**: `meltano-ingestion` loads SEC insider TSVs from GCS and SEC company tickers JSON into BigQuery via `tap-csv` and `target-bigquery`.

### Setup Instructions

#### 1. Prerequisites
- Python 3.10+
- [Meltano](https://docs.meltano.com/)
- Google Cloud Platform (GCP) account with BigQuery enabled.

#### 2. Environment Variables
Create a `.env` file in the root directory and add the necessary API keys and credentials:
```env
FINNHUB_API_KEY=your_finnhub_key
VANTAGE_API_KEY=your_alpha_vantage_key
TARGET_BIGQUERY_CREDENTIALS_PATH=/absolute/path/to/gcp-service-account.json
```

#### 3. Running the SEC Ingestion Pipeline
To ingest the SEC Insider data into BigQuery:
```bash
# 1. Download and extract the SEC TSV data files
python scripts/download_sec.py

# 2. Navigate to the Meltano project
cd meltano-ingestion

# 3. Install Meltano plugins
meltano install

# 4. Run the pipeline to BigQuery
meltano run tap-csv target-bigquery
```
*(For a more detailed local testing guide using JSONL, refer to `docs/meltano_sec_ingestion_guide.md`).*

### Next Steps / Future Work
- Implement data transformations (dbt) within BigQuery to create star-schema data marts.
- Develop a web application to visualize the stock analytics and insider trading signals.
- Automate technical analysis (e.g., Simple Moving Averages, Support & Resistance levels) utilizing AI/LLM tools.