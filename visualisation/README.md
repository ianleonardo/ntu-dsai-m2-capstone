# Running Insider Alpha Dashboard Locally

This dashboard consists of a **FastAPI backend** and a **Next.js frontend**.

## Prerequisites
- **Python 3.11+**
- **Node.js 20+**
- **Google Cloud Authentication**: Ensure you have access to BigQuery. Run `gcloud auth application-default login` if you haven't already.

---

## Method 1: Running with Docker (Recommended)

The easiest way to run the full stack is using the provided Dockerfile.

```bash
# From the project root
cd visualisation

# Build the image
docker build -t insider-alpha-dashboard .

# Run the container
# Note: You may need to mount your Google credentials if not already in the environment
docker run -p 3000:3000 -p 8000:8000 --env-file ../.env insider-alpha-dashboard
```
The dashboard will be available at `http://localhost:3000`.

---

## Method 2: Manual Run (Development)

Run both services in **two separate terminal windows**.

### Terminal 1 — Backend (FastAPI)

```bash
cd visualisation
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```
Backend API will be at `http://localhost:8000`.

> **Note:** The virtual environment (`venv`) must be activated **before** running uvicorn. If you see `zsh: command not found: uvicorn`, it means the venv is not active.

### Terminal 2 — Frontend (Next.js)

```bash
cd visualisation/frontend
npm install
npm run dev
```
Frontend will be at `http://localhost:3000`.

---

## Configuration
Ensure your `.env` file in the project root is correctly configured:
- `GOOGLE_PROJECT_ID`: Your GCP project ID.
- `BIGQUERY_DATASET`: The dataset name (e.g., `insider_transactions`).
