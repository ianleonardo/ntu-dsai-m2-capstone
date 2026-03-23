# Running Insider Alpha Dashboard Locally

This dashboard consists of a **FastAPI backend** and a **Next.js frontend**.

## Prerequisites
- **[uv](https://docs.astral.sh/uv/)** (Python toolchain; installs a managed interpreter and deps—no manual `venv` / `activate`)
- **Python 3.11+** (optional if you rely on `uv python install 3.11`)
- **Node.js 20+** (Next.js frontend only—uv does not replace npm here)
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

### Terminal 1 — Backend (FastAPI, uv)

From the repo root:

```bash
cd visualisation/backend
uv sync          # install locked deps (creates/updates `.venv` here; gitignored)
uv run uvicorn main:app --reload --port 8000
```

Backend API will be at `http://localhost:8000`.

**Why uv instead of `python -m venv`:** You never run `activate`. `uv sync` + `uv run …` use the project lockfile (`uv.lock`) for reproducible installs. The `.venv` under `backend/` is only uv’s managed environment, not a hand-rolled venv workflow.

To pin the interpreter to 3.11 (matches Docker), run once: `uv python install 3.11` (optional; `backend/.python-version` requests 3.11).

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

### Frontend → API (development)
- If **`NEXT_PUBLIC_API_URL` is not set**, the browser calls **`http://127.0.0.1:8000/api/...`** directly (FastAPI’s CORS allows this). That avoids **Next.js dev rewrite proxy timeouts** on slow BigQuery responses (`ECONNRESET` / socket hang up). Start the backend on port **8000**.
- To use the same-origin rewrite instead, set **`NEXT_PUBLIC_API_URL=/insider-api`** (and keep **`next.config.ts` rewrites**; override the target with **`BACKEND_URL`** if the API is not on 127.0.0.1:8000).
- Production / `next start`: set **`NEXT_PUBLIC_API_URL`** to your deployed API base URL including `/api`.

### BigQuery mart (`sp500_insider_transactions`)
The dashboard expects this dbt model as a **native BigQuery table** (not a view): **partition** `TRANS_DATE` by **month** (aligned with API filters; BigQuery allows at most 4000 partitions; daily partitioning exceeds that on long SEC history), **cluster** `symbol_norm`, with **`symbol_norm`** and **`issuer_gics_sector`** denormalized at build time. After changing upstream data or this model, run:

```bash
cd dataprocessing/dbt_insider_transactions
dbt run -m sp500_insider_transactions
```

Until this runs successfully, `/api/transactions` and related queries will error if the table lacks `symbol_norm` / `issuer_gics_sector`.

## Behaviour notes
- **Overview vs Detailed Transactions**: The selected date range is stored in `localStorage` (`insider_dashboard_date_range`) when you click **Apply** on **Detailed Transactions** or **Insider clusters** (or when changing dates on Overview), so pages can stay aligned.
- **Dashboard numbers**: The pipeline is **non-derivative only** (no derivative SEC tables in dbt or Meltano load). Purchase/sale dollars use **`est_acquire_value` / `est_dispose_value`** (shares×price rolled up in dbt on the mart) when fact dollar columns are empty. The FastAPI layer reads **only** `sp500_insider_transactions` (no runtime joins to staging).
- **Caching (API)**: Summary, top transactions, and clusters ~2–3 minutes; **transactions** ~5 minutes, larger in-memory key set (see `CACHE_TRANSACTIONS_*` in backend config). **Cluster breakdown** uses a dedicated cache (~10 minutes, `CACHE_CLUSTER_BREAKDOWN_TTL_SECONDS`). S&P 500 and **split** search directory endpoints ~1 hour. **Insider directory** rows come from **distinct names in `sp500_insider_transactions.reporting_owner_names`** (split on `|`, same substrings as the transaction table), enriched from `dim_sec_reporting_owner` when the name matches; capped at **25k** as a safety limit. On **startup**, search directory + **first page of transactions** (default date window, no filters) are warmed. The legacy **`GET /api/search-directory`** returns a small `use_split_endpoints` hint.
- **Search UI**: One search box with **Markets (stocks)** vs **Insiders** sections; **comma / semicolon / newline** separate multiple terms; chips show company names for tickers. Directory is cached in `sessionStorage` for 24h after the first successful load (bump `SEARCH_DIRECTORY_CACHE_KEY` when the directory semantics change).
- **Detailed Transactions table**: Custom layout (ticker + transaction-size tier + sector chip, insider, role pills, dates + SEC viewer link, type, value/price, shares vs held). **GICS sector** is on the mart as **`issuer_gics_sector`**. **Ret / Curr.** uses **`sp500_stock_daily`** closes via **`/api/search-directory/stocks`**. **Pagination** (50 rows per page): the **`search`** query string is split into tokens — **short symbol-shaped tokens** (≤5 chars, or with a dot e.g. `BRK.B`) become **`symbol_norm IN (...)`**; other tokens use **LIKE** on symbol, issuer, and **`reporting_owner_names`**; tokens are combined with **OR**. Single-token queries keep the previous symbol-vs-text behavior. Filtered responses use **`has_more`** without exact **`total`** when needed. The API uses a **fixed column list** (not `SELECT *`).
- **Market Activity Overview**: Summary + top buys/sells for the **current start/end** are stored in `sessionStorage` (`market_activity_overview_v1`) so revisiting Overview with the **same period** skips API calls until you change the date range (server still caches ~2m per period).
- **Clusters**: Weekly buckets, same ticker, **≥2 distinct reporting-owner CIKs** (via **`dim_sec_reporting_owner`**) and **`min_filings`**; defaults to **Cluster Buys**. **Search + date range + Apply** match Detailed Transactions. **Signal** shows that **distinct CIK count** plus **CEO / CFO** chips — see [`docs/insider_clusters.md`](docs/insider_clusters.md). **Analyze** loads **`GET /api/clusters/breakdown`** (single-pass mart + owner join; longer-lived cache). Dates in the table use **DD MMM yyyy**. Cluster UI choices persist in **`insider_clusters_ui_v1`**. **Price / cost / return** uses `implied_price_per_share` and **`last_close`**. **Price &lt; Cost** filters client-side.
