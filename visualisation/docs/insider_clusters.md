# Insider clusters (API + UI)

## What is a cluster?

Rows group **S&P insider transactions** from `sp500_insider_transactions` by:

- **Ticker** (`ISSUERTRADINGSYMBOL` in the query result)
- **Calendar week** (ISO week, Monday start) of **transaction date** (`TRANS_DATE`)
- **Side**: **buy** uses acquire value / shares; **sell** uses dispose value / shares

A row is returned only if:

1. There are at least **`min_filings`** distinct filings (`ACCESSION_NUMBER`) in that bucket, and  
2. There are at least **two distinct reporting owners** by **`RPTOWNERCIK`** on those filings, joined via **`dim_sec_reporting_owner`** (same table as breakdown).  
   So **one insider filing multiple times in the week does not form a cluster** by themselves.

Results are ordered by **cluster dollar value** on the server; the UI can re-sort.

## Signal column (insiders + CEO / CFO)

- **Insider count** — `insider_count` in the API equals **`COUNT(DISTINCT RPTOWNERCIK)`** for owners linked to filings in that cluster (non-null CIK only).
- **CEO / CFO** — UI chips when aggregated **titles** (`reporting_owner_titles`) and **roles** (`reporting_owner_role_types`) match simple patterns (CEO / Chief Executive; CFO / Chief Financial).

## Cluster breakdown (`GET /api/clusters/breakdown`)

Owner-level rows for one cluster: **ticker** + **week_start** + **side** + **start_date / end_date**. Uses a **single** mart slice CTE (accession, date, amount) joined to **`dim_sec_reporting_owner`** — no second full mart scan. Responses are cached separately (**~10 minutes** by default, see `CACHE_CLUSTER_BREAKDOWN_TTL_SECONDS`) so repeat **Analyze** is fast.

Each row is an insider on a filing in that cluster, with **filing-level** dollar amount for the chosen side (repeated if several insiders are on the same Form 4).

## Search filter

Same as **Detailed Transactions**: one **`search`** string split on commas, semicolons, and newlines. Each token is either a **symbol** (strict rules when multiple tokens: length ≤5 or contains `.`, e.g. `BRK.B`) → **`symbol_norm IN (...)`**, or **free text** → `LIKE` on symbol, issuer, and reporting owner names. Matching uses **OR** across tokens.

## Price / cost / return

- **Cost** — implied $/share: cluster side dollar sum ÷ summed side-specific non-derivative shares (`implied_price_per_share` from the API).
- **Curr.** — latest close from the cached S&P directory (same source as Detailed Transactions).
- **Return** — \((\text{curr} - \text{cost}) / \text{cost}\) when both are valid.

## Dates in the UI

Main table and breakdown use **`DD MMM yyyy`** (e.g. `22 Mar 2026`) via `formatIsoDateLabel` on the calendar date part of each ISO timestamp.

## UI persistence

Cluster page choices (buy/sell, cluster size, Price &lt; Cost, search inputs, sort) are stored under **`insider_clusters_ui_v1`** in `localStorage`, separate from the shared date range (`insider_dashboard_date_range`).
