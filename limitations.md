# Pipeline Limitations


## Limitation 1
Data validation is currently basic, with simple checks like check nulls. Future iterations will improve on this and build out a stronger validation in the pipeline to ensure better data quality.

## Limitation 2
S&P 500 data in using just a current view and does not include historical S&P 500 information. Future iterations will include this data for a more well rounded analysis, and also include options to load in data from other index funds.

## Limitation 3
We hve slight variations on how data is ingested, this to done intentionally due to data scource characteristics. Future iterations will further improve on this workflow to better standardise the ingestion patterns for consistency, and to make it easier for debugging.

---

## Overview

The pipeline is a well-structured ELT architecture that works as a batch analytics system. The stack choices are sound, the warehouse model is clean, and dbt testing is integrated into orchestration. The main gaps are around operational completeness, data freshness accuracy, and consistency across ingestion paths.

## Known Limitations

### 1. Insider data freshness is quarterly, not daily

SEC ingestion is built around quarterly ZIP downloads. All schedules run quarterly — the "daily refresh" claim applies to stock prices only. Insider transaction data is structurally 3 months behind.

### 2. Schedules are defined but not fully wired

`quarterly_sec_schedule_context()` and similar run-config functions are written but never attached to their `ScheduleDefinition` objects. Without explicit config, scheduled runs won't know which year/quarter to load and may fail silently.

### 3. Business logic is computed during ingestion

`fetch_sp500_stock_daily_yfinance_to_jsonl.py` computes SMA200, MACD, and trade signals before data lands in BigQuery. This violates the ELT pattern — changing any indicator requires a full re-ingestion rather than just a dbt rebuild.

### 4. Transaction date parsing is inconsistent

The `parse_sec_date` macro handles complex SEC date formats but is only applied to filing-level dates in `dim_sec_submission`. `fct_sec_nonderiv_line` uses plain `SAFE_CAST` for `TRANS_DATE`, and `fct_insider_transactions` silently falls back to report date or filing date if the cast fails. Transaction timing can be silently wrong.

### 5. S&P 500 universe is a current snapshot, not historical

The pipeline downloads today's S&P 500 constituent list and applies it to all historical filings. Historical analysis of "S&P 500 insiders" reflects today's index membership, not membership at the time of each filing.

### 6. Market data has no dbt source definition or quality testing

`sp500_stock_daily` is the highest-frequency dataset but has no entry in `_sources.yml`. The SEC path runs through ingestion → dbt run → dbt test; the stock path stops at Meltano load with no contract enforcement or freshness checks.

### 7. Two ingestion patterns with inconsistent guarantees

SEC uses Python → BigQuery direct with post-load deduplication. Market data uses Python → JSONL → Meltano → BigQuery with upsert semantics. These behave differently on reruns, failures, and retries, increasing maintenance cost and cognitive load.

### 8. S&P 500 constituent list is fetched independently in two places

`download_sync_sp500_companies.py` and `fetch_sp500_stock_daily_yfinance_to_jsonl.py` each download the constituent list independently. If they run at different times, the company dimension and price universe can drift out of sync.
