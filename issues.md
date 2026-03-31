# Data Pipeline Review

This document summarizes key strengths, weaknesses, alternatives, and design tradeoffs in the current data pipeline.

## Overall View

The pipeline is strong for a capstone or early production-style project:

- The stack choices are sensible: Dagster for orchestration, dbt for modeling, BigQuery for warehousing, and FastAPI for serving.
- The final serving mart is practical and optimized for dashboard usage.
- The repository shows clear thinking around modularity, orchestration, and business-friendly modeling.

The main weakness is not that the architecture is fundamentally wrong. It is that the system currently mixes multiple ingestion styles and a few partially-complete operational patterns, which increases maintenance burden and makes the end-to-end contract less consistent than it could be.

## Strengths

### 1. Practical tool choices

Pros:

- Dagster gives clear orchestration boundaries and a UI for running jobs.
- dbt provides a good structure for transformations, tests, and lineage.
- BigQuery is a strong fit for analytics-heavy workloads and dashboard serving.
- FastAPI reads from already-curated marts instead of rebuilding logic on every request.

Why this is good:

- The stack is modern, understandable, and appropriate for analytical backend work.
- Each layer has a reasonably clear responsibility.

### 2. Good serving-model design

Pros:

- `sp500_insider_transactions` is shaped for the frontend use case instead of leaving all complexity in the API.
- Important fields like `symbol_norm` and `issuer_gics_sector` are denormalized into the serving mart.
- The backend mostly queries a small set of well-defined tables.

Why this is good:

- Precomputing business logic in dbt reduces repeated joins and query complexity in the API.
- This improves dashboard responsiveness and keeps the backend simpler.

### 3. Clear business-oriented modeling

Pros:

- The dbt layer converts raw SEC data into filing-level and owner-level concepts that are easy to reason about.
- Role classification and transaction labeling are useful business abstractions.
- The project does not expose raw SEC structures directly to the UI.

Why this is good:

- The warehouse is designed for analysis, not just storage.
- This makes the pipeline easier for downstream consumers to use.

## Main Criticisms

### 1. The ingestion architecture is fragmented

Current state:

- SEC data is loaded directly from SEC to BigQuery through Python and Dagster.
- Stock-price and some reference data are loaded through JSONL + Meltano.
- Some reference data also still carries older GCS-related handling.

Cons:

- There are multiple ingestion paradigms in one repo.
- Operational behavior differs between sources.
- Idempotency, retries, and recovery are handled differently across pipelines.
- New contributors have to understand more patterns than necessary.

Why this is a problem:

- In data engineering, consistency often matters more than local optimization.
- Even if each path works individually, mixed patterns make failures harder to debug and systems harder to evolve.

Alternative:

- Standardize on one ingestion style:
- Option A: Dagster + direct Python/BigQuery loaders for all sources.
- Option B: Meltano or another EL framework for all raw loads.

Rationale:

- A single ingestion approach reduces cognitive load, testing complexity, and operational drift.

### 2. The pipeline is not fully orchestrated end-to-end

Current state:

- The Dagster repository includes SEC direct ingestion, dbt transformation, and the S&P 500 stock daily pipeline.
- `sp500_companies` appears to be maintained separately through manual or wrapper-script execution rather than as a first-class orchestrated Dagster asset.

Cons:

- The final mart depends on data that is not governed by the same orchestration layer.
- Freshness of `sp500_companies` is implied rather than enforced.
- A successful dbt run can still be built on stale reference data.

Why this is a problem:

- If a dataset affects the correctness of the final mart, it should be visible in orchestration and freshness checks.
- Hidden prerequisites are a common source of “the pipeline succeeded, but the data is wrong” problems.

Alternative:

- Make `sp500_companies` a formal Dagster asset with freshness monitoring and dependency wiring into dbt execution.

Rationale:

- This makes the operational graph match the real data dependency graph.

### 3. The SEC load path relies on post-load deduplication

Current state:

- SEC data is streamed into BigQuery.
- After load, the same tables are deduped using a `WRITE_TRUNCATE` query based on natural keys.

Pros:

- The approach is simple to understand.
- It is easy to rerun without building a more complex merge framework.
- It is adequate for a capstone-scale pipeline.

Cons:

- Idempotency is not guaranteed at write time.
- Correctness depends on a second cleanup step succeeding.
- Reprocessing large tables can be more expensive than load-time upserts.
- Failures between insert and dedupe can leave temporary duplicate states.

Why this is a problem:

- Post-load cleanup works, but it is less robust than designing the raw landing process to be idempotent by construction.

Alternative:

- Stage into temporary tables and use `MERGE`.
- Or use BigQuery load jobs or another write path with stronger upsert semantics.

Rationale:

- Stronger write semantics reduce cleanup work and improve operational safety.

### 4. The source-of-truth for the S&P 500 universe is duplicated

Current state:

- The S&P 500 constituents are downloaded for the company dimension.
- The stock-price ingestion path also downloads the S&P 500 list independently.

Cons:

- The same universe definition exists in more than one place.
- If those runs happen at different times, the price universe and the company dimension can drift.
- This creates avoidable inconsistency in the data warehouse.

Why this is a problem:

- Security-master or universe definitions should ideally be centralized.
- Re-downloading them independently makes the pipeline less reproducible.

Alternative:

- Create one authoritative reference asset for S&P 500 constituents.
- Make downstream price ingestion read from that authoritative dataset instead of calling the external source again.

Rationale:

- One source of truth improves consistency and simplifies auditing.

### 5. Data quality checks are present but shallow in the highest-value layers

Current state:

- Source and model tests exist in dbt.
- Basic `not_null` and some uniqueness checks are in place.
- The final serving mart has documentation but relatively limited business-rule testing.

Pros:

- There is already a testing culture in the project.
- The dbt test step is integrated into Dagster.

Cons:

- There are limited reconciliation tests for the final serving tables.
- There is little evidence of explicit freshness tests on source data.
- Match quality between SEC issuers and S&P 500 companies is not strongly enforced.

Why this is a problem:

- The most important tables should have the strictest contracts.
- Basic schema tests alone do not guarantee that business logic stayed correct.

Alternative:

- Add stronger mart-level tests:
- row-count reconciliation between raw and curated layers
- uniqueness and non-null expectations where the grain is known
- issuer match-rate thresholds
- freshness checks for reference and market data

Rationale:

- Good data quality is not just “no nulls”; it is confidence that the warehouse still represents the business correctly.

### 6. The fact model is optimized for serving but loses analytical detail

Current state:

- `fct_insider_transactions` is modeled at filing grain, not raw line-item grain.
- Owner names, role types, and titles are aggregated into strings.

Pros:

- The model is very convenient for API and dashboard usage.
- It reduces runtime joins and simplifies client development.

Cons:

- Some filing-level aggregation is lossy.
- Multi-line and multi-owner nuance is harder to analyze later.
- Aggregated strings are good for display but not ideal as long-term analytical structures.

Why this is a problem:

- A serving-friendly model is not always an analytics-friendly canonical model.

Alternative:

- Keep both:
- a curated line-level fact for deep analysis
- a denormalized serving mart for the frontend

Rationale:

- Preserving lower-grain detail gives future flexibility without losing current performance benefits.

### 7. The API layer still contains significant SQL logic

Current state:

- The backend queries BigQuery directly and embeds filtering logic in handwritten SQL.
- Caching helps reduce repeated load.

Pros:

- This keeps the service small and straightforward.
- It avoids building an unnecessary middle abstraction too early.

Cons:

- Business logic is split between dbt and application SQL.
- BigQuery-specific query strings are embedded in the backend.
- The API becomes tightly coupled to the warehouse implementation.

Why this is a problem:

- Over time, logic split across dbt and app code is harder to test and govern.
- It also increases the cost of changing warehouse schema or moving platforms.

Alternative:

- Push more reusable logic into stable marts or parameterized views.
- Keep the API focused on parameter handling, validation, and response formatting.

Rationale:

- The cleaner the contract between warehouse and API, the easier the system is to maintain.

### 8. Too many pieces are optimized locally instead of governed globally

Current state:

- Individual parts of the system are thoughtfully implemented.
- But several important assumptions remain implicit:
- reference data freshness
- schedule config behavior
- ingestion consistency
- grain and contract boundaries

Cons:

- The architecture feels slightly transitional rather than fully settled.
- The system likely works well when operated by the current team, but may become harder to maintain as ownership expands.

Why this is a problem:

- Pipelines often fail not because a component is bad, but because the contracts between components are under-specified.

Alternative:

- Treat the repo more explicitly as a governed data platform:
- define authoritative assets
- define freshness contracts
- define ownership of raw, curated, and serving layers
- reduce overlapping ingestion approaches

Rationale:

- Strong contracts scale better than clever implementation details.

## Summary of Tradeoffs

### What the current design does well

- Fast iteration
- Clear capstone/demo value
- Good dashboard-serving performance
- Manageable codebase size
- Practical use of modern data tools

### What it sacrifices

- Consistency of ingestion patterns
- Strong end-to-end operational governance
- Fully centralized source-of-truth handling
- Maximum analytical flexibility in curated facts
- Stronger idempotency and contract enforcement

## Recommended Priorities

If this pipeline were to be hardened further, the most valuable improvements would be:

1. Standardize ingestion patterns across all sources.
2. Promote `sp500_companies` into the same orchestrated dependency graph as the other critical datasets.
3. Replace SEC post-load dedupe with a more strongly idempotent load strategy.
4. Add stronger dbt tests at the mart and freshness level.
5. Preserve a lower-grain curated fact alongside the current serving-focused mart.

## Final Assessment

This is a good pipeline with real strengths. The main criticisms are about architectural coherence and operational maturity, not about basic competency. The project already demonstrates a strong understanding of modern data engineering concepts. The next step is less about adding more tools and more about reducing overlap, formalizing contracts, and making the system more internally consistent.
