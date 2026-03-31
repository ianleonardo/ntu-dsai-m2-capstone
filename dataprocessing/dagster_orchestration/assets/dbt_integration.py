"""
Dagster assets for dbt integration with SEC insider transactions transformation.
"""

import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_dbt import DbtCliResource
from dagster_dbt.core.dbt_cli_invocation import DbtCliInvocation

# Resolve repo root and dbt project directory
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = REPO_ROOT / "dataprocessing" / "dbt_insider_transactions"


def _short_test_id(unique_id: str) -> str:
    if unique_id.startswith("test."):
        return unique_id[5:]
    return unique_id


def _summarize_dbt_test_run_results(
    run_results: Mapping[str, Any],
) -> tuple[dict[str, MetadataValue], str]:
    """Build Dagster metadata + markdown report from dbt run_results.json after `dbt test`."""
    rows = run_results.get("results") or []
    tests = [r for r in rows if str(r.get("unique_id", "")).startswith("test.")]
    elapsed = run_results.get("elapsed_time")
    gen_at = (run_results.get("metadata") or {}).get("generated_at")

    status_counts: Counter[str] = Counter()
    for t in tests:
        status_counts[str(t.get("status") or "unknown").lower()] += 1

    # dbt versions may report passing tests as "pass" or "success"
    passed = status_counts.get("pass", 0) + status_counts.get("success", 0)
    failed = status_counts.get("fail", 0) + status_counts.get("error", 0)
    skipped = status_counts.get("skipped", 0) + status_counts.get("skip", 0)
    warned = status_counts.get("warn", 0)

    not_ok = [t for t in tests if str(t.get("status", "")).lower() in ("fail", "error", "warn")]

    lines = [
        "### dbt test results",
        "",
        f"- **Total tests:** {len(tests)}",
        f"- **Passed:** {passed}",
        f"- **Failed / error:** {failed}",
        f"- **Warn:** {warned}",
        f"- **Skipped:** {skipped}",
    ]
    if elapsed is not None:
        lines.append(f"- **Wall time (dbt):** {float(elapsed):.2f}s")
    if gen_at:
        lines.append(f"- **Artifact generated_at:** {gen_at}")
    if status_counts:
        lines.extend(["", "**Raw status counts:**", ""])
        for st, c in sorted(status_counts.items()):
            lines.append(f"- `{st}`: {c}")

    if not_ok:
        lines.extend(["", "### Non-passing tests", ""])
        for t in not_ok[:50]:
            uid = _short_test_id(str(t.get("unique_id", "")))
            st = t.get("status")
            msg = t.get("message") or ""
            n_fail = t.get("failures")
            fail_part = f", failures={n_fail}" if n_fail is not None else ""
            lines.append(f"- **{uid}** — `{st}`{fail_part}")
            if msg:
                lines.append(f"  - {msg[:500]}{'…' if len(str(msg)) > 500 else ''}")
        if len(not_ok) > 50:
            lines.append(f"- … and {len(not_ok) - 50} more")

    md = "\n".join(lines)

    meta: dict[str, MetadataValue] = {
        "dbt_test_total": MetadataValue.int(len(tests)),
        "dbt_test_passed": MetadataValue.int(passed),
        "dbt_test_failed": MetadataValue.int(failed),
        "dbt_test_skipped": MetadataValue.int(skipped),
        "dbt_test_warn": MetadataValue.int(warned),
        "dbt_test_report": MetadataValue.md(md),
    }
    if elapsed is not None:
        meta["dbt_test_elapsed_seconds"] = MetadataValue.float(round(float(elapsed), 4))

    return meta, md


def _test_report_from_invocation(inv: DbtCliInvocation) -> tuple[dict[str, MetadataValue], str]:
    try:
        rr = inv.get_artifact("run_results.json")
    except Exception as e:
        fallback_md = f"### dbt test results\n\nCould not read `run_results.json`: `{e}`"
        return {
            "dbt_test_total": MetadataValue.int(0),
            "dbt_test_report": MetadataValue.md(fallback_md),
        }, fallback_md
    return _summarize_dbt_test_run_results(rr)


@asset(
    group_name="dbt_transformation",
    description="Runs dbt run + dbt test for SEC insider transactions",
    deps=["sec_direct_ingestion"],
)
def dbt_insider_transformation(context: AssetExecutionContext) -> MaterializeResult:
    """
    Runs `dbt run` then `dbt test` for the insider_transactions dbt project.
    Depends on BigQuery raw tables (e.g. from SEC direct ingestion).
    """
    dbt_cli = DbtCliResource(
        project_dir=os.fspath(DBT_PROJECT_DIR),
        profiles_dir=os.fspath(DBT_PROJECT_DIR),
    )

    # Do not pass context= into dbt.cli() here: that path is for @dbt_assets and requires
    # dbt manifest metadata on this asset (raises DagsterInvariantViolationError otherwise).
    run_inv = dbt_cli.cli(["run"]).wait()
    if not run_inv.is_successful():
        err = run_inv.get_error()
        context.log.error(f"dbt run failed: {err}")
        raise RuntimeError(f"dbt run failed: {err}")
    context.log.info("dbt run completed successfully")

    test_inv = dbt_cli.cli(["test"], raise_on_error=False).wait()
    test_meta, test_md = _test_report_from_invocation(test_inv)
    context.log.info(test_md)

    if not test_inv.is_successful():
        err = test_inv.get_error()
        context.log.error(f"dbt test failed: {err}")
        raise RuntimeError(f"dbt test failed: {err}")

    return MaterializeResult(metadata=test_meta)


@asset(
    group_name="dbt_transformation",
    description="Materialize sp500_insider_transactions (and dependencies) after Form4 monthly ingestion.",
    deps=["sec_form4_monthly_ingestion"],
)
def dbt_sp500_insider_transactions_form4(context: AssetExecutionContext) -> MaterializeResult:
    """
    Runs dbt for sp500_insider_transactions only, scoped for the Form4 monthly pipeline.
    """
    dbt_cli = DbtCliResource(
        project_dir=os.fspath(DBT_PROJECT_DIR),
        profiles_dir=os.fspath(DBT_PROJECT_DIR),
    )

    select_expr = "sp500_insider_transactions+"
    run_inv = dbt_cli.cli(["run", "--select", select_expr]).wait()
    if not run_inv.is_successful():
        err = run_inv.get_error()
        context.log.error(f"dbt run ({select_expr}) failed: {err}")
        raise RuntimeError(f"dbt run ({select_expr}) failed: {err}")
    context.log.info(f"dbt run completed for selection: {select_expr}")

    test_inv = dbt_cli.cli(["test", "--select", select_expr], raise_on_error=False).wait()
    test_meta, test_md = _test_report_from_invocation(test_inv)
    context.log.info(test_md)

    if not test_inv.is_successful():
        err = test_inv.get_error()
        context.log.error(f"dbt test ({select_expr}) failed: {err}")
        raise RuntimeError(f"dbt test ({select_expr}) failed: {err}")

    test_meta["dbt_selection"] = MetadataValue.text(select_expr)
    return MaterializeResult(metadata=test_meta)


# If you later want fine-grained dbt asset integration (one asset per dbt model),
# you can reintroduce dbt_dbt's `@dbt_assets` integration here using the
# signature that matches your installed dagster-dbt version.
