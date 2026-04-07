"""Orchestrator for the Data Quality Assessment feature.

Resolves columns from standard procurement field names, computes SQL-based
metrics for each parameter group, calls the AI for insights, and assembles
the final response payload.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from shared.db import get_meta, read_table_columns, table_exists, table_row_count

from .ai_prompt import generate_dqa_insights
from .metrics import (
    compute_alphanumeric_spend,
    compute_currency_metrics,
    compute_currency_quality_analysis,
    compute_date_metrics,
    compute_description_metrics,
    compute_fill_rates,
    compute_fill_rate_summary,
    compute_non_procurable_spend,
    compute_spend_metrics,
    compute_supplier_metrics,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standard field names per parameter group (strict mode — exact match only)
# ---------------------------------------------------------------------------

DATE_COLUMNS: list[str] = [
    "Invoice Date",
    "Goods Receipt Date",
    "Payment date",
    "PO Document Date",
    "Contract End Date",
    "Contract Start Date",
]

SPEND_COLUMNS: list[str] = [
    "Total Amount paid in Reporting Currency",
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
    "PO Total Amount in reporting currency",
]

SUPPLIER_COLUMNS: list[str] = [
    "Vendor Name",
]

DESCRIPTION_COLUMNS: list[str] = [
    "Invoice Line Description",
    "PO Line Item Description 1",
    "PO Material Group Description",
    "PO Material Description",
    "PO Line Item Description 2",
]

CURRENCY_COLUMNS: list[str] = [
    "Local Currency Code",
    "PO Local Currency Code",
]

# Paired spend columns for each currency column (local, reporting)
_CURRENCY_SPEND_PAIRS: dict[str, tuple[str, str]] = {
    "Local Currency Code": (
        "Total Amount paid in Local Currency",
        "Total Amount paid in Reporting Currency",
    ),
    "PO Local Currency Code": (
        "PO Total Amount in Local Currency",
        "PO Total Amount in reporting currency",
    ),
}

# Preferred spend column for Pareto (reporting currency first, then local)
_PARETO_SPEND_PREFERENCE: list[str] = [
    "Total Amount paid in Reporting Currency",
    "PO Total Amount in reporting currency",
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
]

# Preferred spend column for alphanumeric spend (reporting first, then local)
_ALPHA_SPEND_PREFERENCE: list[str] = [
    "Total Amount paid in Reporting Currency",
    "Total Amount paid in Local Currency",
]


def _resolve_columns(
    available: set[str], candidates: list[str]
) -> list[str]:
    """Return the subset of *candidates* that exist in *available*."""
    return [c for c in candidates if c in available]


def _pick_pareto_spend_col(available: set[str]) -> str | None:
    """Pick the best spend column for Pareto, preferring reporting currency."""
    for c in _PARETO_SPEND_PREFERENCE:
        if c in available:
            return c
    return None


def _make_key(group: str, column: str) -> str:
    """Deterministic key shared between metrics payload and AI response."""
    return f"{group}__{column}"


def _resolve_concat_desc_columns(
    conn: sqlite3.Connection, available: set[str]
) -> list[str]:
    """Return concat columns whose source columns overlap with DESCRIPTION_COLUMNS."""
    concat_meta: dict = get_meta(conn, "concatColumns") or {}
    desc_set = set(DESCRIPTION_COLUMNS)
    result: list[str] = []
    for _group_id, entries in concat_meta.items():
        for entry in entries:
            col_name = entry["column_name"]
            sources = set(entry.get("source_columns", []))
            if col_name in available and sources & desc_set:
                result.append(col_name)
    return result


# Map spend columns to their corresponding currency code column
_SPEND_TO_CURRENCY_CODE: dict[str, str] = {
    "Total Amount paid in Local Currency": "Local Currency Code",
    "PO Total Amount in Local Currency": "PO Local Currency Code",
}


def _detect_currency_label(
    spend_col: str | None,
    conn: sqlite3.Connection | None = None,
    table_name: str | None = None,
    available: set[str] | None = None,
) -> str | None:
    """Resolve the actual currency code(s) for a spend column.

    For local-currency spend columns, queries the paired currency-code column
    to return the actual code (e.g. 'USD') or 'Multi-currency' when there are
    multiple distinct codes.  For reporting-currency spend columns (no paired
    code column), falls back to 'Reporting Currency'.
    """
    if spend_col is None:
        return None

    # Try to resolve actual currency code from paired column
    code_col = _SPEND_TO_CURRENCY_CODE.get(spend_col)
    if code_col and conn is not None and table_name and available and code_col in available:
        from shared.db import quote_id
        qc = quote_id(code_col)
        tbl = quote_id(table_name)
        rows = conn.execute(
            f"SELECT DISTINCT UPPER(TRIM({qc})) AS code FROM {tbl} "
            f"WHERE {qc} IS NOT NULL AND TRIM({qc}) != '' LIMIT 3"
        ).fetchall()
        codes = [str(r["code"]) for r in rows if r["code"]]
        if len(codes) == 1:
            return codes[0]
        if len(codes) > 1:
            return "Multi-currency"

    # Fallback: use the spend column name itself as the label
    return spend_col


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_data_quality_assessment(
    conn: sqlite3.Connection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Run the full DQA pipeline on *table_name*.

    Args:
        conn: SQLite session connection.
        table_name: Table to analyse (e.g. ``final_merged_v1``).
        api_key: Portkey / OpenAI key for insight generation.

    Returns:
        JSON-serialisable dict with ``totalRows`` and ``parameters`` list.

    Raises:
        ValueError: If the table does not exist or the AI call fails.
    """
    if not table_exists(conn, table_name):
        raise ValueError(f"Table '{table_name}' not found in session.")

    available = set(read_table_columns(conn, table_name))
    total_rows = table_row_count(conn, table_name)

    # Fill rate summary for all user columns
    fill_rate_summary = compute_fill_rate_summary(conn, table_name)

    # Resolve columns per group
    date_cols = _resolve_columns(available, DATE_COLUMNS)
    spend_cols = _resolve_columns(available, SPEND_COLUMNS)
    supplier_cols = _resolve_columns(available, SUPPLIER_COLUMNS)
    desc_cols = _resolve_columns(available, DESCRIPTION_COLUMNS)
    currency_cols = _resolve_columns(available, CURRENCY_COLUMNS)
    concat_desc_cols = _resolve_concat_desc_columns(conn, available)

    all_resolved = date_cols + spend_cols + supplier_cols + desc_cols + concat_desc_cols + currency_cols
    fill_rates = compute_fill_rates(conn, table_name, all_resolved) if all_resolved else {}

    pareto_spend_col = _pick_pareto_spend_col(available)

    # ── Build per-group results ────────────────────────────────────────────

    parameters: list[dict[str, Any]] = []

    # Date
    date_entries: list[dict[str, Any]] = []
    for col in DATE_COLUMNS:
        key = _make_key("date", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_date_metrics(conn, table_name, col)
            date_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })
        else:
            date_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": 0.0,
                "mapped": False,
                "stats": {},
            })
    parameters.append({"group": "Date", "columns": date_entries})

    # Currency (rendered before Spend so panels are adjacent)
    currency_entries: list[dict[str, Any]] = []
    for col in CURRENCY_COLUMNS:
        key = _make_key("currency", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_currency_metrics(conn, table_name, col)
            pair = _CURRENCY_SPEND_PAIRS.get(col, (None, None))
            local_spend = pair[0] if pair[0] in available else None
            reporting_spend = pair[1] if pair[1] in available else None
            stats["currencyQuality"] = compute_currency_quality_analysis(
                conn, table_name, col,
                local_spend, reporting_spend, total_rows,
            )
            stats["hasLocalSpend"] = local_spend is not None
            stats["hasReportingSpend"] = reporting_spend is not None
            currency_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })
        else:
            currency_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": 0.0,
                "mapped": False,
                "stats": {},
            })
    parameters.append({"group": "Currency", "columns": currency_entries})

    # Spend
    _REPORTING_SPEND_COLS = {
        "Total Amount paid in Reporting Currency",
        "PO Total Amount in reporting currency",
    }
    # Map local-currency spend columns to their reporting-currency counterpart
    _LOCAL_TO_REPORTING: dict[str, str] = {
        "Total Amount paid in Local Currency": "Total Amount paid in Reporting Currency",
        "PO Total Amount in Local Currency": "PO Total Amount in reporting currency",
    }
    spend_entries: list[dict[str, Any]] = []
    for col in SPEND_COLUMNS:
        key = _make_key("spend", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_spend_metrics(conn, table_name, col)
            # Attach per-currency breakdown for local-currency spend columns
            paired_ccy = _SPEND_TO_CURRENCY_CODE.get(col)
            if paired_ccy and paired_ccy in available:
                stats["spendByCurrency"] = compute_currency_quality_analysis(
                    conn, table_name, paired_ccy,
                    col, None, total_rows,
                )
            # Flag when the corresponding reporting-currency column is absent
            reporting_counterpart = _LOCAL_TO_REPORTING.get(col)
            if reporting_counterpart and reporting_counterpart not in available:
                stats["reportingCurrencyMissing"] = True
            spend_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })
        else:
            spend_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": 0.0,
                "mapped": False,
                "stats": {},
            })
    parameters.append({"group": "Spend", "columns": spend_entries})

    # Supplier
    pareto_spend_standardized = (
        pareto_spend_col in _REPORTING_SPEND_COLS if pareto_spend_col else False
    )
    supplier_entries: list[dict[str, Any]] = []
    for col in SUPPLIER_COLUMNS:
        key = _make_key("supplier", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_supplier_metrics(
                conn, table_name, col, pareto_spend_col
            )
            stats["paretoSpendStandardized"] = pareto_spend_standardized
            supplier_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })
        else:
            supplier_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": 0.0,
                "mapped": False,
                "stats": {},
            })
    parameters.append({"group": "Supplier", "columns": supplier_entries})

    # Description (standard + concatenated description columns)
    all_desc_candidates = DESCRIPTION_COLUMNS + concat_desc_cols
    alpha_spend_col: str | None = None
    for c in _ALPHA_SPEND_PREFERENCE:
        if c in available:
            alpha_spend_col = c
            break
    _LOCAL_SPEND_COLS = {"Total Amount paid in Local Currency", "PO Total Amount in Local Currency"}
    alpha_currency_col: str | None = (
        currency_cols[0] if currency_cols and alpha_spend_col in _LOCAL_SPEND_COLS else None
    )

    desc_entries: list[dict[str, Any]] = []
    for col in all_desc_candidates:
        key = _make_key("description", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_description_metrics(conn, table_name, col)
            np_stats = compute_non_procurable_spend(
                conn, table_name, col, pareto_spend_col,
                currency_column=alpha_currency_col,
            )
            stats.update(np_stats)
            stats["currencyLabel"] = _detect_currency_label(pareto_spend_col, conn, table_name, available)
            alpha_stats = compute_alphanumeric_spend(
                conn, table_name, col, alpha_spend_col, alpha_currency_col
            )
            stats.update(alpha_stats)
            desc_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })
        else:
            desc_entries.append({
                "columnName": col,
                "parameterKey": key,
                "fillRate": 0.0,
                "mapped": False,
                "stats": {},
            })
    parameters.append({"group": "Description", "columns": desc_entries})

    # ── AI insights ────────────────────────────────────────────────────────

    ai_payload = {"totalRows": total_rows, "parameters": parameters}
    insights_map = generate_dqa_insights(ai_payload, api_key)

    # Merge insights back into parameter entries
    for group in parameters:
        for entry in group["columns"]:
            pk = entry["parameterKey"]
            if not entry["mapped"]:
                entry["insight"] = "Column not present in data."
            else:
                entry["insight"] = insights_map.get(pk, "")

    return {
        "totalRows": total_rows,
        "parameters": parameters,
        "fillRateSummary": fill_rate_summary,
    }
