"""Orchestrator for the Data Quality Assessment feature.

Resolves columns from standard procurement field names, computes SQL-based
metrics for each parameter group, calls the AI for insights, and assembles
the final response payload.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from shared.db import read_table_columns, table_exists, table_row_count

from .ai_prompt import generate_dqa_insights
from .metrics import (
    compute_currency_metrics,
    compute_date_metrics,
    compute_description_metrics,
    compute_fill_rates,
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

# Preferred spend column for Pareto (reporting currency first, then local)
_PARETO_SPEND_PREFERENCE: list[str] = [
    "Total Amount paid in Reporting Currency",
    "PO Total Amount in reporting currency",
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
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

    # Resolve columns per group
    date_cols = _resolve_columns(available, DATE_COLUMNS)
    spend_cols = _resolve_columns(available, SPEND_COLUMNS)
    supplier_cols = _resolve_columns(available, SUPPLIER_COLUMNS)
    desc_cols = _resolve_columns(available, DESCRIPTION_COLUMNS)
    currency_cols = _resolve_columns(available, CURRENCY_COLUMNS)

    all_resolved = date_cols + spend_cols + supplier_cols + desc_cols + currency_cols
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

    # Spend
    spend_entries: list[dict[str, Any]] = []
    for col in SPEND_COLUMNS:
        key = _make_key("spend", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_spend_metrics(conn, table_name, col)
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
    supplier_entries: list[dict[str, Any]] = []
    for col in SUPPLIER_COLUMNS:
        key = _make_key("supplier", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_supplier_metrics(
                conn, table_name, col, pareto_spend_col
            )
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

    # Description
    desc_entries: list[dict[str, Any]] = []
    for col in DESCRIPTION_COLUMNS:
        key = _make_key("description", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_description_metrics(conn, table_name, col)
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

    # Currency
    currency_entries: list[dict[str, Any]] = []
    for col in CURRENCY_COLUMNS:
        key = _make_key("currency", col)
        if col in available:
            fr = fill_rates[col]
            stats = compute_currency_metrics(conn, table_name, col)
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

    return {"totalRows": total_rows, "parameters": parameters}
