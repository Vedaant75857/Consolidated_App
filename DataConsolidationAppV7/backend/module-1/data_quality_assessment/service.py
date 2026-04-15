"""Orchestrator for the redesigned Data Quality Assessment.

Provides per-panel entry points split into SQL and AI phases so the route
layer can hold the session lock only during database work and release it
before making slow AI calls.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, read_table_columns, table_exists, table_row_count

from .currency_analysis import run_currency_analysis_sql, run_currency_analysis_ai
from .country_region_analysis import run_country_region_analysis_sql, run_country_region_analysis_ai
from .date_analysis import run_date_analysis_sql, run_date_analysis_ai
from .fill_rate_analysis import run_fill_rate_analysis, run_spend_bifurcation
from .payment_terms_analysis import run_payment_terms_analysis_sql, run_payment_terms_analysis_ai
from .supplier_analysis import run_supplier_analysis_sql, run_supplier_analysis_ai

logger = logging.getLogger(__name__)

__all__ = [
    "TableMissingError",
    "run_dqa_date_sql",
    "run_dqa_date_ai",
    "run_dqa_currency_sql",
    "run_dqa_currency_ai",
    "run_dqa_payment_terms_sql",
    "run_dqa_payment_terms_ai",
    "run_dqa_country_region_sql",
    "run_dqa_country_region_ai",
    "run_dqa_supplier_sql",
    "run_dqa_supplier_ai",
    "run_dqa_fill_rate",
    "run_dqa_spend_bifurcation",
]


class TableMissingError(Exception):
    """Raised when a specific table is missing from the session DB,
    but the session itself is still alive (DB file exists)."""
    pass


def _validate_table(conn: DuckDBConnection, table_name: str) -> str:
    """Validate the table exists, with a limited fallback to ``final_merged``.

    Returns the resolved table name.

    Raises:
        TableMissingError: If the table doesn't exist in the session DB.
    """
    if table_exists(conn, table_name):
        return table_name

    # For versioned names, fall back to the canonical 'final_merged' table
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.warning(
            "Table '%s' not found, falling back to 'final_merged'", table_name,
        )
        return "final_merged"

    # Force a checkpoint to handle WAL visibility issues
    try:
        conn.execute("CHECKPOINT")
        conn.commit()
    except Exception:
        pass

    if table_exists(conn, table_name):
        logger.info("Table '%s' found after CHECKPOINT", table_name)
        return table_name
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.info(
            "Table 'final_merged' found after CHECKPOINT (was looking for '%s')",
            table_name,
        )
        return "final_merged"

    raise TableMissingError(f"Table '{table_name}' not found. Please re-run the Merge step.")


# ── Per-panel dispatchers: SQL phase (call under session lock) ────────────


def run_dqa_date_sql(
    conn: DuckDBConnection,
    table_name: str,
    date_column: str | None = None,
) -> dict[str, Any]:
    """Date SQL phase — format detection + spend pivot. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_date_analysis_sql(conn, table_name, date_column)


def run_dqa_date_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Date AI phase — generate insight. Safe to call without lock."""
    return run_date_analysis_ai(sql_result, api_key)


def run_dqa_currency_sql(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Currency SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_currency_analysis_sql(conn, table_name)


def run_dqa_currency_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Currency AI phase. Safe to call without lock."""
    return run_currency_analysis_ai(sql_result, api_key)


def run_dqa_payment_terms_sql(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Payment terms SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_payment_terms_analysis_sql(conn, table_name)


def run_dqa_payment_terms_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Payment terms AI phase. Safe to call without lock."""
    return run_payment_terms_analysis_ai(sql_result, api_key)


def run_dqa_country_region_sql(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Country/Region SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_country_region_analysis_sql(conn, table_name)


def run_dqa_country_region_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Country/Region AI phase. Safe to call without lock."""
    return run_country_region_analysis_ai(sql_result, api_key)


def run_dqa_supplier_sql(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Supplier SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_supplier_analysis_sql(conn, table_name)


def run_dqa_supplier_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Supplier AI phase. Safe to call without lock."""
    return run_supplier_analysis_ai(sql_result, api_key)


def run_dqa_fill_rate(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Per-column fill rate (SQL only, no AI). Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_fill_rate_analysis(conn, table_name)


def run_dqa_spend_bifurcation(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Spend bifurcation (SQL only, no AI). Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_spend_bifurcation(conn, table_name)
