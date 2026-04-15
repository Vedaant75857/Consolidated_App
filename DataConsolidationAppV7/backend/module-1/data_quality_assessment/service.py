"""Orchestrator for the redesigned Data Quality Assessment.

Provides per-panel entry points that the route layer calls directly.
Each function resolves the table, validates inputs, and delegates to the
appropriate analysis module.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, read_table_columns, table_exists, table_row_count

from .currency_analysis import run_currency_analysis
from .country_region_analysis import run_country_region_analysis
from .date_analysis import run_date_analysis
from .fill_rate_analysis import run_fill_rate_analysis, run_spend_bifurcation
from .payment_terms_analysis import run_payment_terms_analysis
from .supplier_analysis import run_supplier_analysis

logger = logging.getLogger(__name__)

__all__ = [
    "TableMissingError",
    "run_dqa_date",
    "run_dqa_currency",
    "run_dqa_payment_terms",
    "run_dqa_country_region",
    "run_dqa_supplier",
    "run_dqa_fill_rate",
    "run_dqa_spend_bifurcation",
]


class TableMissingError(Exception):
    """Raised when a specific table is missing from the session DB,
    but the session itself is still alive (DB file exists)."""
    pass


def _validate_table(conn: DuckDBConnection, table_name: str) -> str:
    """Validate the table exists, falling back to 'final_merged' for versioned names.

    Returns the resolved table name (may differ from input if fallback was used).

    Raises:
        TableMissingError: If the table doesn't exist in the session DB.
    """
    # Try the requested table first
    if table_exists(conn, table_name):
        return table_name

    # For versioned names, fall back to the canonical 'final_merged' table
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.warning(
            "Table '%s' not found, falling back to 'final_merged'", table_name,
        )
        return "final_merged"

    # Force a checkpoint + fresh read to handle WAL visibility issues
    try:
        conn.execute("CHECKPOINT")
        conn.commit()
    except Exception:
        pass

    # Re-check after checkpoint
    if table_exists(conn, table_name):
        logger.info("Table '%s' found after CHECKPOINT", table_name)
        return table_name
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.info("Table 'final_merged' found after CHECKPOINT (was looking for '%s')", table_name)
        return "final_merged"

    # Last resort: discover any final_merged_v* table or final_merged
    try:
        all_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name LIKE 'final_merged%' ORDER BY table_name DESC"
        ).fetchall()
        table_names = [r["table_name"] for r in all_tables]
        logger.warning(
            "Table '%s' not found. All final_merged* tables in DB: %s",
            table_name, table_names,
        )
        if table_names:
            found = table_names[0]
            logger.warning("Using discovered fallback table '%s'", found)
            return found
    except Exception as exc:
        logger.error("Error discovering tables: %s", exc)

    raise TableMissingError(f"Table '{table_name}' not found. Please re-run the Merge step.")


# ── Per-panel dispatchers ─────────────────────────────────────────────────


def run_dqa_date(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
    date_column: str | None = None,
) -> dict[str, Any]:
    """Date format check + spend pivot + AI insight.

    Args:
        conn: SQLite session connection.
        table_name: Target table (e.g. ``final_merged_v1``).
        api_key: Portkey / OpenAI API key.
        date_column: Optional selected date column (auto-picks if omitted).

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_date_analysis(conn, table_name, date_column, api_key)


def run_dqa_currency(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Currency quality table + AI insight.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: Portkey / OpenAI API key.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_currency_analysis(conn, table_name, api_key)


def run_dqa_payment_terms(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Payment terms breakdown + AI insight.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: Portkey / OpenAI API key.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_payment_terms_analysis(conn, table_name, api_key)


def run_dqa_country_region(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Country/Region unique values + AI standardisation insight.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: Portkey / OpenAI API key.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_country_region_analysis(conn, table_name, api_key)


def run_dqa_supplier(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Supplier name list + AI normalisation insight.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: Portkey / OpenAI API key.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_supplier_analysis(conn, table_name, api_key)


def run_dqa_fill_rate(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Per-column fill rate with spend coverage.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_fill_rate_analysis(conn, table_name)


def run_dqa_spend_bifurcation(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Positive vs negative spend bifurcation.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.

    Returns:
        JSON-serialisable result dict.
    """
    table_name = _validate_table(conn, table_name)
    return run_spend_bifurcation(conn, table_name)
