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
from .payment_terms_analysis import run_payment_terms_analysis
from .supplier_analysis import run_supplier_analysis

logger = logging.getLogger(__name__)

__all__ = [
    "run_dqa_date",
    "run_dqa_currency",
    "run_dqa_payment_terms",
    "run_dqa_country_region",
    "run_dqa_supplier",
]


def _validate_table(conn: DuckDBConnection, table_name: str) -> str:
    """Validate the table exists, falling back to 'final_merged' for versioned names.

    Returns the resolved table name (may differ from input if fallback was used).
    """
    if table_exists(conn, table_name):
        return table_name
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.warning(
            "Table '%s' not found, falling back to 'final_merged'", table_name,
        )
        return "final_merged"
    raise ValueError(f"Table '{table_name}' not found in session.")


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
