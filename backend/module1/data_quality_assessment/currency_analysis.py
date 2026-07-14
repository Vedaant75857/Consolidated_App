"""Currency analysis panel for the Data Quality Assessment.

Uses AI-identified currency columns to build a multi-column value distribution
table with fill rates.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, read_table_columns

from .column_resolver import find_currency_columns
from .value_distribution import compute_value_distribution_table, merge_identified_columns

logger = logging.getLogger(__name__)


def run_currency_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    identified_columns: list[str] | None = None,
) -> dict[str, Any]:
    """SQL-only phase: multi-column currency value distribution.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        identified_columns: AI-identified currency column names.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``distributionTable``.
    """
    available = set(read_table_columns(conn, table_name))
    columns = merge_identified_columns(
        identified_columns, available, find_currency_columns,
    )
    distribution = compute_value_distribution_table(conn, table_name, columns)

    return {"distributionTable": distribution}


def run_currency_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """No-op AI phase kept for API compatibility."""
    return sql_result
