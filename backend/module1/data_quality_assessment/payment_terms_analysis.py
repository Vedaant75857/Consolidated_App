"""Payment Terms analysis panel for the Data Quality Assessment.

Uses AI-identified payment terms columns to build a multi-column value
distribution table with fill rates.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, filter_data_columns, read_table_columns

from .column_resolver import find_payment_terms_columns
from .value_distribution import compute_value_distribution_table, merge_identified_columns

logger = logging.getLogger(__name__)


def run_payment_terms_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    identified_columns: list[str] | None = None,
) -> dict[str, Any]:
    """SQL-only phase: multi-column payment terms value distribution.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        identified_columns: AI-identified payment terms column names.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``distributionTable``.
    """
    available = set(filter_data_columns(read_table_columns(conn, table_name)))
    columns = merge_identified_columns(
        identified_columns, available, find_payment_terms_columns,
    )
    distribution = compute_value_distribution_table(conn, table_name, columns)

    return {"distributionTable": distribution}


def run_payment_terms_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """No-op AI phase kept for API compatibility."""
    return sql_result
