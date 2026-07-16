"""Country / Region analysis panel for the Data Quality Assessment.

Uses AI-identified country and region columns to build multi-column value
distribution tables with fill rates.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, filter_data_columns, read_table_columns

from .column_resolver import find_country_columns, resolve_all_columns
from .value_distribution import compute_value_distribution_table, merge_identified_columns

logger = logging.getLogger(__name__)


def _find_region_columns(available: set[str]) -> list[str]:
    """Return all region-type columns found in the table."""
    resolved = resolve_all_columns(available, "region")
    resolved_set = set(resolved)
    extras = sorted(
        c for c in available
        if "region" in c.lower() and c not in resolved_set
    )
    return resolved + extras


def run_country_region_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    identified_country_columns: list[str] | None = None,
    identified_region_columns: list[str] | None = None,
) -> dict[str, Any]:
    """SQL-only phase: multi-column country and region value distributions.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        identified_country_columns: AI-identified country column names.
        identified_region_columns: AI-identified region column names.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``countryTable`` and ``regionTable``.
    """
    available = set(filter_data_columns(read_table_columns(conn, table_name)))

    country_cols = merge_identified_columns(
        identified_country_columns, available, find_country_columns,
    )
    region_cols = merge_identified_columns(
        identified_region_columns, available, _find_region_columns,
    )

    return {
        "countryTable": compute_value_distribution_table(
            conn, table_name, country_cols,
        ),
        "regionTable": compute_value_distribution_table(
            conn, table_name, region_cols,
        ),
    }


def run_country_region_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """No-op AI phase kept for API compatibility."""
    return sql_result
