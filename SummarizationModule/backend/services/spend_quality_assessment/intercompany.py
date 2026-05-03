"""Intercompany Spend — keyword-based spend search across vendor name columns.

Lets users pick vendor-related columns from ``analysis_data``, enter keywords,
and see how many rows match plus the total spend (reporting currency) for each.
Mirrors the Not Procurable Spend functionality but scoped to vendor columns.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from shared.duckdb_compat import DuckDBConnection

logger = logging.getLogger(__name__)

VENDOR_FIELD_KEYS: list[str] = [
    "supplier",
    "vendor_country",
]

VENDOR_DISPLAY_NAMES: dict[str, str] = {
    "supplier": "Vendor Name",
    "vendor_country": "Vendor Country",
}


def _quote_id(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


def get_vendor_searchable_columns(
    conn: DuckDBConnection,
) -> list[dict[str, str]]:
    """Return the vendor columns in ``analysis_data`` available for keyword search.

    Only columns that actually exist in the table *and* appear in
    ``VENDOR_FIELD_KEYS`` are returned, preserving the preferred order.

    Args:
        conn: DuckDB session connection.

    Returns:
        List of ``{"fieldKey": str, "displayName": str}`` dicts.
    """
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'analysis_data' ORDER BY ordinal_position"
    ).fetchall()
    present: set[str] = {str(r[0]) for r in rows}

    return [
        {"fieldKey": fk, "displayName": VENDOR_DISPLAY_NAMES.get(fk, fk)}
        for fk in VENDOR_FIELD_KEYS
        if fk in present
    ]


def search_intercompany_keyword(
    conn: DuckDBConnection,
    columns: list[str],
    keyword: str,
) -> dict[str, Any]:
    """Search ``analysis_data`` for rows where *keyword* appears in any of *columns*.

    The match is case-insensitive (``ILIKE``). Spend is summed from
    ``total_spend`` (reporting currency).

    Args:
        conn: DuckDB session connection.
        columns: Column names to search (must be a subset of ``VENDOR_FIELD_KEYS``).
        keyword: The search term (non-empty string).

    Returns:
        ``{"keyword": str, "matchingRows": int, "totalSpend": float}``

    Raises:
        ValueError: If *columns* is empty, *keyword* is blank, or a column name
                    is not in the allowed set.
    """
    if not keyword or not keyword.strip():
        raise ValueError("Keyword must not be empty.")
    if not columns:
        raise ValueError("At least one column must be selected.")

    allowed = set(VENDOR_FIELD_KEYS)
    invalid = [c for c in columns if c not in allowed]
    if invalid:
        raise ValueError(f"Invalid column(s): {', '.join(invalid)}")

    keyword = keyword.strip()
    escaped = re.sub(r"([%_\\])", r"\\\1", keyword)
    like_pattern = f"%{escaped}%"

    where_clauses = [
        f"{_quote_id(col)} ILIKE ?" for col in columns
    ]
    where_sql = " OR ".join(where_clauses)
    params = [like_pattern] * len(columns)

    sql = (
        "SELECT COUNT(*) AS match_count, "
        "  COALESCE(SUM(TRY_CAST(total_spend AS DOUBLE)), 0) AS total_spend "
        f'FROM "analysis_data" '
        f"WHERE {where_sql}"
    )

    row = conn.execute(sql, params).fetchone()
    return {
        "keyword": keyword,
        "matchingRows": int(row[0] or 0),
        "totalSpend": round(float(row[1] or 0)),
    }
