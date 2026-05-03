"""Not Procurable Spend — keyword-based spend search across description columns.

Lets users pick one or more text columns from ``analysis_data``, enter keywords,
and see how many rows match plus the total spend (reporting currency) for each.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from shared.duckdb_compat import DuckDBConnection

logger = logging.getLogger(__name__)

# String-type columns suitable for free-text keyword search.
# Order here determines the display order in the frontend column selector.
SEARCHABLE_FIELD_KEYS: list[str] = [
    "description",
    "po_material_description",
    "l1",
    "l2",
    "l3",
    "l4",
    "supplier",
    "business_unit",
    "plant_name",
]

# Human-readable labels keyed by field name.
FIELD_DISPLAY_NAMES: dict[str, str] = {
    "description": "Description",
    "po_material_description": "PO Material Description",
    "l1": "Spend Classification L1",
    "l2": "Spend Classification L2",
    "l3": "Spend Classification L3",
    "l4": "Spend Classification L4",
    "supplier": "Vendor Name",
    "business_unit": "Business Unit",
    "plant_name": "Plant Name",
}


def _quote_id(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


def get_searchable_columns(
    conn: DuckDBConnection,
) -> list[dict[str, str]]:
    """Return the text columns in ``analysis_data`` that are available for keyword search.

    Only columns that actually exist in the table *and* appear in
    ``SEARCHABLE_FIELD_KEYS`` are returned, preserving the preferred order.

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
        {"fieldKey": fk, "displayName": FIELD_DISPLAY_NAMES.get(fk, fk)}
        for fk in SEARCHABLE_FIELD_KEYS
        if fk in present
    ]


def search_keyword_spend(
    conn: DuckDBConnection,
    columns: list[str],
    keyword: str,
) -> dict[str, Any]:
    """Search ``analysis_data`` for rows where *keyword* appears in any of *columns*.

    The match is case-insensitive (``ILIKE``).  Spend is summed from
    ``total_spend`` (reporting currency).

    Args:
        conn: DuckDB session connection.
        columns: Column names to search (must be a subset of ``SEARCHABLE_FIELD_KEYS``).
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

    allowed = set(SEARCHABLE_FIELD_KEYS)
    invalid = [c for c in columns if c not in allowed]
    if invalid:
        raise ValueError(f"Invalid column(s): {', '.join(invalid)}")

    keyword = keyword.strip()
    # Escape DuckDB LIKE special characters in the keyword
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
