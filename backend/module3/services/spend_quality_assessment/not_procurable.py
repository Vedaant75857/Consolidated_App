"""Not Procurable Spend — keyword scan across description / taxonomy columns."""

from __future__ import annotations

import logging
import re
from typing import Any

from shared.duckdb_compat import DuckDBConnection

from services.spend_quality_assessment.analysis_data import (
    field_mapped_with_data,
    get_mapped_field_keys,
    list_analysis_columns,
    quote_id,
)

logger = logging.getLogger(__name__)

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

NON_PROCURABLE_KEYWORDS: list[str] = [
    "customs duties",
    "government fee",
    "license fee",
    "legal charges",
    "bank charges",
    "currency adjustment",
    "write-off",
    "taxes",
    "tax",
    "rebate",
    "duty",
    "freight",
    "shipping",
    "insurance",
    "penalty",
    "fine",
    "interest",
    "surcharge",
    "payroll",
    "utilities",
]


def get_searchable_columns(
    conn: DuckDBConnection,
) -> list[dict[str, Any]]:
    """Return searchable columns that are mapped with data."""
    mapped = get_mapped_field_keys(conn, SEARCHABLE_FIELD_KEYS)
    return [
        {
            "fieldKey": fk,
            "displayName": FIELD_DISPLAY_NAMES.get(fk, fk),
            "hasData": True,
        }
        for fk in mapped
    ]


def _resolve_columns(
    conn: DuckDBConnection,
    columns: list[str] | None,
) -> list[str]:
    available = {c["fieldKey"] for c in get_searchable_columns(conn)}
    if not available:
        raise ValueError(
            "No searchable columns with data. Map Description (or L1–L4) in Step 3."
        )
    if not columns:
        if "description" in available:
            return ["description"]
        return [next(iter(available))]
    invalid = [c for c in columns if c not in available]
    if invalid:
        raise ValueError(f"Invalid or empty column(s): {', '.join(invalid)}")
    return columns


def search_keyword_spend(
    conn: DuckDBConnection,
    columns: list[str] | None,
    keyword: str,
) -> dict[str, Any]:
    """Search for *keyword* across *columns* (case-insensitive)."""
    if not keyword or not keyword.strip():
        raise ValueError("Keyword must not be empty.")
    use_cols = _resolve_columns(conn, columns)
    keyword = keyword.strip()
    escaped = re.sub(r"([%_\\])", r"\\\1", keyword)
    like_pattern = f"%{escaped}%"

    where_clauses = [f"{quote_id(col)} ILIKE ?" for col in use_cols]
    where_sql = " OR ".join(where_clauses)
    params = [like_pattern] * len(use_cols)

    row = conn.execute(
        "SELECT COUNT(*) AS match_count, "
        "  COALESCE(SUM(TRY_CAST(total_spend AS DOUBLE)), 0) AS total_spend "
        f'FROM "analysis_data" WHERE {where_sql}',
        params,
    ).fetchone()
    return {
        "keyword": keyword,
        "matchingRows": int(row[0] or 0),
        "totalSpend": round(float(row[1] or 0)),
    }


def detect_non_procurable_spend(
    conn: DuckDBConnection,
    columns: list[str] | None = None,
) -> dict[str, Any]:
    """Auto-scan using built-in non-procurable keywords."""
    if not field_mapped_with_data(conn, "total_spend"):
        return {
            "feasible": False,
            "message": "Total Spend is not mapped. Confirm column mapping in Step 3.",
            "columnsUsed": [],
            "keywords": [],
            "uniqueMatchingRows": 0,
            "uniqueTotalSpend": 0,
        }

    searchable = get_searchable_columns(conn)
    if not searchable:
        return {
            "feasible": False,
            "message": (
                "No description columns with data found. Map Description, "
                "PO Material Description, or L1–L4 in Step 3."
            ),
            "columnsUsed": [],
            "keywords": [],
            "uniqueMatchingRows": 0,
            "uniqueTotalSpend": 0,
        }

    use_cols = _resolve_columns(conn, columns)
    keyword_results: list[dict[str, Any]] = []
    for kw in NON_PROCURABLE_KEYWORDS:
        result = search_keyword_spend(conn, use_cols, kw)
        if result["matchingRows"] > 0:
            keyword_results.append(result)

    where_parts: list[str] = []
    params: list[str] = []
    for kw in NON_PROCURABLE_KEYWORDS:
        escaped = re.sub(r"([%_\\])", r"\\\1", kw)
        pattern = f"%{escaped}%"
        col_clauses = [f"{quote_id(col)} ILIKE ?" for col in use_cols]
        where_parts.append(f"({' OR '.join(col_clauses)})")
        params.extend([pattern] * len(use_cols))

    union_sql = " OR ".join(where_parts)
    summary = conn.execute(
        "SELECT COUNT(*) AS match_count, "
        "  COALESCE(SUM(TRY_CAST(total_spend AS DOUBLE)), 0) AS total_spend "
        f'FROM "analysis_data" WHERE {union_sql}',
        params,
    ).fetchone()

    return {
        "feasible": True,
        "message": (
            "Scan completed. No built-in keyword matches were found in the "
            "selected columns."
            if not keyword_results
            else None
        ),
        "columnsUsed": use_cols,
        "keywords": keyword_results,
        "uniqueMatchingRows": int(summary[0] or 0),
        "uniqueTotalSpend": round(float(summary[1] or 0)),
    }
