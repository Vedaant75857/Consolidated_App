"""Intercompany Spend — scan vendor names for the client company name."""

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

VENDOR_COLUMN = "supplier"

VENDOR_DISPLAY_NAMES: dict[str, str] = {
    "supplier": "Vendor Name",
}


def get_vendor_searchable_columns(
    conn: DuckDBConnection,
) -> list[dict[str, str]]:
    """Return vendor name column when mapped and populated."""
    mapped = get_mapped_field_keys(conn, [VENDOR_COLUMN])
    return [
        {"fieldKey": fk, "displayName": VENDOR_DISPLAY_NAMES.get(fk, fk)}
        for fk in mapped
    ]


def search_intercompany_keyword(
    conn: DuckDBConnection,
    client_name: str,
) -> dict[str, Any]:
    """Search vendor names for *client_name* (case-insensitive substring match)."""
    if not client_name or not client_name.strip():
        raise ValueError("Client name must not be empty.")

    if not field_mapped_with_data(conn, VENDOR_COLUMN):
        raise ValueError(
            "Vendor Name is not mapped or has no data. Map supplier in Step 3."
        )

    keyword = client_name.strip()
    escaped = re.sub(r"([%_\\])", r"\\\1", keyword)
    like_pattern = f"%{escaped}%"
    qc = quote_id(VENDOR_COLUMN)

    row = conn.execute(
        "SELECT COUNT(*) AS match_count, "
        "  COALESCE(SUM(TRY_CAST(total_spend AS DOUBLE)), 0) AS total_spend "
        f'FROM "analysis_data" WHERE {qc} ILIKE ?',
        [like_pattern],
    ).fetchone()

    return {
        "keyword": keyword,
        "matchingRows": int(row[0] or 0),
        "totalSpend": round(float(row[1] or 0)),
    }


def suggest_intercompany_vendors(
    conn: DuckDBConnection,
    client_name: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return distinct vendor names that may be intercompany matches."""
    if not client_name or not client_name.strip():
        return []
    if not field_mapped_with_data(conn, VENDOR_COLUMN):
        return []

    keyword = client_name.strip()
    escaped = re.sub(r"([%_\\])", r"\\\1", keyword)
    like_pattern = f"%{escaped}%"
    qc = quote_id(VENDOR_COLUMN)

    rows = conn.execute(
        f"""
        SELECT
            TRIM(CAST({qc} AS VARCHAR)) AS vendor,
            COUNT(*) AS row_count,
            COALESCE(SUM(TRY_CAST(total_spend AS DOUBLE)), 0) AS spend
        FROM "analysis_data"
        WHERE {qc} IS NOT NULL
          AND TRIM(CAST({qc} AS VARCHAR)) != ''
          AND {qc} ILIKE ?
        GROUP BY 1
        ORDER BY spend DESC
        LIMIT ?
        """,
        [like_pattern, limit],
    ).fetchall()

    return [
        {
            "vendor": str(r[0]),
            "matchingRows": int(r[1] or 0),
            "totalSpend": round(float(r[2] or 0)),
        }
        for r in rows
    ]


def detect_intercompany_spend(
    conn: DuckDBConnection,
    client_name: str,
) -> dict[str, Any]:
    """Scan supplier column for the client company name."""
    if not field_mapped_with_data(conn, "total_spend"):
        return {
            "feasible": False,
            "message": "Total Spend is not mapped. Confirm column mapping in Step 3.",
            "clientName": client_name,
            "summary": None,
            "vendors": [],
        }

    if not field_mapped_with_data(conn, VENDOR_COLUMN):
        return {
            "feasible": False,
            "message": (
                "Vendor Name is not mapped or has no data. Map the supplier "
                "column in Step 3."
            ),
            "clientName": client_name,
            "summary": None,
            "vendors": [],
        }

    if not client_name or not client_name.strip():
        return {
            "feasible": False,
            "message": "Enter your client company name to scan vendor names.",
            "clientName": client_name,
            "summary": None,
            "vendors": [],
        }

    summary = search_intercompany_keyword(conn, client_name)
    vendors = suggest_intercompany_vendors(conn, client_name)

    return {
        "feasible": True,
        "message": (
            f"No vendor names contain \"{client_name.strip()}\"."
            if summary["matchingRows"] == 0
            else None
        ),
        "clientName": client_name.strip(),
        "summary": summary,
        "vendors": vendors,
    }
