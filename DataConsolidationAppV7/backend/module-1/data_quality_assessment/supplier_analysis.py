"""Supplier analysis panel for the Data Quality Assessment.

Extracts top suppliers by spend, sends alphabetically sorted names to AI for
normalisation opportunity detection.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_supplier_insight
from .metrics import _non_null_condition, _safe_pct, find_column

logger = logging.getLogger(__name__)

VENDOR_COLUMN = "Vendor Name"

_SPEND_PREFERENCE: list[str] = [
    "Total Amount paid in Reporting Currency",
    "PO Total Amount in reporting currency",
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
]

TOP_N = 1000


def _pick_spend_column(available: set[str]) -> str | None:
    """Return the highest-priority spend column present in the table (case-insensitive)."""
    return find_column(available, _SPEND_PREFERENCE)


def _numeric_spend_expr(qs: str) -> str:
    """SQL expression that safely casts a spend column to REAL."""
    return (
        f"CASE WHEN regexp_matches(TRIM({qs}), '[0-9]') "
        f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$') "
        f"THEN CAST({qs} AS REAL) ELSE 0 END"
    )


def run_supplier_analysis(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Run the supplier analysis for the DQA supplier panel.

    Retrieves the top 1 000 suppliers by spend, sorts them alphabetically,
    and sends the name list to AI for normalisation assessment.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: API key for AI insight generation.

    Returns:
        JSON-serialisable dict with ``aiInsight``, ``supplierCount``,
        ``totalSpendCovered``, ``spendColumn``, and ``exists``.
    """
    available = set(read_table_columns(conn, table_name))

    vendor_col = find_column(available, [VENDOR_COLUMN])
    if vendor_col is None:
        return {
            "exists": False,
            "supplierCount": 0,
            "totalSpendCovered": 0,
            "spendColumn": None,
            "aiInsight": "No 'Vendor Name' column found in the dataset.",
        }

    tbl = quote_id(table_name)
    qv = quote_id(vendor_col)
    nn = _non_null_condition(qv)
    spend_col = _pick_spend_column(available)

    if spend_col:
        qs = quote_id(spend_col)
        spend_expr = _numeric_spend_expr(qs)

        rows = conn.execute(
            f"SELECT TRIM({qv}) AS vendor, "
            f"SUM({spend_expr}) AS spend "
            f"FROM {tbl} WHERE {nn} "
            f"GROUP BY TRIM({qv}) ORDER BY spend DESC "
            f"LIMIT ?",
            (TOP_N,),
        ).fetchall()

        supplier_names = sorted(str(r["vendor"]) for r in rows)
        total_spend_covered = sum(float(r["spend"] or 0) for r in rows)
    else:
        rows = conn.execute(
            f"SELECT DISTINCT TRIM({qv}) AS vendor "
            f"FROM {tbl} WHERE {nn} LIMIT ?",
            (TOP_N,),
        ).fetchall()

        supplier_names = sorted(str(r["vendor"]) for r in rows)
        total_spend_covered = 0

    # Count total unique suppliers (not just top N)
    total_unique_row = conn.execute(
        f"SELECT COUNT(DISTINCT TRIM({qv})) AS cnt FROM {tbl} WHERE {nn}"
    ).fetchone()
    total_unique = int(total_unique_row["cnt"] or 0)

    # AI insight — send alphabetically sorted name list
    ai_payload = {
        "supplierNames": supplier_names,
        "count": len(supplier_names),
        "totalUniqueSuppliers": total_unique,
    }
    try:
        ai_insight = generate_supplier_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Supplier AI insight generation failed: %s", exc)
        ai_insight = "AI insight generation failed."

    return {
        "exists": True,
        "supplierCount": total_unique,
        "topNCount": len(supplier_names),
        "totalSpendCovered": round(total_spend_covered),
        "spendColumn": spend_col,
        "aiInsight": ai_insight,
    }
