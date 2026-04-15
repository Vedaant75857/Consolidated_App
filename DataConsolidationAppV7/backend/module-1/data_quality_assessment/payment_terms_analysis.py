"""Payment Terms analysis panel for the Data Quality Assessment.

Discovers unique payment terms, computes spend breakdown, and generates AI
insights on standardisation opportunities.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_payment_terms_insight
from .metrics import _non_null_condition, _safe_pct, find_column

logger = logging.getLogger(__name__)

PAYMENT_TERMS_COLUMN = "Payment Terms"

# Spend column priority: reporting first, then local
_SPEND_PREFERENCE: list[str] = [
    "Total Amount paid in Reporting Currency",
    "PO Total Amount in reporting currency",
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
]


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


def run_payment_terms_analysis(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Run the payment terms analysis for the DQA panel.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: API key for AI insight generation.

    Returns:
        JSON-serialisable dict with ``paymentTerms`` (list of
        ``{term, spend, pctOfTotal}``), ``totalSpend``, ``uniqueCount``,
        ``aiInsight``, ``exists``, and ``spendColumn``.
    """
    available = set(read_table_columns(conn, table_name))

    pt_col = find_column(available, [PAYMENT_TERMS_COLUMN])
    if pt_col is None:
        return {
            "exists": False,
            "paymentTerms": [],
            "totalSpend": 0,
            "uniqueCount": 0,
            "spendColumn": None,
            "aiInsight": "No 'Payment Terms' column found in the dataset.",
        }

    tbl = quote_id(table_name)
    qpt = quote_id(pt_col)
    nn = _non_null_condition(qpt)

    spend_col = _pick_spend_column(available)

    if spend_col:
        qs = quote_id(spend_col)
        spend_expr = _numeric_spend_expr(qs)

        rows = conn.execute(
            f"SELECT TRIM({qpt}) AS term, "
            f"SUM({spend_expr}) AS spend, COUNT(*) AS row_ct "
            f"FROM {tbl} WHERE {nn} "
            f"GROUP BY TRIM({qpt}) ORDER BY spend DESC"
        ).fetchall()

        total_spend = sum(float(r["spend"] or 0) for r in rows)

        payment_terms = [
            {
                "term": str(r["term"]),
                "spend": round(float(r["spend"] or 0)),
                "rowCount": int(r["row_ct"]),
                "pctOfTotal": _safe_pct(
                    round(float(r["spend"] or 0)),
                    round(total_spend) if total_spend else 1,
                ),
            }
            for r in rows
        ]
    else:
        # No spend column — just list unique terms with row counts
        rows = conn.execute(
            f"SELECT TRIM({qpt}) AS term, COUNT(*) AS row_ct "
            f"FROM {tbl} WHERE {nn} "
            f"GROUP BY TRIM({qpt}) ORDER BY row_ct DESC"
        ).fetchall()

        total_spend = 0
        payment_terms = [
            {
                "term": str(r["term"]),
                "spend": None,
                "rowCount": int(r["row_ct"]),
                "pctOfTotal": None,
            }
            for r in rows
        ]

    unique_count = len(payment_terms)

    # AI insight
    ai_payload = {
        "paymentTerms": payment_terms[:200],  # cap to keep prompt within limits
        "totalSpend": round(total_spend),
        "uniqueCount": unique_count,
    }
    try:
        ai_insight = generate_payment_terms_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Payment terms AI insight generation failed: %s", exc)
        ai_insight = "AI insight generation failed."

    return {
        "exists": True,
        "paymentTerms": payment_terms,
        "totalSpend": round(total_spend),
        "uniqueCount": unique_count,
        "spendColumn": spend_col,
        "aiInsight": ai_insight,
    }
