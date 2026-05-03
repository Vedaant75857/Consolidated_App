"""Payment Terms analysis panel for the Data Quality Assessment.

Discovers unique payment terms, computes spend breakdown, and generates AI
insights on standardisation opportunities.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_payment_terms_insight
from .column_resolver import find_payment_terms_columns, pick_spend_column, resolve_column
from .metrics import _non_null_condition, _safe_pct, numeric_spend_expr

logger = logging.getLogger(__name__)


def run_payment_terms_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    payment_terms_column: str | None = None,
) -> dict[str, Any]:
    """SQL-only phase: payment terms breakdown.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        payment_terms_column: User-selected payment terms column override.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``aiInsight`` set to ``None``.
    """
    available = set(read_table_columns(conn, table_name))
    available_pt_cols = find_payment_terms_columns(available)

    if payment_terms_column and payment_terms_column in available:
        pt_col = payment_terms_column
    else:
        pt_col = resolve_column(available, "payment_terms", fuzzy=False)

    if pt_col is None:
        return {
            "exists": False,
            "availablePaymentTermsColumns": available_pt_cols,
            "paymentTerms": [],
            "totalSpend": 0,
            "uniqueCount": 0,
            "spendColumn": None,
            "isReporting": False,
            "currencyColumns": [],
            "aiInsight": None,
        }

    tbl = quote_id(table_name)
    qpt = quote_id(pt_col)
    nn = _non_null_condition(qpt)

    spend_col, is_reporting = pick_spend_column(available)
    total_rows = conn.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()["cnt"]

    if spend_col:
        qs = quote_id(spend_col)
        spend_expr = numeric_spend_expr(qs)

        rows = conn.execute(
            f"SELECT TRIM({qpt}) AS term, "
            f"SUM({spend_expr}) AS spend, COUNT(*) AS row_ct "
            f"FROM {tbl} WHERE {nn} "
            f"GROUP BY TRIM({qpt}) ORDER BY spend DESC"
        ).fetchall()

        total_spend = sum(float(r["spend"] or 0) for r in rows)

        # Per-currency spend breakdown when using local currency
        currency_columns: list[str] = []
        ccy_spend_map: dict[str, dict[str, float]] = {}

        if not is_reporting:
            ccy_col = resolve_column(available, "currency_code", fuzzy=False)
            if ccy_col:
                qcc = quote_id(ccy_col)
                ccy_rows = conn.execute(
                    f"SELECT TRIM({qpt}) AS term, UPPER(TRIM({qcc})) AS ccy, "
                    f"SUM({spend_expr}) AS spend "
                    f"FROM {tbl} WHERE {nn} "
                    f"AND {qcc} IS NOT NULL AND TRIM({qcc}) != '' "
                    f"GROUP BY TRIM({qpt}), UPPER(TRIM({qcc}))"
                ).fetchall()
                for r in ccy_rows:
                    term = str(r["term"])
                    ccy = str(r["ccy"])
                    if ccy not in currency_columns:
                        currency_columns.append(ccy)
                    ccy_spend_map.setdefault(term, {})[ccy] = round(float(r["spend"] or 0))
                currency_columns.sort()

        payment_terms = [
            {
                "term": str(r["term"]),
                "spend": round(float(r["spend"] or 0)),
                "rowCount": int(r["row_ct"]),
                "pctOfTotal": _safe_pct(
                    round(float(r["spend"] or 0)),
                    round(total_spend) if total_spend else 1,
                ),
                "pctOfRows": _safe_pct(int(r["row_ct"]), total_rows),
                "currencySpend": ccy_spend_map.get(str(r["term"]), {}),
            }
            for r in rows
        ]
    else:
        currency_columns = []
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
                "pctOfRows": _safe_pct(int(r["row_ct"]), total_rows),
                "currencySpend": {},
            }
            for r in rows
        ]

    unique_count = len(payment_terms)

    return {
        "exists": True,
        "availablePaymentTermsColumns": available_pt_cols,
        "paymentTerms": payment_terms,
        "totalSpend": round(total_spend),
        "uniqueCount": unique_count,
        "spendColumn": spend_col,
        "isReporting": is_reporting,
        "currencyColumns": currency_columns,
        "aiInsight": None,
    }


def run_payment_terms_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate insight from pre-computed SQL data.

    Safe to call without any database lock held.
    """
    if sql_result.get("aiInsight") is not None:
        return sql_result

    ai_payload = {
        "paymentTerms": sql_result["paymentTerms"][:200],
        "totalSpend": sql_result["totalSpend"],
        "uniqueCount": sql_result["uniqueCount"],
    }
    try:
        sql_result["aiInsight"] = generate_payment_terms_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Payment terms AI insight generation failed: %s", exc)
        sql_result["aiInsight"] = ["AI insight generation failed."]

    return sql_result
