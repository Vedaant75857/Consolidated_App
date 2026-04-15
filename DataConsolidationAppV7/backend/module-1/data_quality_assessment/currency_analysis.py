"""Currency analysis panel for the Data Quality Assessment.

Reuses existing currency metrics from ``metrics.py`` and adds an AI insight
call for currency standardisation recommendations.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns, table_row_count

from .ai_prompts import generate_currency_insight
from .metrics import (
    _non_null_condition,
    _safe_pct,
    compute_currency_metrics,
    compute_currency_quality_analysis,
    find_column,
)

logger = logging.getLogger(__name__)

CURRENCY_COLUMNS: list[str] = [
    "Local Currency Code",
    "PO Local Currency Code",
]

_CURRENCY_SPEND_PAIRS: dict[str, tuple[str, str]] = {
    "Local Currency Code": (
        "Total Amount paid in Local Currency",
        "Total Amount paid in Reporting Currency",
    ),
    "PO Local Currency Code": (
        "PO Total Amount in Local Currency",
        "PO Total Amount in reporting currency",
    ),
}


def run_currency_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """SQL-only phase: currency metrics and per-currency breakdown.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``aiInsight`` set to ``None``.
    """
    available = set(read_table_columns(conn, table_name))
    total_rows = table_row_count(conn, table_name)

    currency_col = find_column(available, CURRENCY_COLUMNS)

    if currency_col is None:
        return {
            "exists": False,
            "currencyColumn": None,
            "distinctCount": 0,
            "codes": [],
            "currencyTable": [],
            "hasLocalSpend": False,
            "hasReportingSpend": False,
            "aiInsight": "No currency columns found in the dataset.",
        }

    basic = compute_currency_metrics(conn, table_name, currency_col)

    matched_key = find_column(set(_CURRENCY_SPEND_PAIRS.keys()), [currency_col])
    pair = _CURRENCY_SPEND_PAIRS.get(matched_key, (None, None)) if matched_key else (None, None)
    local_spend = find_column(available, [pair[0]]) if pair[0] else None
    reporting_spend = find_column(available, [pair[1]]) if pair[1] else None

    currency_table = compute_currency_quality_analysis(
        conn, table_name, currency_col,
        local_spend, reporting_spend, total_rows,
    )

    return {
        "exists": True,
        "currencyColumn": currency_col,
        "distinctCount": basic["distinctCount"],
        "codes": basic["codes"],
        "currencyTable": currency_table,
        "hasLocalSpend": local_spend is not None,
        "hasReportingSpend": reporting_spend is not None,
        "aiInsight": None,
    }


def run_currency_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate insight from pre-computed SQL data.

    Safe to call without any database lock held.
    """
    if sql_result.get("aiInsight") is not None:
        return sql_result

    ai_payload = {
        "currencyTable": sql_result["currencyTable"],
        "distinctCount": sql_result["distinctCount"],
        "codes": sql_result["codes"],
    }
    try:
        sql_result["aiInsight"] = generate_currency_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Currency AI insight generation failed: %s", exc)
        sql_result["aiInsight"] = "AI insight generation failed."

    return sql_result
