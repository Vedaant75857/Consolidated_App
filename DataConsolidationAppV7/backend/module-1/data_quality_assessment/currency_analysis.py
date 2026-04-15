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


def run_currency_analysis(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Run the full currency analysis for the DQA currency panel.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: API key for AI insight generation.

    Returns:
        JSON-serialisable dict with ``currencyTable``, ``distinctCount``,
        ``codes``, ``aiInsight``, and ``exists``.
    """
    available = set(read_table_columns(conn, table_name))
    total_rows = table_row_count(conn, table_name)

    # Find the first available currency column (case-insensitive)
    currency_col = find_column(available, CURRENCY_COLUMNS)

    if currency_col is None:
        return {
            "exists": False,
            "currencyColumn": None,
            "distinctCount": 0,
            "codes": [],
            "currencyTable": [],
            "aiInsight": "No currency columns found in the dataset.",
        }

    # Basic metrics (distinct count + code list)
    basic = compute_currency_metrics(conn, table_name, currency_col)

    # Paired spend columns -- look up via the canonical key that matched
    matched_key = find_column(set(_CURRENCY_SPEND_PAIRS.keys()), [currency_col])
    pair = _CURRENCY_SPEND_PAIRS.get(matched_key, (None, None)) if matched_key else (None, None)
    local_spend = find_column(available, [pair[0]]) if pair[0] else None
    reporting_spend = find_column(available, [pair[1]]) if pair[1] else None

    # Detailed per-currency breakdown
    currency_table = compute_currency_quality_analysis(
        conn, table_name, currency_col,
        local_spend, reporting_spend, total_rows,
    )

    # AI insight
    ai_payload = {
        "currencyTable": currency_table,
        "distinctCount": basic["distinctCount"],
        "codes": basic["codes"],
    }
    try:
        ai_insight = generate_currency_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Currency AI insight generation failed: %s", exc)
        ai_insight = "AI insight generation failed."

    return {
        "exists": True,
        "currencyColumn": currency_col,
        "distinctCount": basic["distinctCount"],
        "codes": basic["codes"],
        "currencyTable": currency_table,
        "hasLocalSpend": local_spend is not None,
        "hasReportingSpend": reporting_spend is not None,
        "aiInsight": ai_insight,
    }
