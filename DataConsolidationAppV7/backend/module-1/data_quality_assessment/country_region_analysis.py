"""Country / Region analysis panel for the Data Quality Assessment.

Collects unique country and region values and asks AI whether standardisation
is needed.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_country_region_insight
from .column_resolver import find_country_columns, resolve_column
from .metrics import _non_null_condition

logger = logging.getLogger(__name__)


def _unique_values(
    conn: DuckDBConnection,
    table_name: str,
    column: str,
    limit: int = 1000,
) -> list[str]:
    """Return distinct non-empty values for a column, capped at *limit*."""
    tbl = quote_id(table_name)
    qc = quote_id(column)
    nn = _non_null_condition(qc)
    rows = conn.execute(
        f"SELECT DISTINCT TRIM({qc}) AS val FROM {tbl} "
        f"WHERE {nn} LIMIT ?",
        (limit,),
    ).fetchall()
    return [str(r["val"]) for r in rows]


def run_country_region_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    country_column: str | None = None,
) -> dict[str, Any]:
    """SQL-only phase: unique country/region values.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        country_column: User-selected country column override. If None,
            defaults to the first available country column.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with AI insight fields set to ``None``.
    """
    available = set(read_table_columns(conn, table_name))

    available_country_cols = find_country_columns(available)

    # Resolve user's selection or default to first available
    country_col: str | None = None
    if country_column and country_column in available:
        country_col = country_column
    elif available_country_cols:
        country_col = available_country_cols[0]

    country_values: list[str] | None = None
    if country_col:
        country_values = _unique_values(conn, table_name, country_col)

    region_col = resolve_column(available, "region", fuzzy=False)

    region_values: list[str] | None = None
    if region_col:
        region_values = _unique_values(conn, table_name, region_col)

    if country_col is None and region_col is None:
        return {
            "availableCountryColumns": available_country_cols,
            "countryColumn": None,
            "regionColumn": None,
            "countryValues": None,
            "regionValues": None,
            "countryAiInsight": None,
            "regionAiInsight": None,
        }

    return {
        "availableCountryColumns": available_country_cols,
        "countryColumn": country_col,
        "regionColumn": region_col,
        "countryValues": country_values,
        "regionValues": region_values,
        "countryAiInsight": None,
        "regionAiInsight": None,
    }


def run_country_region_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate insight from pre-computed SQL data.

    Safe to call without any database lock held.
    """
    if sql_result.get("countryAiInsight") is not None:
        return sql_result

    ai_payload = {
        "countryValues": sql_result["countryValues"],
        "regionValues": sql_result["regionValues"],
        "countryColumn": sql_result["countryColumn"],
        "regionColumn": sql_result["regionColumn"],
    }
    try:
        insights = generate_country_region_insight(ai_payload, api_key)
        sql_result["countryAiInsight"] = insights.get("countryInsight")
        sql_result["regionAiInsight"] = insights.get("regionInsight")
    except Exception as exc:
        logger.warning("Country/region AI insight generation failed: %s", exc)
        sql_result["countryAiInsight"] = (
            ["AI insight generation failed."] if sql_result["countryColumn"] else None
        )
        sql_result["regionAiInsight"] = (
            ["AI insight generation failed."] if sql_result["regionColumn"] else None
        )

    return sql_result
