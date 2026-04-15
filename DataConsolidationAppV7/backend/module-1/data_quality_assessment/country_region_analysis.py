"""Country / Region analysis panel for the Data Quality Assessment.

Collects unique country and region values and asks AI whether standardisation
is needed.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_country_region_insight
from .metrics import _non_null_condition, find_column

logger = logging.getLogger(__name__)

COUNTRY_COLUMNS: list[str] = [
    "Country Code",
    "Country",
    "Supplier Country",
    "Supplier_Country",
    "Vendor Country",
    "Vendor_Country",
    "Country of Origin",
]
REGION_COLUMN = "Region"


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


def run_country_region_analysis(
    conn: DuckDBConnection,
    table_name: str,
    api_key: str,
) -> dict[str, Any]:
    """Run country and region analysis for the DQA panel.

    Args:
        conn: SQLite session connection.
        table_name: Target table.
        api_key: API key for AI insight generation.

    Returns:
        JSON-serialisable dict with ``countryData``, ``regionData``,
        ``countryAiInsight``, ``regionAiInsight``.
    """
    available = set(read_table_columns(conn, table_name))

    # Country — pick the first available column (case-insensitive)
    country_col = find_column(available, COUNTRY_COLUMNS)

    country_values: list[str] | None = None
    if country_col:
        country_values = _unique_values(conn, table_name, country_col)

    # Region (case-insensitive)
    region_col = find_column(available, [REGION_COLUMN])

    region_values: list[str] | None = None
    if region_col:
        region_values = _unique_values(conn, table_name, region_col)

    if country_col is None and region_col is None:
        return {
            "countryColumn": None,
            "regionColumn": None,
            "countryValues": None,
            "regionValues": None,
            "countryAiInsight": "No country or region columns found in the dataset.",
            "regionAiInsight": None,
        }

    # AI insight
    ai_payload = {
        "countryValues": country_values,
        "regionValues": region_values,
        "countryColumn": country_col,
        "regionColumn": region_col,
    }
    try:
        insights = generate_country_region_insight(ai_payload, api_key)
        country_insight = insights.get("countryInsight")
        region_insight = insights.get("regionInsight")
    except Exception as exc:
        logger.warning("Country/region AI insight generation failed: %s", exc)
        country_insight = "AI insight generation failed." if country_col else None
        region_insight = "AI insight generation failed." if region_col else None

    return {
        "countryColumn": country_col,
        "regionColumn": region_col,
        "countryValues": country_values,
        "regionValues": region_values,
        "countryAiInsight": country_insight,
        "regionAiInsight": region_insight,
    }
