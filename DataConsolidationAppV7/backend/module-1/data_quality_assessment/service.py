"""Orchestrator for the redesigned Data Quality Assessment.

Provides per-panel entry points split into SQL and AI phases so the route
layer can hold the session lock only during database work and release it
before making slow AI calls.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, read_table_columns, table_exists, table_row_count

from .ai_prompts import generate_entity_insights, generate_financial_insights
from .column_resolver_ai import suggest_columns_ai
from .currency_analysis import run_currency_analysis_sql, run_currency_analysis_ai
from .country_region_analysis import run_country_region_analysis_sql, run_country_region_analysis_ai
from .date_analysis import run_date_analysis_sql, run_date_analysis_ai
from .fill_rate_analysis import run_fill_rate_analysis, run_spend_bifurcation
from .payment_terms_analysis import run_payment_terms_analysis_sql, run_payment_terms_analysis_ai
from .supplier_analysis import run_supplier_analysis_sql, run_supplier_analysis_ai

logger = logging.getLogger(__name__)

__all__ = [
    "TableMissingError",
    "collect_column_samples",
    "run_dqa_suggest_columns",
    "run_dqa_date_sql",
    "run_dqa_date_ai",
    "run_dqa_currency_sql",
    "run_dqa_currency_ai",
    "run_dqa_payment_terms_sql",
    "run_dqa_payment_terms_ai",
    "run_dqa_country_region_sql",
    "run_dqa_country_region_ai",
    "run_dqa_supplier_sql",
    "run_dqa_supplier_ai",
    "run_dqa_fill_rate",
    "run_dqa_spend_bifurcation",
    "run_dqa_financial_ai",
    "run_dqa_entity_ai",
    "run_dqa_all_sql",
]


class TableMissingError(Exception):
    """Raised when a specific table is missing from the session DB,
    but the session itself is still alive (DB file exists)."""
    pass


def _validate_table(conn: DuckDBConnection, table_name: str) -> str:
    """Validate the table exists, with a limited fallback to ``final_merged``.

    Returns the resolved table name.

    Raises:
        TableMissingError: If the table doesn't exist in the session DB.
    """
    if table_exists(conn, table_name):
        return table_name

    # For versioned names, fall back to the canonical 'final_merged' table
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.warning(
            "Table '%s' not found, falling back to 'final_merged'", table_name,
        )
        return "final_merged"

    # Force a checkpoint to handle WAL visibility issues
    try:
        conn.execute("CHECKPOINT")
        conn.commit()
    except Exception:
        pass

    if table_exists(conn, table_name):
        logger.info("Table '%s' found after CHECKPOINT", table_name)
        return table_name
    if table_name.startswith("final_merged_v") and table_exists(conn, "final_merged"):
        logger.info(
            "Table 'final_merged' found after CHECKPOINT (was looking for '%s')",
            table_name,
        )
        return "final_merged"

    raise TableMissingError(f"Table '{table_name}' not found. Please re-run the Merge step.")


# ── Column suggestion (AI-assisted) ───────────────────────────────────────


def collect_column_samples(
    conn: DuckDBConnection,
    table_name: str,
    sample_size: int = 5,
) -> dict[str, list[str]]:
    """Read column names and a few non-null sample values for each column.

    Must be called under the session lock.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        sample_size: Max distinct non-null values to sample per column.

    Returns:
        Dict mapping column name -> list of sample value strings.
    """
    from shared.db import quote_id

    table_name = _validate_table(conn, table_name)
    columns = read_table_columns(conn, table_name)
    tbl = quote_id(table_name)
    samples: dict[str, list[str]] = {}
    for col in columns:
        qc = quote_id(col)
        rows = conn.execute(
            f"SELECT DISTINCT TRIM(CAST({qc} AS TEXT)) AS v "
            f"FROM {tbl} "
            f"WHERE {qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != '' "
            f"LIMIT ?",
            (sample_size,),
        ).fetchall()
        samples[col] = [str(r["v"]) for r in rows]
    return samples


def run_dqa_suggest_columns(
    column_samples: dict[str, list[str]],
    api_key: str,
) -> dict[str, list[str]]:
    """Ask the LLM to suggest the top-3 columns per DQA role.

    Safe to call without the session lock.
    """
    return suggest_columns_ai(column_samples, api_key)


# ── Per-panel dispatchers: SQL phase (call under session lock) ────────────


def run_dqa_date_sql(
    conn: DuckDBConnection,
    table_name: str,
    date_column: str | None = None,
) -> dict[str, Any]:
    """Date SQL phase — format detection + spend pivot. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_date_analysis_sql(conn, table_name, date_column)


def run_dqa_date_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Date AI phase — generate insight. Safe to call without lock."""
    return run_date_analysis_ai(sql_result, api_key)


def run_dqa_currency_sql(
    conn: DuckDBConnection,
    table_name: str,
    currency_column: str | None = None,
) -> dict[str, Any]:
    """Currency SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_currency_analysis_sql(conn, table_name, currency_column)


def run_dqa_currency_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Currency AI phase. Safe to call without lock."""
    return run_currency_analysis_ai(sql_result, api_key)


def run_dqa_payment_terms_sql(
    conn: DuckDBConnection,
    table_name: str,
    payment_terms_column: str | None = None,
) -> dict[str, Any]:
    """Payment terms SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_payment_terms_analysis_sql(conn, table_name, payment_terms_column)


def run_dqa_payment_terms_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Payment terms AI phase. Safe to call without lock."""
    return run_payment_terms_analysis_ai(sql_result, api_key)


def run_dqa_country_region_sql(
    conn: DuckDBConnection,
    table_name: str,
    country_column: str | None = None,
) -> dict[str, Any]:
    """Country/Region SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_country_region_analysis_sql(conn, table_name, country_column)


def run_dqa_country_region_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Country/Region AI phase. Safe to call without lock."""
    return run_country_region_analysis_ai(sql_result, api_key)


def run_dqa_supplier_sql(
    conn: DuckDBConnection,
    table_name: str,
    vendor_column: str | None = None,
) -> dict[str, Any]:
    """Supplier SQL phase. Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_supplier_analysis_sql(conn, table_name, vendor_column)


def run_dqa_supplier_ai(sql_result: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Supplier AI phase. Safe to call without lock."""
    return run_supplier_analysis_ai(sql_result, api_key)


def run_dqa_fill_rate(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Per-column fill rate (SQL only, no AI). Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_fill_rate_analysis(conn, table_name)


def run_dqa_spend_bifurcation(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Spend bifurcation (SQL only, no AI). Call under session lock."""
    table_name = _validate_table(conn, table_name)
    return run_spend_bifurcation(conn, table_name)


# ── Consolidated AI dispatchers ───────────────────────────────────────────


def run_dqa_financial_ai(
    date_sql: dict[str, Any] | None,
    currency_sql: dict[str, Any] | None,
    payment_sql: dict[str, Any] | None,
    api_key: str,
) -> dict[str, list[str] | None]:
    """Single LLM call for date + currency + payment terms insights.

    Safe to call without the session lock.
    """
    date_payload = _build_date_ai_payload(date_sql) if date_sql else None
    currency_payload = _build_currency_ai_payload(currency_sql) if currency_sql else None
    payment_payload = _build_payment_ai_payload(payment_sql) if payment_sql else None

    try:
        return generate_financial_insights(
            date_payload, currency_payload, payment_payload, api_key,
        )
    except Exception as exc:
        logger.warning("Financial AI insights failed: %s", exc)
        return {
            "dateInsight": None,
            "currencyInsight": None,
            "paymentTermsInsight": None,
        }


def run_dqa_entity_ai(
    country_sql: dict[str, Any] | None,
    supplier_sql: dict[str, Any] | None,
    api_key: str,
) -> dict[str, list[str] | None]:
    """Single LLM call for country/region + supplier insights.

    Safe to call without the session lock.
    """
    country_payload = None
    region_payload = None
    if country_sql:
        country_payload = {
            "countryValues": country_sql.get("countryValues"),
            "countryColumn": country_sql.get("countryColumn"),
        }
        region_payload = {
            "regionValues": country_sql.get("regionValues"),
            "regionColumn": country_sql.get("regionColumn"),
        }

    supplier_payload = None
    if supplier_sql and supplier_sql.get("exists"):
        supplier_payload = {
            "supplierNames": supplier_sql.get("_supplierNames", [])[:200],
            "supplierCount": supplier_sql.get("supplierCount"),
            "hasReportingSpend": supplier_sql.get("hasReportingSpend"),
            "top20": supplier_sql.get("top20", []),
        }

    try:
        return generate_entity_insights(
            country_payload, region_payload, supplier_payload, api_key,
        )
    except Exception as exc:
        logger.warning("Entity AI insights failed: %s", exc)
        return {
            "countryInsight": None,
            "regionInsight": None,
            "supplierInsight": None,
        }


def run_dqa_all_sql(
    conn: DuckDBConnection,
    table_name: str,
    date_column: str | None = None,
    country_column: str | None = None,
    currency_column: str | None = None,
    payment_terms_column: str | None = None,
    vendor_column: str | None = None,
) -> dict[str, Any]:
    """Run all SQL panels under a single lock acquisition.

    Returns a dict keyed by panel name with each panel's SQL result.
    """
    table_name = _validate_table(conn, table_name)
    return {
        "date": run_date_analysis_sql(conn, table_name, date_column),
        "currency": run_currency_analysis_sql(conn, table_name, currency_column),
        "paymentTerms": run_payment_terms_analysis_sql(conn, table_name, payment_terms_column),
        "countryRegion": run_country_region_analysis_sql(conn, table_name, country_column),
        "supplier": run_supplier_analysis_sql(conn, table_name, vendor_column),
        "fillRate": run_fill_rate_analysis(conn, table_name),
        "spendBifurcation": run_spend_bifurcation(conn, table_name),
    }


# ── Private payload builders for consolidated AI calls ────────────────────


def _build_date_ai_payload(sql: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields the AI prompt needs from date SQL results."""
    return {
        "formatTable": sql.get("formatTable"),
        "consistent": sql.get("consistent"),
        "pivotData": sql.get("pivotData"),
        "selectedColumn": sql.get("selectedColumn"),
    }


def _build_currency_ai_payload(sql: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields the AI prompt needs from currency SQL results."""
    return {
        "currencyTable": sql.get("currencyTable"),
        "distinctCount": sql.get("distinctCount"),
        "codes": sql.get("codes"),
    }


def _build_payment_ai_payload(sql: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields the AI prompt needs from payment terms SQL results."""
    return {
        "paymentTerms": sql.get("paymentTerms"),
        "totalSpend": sql.get("totalSpend"),
        "uniqueCount": sql.get("uniqueCount"),
    }
