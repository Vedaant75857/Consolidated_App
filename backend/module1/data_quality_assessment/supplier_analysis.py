"""Supplier analysis panel for the Data Quality Assessment.

Extracts top suppliers by spend, sends alphabetically sorted names to AI for
normalisation opportunity detection.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns

from .ai_prompts import generate_supplier_insight
from .column_resolver import find_supplier_columns, resolve_column
from .metrics import _non_null_condition, _safe_pct, numeric_spend_expr

logger = logging.getLogger(__name__)

TOP_N = 1000


def run_supplier_analysis_sql(
    conn: DuckDBConnection,
    table_name: str,
    vendor_column: str | None = None,
) -> dict[str, Any]:
    """SQL-only phase: top suppliers by spend and unique count.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        vendor_column: User-selected vendor/supplier column override.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with ``aiInsight`` set to ``None``.
        Includes a ``_supplierNames`` key (list) used by the AI phase
        but stripped before returning to the client.
    """
    available = set(read_table_columns(conn, table_name))
    available_supplier_cols = find_supplier_columns(available)

    if vendor_column and vendor_column in available:
        vendor_col = vendor_column
    else:
        vendor_col = resolve_column(available, "vendor_name", fuzzy=True)

    if vendor_col is None:
        return {
            "exists": False,
            "availableSupplierColumns": available_supplier_cols,
            "supplierCount": 0,
            "topNCount": 0,
            "totalSpendCovered": 0,
            "spendColumn": None,
            "hasReportingSpend": False,
            "paretoVendorCount": None,
            "paretoVendorPct": None,
            "top20": [],
            "aiInsight": None,
            "_supplierNames": [],
        }

    tbl = quote_id(table_name)
    qv = quote_id(vendor_col)
    nn = _non_null_condition(qv)

    reporting_spend_col = resolve_column(available, "spend_reporting", fuzzy=False)
    spend_col = reporting_spend_col
    has_reporting = reporting_spend_col is not None
    if not spend_col:
        spend_col = resolve_column(available, "spend_local", fuzzy=False)

    if spend_col:
        qs = quote_id(spend_col)
        spend_expr = numeric_spend_expr(qs)

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

        # Pareto (80/20) analysis
        pareto_count = 0
        cumulative = 0.0
        threshold = total_spend_covered * 0.80
        for r in rows:
            cumulative += float(r["spend"] or 0)
            pareto_count += 1
            if cumulative >= threshold:
                break

        # Top 20 suppliers by spend (for deep dive table)
        top20 = [
            {"vendor": str(r["vendor"]), "spend": round(float(r["spend"] or 0))}
            for r in rows[:20]
        ]
    else:
        rows = conn.execute(
            f"SELECT DISTINCT TRIM({qv}) AS vendor "
            f"FROM {tbl} WHERE {nn} LIMIT ?",
            (TOP_N,),
        ).fetchall()

        supplier_names = sorted(str(r["vendor"]) for r in rows)
        total_spend_covered = 0
        pareto_count = 0
        top20 = []

    total_unique_row = conn.execute(
        f"SELECT COUNT(DISTINCT TRIM({qv})) AS cnt FROM {tbl} WHERE {nn}"
    ).fetchone()
    total_unique = int(total_unique_row["cnt"] or 0)

    return {
        "exists": True,
        "availableSupplierColumns": available_supplier_cols,
        "supplierCount": total_unique,
        "topNCount": len(supplier_names),
        "totalSpendCovered": round(total_spend_covered),
        "spendColumn": spend_col,
        "hasReportingSpend": has_reporting,
        "paretoVendorCount": pareto_count if spend_col else None,
        "paretoVendorPct": _safe_pct(pareto_count, total_unique) if spend_col and total_unique > 0 else None,
        "top20": top20,
        "aiInsight": None,
        "_supplierNames": supplier_names,
    }


def run_supplier_analysis_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate insight from pre-computed SQL data.

    Safe to call without any database lock held.
    Removes the internal ``_supplierNames`` key before returning.
    """
    supplier_names = sql_result.pop("_supplierNames", [])

    if sql_result.get("aiInsight") is not None:
        return sql_result

    ai_payload = {
        "supplierNames": supplier_names,
        "count": len(supplier_names),
        "totalUniqueSuppliers": sql_result["supplierCount"],
    }
    try:
        sql_result["aiInsight"] = generate_supplier_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Supplier AI insight generation failed: %s", exc)
        sql_result["aiInsight"] = ["AI insight generation failed."]

    return sql_result
