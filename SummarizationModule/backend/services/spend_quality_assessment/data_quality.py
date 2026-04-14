"""Spend Quality Assessment – Data Quality for the Spend Summarizer.

Rebuilt to produce three panels:
  1. Date Spend Pivot (years x months)
  2. Combined Spend + Supplier Pareto Analysis (80/85/90/95/99%)
  3. Description Quality Analysis (per description column)

Operates on the ``analysis_data`` table produced by column mapping.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.duckdb_compat import DuckDBConnection
from services.spend_quality_assessment.description_quality import (
    DESCRIPTION_FIELD_KEYS,
    run_description_quality_analysis,
)

logger = logging.getLogger(__name__)

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

PARETO_THRESHOLDS = [80, 85, 90, 95, 99]


# ═══════════════════════════════════════════════════════════════════════════
# A.  SQL Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _quote_id(name: str) -> str:
    return f'"{name}"'


def _nn(qc: str) -> str:
    return f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"


# ═══════════════════════════════════════════════════════════════════════════
# B.  Panel 1 – Date Spend Pivot
# ═══════════════════════════════════════════════════════════════════════════

def _compute_date_spend_pivot(
    conn: DuckDBConnection,
) -> dict[str, Any]:
    """Build a year x month pivot of total_spend from invoice_date.

    Returns:
        {
          "years": [2022, 2023, ...],
          "months": ["Jan", ..., "Dec"],
          "cells": {"2022": {"1": 123, "2": 456, ...}, ...},
          "feasible": True/False
        }
    """
    pragma = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'analysis_data' ORDER BY ordinal_position"
    ).fetchall()
    cols = {str(r[0]) for r in pragma}

    if "invoice_date" not in cols or "total_spend" not in cols:
        return {
            "years": [],
            "months": MONTH_NAMES,
            "cells": {},
            "feasible": False,
            "message": "invoice_date or total_spend not mapped.",
        }

    rows = conn.execute(
        """
        SELECT
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) AS yr,
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) AS mo,
            SUM(CASE
                WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
                THEN CAST(total_spend AS REAL) ELSE 0 END) AS spend
        FROM "analysis_data"
        WHERE invoice_date IS NOT NULL
          AND TRIM(CAST(invoice_date AS TEXT)) != ''
          AND TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
        GROUP BY yr, mo
        ORDER BY yr, mo
        """
    ).fetchall()

    if not rows:
        return {
            "years": [],
            "months": MONTH_NAMES,
            "cells": {},
            "feasible": False,
            "message": "No valid date-spend data found.",
        }

    cells: dict[str, dict[str, float]] = {}
    year_set: set[int] = set()

    for yr, mo, spend in rows:
        if yr is None or mo is None:
            continue
        yr_str = str(int(yr))
        mo_str = str(int(mo))
        year_set.add(int(yr))
        if yr_str not in cells:
            cells[yr_str] = {}
        cells[yr_str][mo_str] = round(float(spend or 0))

    years = sorted(year_set)

    # Fill missing months with 0
    for yr in years:
        yr_str = str(yr)
        if yr_str not in cells:
            cells[yr_str] = {}
        for m in range(1, 13):
            cells[yr_str].setdefault(str(m), 0)

    return {
        "years": years,
        "months": MONTH_NAMES,
        "cells": cells,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# C.  Panel 2 – Combined Spend + Supplier Pareto Analysis
# ═══════════════════════════════════════════════════════════════════════════

def _compute_pareto_analysis(
    conn: DuckDBConnection,
    available_columns: set[str],
) -> dict[str, Any]:
    """Compute Pareto metrics at 80/85/90/95/99% spend thresholds.

    For each threshold, returns:
      - totalSpend: cumulative spend for rows in the top X%
      - transactionCount: number of rows
      - uniqueTransactions: unique (vendor + best_description) combos
      - supplierCount: unique vendors

    The "best description" uses the fallback chain:
    invoice_description -> po_description -> material_description -> gl_account_description

    Returns:
        {
          "thresholds": [80, 85, 90, 95, 99],
          "metrics": { "80": {...}, "85": {...}, ... },
          "feasible": True/False,
          "totalDatasetSpend": float
        }
    """
    if "total_spend" not in available_columns:
        return {
            "thresholds": PARETO_THRESHOLDS,
            "metrics": {},
            "feasible": False,
            "message": "total_spend not mapped.",
            "totalDatasetSpend": 0,
        }

    desc_cols_available = [
        fk for fk in DESCRIPTION_FIELD_KEYS if fk in available_columns
    ]
    if desc_cols_available:
        coalesce_parts = ", ".join(
            f"NULLIF(TRIM(CAST({_quote_id(fk)} AS TEXT)), '')"
            for fk in desc_cols_available
        )
        best_desc_expr = f"COALESCE({coalesce_parts}, '')"
    else:
        best_desc_expr = "''"

    supplier_expr = (
        f"TRIM(CAST({_quote_id('supplier')} AS TEXT))"
        if "supplier" in available_columns else "''"
    )

    # Pure SQL Pareto: use window functions to avoid loading all rows into Python
    total_row = conn.execute(
        "SELECT SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL "
        "THEN TRY_CAST(total_spend AS REAL) ELSE 0 END) AS total "
        'FROM "analysis_data"'
    ).fetchone()
    total_spend = float(total_row[0] or 0) if total_row else 0.0

    if total_spend <= 0:
        return {
            "thresholds": PARETO_THRESHOLDS,
            "metrics": {},
            "feasible": False,
            "message": "Total spend is zero or no spend data found.",
            "totalDatasetSpend": 0,
        }

    # Build a CTE with cumulative spend via window function, then aggregate
    # per-threshold metrics entirely in SQL
    thresholds_sql = ", ".join(str(t) for t in PARETO_THRESHOLDS)
    pareto_sql = f"""
    WITH ranked AS (
        SELECT
            CASE WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
                 THEN TRY_CAST(total_spend AS REAL) ELSE 0 END AS spend,
            {supplier_expr} AS vendor,
            {best_desc_expr} AS best_desc,
            SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
                     THEN TRY_CAST(total_spend AS REAL) ELSE 0 END)
                OVER (ORDER BY CASE WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
                                    THEN TRY_CAST(total_spend AS REAL) ELSE 0 END DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_spend
        FROM "analysis_data"
    ),
    thresholds(t) AS (VALUES {', '.join(f'({t})' for t in PARETO_THRESHOLDS)})
    SELECT
        t.t AS threshold,
        COALESCE(ROUND(MAX(r.cum_spend)), 0) AS total_spend,
        COUNT(r.spend) AS txn_count,
        COUNT(DISTINCT (r.vendor || '|||' || r.best_desc)) AS unique_txn,
        COUNT(DISTINCT CASE WHEN r.vendor != '' THEN r.vendor END) AS supplier_count
    FROM thresholds t
    LEFT JOIN ranked r ON r.cum_spend <= {total_spend} * t.t / 100.0
                       OR r.cum_spend = (
                           SELECT MIN(cum_spend) FROM ranked
                           WHERE cum_spend >= {total_spend} * t.t / 100.0
                       )
    WHERE r.cum_spend <= (
        SELECT MIN(cum_spend) FROM ranked
        WHERE cum_spend >= {total_spend} * t.t / 100.0
    )
    GROUP BY t.t
    ORDER BY t.t
    """

    try:
        rows = conn.execute(pareto_sql).fetchall()
    except Exception:
        logger.warning("SQL Pareto failed, falling back to Python-based computation")
        rows = []

    metrics: dict[str, dict[str, Any]] = {}

    if rows:
        for r in rows:
            t = int(r[0])
            metrics[str(t)] = {
                "totalSpend": int(r[1]),
                "transactionCount": int(r[2]),
                "uniqueTransactions": int(r[3]),
                "supplierCount": int(r[4]),
            }
    else:
        # Fallback: lightweight Python computation using fetchmany
        cursor = conn.execute(
            f"SELECT "
            f"  CASE WHEN TRY_CAST(total_spend AS DOUBLE) IS NOT NULL "
            f"       THEN TRY_CAST(total_spend AS REAL) ELSE 0 END AS spend, "
            f"  {supplier_expr} AS vendor, "
            f"  {best_desc_expr} AS best_desc "
            f'FROM "analysis_data" '
            f"ORDER BY spend DESC"
        )
        cumulative = 0.0
        txn_count = 0
        vendors: set[str] = set()
        unique_combos: set[str] = set()
        threshold_idx = 0

        while True:
            batch = cursor.fetchmany(5000)
            if not batch:
                break
            for spend_val, vendor, best_desc in batch:
                spend_f = float(spend_val or 0)
                cumulative += spend_f
                txn_count += 1
                if vendor:
                    vendors.add(vendor)
                unique_combos.add(f"{vendor or ''}|||{best_desc or ''}")

                while (
                    threshold_idx < len(PARETO_THRESHOLDS)
                    and cumulative >= total_spend * PARETO_THRESHOLDS[threshold_idx] / 100.0
                ):
                    t = PARETO_THRESHOLDS[threshold_idx]
                    metrics[str(t)] = {
                        "totalSpend": round(cumulative),
                        "transactionCount": txn_count,
                        "uniqueTransactions": len(unique_combos),
                        "supplierCount": len(vendors),
                    }
                    threshold_idx += 1

        while threshold_idx < len(PARETO_THRESHOLDS):
            t = PARETO_THRESHOLDS[threshold_idx]
            metrics[str(t)] = {
                "totalSpend": round(cumulative),
                "transactionCount": txn_count,
                "uniqueTransactions": len(unique_combos),
                "supplierCount": len(vendors),
            }
            threshold_idx += 1

    # Fill any thresholds not yet in metrics
    for t in PARETO_THRESHOLDS:
        if str(t) not in metrics:
            metrics[str(t)] = {
                "totalSpend": round(total_spend),
                "transactionCount": 0,
                "uniqueTransactions": 0,
                "supplierCount": 0,
            }

    return {
        "thresholds": PARETO_THRESHOLDS,
        "metrics": metrics,
        "feasible": True,
        "totalDatasetSpend": round(total_spend),
    }


# ═══════════════════════════════════════════════════════════════════════════
# D.  Public Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def run_executive_summary(
    conn: DuckDBConnection,
    api_key: str,
) -> dict[str, Any]:
    """Run the full Spend Quality Assessment pipeline on ``analysis_data``.

    Returns:
        JSON-serialisable dict with three panels:
        - datePivot: year x month spend matrix
        - paretoAnalysis: spend/transaction/supplier metrics at multiple thresholds
        - descriptionQuality: per-description-column analysis with AI insights
    """
    exists = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'analysis_data'"
    ).fetchone()
    if not exists:
        raise ValueError(
            "Table 'analysis_data' not found. Please confirm column mapping first."
        )

    total_rows: int = conn.execute(
        'SELECT COUNT(*) FROM "analysis_data"'
    ).fetchone()[0]

    pragma_rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'analysis_data' ORDER BY ordinal_position"
    ).fetchall()
    available: set[str] = {str(r[0]) for r in pragma_rows}

    logger.info(
        "Running Spend Quality Assessment: %d rows, %d columns",
        total_rows, len(available),
    )

    # Panel 1: Date Spend Pivot
    date_pivot = _compute_date_spend_pivot(conn)

    # Panel 2: Pareto Analysis
    pareto = _compute_pareto_analysis(conn, available)

    # Panel 3: Description Quality Analysis (with AI)
    desc_quality = run_description_quality_analysis(conn, available, api_key)

    return {
        "totalRows": total_rows,
        "datePivot": date_pivot,
        "paretoAnalysis": pareto,
        "descriptionQuality": desc_quality,
    }
