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

import pandas as pd

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
            SUM(TRY_CAST(total_spend AS DOUBLE)) AS spend
        FROM "analysis_data"
        WHERE invoice_date IS NOT NULL
          AND TRIM(CAST(invoice_date AS TEXT)) != ''
          AND TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
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

    Uses the same supplier-grouped approach as the Dashboard's compute_pareto:
      1. Load rows into a DataFrame, coerce total_spend to numeric
      2. Drop rows where supplier or total_spend is missing
      3. Exclude negative/zero spend from the ranking
      4. Group by supplier, sum spend per supplier
      5. Sort descending, compute cumulative %
      6. Walk suppliers to find the cutoff index for each threshold
      7. For each threshold, go back to the raw rows to count transactions

    Args:
        conn: DuckDB session connection.
        available_columns: Set of column names present in analysis_data.

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

    has_supplier = "supplier" in available_columns

    # Determine which description columns are available for unique-transaction counting
    desc_cols = [fk for fk in DESCRIPTION_FIELD_KEYS if fk in available_columns]

    # --- Load raw data into a DataFrame (mirrors view_engine._load_analysis_df) ---
    select_cols = ["total_spend"]
    if has_supplier:
        select_cols.append("supplier")
    select_cols.extend(desc_cols)

    quoted = ", ".join(_quote_id(c) for c in select_cols)
    df = conn._conn.execute(f'SELECT {quoted} FROM "analysis_data"').df()

    df["total_spend"] = pd.to_numeric(df["total_spend"], errors="coerce")

    if has_supplier:
        df["supplier"] = (
            df["supplier"].astype(str)
            .replace({"nan": None, "None": None, "": None})
        )
    else:
        df["supplier"] = None

    # Build best_description from the first non-empty description column per row
    if desc_cols:
        for dc in desc_cols:
            df[dc] = df[dc].astype(str).replace({"nan": None, "None": None, "": None})
        df["_best_desc"] = df[desc_cols].bfill(axis=1).iloc[:, 0].fillna("")
    else:
        df["_best_desc"] = ""

    # --- Clean: drop rows with missing supplier or spend ---
    work = df.dropna(subset=["supplier", "total_spend"]).copy()
    work = work[work["supplier"].str.strip() != ""]

    # Exclude negative/zero spend from Pareto ranking
    positive = work[work["total_spend"] > 0].copy()

    if positive.empty:
        return {
            "thresholds": PARETO_THRESHOLDS,
            "metrics": {},
            "feasible": False,
            "message": "No positive spend data found after filtering.",
            "totalDatasetSpend": 0,
        }

    # --- Group by supplier, sum spend ---
    supplier_spend = (
        positive.groupby("supplier")["total_spend"]
        .sum()
        .reset_index()
        .sort_values("total_spend", ascending=False)
        .reset_index(drop=True)
    )
    grand_total = supplier_spend["total_spend"].sum()
    supplier_spend["cum_pct"] = (
        supplier_spend["total_spend"].cumsum() / max(grand_total, 1e-9) * 100
    ).round(4)

    # --- Walk thresholds ---
    metrics: dict[str, dict[str, Any]] = {}

    for t in PARETO_THRESHOLDS:
        # Find the first supplier index where cumulative % reaches the threshold
        crossed = supplier_spend[supplier_spend["cum_pct"] >= t].index
        if len(crossed) > 0:
            cutoff_idx = crossed[0]
        else:
            cutoff_idx = len(supplier_spend) - 1

        top_suppliers = supplier_spend.loc[:cutoff_idx]
        top_names = set(top_suppliers["supplier"])
        bucket_spend = top_suppliers["total_spend"].sum()

        # Go back to the raw positive-spend rows for those suppliers
        bucket_rows = positive[positive["supplier"].isin(top_names)]
        txn_count = len(bucket_rows)
        unique_txns = bucket_rows.apply(
            lambda r: (r["supplier"] or "") + "|||" + (r["_best_desc"] or ""),
            axis=1,
        ).nunique()

        metrics[str(t)] = {
            "totalSpend": round(bucket_spend),
            "transactionCount": txn_count,
            "uniqueTransactions": unique_txns,
            "supplierCount": len(top_names),
        }

    return {
        "thresholds": PARETO_THRESHOLDS,
        "metrics": metrics,
        "feasible": True,
        "totalDatasetSpend": round(grand_total),
    }


# ═══════════════════════════════════════════════════════════════════════════
# D.  Spend Bifurcation
# ═══════════════════════════════════════════════════════════════════════════

def _compute_spend_bifurcation(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute positive/negative spend for both reporting and local currencies.

    Returns both views so the frontend can toggle between them.
    """
    result: dict[str, Any] = {"reporting": None, "local": None}

    # Reporting currency (total_spend)
    if "total_spend" in available:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) > 0
                    THEN CAST(total_spend AS REAL) ELSE 0 END) AS pos,
                SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) < 0
                    THEN CAST(total_spend AS REAL) ELSE 0 END) AS neg
            FROM "analysis_data"
            """
        ).fetchone()
        result["reporting"] = {
            "positiveSpend": round(float(row[0] or 0)),
            "negativeSpend": round(float(row[1] or 0)),
        }

    # Local currency (grouped by currency code when available, aggregate otherwise)
    if "local_spend" in available and "currency" in available:
        rows = conn.execute(
            """
            SELECT
                UPPER(TRIM(CAST(currency AS TEXT))) AS code,
                SUM(CASE WHEN TRY_CAST(local_spend AS DOUBLE) > 0
                    THEN CAST(local_spend AS REAL) ELSE 0 END) AS pos,
                SUM(CASE WHEN TRY_CAST(local_spend AS DOUBLE) < 0
                    THEN CAST(local_spend AS REAL) ELSE 0 END) AS neg
            FROM "analysis_data"
            WHERE currency IS NOT NULL AND TRIM(CAST(currency AS TEXT)) != ''
            GROUP BY UPPER(TRIM(CAST(currency AS TEXT)))
            ORDER BY pos DESC
            """
        ).fetchall()
        result["local"] = [
            {
                "code": str(r[0]),
                "positiveSpend": round(float(r[1] or 0)),
                "negativeSpend": round(float(r[2] or 0)),
            }
            for r in rows
        ]
    elif "local_spend" in available:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN TRY_CAST(local_spend AS DOUBLE) > 0
                    THEN CAST(local_spend AS REAL) ELSE 0 END) AS pos,
                SUM(CASE WHEN TRY_CAST(local_spend AS DOUBLE) < 0
                    THEN CAST(local_spend AS REAL) ELSE 0 END) AS neg
            FROM "analysis_data"
            """
        ).fetchone()
        result["local"] = {
            "positiveSpend": round(float(row[0] or 0)),
            "negativeSpend": round(float(row[1] or 0)),
        }

    return result


# ═══════════════════════════════════════════════════════════════════════════
# E.  Public Orchestrator
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

    # Panel 1b: Spend Bifurcation
    spend_bifurcation = _compute_spend_bifurcation(conn, available)

    # Panel 2: Pareto Analysis
    pareto = _compute_pareto_analysis(conn, available)

    # Panel 3: Description Quality Analysis (with AI)
    desc_quality = run_description_quality_analysis(conn, available, api_key)

    return {
        "totalRows": total_rows,
        "datePivot": date_pivot,
        "spendBifurcation": spend_bifurcation,
        "paretoAnalysis": pareto,
        "descriptionQuality": desc_quality,
    }
