"""Spend Quality Assessment – Data Quality for the Spend Summarizer.

Produces the following panels for the Executive Summary:
  1. Date Spend Pivot (years x months)
  2. Combined Spend + Supplier Pareto Analysis (80/85/90/95%)
  3. Description Quality Analysis (per description column)
  4. Date Period (min/max invoice_date with period label)
  5. Spend Breakdown (LTM, FY current/prior, YoY)
  6. Supplier Breakdown (count, 80% threshold, top-10, duplicates)
  7. Categorization Effort (description stats + AI cost estimate)
  8. Flags (spend consistency, description/vendor quality, null columns)
  9. Column Fill Rate (fill rate + spend coverage per column)

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
    _generate_categorization_recommendation,
)

logger = logging.getLogger(__name__)

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

PARETO_THRESHOLDS = [80, 85, 90, 95]

# GPT-4o public rates as placeholder; TODO: swap to gpt-5.4 rates when provided
GPT_INPUT_RATE_PER_TOKEN = 2.50 / 1_000_000   # $2.50 per 1M input tokens
GPT_OUTPUT_RATE_PER_TOKEN = 10.00 / 1_000_000  # $10.00 per 1M output tokens
AVG_CHARS_PER_TOKEN = 4  # standard OpenAI estimate
AVG_OUTPUT_TOKENS_PER_ROW = 20  # estimated tokens per categorization label
ROW_COUNT_THRESHOLD_CREACTIVES = 400_000


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
        pos = float(row[0] or 0)
        neg = float(row[1] or 0)

        neg_row_count = conn.execute(
            'SELECT COUNT(*) FROM "analysis_data" '
            "WHERE TRY_CAST(total_spend AS DOUBLE) < 0"
        ).fetchone()[0]

        result["reporting"] = {
            "positiveSpend": round(pos),
            "negativeSpend": round(neg),
            "negPctOfPos": round(abs(neg) / pos * 100, 1) if pos > 0 else 0,
            "negRowCount": int(neg_row_count or 0),
            "netSpend": round(pos + neg),
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
# D2. Date Period
# ═══════════════════════════════════════════════════════════════════════════

def _compute_date_period(
    conn: DuckDBConnection,
) -> dict[str, Any]:
    """Extract MIN/MAX of invoice_date and compute period metadata.

    Args:
        conn: DuckDB session connection.

    Returns:
        Dict with startDate, endDate, periodLabel, monthsCovered, and feasible flag.
        If invoice_date is not mapped, returns feasible=False with a message.
    """
    pragma = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'analysis_data' ORDER BY ordinal_position"
    ).fetchall()
    cols = {str(r[0]) for r in pragma}

    if "invoice_date" not in cols:
        return {"feasible": False, "message": "invoice_date not mapped."}

    row = conn.execute(
        """
        SELECT
            MIN(TRY_CAST(invoice_date AS TIMESTAMP)),
            MAX(TRY_CAST(invoice_date AS TIMESTAMP))
        FROM "analysis_data"
        WHERE invoice_date IS NOT NULL
          AND TRIM(CAST(invoice_date AS TEXT)) != ''
          AND TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
        """
    ).fetchone()

    if not row or row[0] is None or row[1] is None:
        return {"feasible": False, "message": "No valid invoice dates found."}

    min_date = row[0]
    max_date = row[1]

    start_label = min_date.strftime("%b %Y")
    end_label = max_date.strftime("%b %Y")

    # Count distinct year-month combinations
    months_row = conn.execute(
        """
        SELECT COUNT(DISTINCT (
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS TEXT) || '-' ||
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS TEXT)
        ))
        FROM "analysis_data"
        WHERE invoice_date IS NOT NULL
          AND TRIM(CAST(invoice_date AS TEXT)) != ''
          AND TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
        """
    ).fetchone()
    months_covered = int(months_row[0] or 0)

    return {
        "startDate": min_date.strftime("%Y-%m-%d"),
        "endDate": max_date.strftime("%Y-%m-%d"),
        "periodLabel": f"{start_label} \u2013 {end_label}",
        "monthsCovered": months_covered,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D3. Spend Breakdown (LTM + FY)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_spend_breakdown(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute LTM spend and fiscal-year spend with year-over-year change.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.

    Returns:
        Dict with ltmSpend, currentFySpend, priorFySpend, YoY metrics, and feasible flag.
    """
    if "total_spend" not in available or "invoice_date" not in available:
        return {"feasible": False, "message": "total_spend or invoice_date not mapped."}

    # Reference date = max date in the dataset
    max_row = conn.execute(
        """
        SELECT MAX(TRY_CAST(invoice_date AS TIMESTAMP))
        FROM "analysis_data"
        WHERE invoice_date IS NOT NULL
          AND TRIM(CAST(invoice_date AS TEXT)) != ''
          AND TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
        """
    ).fetchone()

    if not max_row or max_row[0] is None:
        return {"feasible": False, "message": "No valid invoice dates found."}

    max_date = max_row[0]
    max_year = max_date.year
    max_month = max_date.month

    # LTM: 12 months prior to and including the max date month
    ltm_row = conn.execute(
        f"""
        SELECT SUM(TRY_CAST(total_spend AS DOUBLE))
        FROM "analysis_data"
        WHERE TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
          AND (
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) * 12 +
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER)
          ) > ({max_year} * 12 + {max_month} - 12)
          AND (
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) * 12 +
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER)
          ) <= ({max_year} * 12 + {max_month})
        """
    ).fetchone()
    ltm_spend = float(ltm_row[0] or 0)

    # Per-year spend
    year_rows = conn.execute(
        """
        SELECT
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) AS yr,
            SUM(TRY_CAST(total_spend AS DOUBLE)) AS spend
        FROM "analysis_data"
        WHERE TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
        GROUP BY yr
        ORDER BY yr
        """
    ).fetchall()

    year_spend = {int(r[0]): float(r[1] or 0) for r in year_rows if r[0] is not None}

    current_fy_spend = year_spend.get(max_year, 0.0)
    prior_fy_spend = year_spend.get(max_year - 1, 0.0)

    yoy_abs = current_fy_spend - prior_fy_spend
    yoy_pct = (
        ((current_fy_spend - prior_fy_spend) / abs(prior_fy_spend)) * 100
        if prior_fy_spend != 0 else 0.0
    )

    return {
        "ltmSpend": round(ltm_spend),
        "currentFySpend": round(current_fy_spend),
        "priorFySpend": round(prior_fy_spend),
        "currentFyLabel": f"FY{max_year}",
        "priorFyLabel": f"FY{max_year - 1}",
        "yoyAbs": round(yoy_abs),
        "yoyPct": round(yoy_pct, 1),
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D4. Supplier Breakdown
# ═══════════════════════════════════════════════════════════════════════════

def _compute_supplier_breakdown(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute supplier statistics: count, 80% threshold, top-10, and duplicates.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.

    Returns:
        Dict with totalSuppliers, suppliersTo80Pct, top10, duplicateNameFlags,
        and feasible flag.
    """
    if "supplier" not in available or "total_spend" not in available:
        return {"feasible": False, "message": "supplier or total_spend not mapped."}

    # Group by supplier, sum spend, rank descending
    rows = conn.execute(
        """
        SELECT
            TRIM(CAST(supplier AS TEXT)) AS sup,
            SUM(TRY_CAST(total_spend AS DOUBLE)) AS spend
        FROM "analysis_data"
        WHERE supplier IS NOT NULL
          AND TRIM(CAST(supplier AS TEXT)) != ''
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
        GROUP BY sup
        ORDER BY spend DESC
        """
    ).fetchall()

    if not rows:
        return {"feasible": False, "message": "No valid supplier-spend data."}

    total_suppliers = len(rows)
    grand_total = sum(float(r[1] or 0) for r in rows)

    # Find how many suppliers cover 80% of spend
    cumulative = 0.0
    suppliers_to_80 = 0
    threshold_80 = grand_total * 0.80
    for r in rows:
        cumulative += float(r[1] or 0)
        suppliers_to_80 += 1
        if cumulative >= threshold_80:
            break

    # Top 10 suppliers
    top10 = []
    for r in rows[:10]:
        spend = float(r[1] or 0)
        share = (spend / grand_total * 100) if grand_total > 0 else 0.0
        top10.append({
            "supplier": str(r[0]),
            "spend": round(spend),
            "sharePct": round(share, 1),
        })

    # Duplicate-like names: LOWER(TRIM(name)) appears more than once
    dup_row = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT LOWER(TRIM(CAST(supplier AS TEXT))) AS norm
            FROM "analysis_data"
            WHERE supplier IS NOT NULL
              AND TRIM(CAST(supplier AS TEXT)) != ''
            GROUP BY TRIM(CAST(supplier AS TEXT))
        ) sub
        WHERE norm IN (
            SELECT LOWER(TRIM(CAST(supplier AS TEXT)))
            FROM "analysis_data"
            WHERE supplier IS NOT NULL
              AND TRIM(CAST(supplier AS TEXT)) != ''
            GROUP BY TRIM(CAST(supplier AS TEXT))
            HAVING COUNT(DISTINCT TRIM(CAST(supplier AS TEXT))) > 0
        )
        """
    ).fetchone()

    # Simpler approach: count suppliers whose normalised form has multiple variants
    dup_count_row = conn.execute(
        """
        SELECT SUM(variant_count) FROM (
            SELECT LOWER(TRIM(CAST(supplier AS TEXT))) AS norm,
                   COUNT(DISTINCT TRIM(CAST(supplier AS TEXT))) AS variant_count
            FROM "analysis_data"
            WHERE supplier IS NOT NULL
              AND TRIM(CAST(supplier AS TEXT)) != ''
            GROUP BY norm
            HAVING COUNT(DISTINCT TRIM(CAST(supplier AS TEXT))) > 1
        )
        """
    ).fetchone()
    duplicate_flags = int(dup_count_row[0] or 0) if dup_count_row and dup_count_row[0] else 0

    return {
        "totalSuppliers": total_suppliers,
        "suppliersTo80Pct": suppliers_to_80,
        "top10": top10,
        "duplicateNameFlags": duplicate_flags,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D5. Categorization Effort
# ═══════════════════════════════════════════════════════════════════════════

def _compute_categorization_effort(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute description quality metrics and estimated AI categorization cost.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.

    Returns:
        Dict with metrics, mapAICost, forcedMethod, top1000Descriptions,
        and feasible flag.
    """
    if "description" not in available:
        return {"feasible": False, "message": "description not mapped."}

    has_spend = "total_spend" in available
    has_supplier = "supplier" in available
    qd = _quote_id("description")
    nn_d = _nn(qd)

    # Row count
    row_count = conn.execute('SELECT COUNT(*) FROM "analysis_data"').fetchone()[0]

    # Description stats
    stats_row = conn.execute(
        f"""
        SELECT
            AVG(LENGTH(TRIM(CAST({qd} AS TEXT))) - LENGTH(REPLACE(TRIM(CAST({qd} AS TEXT)), ' ', '')) + 1) AS avg_words,
            AVG(LENGTH(TRIM(CAST({qd} AS TEXT)))) AS avg_chars,
            COUNT(*) AS populated,
            COUNT(DISTINCT TRIM(CAST({qd} AS TEXT))) AS unique_ct
        FROM "analysis_data"
        WHERE {nn_d}
        """
    ).fetchone()

    avg_word_count = float(stats_row[0] or 0)
    avg_char_length = float(stats_row[1] or 0)
    populated_count = int(stats_row[2] or 0)
    unique_count = int(stats_row[3] or 0)
    fill_rate = (populated_count / int(row_count) * 100) if row_count > 0 else 0.0

    # Distinct vendor-description pairs
    if has_supplier:
        pairs_row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT (
                COALESCE(TRIM(CAST(supplier AS TEXT)), '') || '|||' ||
                TRIM(CAST({qd} AS TEXT))
            ))
            FROM "analysis_data"
            WHERE {nn_d}
            """
        ).fetchone()
        distinct_pairs = int(pairs_row[0] or 0)
    else:
        distinct_pairs = unique_count

    # Top 1000 descriptions by spend
    top1000: list[dict[str, Any]] = []
    if has_spend:
        top_rows = conn.execute(
            f"""
            SELECT TRIM(CAST({qd} AS TEXT)) AS desc_val,
                   SUM(TRY_CAST(total_spend AS DOUBLE)) AS spend
            FROM "analysis_data"
            WHERE {nn_d}
            GROUP BY desc_val
            ORDER BY spend DESC
            LIMIT 1000
            """
        ).fetchall()
        top1000 = [
            {"description": str(r[0]), "spend": round(float(r[1] or 0))}
            for r in top_rows
        ]

    # Cost calculator
    avg_chars_per_pair = avg_char_length if avg_char_length > 0 else 1.0
    estimated_input_tokens = distinct_pairs * (avg_chars_per_pair / AVG_CHARS_PER_TOKEN)
    estimated_output_tokens = distinct_pairs * AVG_OUTPUT_TOKENS_PER_ROW
    map_ai_cost = (
        estimated_input_tokens * GPT_INPUT_RATE_PER_TOKEN +
        estimated_output_tokens * GPT_OUTPUT_RATE_PER_TOKEN
    )

    forced_method = "Creactives" if row_count > ROW_COUNT_THRESHOLD_CREACTIVES else None

    return {
        "metrics": {
            "rowCount": int(row_count),
            "avgWordCount": round(avg_word_count, 1),
            "avgCharLength": round(avg_char_length, 1),
            "fillRate": round(fill_rate, 1),
            "uniqueCount": unique_count,
            "distinctPairs": distinct_pairs,
            "sampledCount": len(top1000),
        },
        "mapAICost": round(map_ai_cost, 2),
        "forcedMethod": forced_method,
        "top1000Descriptions": top1000,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D6. Flags
# ═══════════════════════════════════════════════════════════════════════════

_PRIORITY_COLUMNS = [
    "supplier", "description", "invoice_date", "total_spend",
    "currency", "po_material_description", "business_unit",
    "plant_name", "vendor_country",
]


def _compute_flags(
    conn: DuckDBConnection,
    available: set[str],
    date_pivot_cells: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Compute conditional quality flags for the dataset.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.
        date_pivot_cells: Optional cells dict from _compute_date_spend_pivot result.

    Returns:
        Dict with flag keys: spendConsistency, descriptionQuality, vendorQuality,
        nullColumns. Each is None if conditions not met, else a dict with details.
    """
    total_rows = conn.execute('SELECT COUNT(*) FROM "analysis_data"').fetchone()[0]
    total_rows = int(total_rows or 0)

    # --- Spend Consistency ---
    spend_consistency = None
    if date_pivot_cells:
        monthly_spends: list[tuple[str, float]] = []
        for yr_str, months in date_pivot_cells.items():
            for mo_str, spend in months.items():
                label = f"{MONTH_NAMES[int(mo_str) - 1]} {yr_str}"
                monthly_spends.append((label, float(spend)))

        if monthly_spends:
            total_monthly = sum(s for _, s in monthly_spends)
            avg_monthly = total_monthly / len(monthly_spends) if monthly_spends else 0

            if avg_monthly > 0:
                flagged = []
                for label, spend in monthly_spends:
                    if spend > 0:
                        deviation = ((spend - avg_monthly) / avg_monthly) * 100
                        if spend > avg_monthly * 1.2 or spend < avg_monthly * 0.8:
                            flagged.append({
                                "month": label,
                                "spend": round(spend),
                                "deviationPct": round(deviation, 1),
                            })
                if flagged:
                    spend_consistency = {
                        "flaggedMonths": flagged,
                        "avgMonthlySpend": round(avg_monthly),
                    }

    # --- Description Quality ---
    desc_quality_flag = None
    if "description" in available and total_rows > 0:
        qd = _quote_id("description")
        nn_d = _nn(qd)
        desc_stats = conn.execute(
            f"""
            SELECT
                COUNT(CASE WHEN {nn_d} THEN 1 END) AS populated,
                AVG(CASE WHEN {nn_d}
                    THEN LENGTH(TRIM(CAST({qd} AS TEXT))) -
                         LENGTH(REPLACE(TRIM(CAST({qd} AS TEXT)), ' ', '')) + 1
                END) AS avg_words
            FROM "analysis_data"
            """
        ).fetchone()
        desc_populated = int(desc_stats[0] or 0)
        desc_fill = (desc_populated / total_rows * 100) if total_rows > 0 else 0
        desc_avg_words = float(desc_stats[1] or 0)

        if desc_fill < 70 or desc_avg_words < 2:
            msg_parts = []
            if desc_fill < 70:
                msg_parts.append(f"Fill rate {desc_fill:.1f}% is below 70% threshold")
            if desc_avg_words < 2:
                msg_parts.append(f"Avg word count {desc_avg_words:.1f} suggests low-quality entries")
            desc_quality_flag = {
                "fillRate": round(desc_fill, 1),
                "avgWordCount": round(desc_avg_words, 1),
                "message": "; ".join(msg_parts),
            }

    # --- Vendor Quality ---
    vendor_quality_flag = None
    if "supplier" in available and total_rows > 0:
        qs = _quote_id("supplier")
        nn_s = _nn(qs)
        vendor_stats = conn.execute(
            f'SELECT COUNT(CASE WHEN {nn_s} THEN 1 END) FROM "analysis_data"'
        ).fetchone()
        vendor_populated = int(vendor_stats[0] or 0)
        vendor_fill = (vendor_populated / total_rows * 100) if total_rows > 0 else 0

        if vendor_fill < 75:
            vendor_quality_flag = {
                "fillRate": round(vendor_fill, 1),
                "populatedCount": vendor_populated,
                "totalRows": total_rows,
            }

    # --- Null Columns ---
    null_columns_flag = None
    # Compute total base spend for spend_coverage
    has_spend = "total_spend" in available
    total_base_spend = 0.0
    if has_spend:
        ts_row = conn.execute(
            'SELECT SUM(TRY_CAST(total_spend AS DOUBLE)) FROM "analysis_data"'
        ).fetchone()
        total_base_spend = float(ts_row[0] or 0)

    flagged_columns: list[dict[str, Any]] = []
    for col in _PRIORITY_COLUMNS:
        if col not in available:
            continue
        qc = _quote_id(col)
        nn_c = _nn(qc)

        col_fill_row = conn.execute(
            f'SELECT COUNT(CASE WHEN {nn_c} THEN 1 END) FROM "analysis_data"'
        ).fetchone()
        col_populated = int(col_fill_row[0] or 0)
        col_fill_rate = (col_populated / total_rows * 100) if total_rows > 0 else 0.0

        col_spend_coverage = 100.0
        if has_spend and total_base_spend > 0:
            sc_row = conn.execute(
                f"""
                SELECT SUM(TRY_CAST(total_spend AS DOUBLE))
                FROM "analysis_data"
                WHERE {nn_c}
                """
            ).fetchone()
            populated_spend = float(sc_row[0] or 0)
            col_spend_coverage = (populated_spend / total_base_spend * 100)

        if col_fill_rate < 80 or col_spend_coverage < 90:
            flagged_columns.append({
                "name": col,
                "fillRate": round(col_fill_rate, 1),
                "spendCoverage": round(col_spend_coverage, 1),
            })

    if flagged_columns:
        null_columns_flag = {"flaggedColumns": flagged_columns}

    return {
        "spendConsistency": spend_consistency,
        "descriptionQuality": desc_quality_flag,
        "vendorQuality": vendor_quality_flag,
        "nullColumns": null_columns_flag,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D7. Column Fill Rate
# ═══════════════════════════════════════════════════════════════════════════

def _compute_column_fill_rate(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute fill rate and spend coverage for every mapped column.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.

    Returns:
        Dict with columns list (sorted by spend_coverage desc, nulls last)
        and feasible flag.
    """
    total_rows = conn.execute('SELECT COUNT(*) FROM "analysis_data"').fetchone()[0]
    total_rows = int(total_rows or 0)

    if total_rows == 0:
        return {"columns": [], "feasible": True}

    has_spend = "total_spend" in available
    total_base_spend = 0.0
    if has_spend:
        ts_row = conn.execute(
            'SELECT SUM(TRY_CAST(total_spend AS DOUBLE)) FROM "analysis_data"'
        ).fetchone()
        total_base_spend = float(ts_row[0] or 0)

    columns: list[dict[str, Any]] = []
    for col in sorted(available):
        qc = _quote_id(col)
        nn_c = _nn(qc)

        fill_row = conn.execute(
            f'SELECT COUNT(CASE WHEN {nn_c} THEN 1 END) FROM "analysis_data"'
        ).fetchone()
        col_populated = int(fill_row[0] or 0)
        fill_rate = (col_populated / total_rows * 100) if total_rows > 0 else 0.0

        spend_coverage: float | None = None
        if has_spend and total_base_spend > 0:
            sc_row = conn.execute(
                f"""
                SELECT SUM(TRY_CAST(total_spend AS DOUBLE))
                FROM "analysis_data"
                WHERE {nn_c}
                """
            ).fetchone()
            populated_spend = float(sc_row[0] or 0)
            spend_coverage = round(populated_spend / total_base_spend * 100, 1)

        columns.append({
            "columnName": col,
            "fillRate": round(fill_rate, 1),
            "spendCoverage": spend_coverage,
        })

    # Sort by spend_coverage descending, nulls last
    columns.sort(
        key=lambda x: (x["spendCoverage"] is None, -(x["spendCoverage"] or 0))
    )

    return {"columns": columns, "feasible": True}


# ═══════════════════════════════════════════════════════════════════════════
# E.  Public Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def run_executive_summary_sql(
    conn: DuckDBConnection,
) -> dict[str, Any]:
    """SQL-only phase: compute all panels that don't need an LLM.

    Must be called under the session lock.

    Returns:
        JSON-serialisable dict with pre-computed data for all panels.
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
        "Running Spend Quality Assessment (SQL phase): %d rows, %d columns",
        total_rows, len(available),
    )

    date_pivot = _compute_date_spend_pivot(conn)
    spend_bifurcation = _compute_spend_bifurcation(conn, available)
    pareto = _compute_pareto_analysis(conn, available)

    # SQL-only stats for description columns (AI insight is None)
    desc_quality = run_description_quality_analysis(conn, available, api_key=None)

    return {
        "totalRows": total_rows,
        "datePeriod": _compute_date_period(conn),
        "spendBreakdown": _compute_spend_breakdown(conn, available),
        "supplierBreakdown": _compute_supplier_breakdown(conn, available),
        "categorizationEffort": _compute_categorization_effort(conn, available),
        "flags": _compute_flags(conn, available, date_pivot.get("cells")),
        "columnFillRate": _compute_column_fill_rate(conn, available),
        "datePivot": date_pivot,
        "spendBifurcation": spend_bifurcation,
        "paretoAnalysis": pareto,
        "descriptionQuality": desc_quality,
    }


def run_executive_summary_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate categorization effort recommendation.

    Calls the LLM to produce a categorization recommendation based on the
    pre-computed description metrics. Safe to call without any database lock held.
    """
    cat_effort = sql_result.get("categorizationEffort")
    if not cat_effort or not cat_effort.get("feasible"):
        return sql_result

    try:
        ai_response = _generate_categorization_recommendation(cat_effort, api_key)
    except Exception as exc:
        logger.warning("Categorization recommendation AI failed: %s", exc)
        ai_response = {
            "buckets": {},
            "qualityVerdict": "unknown",
            "recommendedMethod": "MapAI",
            "reasoning": f"AI recommendation unavailable: {exc}",
        }

    # Extract AI fields
    buckets = ai_response.get("buckets", {})
    quality_verdict = ai_response.get("qualityVerdict", "unknown")
    recommended_method = ai_response.get("recommendedMethod", "MapAI")
    reasoning = ai_response.get("reasoning", "")

    # Rule engine override: force Creactives for large datasets
    row_count = cat_effort.get("metrics", {}).get("rowCount", 0)
    if row_count > ROW_COUNT_THRESHOLD_CREACTIVES:
        recommended_method = "Creactives"

    # Compute bucket percentages
    bucket_total = sum(buckets.values()) if buckets else 0
    buckets_pct = {}
    if bucket_total > 0:
        buckets_pct = {
            k: round(v / bucket_total * 100, 1)
            for k, v in buckets.items()
        }

    quality_warning = quality_verdict == "low"

    # Merge AI results into categorizationEffort
    cat_effort["buckets"] = buckets
    cat_effort["bucketsPct"] = buckets_pct
    cat_effort["qualityVerdict"] = quality_verdict
    cat_effort["recommendedMethod"] = recommended_method
    cat_effort["reasoning"] = reasoning
    cat_effort["qualityWarning"] = quality_warning

    return sql_result


def run_executive_summary(
    conn: DuckDBConnection,
    api_key: str,
) -> dict[str, Any]:
    """Run the full Spend Quality Assessment pipeline on ``analysis_data``.

    Convenience wrapper that calls both SQL and AI phases sequentially.
    Prefer calling `run_executive_summary_sql` + `run_executive_summary_ai`
    separately so the AI phase runs outside the session lock.
    """
    sql_result = run_executive_summary_sql(conn)
    return run_executive_summary_ai(sql_result, api_key)
