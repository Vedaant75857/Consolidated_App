"""Spend Quality Assessment – Data Quality for the Spend Summarizer.

Produces the following panels for the Executive Summary:
  1. Date Spend Pivot (years x months)
  2. Combined Spend + Supplier Pareto Analysis (80/85/90/95%)
  3. Description Quality Analysis (per description column)
  4. Date Period (min/max invoice_date with period label)
  5. Spend Breakdown (LTM, FY current/prior, YoY)
  6. Supplier Breakdown (count, 80% threshold, top-10, duplicates)
  7. Categorization Effort (description stats + AI cost estimate)
  8. Column Fill Rate (fill rate + spend coverage per original column)

Operates on the ``analysis_data`` table produced by column mapping.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from shared.ai_client import call_ai_json
from shared.duckdb_compat import DuckDBConnection
from services.spend_quality_assessment.description_quality import (
    DESCRIPTION_FIELD_KEYS,
    run_description_quality_analysis,
    _generate_categorization_recommendation,
    _sample_random_descriptions_from_top_vendors,
    _sample_random_unique_descriptions_all,
)
from services.spend_quality_assessment.ai_prompts import EXECUTIVE_SUMMARY_PROMPT
from services.upload.file_loader import _get_registry

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
EXECUTIVE_SUMMARY_KEYS = [
    "timePeriod",
    "ltmSpend",
    "supplierConcentration",
    "descriptionQuality",
    "categorizationMethod",
]


# ═══════════════════════════════════════════════════════════════════════════
# A.  SQL Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _quote_id(name: str) -> str:
    return f'"{name}"'


def _nn(qc: str) -> str:
    return f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"


def _format_amount(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    val = float(value)
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1_000_000_000:
        return f"{sign}{abs_val / 1_000_000_000:.1f}B"
    if abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:.0f}K"
    return f"{round(val):,}"


def _month_index_to_label(month_index: int) -> str:
    year = (month_index - 1) // 12
    month = (month_index - 1) % 12 + 1
    return f"{MONTH_NAMES[month - 1]} {year}"


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
    """Compute the five reporting-spend bifurcation attributes."""
    if "total_spend" not in available:
        return {
            "positiveSpend": None,
            "positivePctOfNet": None,
            "negativeSpend": None,
            "negativePctOfNet": None,
            "netSpend": None,
            "feasible": False,
            "message": "total_spend not mapped.",
        }

    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) > 0
                THEN TRY_CAST(total_spend AS DOUBLE) ELSE 0 END) AS pos,
            SUM(CASE WHEN TRY_CAST(total_spend AS DOUBLE) < 0
                THEN TRY_CAST(total_spend AS DOUBLE) ELSE 0 END) AS neg
        FROM "analysis_data"
        """
    ).fetchone()
    pos = float(row[0] or 0)
    neg = float(row[1] or 0)
    net = pos + neg

    return {
        "positiveSpend": round(pos),
        "positivePctOfNet": round(pos / net * 100, 1) if net else None,
        "negativeSpend": round(neg),
        "negativePctOfNet": round(neg / net * 100, 1) if net else None,
        "netSpend": round(net),
        "feasible": True,
    }


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

    # LTM: last 12 complete months, excluding the latest month in the file.
    max_month_index = max_year * 12 + max_month
    ltm_end_index = max_month_index - 1
    ltm_start_index = ltm_end_index - 11
    ltm_row = conn.execute(
        f"""
        SELECT SUM(TRY_CAST(total_spend AS DOUBLE))
        FROM "analysis_data"
        WHERE TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
          AND (
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) * 12 +
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER)
          ) >= {ltm_start_index}
          AND (
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) * 12 +
            CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER)
          ) <= {ltm_end_index}
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

    # Latest full annual year = most recent year with all 12 months present
    full_year_rows = conn.execute(
        """
        SELECT
            CAST(strftime('%Y', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER) AS yr,
            COUNT(DISTINCT CAST(strftime('%m', TRY_CAST(invoice_date AS TIMESTAMP)) AS INTEGER)) AS month_ct
        FROM "analysis_data"
        WHERE TRY_CAST(invoice_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(total_spend AS DOUBLE) IS NOT NULL
        GROUP BY yr
        HAVING month_ct = 12
        ORDER BY yr DESC
        LIMIT 1
        """
    ).fetchone()

    latest_full_year: int | None = int(full_year_rows[0]) if full_year_rows else None
    latest_full_year_spend: float | None = (
        year_spend.get(latest_full_year) if latest_full_year else None
    )

    return {
        "ltmSpend": round(ltm_spend),
        "currentFySpend": round(current_fy_spend),
        "priorFySpend": round(prior_fy_spend),
        "currentFyLabel": f"FY{max_year}",
        "priorFyLabel": f"FY{max_year - 1}",
        "ltmPeriodLabel": (
            f"{_month_index_to_label(ltm_start_index)} - "
            f"{_month_index_to_label(ltm_end_index)}"
        ),
        "yoyAbs": round(yoy_abs),
        "yoyPct": round(yoy_pct, 1),
        "latestFullYearSpend": round(latest_full_year_spend) if latest_full_year_spend is not None else None,
        "latestFullYearLabel": str(latest_full_year) if latest_full_year else None,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D4. Supplier Breakdown
# ═══════════════════════════════════════════════════════════════════════════

def _top_80_vendor_cohort(
    conn: DuckDBConnection,
    available: set[str],
) -> list[str]:
    """Return supplier names covering the top 80% of positive spend.

    Used as a single source of truth for both manual-validation pair counts
    and description-quality sampling so the two executive-summary points are
    always consistent.

    Args:
        conn: DuckDB session connection.
        available: Set of column names in analysis_data.

    Returns:
        List of supplier name strings (trimmed). Empty when supplier or
        total_spend isn't mapped, or net spend is non-positive.
    """
    if "supplier" not in available or "total_spend" not in available:
        return []

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
        return []

    grand_total = sum(float(r[1] or 0) for r in rows)
    if grand_total <= 0:
        return []

    threshold = grand_total * 0.80
    cumulative = 0.0
    cohort: list[str] = []
    for r in rows:
        cumulative += float(r[1] or 0)
        cohort.append(str(r[0]))
        if cumulative >= threshold:
            break

    return cohort


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
    if grand_total <= 0:
        return {
            "totalSuppliers": total_suppliers,
            "suppliersTo80Pct": None,
            "top10": [],
            "duplicateNameFlags": 0,
            "feasible": False,
            "message": "Net supplier spend is zero or negative.",
        }

    suppliers_to_80 = len(_top_80_vendor_cohort(conn, available))

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
        Dict with metrics, mapAICost, forcedMethod, random1000Descriptions,
        and feasible flag.
    """
    if "description" not in available:
        return {"feasible": False, "message": "description not mapped."}

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

    # Top-80% vendor cohort for sampling and manual-validation count
    cohort = _top_80_vendor_cohort(conn, available)

    if cohort:
        random1000 = _sample_random_descriptions_from_top_vendors(
            conn, "description", cohort, 1000,
        )
    else:
        random1000 = _sample_random_unique_descriptions_all(conn, "description", 1000)

    # Distinct (vendor, description) pairs in the top-80% vendor cohort
    if cohort:
        placeholders = ", ".join(["?"] * len(cohort))
        vp_row = conn.execute(
            f"SELECT COUNT(DISTINCT ("
            f"  TRIM(CAST(supplier AS TEXT)) || '|||' || TRIM(CAST({qd} AS TEXT))"
            f")) FROM \"analysis_data\" "
            f"WHERE {nn_d} AND TRIM(CAST(supplier AS TEXT)) IN ({placeholders})",
            cohort,
        ).fetchone()
        top_vendor_pairs_count = int(vp_row[0] or 0)
    else:
        top_vendor_pairs_count = distinct_pairs

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
            "sampledCount": len(random1000),
            "topVendorPairsCount": top_vendor_pairs_count,
        },
        "mapAICost": round(map_ai_cost, 2),
        "forcedMethod": forced_method,
        "random1000Descriptions": random1000,
        "feasible": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# D6. Column Fill Rate
# ===========================================================================

def _clean_original_header(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    return text or "UNNAMED"


def _normalise_header(value: Any) -> str:
    text = str(value).strip().upper() if value is not None else ""
    return text or "UNNAMED"


def _display_headers_from_raw(raw_headers: list[Any]) -> dict[str, str]:
    """Return {normalised_deduped_header: display_header} for a raw header row."""
    seen: dict[str, int] = {}
    display_by_col: dict[str, str] = {}

    for raw in raw_headers:
        base_norm = _normalise_header(raw)
        count = seen.get(base_norm, 0)
        seen[base_norm] = count + 1

        data_col = base_norm if count == 0 else f"{base_norm}_{count}"
        display = _clean_original_header(raw)
        display_by_col[data_col] = display if count == 0 else f"{display}_{count}"

    return display_by_col


def _raw_header_display_map(conn: DuckDBConnection, raw_table: str) -> dict[str, str]:
    raw_cols = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        (raw_table,),
    ).fetchall()
    if not raw_cols:
        return {}

    col_list = ", ".join(_quote_id(str(r[0])) for r in raw_cols)
    row = conn.execute(f'SELECT {col_list} FROM "{raw_table}" LIMIT 1').fetchone()
    if not row:
        return {}

    return {
        col: display
        for col, display in _display_headers_from_raw(list(row)).items()
        if col != "RECORD_ID"
    }


def _compute_column_fill_rate(
    conn: DuckDBConnection,
    available: set[str],
) -> dict[str, Any]:
    """Compute fill rate and net-spend coverage for original uploaded columns."""
    registry = _get_registry(conn)
    if not registry:
        return {"columns": [], "feasible": True}

    frames: list[pd.DataFrame] = []
    display_by_col: dict[str, str] = {}
    ordered_cols: list[str] = []

    for entry in registry:
        data_table = entry["data_table"]
        raw_table = entry["raw_table"]
        try:
            df = conn._conn.execute(f'SELECT * FROM "{data_table}"').df()
        except Exception as exc:
            logger.warning("Failed to load original table '%s': %s", data_table, exc)
            continue

        if "RECORD_ID" in df.columns:
            df = df.drop(columns=["RECORD_ID"])

        header_map = _raw_header_display_map(conn, raw_table)
        for col in df.columns:
            if col not in display_by_col:
                display_by_col[col] = header_map.get(col, col)
                ordered_cols.append(col)

        frames.append(df)

    if not frames:
        return {"columns": [], "feasible": True}

    original_df = pd.concat(frames, ignore_index=True)
    total_rows = len(original_df)
    if total_rows == 0:
        return {"columns": [], "feasible": True}

    spend_series: pd.Series | None = None
    total_net_spend = 0.0
    if "total_spend" in available:
        spend_df = conn._conn.execute('SELECT total_spend FROM "analysis_data"').df()
        if len(spend_df) == total_rows:
            spend_series = pd.to_numeric(spend_df["total_spend"], errors="coerce").fillna(0)
            total_net_spend = float(spend_series.sum())

    columns: list[dict[str, Any]] = []
    for idx, col in enumerate(ordered_cols):
        series = original_df[col] if col in original_df else pd.Series([], dtype=str)
        populated_mask = series.notna() & (series.astype(str).str.strip() != "")
        populated_count = int(populated_mask.sum())
        fill_rate = populated_count / total_rows * 100

        spend_coverage: float | None = None
        if spend_series is not None and total_net_spend != 0:
            populated_spend = float(spend_series[populated_mask].sum())
            spend_coverage = round(populated_spend / total_net_spend * 100, 1)

        columns.append({
            "columnName": display_by_col.get(col, col),
            "sourceColumn": col,
            "order": idx,
            "fillRate": round(fill_rate, 1),
            "spendCoverage": spend_coverage,
        })

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
        "columnFillRate": _compute_column_fill_rate(conn, available),
        "datePivot": date_pivot,
        "spendBifurcation": spend_bifurcation,
        "paretoAnalysis": pareto,
        "descriptionQuality": desc_quality,
    }


def _description_quality_sentence(desc: dict[str, Any]) -> str:
    """Build a deterministic one-liner for the Description quality executive row.

    Args:
        desc: The ``descriptionQuality`` section of the executive summary payload.

    Returns:
        Markdown-formatted sentence: verdict + one-liner reasoning.
    """
    if not desc.get("available") or not desc.get("qualityVerdict"):
        return "Description quality is unavailable because the description column is not mapped."

    verdict = desc.get("qualityVerdict")
    reasoning = (desc.get("reasoning") or "").strip()

    sentence = f"Descriptions are **{verdict}** quality"
    if reasoning:
        if not reasoning.endswith("."):
            reasoning += "."
        sentence += f" — {reasoning}"
    else:
        sentence += "."
    return sentence


def _categorization_method_sentence(cat: dict[str, Any]) -> str:
    """Build a deterministic one-liner for the Categorization Method executive row.

    Args:
        cat: The ``categorizationMethod`` section of the executive summary payload.

    Returns:
        Markdown-formatted sentence: method, cost, and manual validation count.
    """
    if not cat.get("available"):
        return "Categorization method is unavailable because description data is not mapped."

    method = cat.get("recommendedMethod", "MapAI")
    cost_text = cat.get("mapAICostText", "N/A")
    n = cat.get("manualValidationCount")

    sentence = f"Recommended method is **{method}**, with an estimated cost of **{cost_text}**"
    if n is not None:
        sentence += f", along with manual validation of **{n:,}** rows"
    return sentence + "."


def _fallback_executive_summary_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    period = payload.get("timePeriod", {})
    ltm = payload.get("ltmSpend", {})
    suppliers = payload.get("supplierConcentration", {})
    desc = payload.get("descriptionQuality", {})
    cat = payload.get("categorizationMethod", {})

    if period.get("available"):
        time_text = (
            f"Data covers **{period.get('periodLabel')}** across "
            f"**{period.get('monthsCovered')}** distinct months."
        )
    else:
        time_text = "Time period is unavailable because invoice date is not mapped or valid."

    if ltm.get("available"):
        ltm_text = (
            f"LTM spend is **{ltm.get('amountText')}** for "
            f"**{ltm.get('periodLabel')}**, excluding the latest month."
        )
        fy_amt = ltm.get("latestFullYearAmountText")
        fy_lbl = ltm.get("latestFullYearLabel")
        if fy_amt and fy_lbl:
            ltm_text += (
                f" Latest full annual year (**{fy_lbl}**) spend was **{fy_amt}**."
            )
    else:
        ltm_text = "LTM spend is unavailable because mapped date or spend values are missing."

    if suppliers.get("available"):
        supplier_text = (
            f"Total suppliers are **{suppliers.get('totalSuppliers'):,}**; "
            f"**{suppliers.get('suppliersTo80Pct'):,}** suppliers cover 80% of net spend."
        )
    elif suppliers.get("totalSuppliers") is not None:
        supplier_text = (
            f"Total suppliers are **{suppliers.get('totalSuppliers'):,}**; "
            "80% supplier concentration is unavailable because net spend is not positive."
        )
    else:
        supplier_text = "Supplier concentration is unavailable because supplier or spend is not mapped."

    return [
        {"key": "timePeriod", "label": "Time period", "text": time_text},
        {"key": "ltmSpend", "label": "LTM spend", "text": ltm_text},
        {"key": "supplierConcentration", "label": "Suppliers", "text": supplier_text},
        {"key": "descriptionQuality", "label": "Description quality", "text": _description_quality_sentence(desc)},
        {"key": "categorizationMethod", "label": "Categorization method", "text": _categorization_method_sentence(cat)},
    ]


def _build_executive_summary_payload(sql_result: dict[str, Any]) -> dict[str, Any]:
    date_period = sql_result.get("datePeriod") or {}
    spend = sql_result.get("spendBreakdown") or {}
    suppliers = sql_result.get("supplierBreakdown") or {}
    cat = sql_result.get("categorizationEffort") or {}
    cat_metrics = cat.get("metrics") or {}

    return {
        "timePeriod": {
            "available": bool(date_period.get("feasible")),
            "periodLabel": date_period.get("periodLabel"),
            "monthsCovered": date_period.get("monthsCovered"),
            "startDate": date_period.get("startDate"),
            "endDate": date_period.get("endDate"),
            "message": date_period.get("message"),
        },
        "ltmSpend": {
            "available": bool(spend.get("feasible")),
            "amount": spend.get("ltmSpend"),
            "amountText": _format_amount(spend.get("ltmSpend")),
            "periodLabel": spend.get("ltmPeriodLabel"),
            "latestFullYearAmount": spend.get("latestFullYearSpend"),
            "latestFullYearAmountText": _format_amount(spend.get("latestFullYearSpend")),
            "latestFullYearLabel": spend.get("latestFullYearLabel"),
            "message": spend.get("message"),
        },
        "supplierConcentration": {
            "available": bool(suppliers.get("feasible")),
            "totalSuppliers": suppliers.get("totalSuppliers"),
            "suppliersTo80Pct": suppliers.get("suppliersTo80Pct"),
            "message": suppliers.get("message"),
        },
        "descriptionQuality": {
            "available": bool(cat.get("feasible")),
            "qualityVerdict": cat.get("qualityVerdict"),
            "reasoning": cat.get("reasoning"),
            "fillRate": cat_metrics.get("fillRate"),
            "avgWordCount": cat_metrics.get("avgWordCount"),
            "sampledCount": cat_metrics.get("sampledCount"),
            "message": cat.get("message"),
        },
        "categorizationMethod": {
            "available": bool(cat.get("feasible")),
            "recommendedMethod": cat.get("recommendedMethod"),
            "mapAICostText": _format_amount(cat.get("mapAICost")),
            "manualValidationCount": cat_metrics.get("topVendorPairsCount"),
            "message": cat.get("message"),
        },
    }


def _normalise_executive_summary_rows(raw: Any, fallback: list[dict[str, str]]) -> list[dict[str, str]]:
    if not isinstance(raw, dict) or not isinstance(raw.get("rows"), list):
        return fallback

    by_key = {
        str(row.get("key")): row
        for row in raw["rows"]
        if isinstance(row, dict) and row.get("key") and row.get("text")
    }
    labels = {row["key"]: row["label"] for row in fallback}

    rows: list[dict[str, str]] = []
    for key in EXECUTIVE_SUMMARY_KEYS:
        source = by_key.get(key)
        if source:
            rows.append({
                "key": key,
                "label": str(source.get("label") or labels[key]),
                "text": str(source.get("text")),
            })
        else:
            rows.append(next(row for row in fallback if row["key"] == key))
    return rows


def _generate_executive_summary_rows(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    payload = _build_executive_summary_payload(sql_result)
    fallback = _fallback_executive_summary_rows(payload)
    try:
        raw = call_ai_json(EXECUTIVE_SUMMARY_PROMPT, payload, api_key=api_key)
        rows = _normalise_executive_summary_rows(raw, fallback)
    except Exception as exc:
        logger.warning("Executive summary AI failed: %s", exc)
        rows = fallback

    # Override description quality and categorization rows with deterministic text.
    desc_payload = payload.get("descriptionQuality", {})
    if desc_payload.get("available") and desc_payload.get("qualityVerdict"):
        desc_sentence = _description_quality_sentence(desc_payload)
        for row in rows:
            if row["key"] == "descriptionQuality":
                row["text"] = desc_sentence
                break

    cat_payload = payload.get("categorizationMethod", {})
    if cat_payload.get("available"):
        cat_sentence = _categorization_method_sentence(cat_payload)
        for row in rows:
            if row["key"] == "categorizationMethod":
                row["text"] = cat_sentence
                break

    return {"rows": rows}


def run_executive_summary_ai(
    sql_result: dict[str, Any],
    api_key: str,
) -> dict[str, Any]:
    """AI phase: generate description classification and summary one-liners.

    Safe to call without any database lock held.
    """
    cat_effort = sql_result.get("categorizationEffort")

    if cat_effort and cat_effort.get("feasible"):
        try:
            ai_response = _generate_categorization_recommendation(cat_effort, api_key)
        except Exception as exc:
            logger.warning("Categorization recommendation AI failed: %s", exc)
            ai_response = {
                "buckets": {"high": 0, "medium": 0, "low": 0},
                "qualityVerdict": "low",
                "recommendedMethod": cat_effort.get("forcedMethod") or "MapAI",
                "reasoning": f"AI recommendation unavailable: {exc}",
            }

        # Extract AI fields
        raw_buckets = ai_response.get("buckets", {})
        buckets = {
            "high": int(raw_buckets.get("high") or 0),
            "medium": int(raw_buckets.get("medium") or 0),
            "low": int(raw_buckets.get("low") or 0),
        }
        quality_verdict = ai_response.get("qualityVerdict", "low")
        if quality_verdict not in {"high", "medium", "low"}:
            quality_verdict = "low"
        recommended_method = ai_response.get("recommendedMethod", "MapAI")
        reasoning = ai_response.get("reasoning", "")

        # Rule engine override: force Creactives for large datasets
        row_count = cat_effort.get("metrics", {}).get("rowCount", 0)
        if row_count > ROW_COUNT_THRESHOLD_CREACTIVES:
            recommended_method = "Creactives"

        # Compute bucket percentages
        bucket_total = sum(buckets.values())
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

    sql_result.pop("flags", None)
    sql_result["executiveSummary"] = _generate_executive_summary_rows(
        sql_result, api_key
    )

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
