"""Date analysis panel for the Data Quality Assessment.

Detects date formats per file, builds a year x month spend pivot table,
and generates AI insights on date quality and spend patterns.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from shared.db import quote_id, read_table_columns

from .ai_prompts import generate_date_insight
from .metrics import (
    _detect_format,
    _non_null_condition,
    _profile_dmy_mdy,
    _safe_pct,
)

logger = logging.getLogger(__name__)

# Standard date column names from header normalisation
DATE_COLUMNS: list[str] = [
    "Invoice Date",
    "Goods Receipt Date",
    "Payment date",
    "PO Document Date",
    "Contract End Date",
    "Contract Start Date",
]

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# Spend column priority: reporting currency first, then local
_REPORTING_SPEND_COLS: list[str] = [
    "Total Amount paid in Reporting Currency",
    "PO Total Amount in reporting currency",
]

_LOCAL_SPEND_COLS: list[str] = [
    "Total Amount paid in Local Currency",
    "PO Total Amount in Local Currency",
]

_CURRENCY_CODE_FOR_SPEND: dict[str, str] = {
    "Total Amount paid in Local Currency": "Local Currency Code",
    "PO Total Amount in Local Currency": "PO Local Currency Code",
}


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════


def _find_available_date_columns(available: set[str]) -> list[str]:
    """Return the subset of standard date columns present in the table."""
    return [c for c in DATE_COLUMNS if c in available]


def _pick_spend_column(available: set[str]) -> tuple[str | None, bool]:
    """Pick the best spend column.

    Returns:
        (column_name, is_reporting_currency)
    """
    for c in _REPORTING_SPEND_COLS:
        if c in available:
            return c, True
    for c in _LOCAL_SPEND_COLS:
        if c in available:
            return c, False
    return None, False


def _pick_currency_code_column(
    spend_col: str | None, available: set[str]
) -> str | None:
    """Return the currency code column paired with a local spend column."""
    if spend_col is None:
        return None
    code_col = _CURRENCY_CODE_FOR_SPEND.get(spend_col)
    return code_col if code_col and code_col in available else None


def _numeric_spend_expr(qs: str) -> str:
    """SQL expression that safely casts a spend column to REAL."""
    return (
        f"CASE WHEN TRIM({qs}) GLOB '*[0-9]*' "
        f"AND TRIM({qs}) NOT GLOB '*[^0-9.eE+-]*' "
        f"THEN CAST({qs} AS REAL) ELSE 0 END"
    )


def _build_date_extract_sql(
    qc: str, dominant_format: str
) -> tuple[str, str]:
    """Return ``(year_sql_expr, month_sql_expr)`` for a known date format.

    Each expression evaluates to an INTEGER suitable for GROUP BY.
    """
    t = f"TRIM({qc})"

    if dominant_format == "YYYY-MM-DD":
        return (
            f"CAST(SUBSTR({t}, 1, 4) AS INTEGER)",
            f"CAST(SUBSTR({t}, 6, 2) AS INTEGER)",
        )
    if dominant_format == "YYYYMMDD":
        return (
            f"CAST(SUBSTR({t}, 1, 4) AS INTEGER)",
            f"CAST(SUBSTR({t}, 5, 2) AS INTEGER)",
        )
    if dominant_format == "YYYY":
        return f"CAST({t} AS INTEGER)", "1"
    if dominant_format == "DD.MM.YYYY":
        return (
            f"CAST(SUBSTR({t}, -4) AS INTEGER)",
            f"CAST(SUBSTR({t}, INSTR({t},'.')+1, "
            f"INSTR(SUBSTR({t}, INSTR({t},'.')+1), '.')-1) AS INTEGER)",
        )
    if dominant_format == "DD/MM/YYYY":
        return (
            f"CAST(SUBSTR({t}, -4) AS INTEGER)",
            f"CAST(SUBSTR({t}, INSTR({t},'/')+1, "
            f"INSTR(SUBSTR({t}, INSTR({t},'/')+1), '/')-1) AS INTEGER)",
        )
    if dominant_format == "MM/DD/YYYY":
        return (
            f"CAST(SUBSTR({t}, -4) AS INTEGER)",
            f"CAST(SUBSTR({t}, 1, INSTR({t},'/')-1) AS INTEGER)",
        )
    if dominant_format == "DD-MM-YYYY":
        return (
            f"CAST(SUBSTR({t}, -4) AS INTEGER)",
            f"CAST(SUBSTR({t}, INSTR({t},'-')+1, "
            f"INSTR(SUBSTR({t}, INSTR({t},'-')+1), '-')-1) AS INTEGER)",
        )
    if dominant_format == "MM-DD-YYYY":
        return (
            f"CAST(SUBSTR({t}, -4) AS INTEGER)",
            f"CAST(SUBSTR({t}, 1, INSTR({t},'-')-1) AS INTEGER)",
        )
    # Fallback: rely on SQLite native date parsing (ISO-ish dates)
    return (
        f"CAST(strftime('%Y', {qc}) AS INTEGER)",
        f"CAST(strftime('%m', {qc}) AS INTEGER)",
    )


# ═══════════════════════════════════════════════════════════════════════════
# Format table builder
# ═══════════════════════════════════════════════════════════════════════════


def _build_format_table(
    conn: sqlite3.Connection,
    table_name: str,
    date_column: str,
    has_file_name: bool,
    sample_per_file: int = 100,
) -> tuple[list[dict[str, Any]], bool, str]:
    """Build per-file format detection table.

    Returns:
        (format_entries, all_consistent, global_dominant_format)
    """
    tbl = quote_id(table_name)
    qc = quote_id(date_column)
    nn = _non_null_condition(qc)

    entries: list[dict[str, Any]] = []
    all_formats: dict[str, int] = {}

    if has_file_name:
        # Global sample for DMY / MDY profiling
        global_sample = conn.execute(
            f"SELECT {qc} AS v FROM {tbl} WHERE {nn} LIMIT 2000"
        ).fetchall()
        order = _profile_dmy_mdy([str(r["v"]) for r in global_sample])

        files = conn.execute(
            f'SELECT DISTINCT "FILE_NAME" AS fn FROM {tbl} '
            f"WHERE \"FILE_NAME\" IS NOT NULL AND TRIM(\"FILE_NAME\") != ''"
        ).fetchall()

        for frow in files:
            fname = str(frow["fn"])
            sampled = conn.execute(
                f"SELECT {qc} AS v FROM {tbl} "
                f"WHERE \"FILE_NAME\" = ? AND {nn} LIMIT ?",
                (fname, sample_per_file),
            ).fetchall()

            counts: dict[str, int] = {}
            examples: dict[str, str] = {}
            for sr in sampled:
                val = str(sr["v"]).strip()
                fmt = _detect_format(val, order)
                counts[fmt] = counts.get(fmt, 0) + 1
                if fmt not in examples:
                    examples[fmt] = val

            if not counts:
                continue

            dominant = max(counts, key=lambda k: counts[k])
            total_sampled = sum(counts.values())
            format_pcts = {
                fmt: round(_safe_pct(ct, total_sampled))
                for fmt, ct in counts.items()
            }
            for fmt, ct in counts.items():
                all_formats[fmt] = all_formats.get(fmt, 0) + ct

            entries.append({
                "fileName": fname,
                "dominantFormat": dominant,
                "formatPcts": format_pcts,
                "examples": examples,
                "consistent": len(counts) <= 1,
            })
    else:
        # No FILE_NAME — treat as a single source
        sampled = conn.execute(
            f"SELECT {qc} AS v FROM {tbl} WHERE {nn} LIMIT 2000"
        ).fetchall()
        order = _profile_dmy_mdy([str(r["v"]) for r in sampled])

        counts: dict[str, int] = {}
        examples: dict[str, str] = {}
        for sr in sampled:
            val = str(sr["v"]).strip()
            fmt = _detect_format(val, order)
            counts[fmt] = counts.get(fmt, 0) + 1
            if fmt not in examples:
                examples[fmt] = val

        if counts:
            dominant = max(counts, key=lambda k: counts[k])
            total_sampled = sum(counts.values())
            format_pcts = {
                fmt: round(_safe_pct(ct, total_sampled))
                for fmt, ct in counts.items()
            }
            all_formats = counts.copy()
            entries.append({
                "fileName": "(all data)",
                "dominantFormat": dominant,
                "formatPcts": format_pcts,
                "examples": examples,
                "consistent": len(counts) <= 1,
            })

    unique_dominants = {e["dominantFormat"] for e in entries}
    all_consistent = len(unique_dominants) <= 1
    global_dominant = (
        max(all_formats, key=lambda k: all_formats[k])
        if all_formats
        else "unknown"
    )
    return entries, all_consistent, global_dominant


# ═══════════════════════════════════════════════════════════════════════════
# Pivot builders
# ═══════════════════════════════════════════════════════════════════════════


def _build_reporting_pivot(
    conn: sqlite3.Connection,
    table_name: str,
    date_column: str,
    spend_column: str,
    dominant_format: str,
) -> dict[str, Any]:
    """Year x Month spend pivot using a single (reporting) currency column."""
    tbl = quote_id(table_name)
    qc = quote_id(date_column)
    qs = quote_id(spend_column)
    nn = _non_null_condition(qc)
    yr_expr, mo_expr = _build_date_extract_sql(qc, dominant_format)
    spend_expr = _numeric_spend_expr(qs)

    rows = conn.execute(
        f"SELECT {yr_expr} AS yr, {mo_expr} AS mo, "
        f"SUM({spend_expr}) AS spend "
        f"FROM {tbl} WHERE {nn} "
        f"GROUP BY yr, mo ORDER BY yr, mo"
    ).fetchall()

    cells: dict[str, dict[str, float]] = {}
    year_set: set[int] = set()

    for r in rows:
        yr_val, mo_val = r["yr"], r["mo"]
        if yr_val is None or mo_val is None:
            continue
        yr_int, mo_int = int(yr_val), int(mo_val)
        if not (1900 <= yr_int <= 2100) or not (1 <= mo_int <= 12):
            continue
        yr_str = str(yr_int)
        year_set.add(yr_int)
        cells.setdefault(yr_str, {})[str(mo_int)] = round(float(r["spend"] or 0))

    years = sorted(year_set)
    for yr in years:
        yr_str = str(yr)
        for m in range(1, 13):
            cells[yr_str].setdefault(str(m), 0)

    return {
        "years": years,
        "months": MONTH_NAMES,
        "cells": cells,
        "type": "reporting",
        "spendColumn": spend_column,
        "feasible": len(years) > 0,
    }


def _build_currency_crosstab_pivot(
    conn: sqlite3.Connection,
    table_name: str,
    date_column: str,
    spend_column: str,
    currency_column: str,
    dominant_format: str,
) -> dict[str, Any]:
    """Month x Currency cross-tab pivot (local-currency fallback)."""
    tbl = quote_id(table_name)
    qc = quote_id(date_column)
    qs = quote_id(spend_column)
    qcc = quote_id(currency_column)
    nn = _non_null_condition(qc)
    yr_expr, mo_expr = _build_date_extract_sql(qc, dominant_format)
    spend_expr = _numeric_spend_expr(qs)

    rows = conn.execute(
        f"SELECT {yr_expr} AS yr, {mo_expr} AS mo, "
        f"UPPER(TRIM({qcc})) AS ccy, "
        f"SUM({spend_expr}) AS spend "
        f"FROM {tbl} WHERE {nn} "
        f"AND {qcc} IS NOT NULL AND TRIM({qcc}) != '' "
        f"GROUP BY yr, mo, ccy ORDER BY yr, mo, ccy"
    ).fetchall()

    if not rows:
        return {
            "rowLabels": [],
            "currencies": [],
            "cells": {},
            "type": "currency_crosstab",
            "feasible": False,
        }

    currency_set: set[str] = set()
    raw_cells: dict[str, dict[str, float]] = {}
    year_set: set[int] = set()

    for r in rows:
        yr_val, mo_val = r["yr"], r["mo"]
        ccy = str(r["ccy"])
        if yr_val is None or mo_val is None:
            continue
        yr_int, mo_int = int(yr_val), int(mo_val)
        if not (1900 <= yr_int <= 2100) or not (1 <= mo_int <= 12):
            continue
        key = f"{yr_int}-{mo_int:02d}"
        year_set.add(yr_int)
        currency_set.add(ccy)
        raw_cells.setdefault(key, {})[ccy] = round(float(r["spend"] or 0))

    currencies = sorted(currency_set)
    years = sorted(year_set)

    cells: dict[str, dict[str, float]] = {}
    row_labels: list[str] = []
    for yr in years:
        for m in range(1, 13):
            label = f"{MONTH_NAMES[m - 1]} {yr}"
            key = f"{yr}-{m:02d}"
            row_labels.append(label)
            cells[label] = {
                ccy: raw_cells.get(key, {}).get(ccy, 0) for ccy in currencies
            }

    return {
        "rowLabels": row_labels,
        "currencies": currencies,
        "cells": cells,
        "type": "currency_crosstab",
        "spendColumn": spend_column,
        "currencyColumn": currency_column,
        "feasible": len(currencies) > 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Public entry point
# ═══════════════════════════════════════════════════════════════════════════


def run_date_analysis(
    conn: sqlite3.Connection,
    table_name: str,
    date_column: str | None,
    api_key: str,
) -> dict[str, Any]:
    """Run the full date analysis for the DQA date panel.

    Args:
        conn: SQLite session connection.
        table_name: Target table (e.g. ``final_merged_v1``).
        date_column: Selected date column, or ``None`` to auto-pick the first
            available one (preferring Invoice Date).
        api_key: API key for AI insight generation.

    Returns:
        JSON-serialisable dict with ``availableDateColumns``,
        ``selectedColumn``, ``formatTable``, ``pivotData``, ``consistent``,
        and ``aiInsight``.
    """
    available = set(read_table_columns(conn, table_name))
    available_date_cols = _find_available_date_columns(available)
    has_file_name = "FILE_NAME" in available

    if not available_date_cols:
        return {
            "availableDateColumns": [],
            "selectedColumn": None,
            "formatTable": [],
            "pivotData": None,
            "consistent": True,
            "aiInsight": "No date columns found in the dataset.",
        }

    if date_column is None or date_column not in available:
        date_column = available_date_cols[0]

    # ── Format detection ──────────────────────────────────────────────────
    format_table, all_consistent, dominant_format = _build_format_table(
        conn, table_name, date_column, has_file_name,
    )

    # ── Spend pivot ───────────────────────────────────────────────────────
    spend_col, is_reporting = _pick_spend_column(available)
    pivot_data: dict[str, Any] | None = None

    if spend_col:
        if is_reporting:
            pivot_data = _build_reporting_pivot(
                conn, table_name, date_column, spend_col, dominant_format,
            )
        else:
            ccy_col = _pick_currency_code_column(spend_col, available)
            if ccy_col:
                pivot_data = _build_currency_crosstab_pivot(
                    conn, table_name, date_column, spend_col,
                    ccy_col, dominant_format,
                )
            else:
                pivot_data = _build_reporting_pivot(
                    conn, table_name, date_column, spend_col, dominant_format,
                )

    # ── AI insight ────────────────────────────────────────────────────────
    ai_payload = {
        "formatTable": format_table,
        "consistent": all_consistent,
        "pivotData": pivot_data,
        "selectedColumn": date_column,
    }
    try:
        ai_insight = generate_date_insight(ai_payload, api_key)
    except Exception as exc:
        logger.warning("Date AI insight generation failed: %s", exc)
        ai_insight = "AI insight generation failed."

    return {
        "availableDateColumns": available_date_cols,
        "selectedColumn": date_column,
        "formatTable": format_table,
        "pivotData": pivot_data,
        "consistent": all_consistent,
        "aiInsight": ai_insight,
    }
