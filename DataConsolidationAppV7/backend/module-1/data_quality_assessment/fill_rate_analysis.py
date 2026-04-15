"""Fill Rate Summary and Spend Bifurcation for the Data Quality Assessment.

Computes per-column fill rates with spend coverage, and positive/negative
spend bifurcation.  No AI calls — pure SQL aggregation.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns, table_row_count

from .metrics import _non_null_condition, _safe_pct, find_column

logger = logging.getLogger(__name__)

_SYSTEM_COLUMNS: set[str] = {"FILE_NAME", "RECORD_ID"}

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


def _pick_spend_column(available: set[str]) -> tuple[str | None, bool]:
    """Pick the best spend column (reporting first, then local).

    Returns:
        (column_name, is_reporting_currency)
    """
    reporting = find_column(available, _REPORTING_SPEND_COLS)
    if reporting is not None:
        return reporting, True
    local = find_column(available, _LOCAL_SPEND_COLS)
    if local is not None:
        return local, False
    return None, False


def _pick_currency_code_column(
    spend_col: str | None, available: set[str],
) -> str | None:
    """Return the currency code column paired with a local spend column."""
    if spend_col is None:
        return None
    matched_key = find_column(set(_CURRENCY_CODE_FOR_SPEND.keys()), [spend_col])
    if not matched_key:
        return None
    code_col = _CURRENCY_CODE_FOR_SPEND[matched_key]
    return find_column(available, [code_col])


def _numeric_spend_expr(qs: str) -> str:
    """SQL expression that safely casts a spend column to REAL (0 for non-numeric)."""
    return (
        f"CASE WHEN regexp_matches(TRIM({qs}), '[0-9]') "
        f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$') "
        f"THEN CAST({qs} AS REAL) ELSE 0 END"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Fill Rate Summary
# ═══════════════════════════════════════════════════════════════════════════


def run_fill_rate_analysis(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Compute per-column fill rate and spend coverage.

    For each column returns:
      - pctRowsCovered: percentage of non-null/non-empty rows
      - spendCoverage: either a single percentage (reporting) or a list of
        {code, pct} dicts (local-by-currency), or None if no spend column.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.

    Returns:
        JSON-serialisable dict with ``columns``, ``spendType``, ``spendColumn``.
    """
    all_cols = read_table_columns(conn, table_name)
    logger.info("Fill rate: table=%s, total_cols=%d", table_name, len(all_cols))
    system_upper = {s.upper() for s in _SYSTEM_COLUMNS}
    columns = [c for c in all_cols if c.upper() not in system_upper]
    if not columns:
        logger.warning("Fill rate: 0 columns after filtering system columns from %d total", len(all_cols))
        return {"columns": [], "spendType": "none", "spendColumn": None}

    available = set(all_cols)
    tbl = quote_id(table_name)
    total_rows = table_row_count(conn, table_name)

    spend_col, is_reporting = _pick_spend_column(available)
    qs = quote_id(spend_col) if spend_col else None
    spend_expr = _numeric_spend_expr(qs) if qs else None

    # Total spend (denominator for reporting %)
    total_spend = 0.0
    if spend_expr:
        row = conn.execute(f"SELECT SUM({spend_expr}) AS ts FROM {tbl}").fetchone()
        total_spend = float(row["ts"] or 0)

    # Per-currency totals (denominator for local %)
    ccy_col = None
    ccy_totals: dict[str, float] = {}
    if spend_col and not is_reporting:
        ccy_col = _pick_currency_code_column(spend_col, available)
        if ccy_col:
            qcc = quote_id(ccy_col)
            ccy_rows = conn.execute(
                f"SELECT UPPER(TRIM({qcc})) AS code, "
                f"SUM({spend_expr}) AS ts "
                f"FROM {tbl} "
                f"WHERE {qcc} IS NOT NULL AND TRIM({qcc}) != '' "
                f"GROUP BY UPPER(TRIM({qcc}))"
            ).fetchall()
            ccy_totals = {str(r["code"]): float(r["ts"] or 0) for r in ccy_rows}

    # Build per-column metrics in a single pass per column
    result_columns: list[dict[str, Any]] = []

    for col in columns:
        qc = quote_id(col)
        nn_cond = _non_null_condition(qc)

        # Fill rate
        fr_row = conn.execute(
            f"SELECT COUNT(CASE WHEN {nn_cond} THEN 1 END) AS filled FROM {tbl}"
        ).fetchone()
        filled = int(fr_row["filled"] or 0)
        pct_rows = round(filled / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Spend coverage
        spend_coverage: Any = None

        if spend_col and is_reporting and spend_expr and total_spend > 0:
            s_row = conn.execute(
                f"SELECT SUM({spend_expr}) AS cs FROM {tbl} WHERE {nn_cond}"
            ).fetchone()
            covered_spend = float(s_row["cs"] or 0)
            spend_coverage = round(covered_spend / total_spend * 100, 2)

        elif spend_col and not is_reporting and ccy_col and spend_expr and ccy_totals:
            qcc = quote_id(ccy_col)
            ccy_rows = conn.execute(
                f"SELECT UPPER(TRIM({qcc})) AS code, "
                f"SUM({spend_expr}) AS cs "
                f"FROM {tbl} "
                f"WHERE {nn_cond} "
                f"AND {qcc} IS NOT NULL AND TRIM({qcc}) != '' "
                f"GROUP BY UPPER(TRIM({qcc}))"
            ).fetchall()
            breakdown: list[dict[str, Any]] = []
            for r in ccy_rows:
                code = str(r["code"])
                covered = float(r["cs"] or 0)
                denom = ccy_totals.get(code, 0)
                pct = round(covered / denom * 100, 0) if denom > 0 else 0
                breakdown.append({"code": code, "pct": pct})
            breakdown.sort(key=lambda x: x["code"])
            spend_coverage = breakdown

        result_columns.append({
            "columnName": col,
            "pctRowsCovered": pct_rows,
            "spendCoverage": spend_coverage,
        })

    spend_type = "reporting" if (spend_col and is_reporting) else (
        "local" if spend_col else "none"
    )
    return {
        "columns": result_columns,
        "spendType": spend_type,
        "spendColumn": spend_col,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Spend Bifurcation
# ═══════════════════════════════════════════════════════════════════════════


def run_spend_bifurcation(
    conn: DuckDBConnection,
    table_name: str,
) -> dict[str, Any]:
    """Compute total positive and negative spend.

    Uses reporting spend if available; falls back to local spend grouped by
    currency.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.

    Returns:
        JSON-serialisable dict with ``type``, spend totals, and ``column``.
    """
    available = set(read_table_columns(conn, table_name))
    spend_col, is_reporting = _pick_spend_column(available)
    tbl = quote_id(table_name)

    if spend_col is None:
        return {"type": "none", "column": None}

    qs = quote_id(spend_col)
    spend_expr = _numeric_spend_expr(qs)
    # Separate expression that preserves actual numeric value (including negatives)
    raw_num = (
        f"CASE WHEN regexp_matches(TRIM({qs}), '[0-9]') "
        f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$') "
        f"THEN CAST({qs} AS REAL) ELSE NULL END"
    )

    if is_reporting:
        row = conn.execute(
            f"SELECT "
            f"  SUM(CASE WHEN ({raw_num}) > 0 THEN ({raw_num}) ELSE 0 END) AS pos, "
            f"  SUM(CASE WHEN ({raw_num}) < 0 THEN ({raw_num}) ELSE 0 END) AS neg "
            f"FROM {tbl}"
        ).fetchone()
        return {
            "type": "reporting",
            "positiveSpend": round(float(row["pos"] or 0)),
            "negativeSpend": round(float(row["neg"] or 0)),
            "column": spend_col,
        }

    # Local spend — group by currency
    ccy_col = _pick_currency_code_column(spend_col, available)
    if not ccy_col:
        # No currency column: single total like reporting
        row = conn.execute(
            f"SELECT "
            f"  SUM(CASE WHEN ({raw_num}) > 0 THEN ({raw_num}) ELSE 0 END) AS pos, "
            f"  SUM(CASE WHEN ({raw_num}) < 0 THEN ({raw_num}) ELSE 0 END) AS neg "
            f"FROM {tbl}"
        ).fetchone()
        return {
            "type": "local_single",
            "positiveSpend": round(float(row["pos"] or 0)),
            "negativeSpend": round(float(row["neg"] or 0)),
            "column": spend_col,
        }

    qcc = quote_id(ccy_col)
    rows = conn.execute(
        f"SELECT UPPER(TRIM({qcc})) AS code, "
        f"  SUM(CASE WHEN ({raw_num}) > 0 THEN ({raw_num}) ELSE 0 END) AS pos, "
        f"  SUM(CASE WHEN ({raw_num}) < 0 THEN ({raw_num}) ELSE 0 END) AS neg "
        f"FROM {tbl} "
        f"WHERE {qcc} IS NOT NULL AND TRIM({qcc}) != '' "
        f"GROUP BY UPPER(TRIM({qcc})) "
        f"ORDER BY pos DESC"
    ).fetchall()

    currencies = [
        {
            "code": str(r["code"]),
            "positiveSpend": round(float(r["pos"] or 0)),
            "negativeSpend": round(float(r["neg"] or 0)),
        }
        for r in rows
    ]
    return {
        "type": "local",
        "currencies": currencies,
        "column": spend_col,
    }
