"""Pure SQL-based metric computation for the Data Quality Assessment.

Each function takes a SQLite connection + table name + resolved column names,
runs aggregate SQL, and returns JSON-serialisable dicts.  No AI calls here.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from shared.db import quote_id, read_table_columns, table_exists, table_row_count


# ---------------------------------------------------------------------------
# Null-proxy aliases (aggressive list, case-insensitive matching)
# ---------------------------------------------------------------------------

NULL_PROXY_VALUES: list[str] = [
    "na", "#na", "n/a", "#n/a", "none", "null", "nan",
    "-", "unknown", "not applicable", "tbd",
    "misc", "miscellaneous", "other", "general",
    "unclassified", "0", ".",
]

# Pre-compiled regex buckets for date format detection
_DATE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("YYYY-MM-DD", re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")),
    ("DD/MM/YYYY", re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),
    ("DD-MM-YYYY", re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$")),
    ("MM/DD/YYYY", re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),
    ("DD.MM.YYYY", re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")),
    ("YYYYMMDD",   re.compile(r"^\d{8}$")),
    ("YYYY",       re.compile(r"^\d{4}$")),
]

_YEAR_RE = re.compile(r"((?:19|20)\d{2})")


def _safe_pct(num: int, den: int) -> float:
    """Return percentage rounded to 2 decimals, 0.0 when denominator is 0."""
    if den == 0:
        return 0.0
    return round(num / den * 100, 2)


# ── helpers ────────────────────────────────────────────────────────────────

def _non_null_condition(qc: str) -> str:
    """SQL fragment: column is non-null and non-empty after trimming."""
    return f"{qc} IS NOT NULL AND TRIM({qc}) != ''"


# ── fill rate (batch) ──────────────────────────────────────────────────────

def compute_fill_rates(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
) -> dict[str, dict[str, Any]]:
    """Single-pass fill rate for multiple columns.

    Returns:
        ``{col_name: {"total": int, "non_null": int, "fill_rate": float}}``
    """
    if not columns:
        return {}
    tbl = quote_id(table_name)
    parts = [
        f"COUNT(CASE WHEN {_non_null_condition(quote_id(c))} THEN 1 END)"
        for c in columns
    ]
    sql = f"SELECT COUNT(*) AS total, {', '.join(parts)} FROM {tbl}"
    row = conn.execute(sql).fetchone()
    total: int = row[0]
    out: dict[str, dict[str, Any]] = {}
    for i, col in enumerate(columns):
        nn = row[1 + i]
        out[col] = {
            "total": total,
            "non_null": nn,
            "null_count": total - nn,
            "fill_rate": _safe_pct(nn, total),
        }
    return out


# ── date metrics ───────────────────────────────────────────────────────────

def _detect_format(value: str) -> str:
    """Classify a single date string into a format bucket."""
    v = value.strip()
    for label, pat in _DATE_PATTERNS:
        if pat.match(v):
            return label
    return "other"


def _extract_year(value: str) -> str | None:
    """Pull a 4-digit year (19xx/20xx) from a date string."""
    m = _YEAR_RE.search(value)
    return m.group(1) if m else None


def compute_date_metrics(
    conn: sqlite3.Connection,
    table_name: str,
    column: str,
    sample_per_file: int = 100,
) -> dict[str, Any]:
    """Compute date column metrics: year range + per-file format consistency.

    Args:
        conn: SQLite connection.
        table_name: Target table.
        column: Date column name.
        sample_per_file: Max values sampled per FILE_NAME for format detection.

    Returns:
        Dict with keys ``minYear``, ``maxYear``, ``formatConsistency``.
    """
    tbl = quote_id(table_name)
    qc = quote_id(column)
    all_cols = set(read_table_columns(conn, table_name))
    has_file_name = "FILE_NAME" in all_cols

    # Year range via sampling distinct non-null values (capped at 5000)
    rows = conn.execute(
        f"SELECT DISTINCT {qc} AS v FROM {tbl} "
        f"WHERE {_non_null_condition(qc)} LIMIT 5000"
    ).fetchall()
    years: list[int] = []
    for r in rows:
        y = _extract_year(str(r["v"]))
        if y:
            years.append(int(y))
    min_year = str(min(years)) if years else None
    max_year = str(max(years)) if years else None

    # Format consistency per FILE_NAME
    format_consistency: dict[str, Any] = {}
    if has_file_name:
        files = conn.execute(
            f"SELECT DISTINCT \"FILE_NAME\" AS fn FROM {tbl} "
            f"WHERE \"FILE_NAME\" IS NOT NULL AND TRIM(\"FILE_NAME\") != ''"
        ).fetchall()
        for frow in files:
            fname = str(frow["fn"])
            sampled = conn.execute(
                f"SELECT {qc} AS v FROM {tbl} "
                f"WHERE \"FILE_NAME\" = ? AND {_non_null_condition(qc)} LIMIT ?",
                (fname, sample_per_file),
            ).fetchall()
            counts: dict[str, int] = {}
            for sr in sampled:
                fmt = _detect_format(str(sr["v"]))
                counts[fmt] = counts.get(fmt, 0) + 1
            dominant = max(counts, key=counts.get) if counts else "unknown"
            format_consistency[fname] = {
                "dominantFormat": dominant,
                "formatCounts": counts,
                "consistent": len(counts) <= 1,
            }
    else:
        # Global fallback when FILE_NAME is absent
        sampled = conn.execute(
            f"SELECT {qc} AS v FROM {tbl} "
            f"WHERE {_non_null_condition(qc)} LIMIT ?",
            (sample_per_file * 10,),
        ).fetchall()
        counts = {}
        for sr in sampled:
            fmt = _detect_format(str(sr["v"]))
            counts[fmt] = counts.get(fmt, 0) + 1
        dominant = max(counts, key=counts.get) if counts else "unknown"
        format_consistency["_global"] = {
            "dominantFormat": dominant,
            "formatCounts": counts,
            "consistent": len(counts) <= 1,
        }

    return {
        "minYear": min_year,
        "maxYear": max_year,
        "formatConsistency": format_consistency,
    }


# ── spend metrics ──────────────────────────────────────────────────────────

def compute_spend_metrics(
    conn: sqlite3.Connection,
    table_name: str,
    column: str,
) -> dict[str, Any]:
    """Compute total spend and non-numeric count for a spend column.

    Returns:
        Dict with ``totalSpend``, ``nonNumericCount``, ``numericCount``.
    """
    tbl = quote_id(table_name)
    qc = quote_id(column)
    row = conn.execute(
        f"SELECT "
        f"  SUM(CASE WHEN TRIM({qc}) GLOB '*[0-9]*' "
        f"       AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*' "
        f"       THEN CAST({qc} AS REAL) ELSE 0 END) AS total_spend, "
        f"  COUNT(CASE WHEN {_non_null_condition(qc)} "
        f"       AND (TRIM({qc}) NOT GLOB '*[0-9]*' "
        f"            OR TRIM({qc}) GLOB '*[^0-9.eE+-]*') "
        f"       THEN 1 END) AS non_numeric, "
        f"  COUNT(CASE WHEN {_non_null_condition(qc)} "
        f"       AND TRIM({qc}) GLOB '*[0-9]*' "
        f"       AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*' "
        f"       THEN 1 END) AS numeric_ct "
        f"FROM {tbl}"
    ).fetchone()
    return {
        "totalSpend": float(row["total_spend"] or 0),
        "nonNumericCount": int(row["non_numeric"] or 0),
        "numericCount": int(row["numeric_ct"] or 0),
    }


# ── supplier metrics ───────────────────────────────────────────────────────

def compute_supplier_metrics(
    conn: sqlite3.Connection,
    table_name: str,
    vendor_col: str,
    spend_col: str | None,
) -> dict[str, Any]:
    """Vendor Name fill rate, unique count, and 80% Pareto analysis.

    Args:
        conn: SQLite connection.
        table_name: Target table.
        vendor_col: Resolved vendor name column.
        spend_col: Spend column for Pareto (None if unavailable).

    Returns:
        Dict with ``uniqueVendors``, ``paretoVendorCount``,
        ``paretoVendorPct``, ``paretoFeasible``.
    """
    tbl = quote_id(table_name)
    qv = quote_id(vendor_col)

    unique_row = conn.execute(
        f"SELECT COUNT(DISTINCT {qv}) AS cnt FROM {tbl} "
        f"WHERE {_non_null_condition(qv)}"
    ).fetchone()
    unique_vendors: int = int(unique_row["cnt"] or 0)

    if not spend_col:
        return {
            "uniqueVendors": unique_vendors,
            "paretoFeasible": False,
            "paretoMessage": "Reporting currency column not present — Pareto calculation not feasible.",
            "paretoVendorCount": None,
            "paretoVendorPct": None,
        }

    qs = quote_id(spend_col)
    vendor_spend_rows = conn.execute(
        f"SELECT {qv} AS vendor, "
        f"  SUM(CAST(CASE WHEN TRIM({qs}) GLOB '*[0-9]*' "
        f"       AND TRIM({qs}) NOT GLOB '*[^0-9.eE+-]*' "
        f"       THEN {qs} ELSE '0' END AS REAL)) AS spend "
        f"FROM {tbl} "
        f"WHERE {_non_null_condition(qv)} "
        f"GROUP BY {qv} ORDER BY spend DESC"
    ).fetchall()

    total_spend = sum(float(r["spend"] or 0) for r in vendor_spend_rows)
    if total_spend <= 0:
        return {
            "uniqueVendors": unique_vendors,
            "paretoFeasible": False,
            "paretoMessage": "Total spend is zero — Pareto calculation not feasible.",
            "paretoVendorCount": None,
            "paretoVendorPct": None,
        }

    cumulative = 0.0
    pareto_count = 0
    threshold = total_spend * 0.80
    for r in vendor_spend_rows:
        cumulative += float(r["spend"] or 0)
        pareto_count += 1
        if cumulative >= threshold:
            break

    return {
        "uniqueVendors": unique_vendors,
        "paretoFeasible": True,
        "paretoVendorCount": pareto_count,
        "paretoVendorPct": _safe_pct(pareto_count, unique_vendors),
        "paretoMessage": None,
    }


# ── description metrics ────────────────────────────────────────────────────

def _build_null_proxy_sql(qc: str) -> str:
    """SQL CASE expression counting null-proxy values (case-insensitive)."""
    conditions = " OR ".join(
        f"LOWER(TRIM({qc})) = '{v}'" for v in NULL_PROXY_VALUES
    )
    return f"COUNT(CASE WHEN ({conditions}) THEN 1 END)"


def compute_description_metrics(
    conn: sqlite3.Connection,
    table_name: str,
    column: str,
) -> dict[str, Any]:
    """Compute description quality metrics for a single column.

    Returns:
        Dict with ``oneWordCount``, ``multiWordCount``,
        ``avgCharLength``, ``nullProxyCount``.
    """
    tbl = quote_id(table_name)
    qc = quote_id(column)
    nn = _non_null_condition(qc)
    proxy_expr = _build_null_proxy_sql(qc)

    sql = (
        f"SELECT "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM({qc})) - LENGTH(REPLACE(TRIM({qc}), ' ', '')) = 0 "
        f"       THEN 1 END) AS one_word, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM({qc})) - LENGTH(REPLACE(TRIM({qc}), ' ', '')) > 0 "
        f"       THEN 1 END) AS multi_word, "
        f"  AVG(CASE WHEN {nn} THEN LENGTH(TRIM({qc})) END) AS avg_len, "
        f"  {proxy_expr} AS proxy_ct "
        f"FROM {tbl}"
    )
    row = conn.execute(sql).fetchone()
    return {
        "oneWordCount": int(row["one_word"] or 0),
        "multiWordCount": int(row["multi_word"] or 0),
        "avgCharLength": round(float(row["avg_len"] or 0), 1),
        "nullProxyCount": int(row["proxy_ct"] or 0),
    }


# ── currency metrics ───────────────────────────────────────────────────────

def compute_currency_metrics(
    conn: sqlite3.Connection,
    table_name: str,
    column: str,
    limit: int = 200,
) -> dict[str, Any]:
    """Distinct currency codes and count for a currency column.

    Returns:
        Dict with ``distinctCount`` and ``codes`` (list[str]).
    """
    tbl = quote_id(table_name)
    qc = quote_id(column)
    rows = conn.execute(
        f"SELECT DISTINCT UPPER(TRIM({qc})) AS code FROM {tbl} "
        f"WHERE {_non_null_condition(qc)} LIMIT ?",
        (limit,),
    ).fetchall()
    codes = [str(r["code"]) for r in rows]
    return {
        "distinctCount": len(codes),
        "codes": codes,
    }
