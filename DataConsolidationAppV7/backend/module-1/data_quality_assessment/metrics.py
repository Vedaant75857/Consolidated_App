"""Pure SQL-based metric computation for the Data Quality Assessment.

Each function takes a SQLite connection + table name + resolved column names,
runs aggregate SQL, and returns JSON-serialisable dicts.  No AI calls here.
"""

from __future__ import annotations

import re
from typing import Any

from shared.db import DuckDBConnection, quote_id, read_table_columns, table_exists, table_row_count


# ---------------------------------------------------------------------------
# Null-proxy aliases (aggressive list, case-insensitive matching)
# ---------------------------------------------------------------------------

NULL_PROXY_VALUES: list[str] = [
    "na", "#na", "n/a", "#n/a", "none", "null", "nan",
    "-", "unknown", "not applicable", "tbd",
    "misc", "miscellaneous", "other", "general",
    "unclassified", "0", ".",
]

NON_PROCURABLE_KEYWORDS: list[str] = [
    "customs duties", "government fee", "license fee", "legal charges",
    "bank charges", "currency adjustment", "write-off",
    "taxes", "tax", "rebate", "duty", "freight", "shipping",
    "insurance", "penalty", "fine", "interest", "surcharge",
]

# Pre-compiled regex buckets for date format detection
# Unambiguous patterns (order doesn't matter)
_PAT_YEAR_FIRST  = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")
_PAT_DOT_SEP     = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")
_PAT_COMPACT8    = re.compile(r"^\d{8}$")
_PAT_YEAR_ONLY   = re.compile(r"^\d{4}$")
# Ambiguous patterns (need DMY/MDY heuristic to resolve)
_PAT_SLASH_SEP   = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
_PAT_DASH_SEP    = re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$")
# Cleaning helpers (adapted from Module 2 normalization agent)
_TIME_SUFFIX_RE  = re.compile(r'\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$', re.IGNORECASE)
_ISO_T_RE        = re.compile(r'T\d{2}:\d{2}(:\d{2})?(\..*?)?(Z|[+-]\d{2}:\d{2})?$', re.IGNORECASE)
_MONTHS_RE       = re.compile(r'(?i)^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)')

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
    conn: DuckDBConnection,
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

def _profile_dmy_mdy(values: list[str]) -> str:
    """Score a list of date strings to determine DMY vs MDY order.

    Adapted from Module 2's ``_profile_date_series`` heuristic.
    Returns ``'DMY'`` or ``'MDY'``.
    """
    score_dmy = score_mdy = 0
    for raw in values:
        s = str(raw).strip()
        if _PAT_COMPACT8.match(s):
            continue
        # Normalise separators
        s = _ISO_T_RE.sub("", _TIME_SUFFIX_RE.sub("", s).strip()).strip()
        s = re.sub(r"[/\.\s,]+", "-", s)
        parts = s.split("-")
        if len(parts) < 2:
            continue
        p0, p1 = parts[0], parts[1]
        try:
            if int(p0) > 12:
                score_dmy += 1
        except ValueError:
            pass
        try:
            if int(p1) > 12:
                score_mdy += 1
        except ValueError:
            pass
        if _MONTHS_RE.match(p0):
            score_mdy += 1
        if _MONTHS_RE.match(p1):
            score_dmy += 1
    return "MDY" if score_mdy > score_dmy else "DMY"


def _detect_format(value: str, order: str = "DMY") -> str:
    """Classify a single date string into a format bucket.

    *order* (``'DMY'`` or ``'MDY'``) resolves ambiguous slash/dash patterns.
    """
    v = value.strip()
    # Unambiguous patterns first
    if _PAT_YEAR_FIRST.match(v):
        return "YYYY-MM-DD"
    if _PAT_DOT_SEP.match(v):
        return "DD.MM.YYYY"
    if _PAT_COMPACT8.match(v):
        return "YYYYMMDD"
    if _PAT_YEAR_ONLY.match(v):
        return "YYYY"
    # Ambiguous slash/dash patterns — use heuristic order
    if _PAT_SLASH_SEP.match(v):
        return "DD/MM/YYYY" if order == "DMY" else "MM/DD/YYYY"
    if _PAT_DASH_SEP.match(v):
        return "DD-MM-YYYY" if order == "DMY" else "MM-DD-YYYY"
    return "other"


def _extract_year(value: str) -> str | None:
    """Pull a 4-digit year (19xx/20xx) from a date string."""
    m = _YEAR_RE.search(value)
    return m.group(1) if m else None


def compute_date_metrics(
    conn: DuckDBConnection,
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

    # Profile DMY vs MDY order from the sampled values
    order = _profile_dmy_mdy([str(r["v"]) for r in rows])

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
                fmt = _detect_format(str(sr["v"]), order)
                counts[fmt] = counts.get(fmt, 0) + 1
            dominant = max(counts, key=counts.get) if counts else "unknown"
            total_sampled = sum(counts.values())
            format_pcts = {fmt: round(_safe_pct(ct, total_sampled)) for fmt, ct in counts.items()}
            format_consistency[fname] = {
                "dominantFormat": dominant,
                "formatCounts": format_pcts,
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
            fmt = _detect_format(str(sr["v"]), order)
            counts[fmt] = counts.get(fmt, 0) + 1
        dominant = max(counts, key=counts.get) if counts else "unknown"
        total_sampled = sum(counts.values())
        format_pcts = {fmt: round(_safe_pct(ct, total_sampled)) for fmt, ct in counts.items()}
        format_consistency["_global"] = {
            "dominantFormat": dominant,
            "formatCounts": format_pcts,
            "consistent": len(counts) <= 1,
        }

    return {
        "minYear": min_year,
        "maxYear": max_year,
        "formatConsistency": format_consistency,
    }


# ── spend metrics ──────────────────────────────────────────────────────────

def compute_spend_metrics(
    conn: DuckDBConnection,
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
        f"  COUNT(*) AS total, "
        f"  SUM(CASE WHEN regexp_matches(TRIM({qc}), '[0-9]') "
        f"       AND regexp_matches(TRIM({qc}), '^[0-9eE.+-]+$') "
        f"       THEN CAST({qc} AS REAL) ELSE 0 END) AS total_spend, "
        f"  COUNT(CASE WHEN {_non_null_condition(qc)} "
        f"       AND (NOT regexp_matches(TRIM({qc}), '[0-9]') "
        f"            OR NOT regexp_matches(TRIM({qc}), '^[0-9eE.+-]+$')) "
        f"       THEN 1 END) AS non_numeric, "
        f"  COUNT(CASE WHEN {_non_null_condition(qc)} "
        f"       AND regexp_matches(TRIM({qc}), '[0-9]') "
        f"       AND regexp_matches(TRIM({qc}), '^[0-9eE.+-]+$') "
        f"       THEN 1 END) AS numeric_ct "
        f"FROM {tbl}"
    ).fetchone()
    total: int = int(row["total"] or 0)
    return {
        "totalSpend": round(float(row["total_spend"] or 0)),
        "nonNumericPct": round(_safe_pct(int(row["non_numeric"] or 0), total)),
        "numericPct": round(_safe_pct(int(row["numeric_ct"] or 0), total)),
    }


# ── supplier metrics ───────────────────────────────────────────────────────

def compute_supplier_metrics(
    conn: DuckDBConnection,
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
        f"  SUM(CAST(CASE WHEN regexp_matches(TRIM({qs}), '[0-9]') "
        f"       AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$') "
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
    conn: DuckDBConnection,
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
        f"  COUNT(*) AS total, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM({qc})) - LENGTH(REPLACE(TRIM({qc}), ' ', '')) = 0 "
        f"       THEN 1 END) AS one_word, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM({qc})) - LENGTH(REPLACE(TRIM({qc}), ' ', '')) > 0 "
        f"       THEN 1 END) AS multi_word, "
        f"  AVG(CASE WHEN {nn} THEN LENGTH(TRIM({qc})) END) AS avg_len, "
        f"  {proxy_expr} AS proxy_ct, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND (regexp_matches(TRIM({qc}), '[A-Za-z]') OR regexp_matches(TRIM({qc}), '[0-9]')) "
        f"       THEN 1 END) AS alphanumeric_ct "
        f"FROM {tbl}"
    )
    row = conn.execute(sql).fetchone()
    total: int = int(row["total"] or 0)
    return {
        "oneWordPct": round(_safe_pct(int(row["one_word"] or 0), total)),
        "multiWordPct": round(_safe_pct(int(row["multi_word"] or 0), total)),
        "avgCharLength": round(float(row["avg_len"] or 0)),
        "nullProxyPct": round(_safe_pct(int(row["proxy_ct"] or 0), total)),
        "alphanumericPct": round(_safe_pct(int(row["alphanumeric_ct"] or 0), total)),
    }


# ── non-procurable spend ──────────────────────────────────────────────────

def compute_non_procurable_spend(
    conn: DuckDBConnection,
    table_name: str,
    desc_column: str,
    spend_column: str | None,
    *,
    currency_column: str | None = None,
) -> dict[str, Any]:
    """Sum spend for rows whose description matches non-procurable keywords.

    Args:
        conn: SQLite connection.
        table_name: Target table.
        desc_column: Description column to check for keyword matches.
        spend_column: Spend column to sum (None if unavailable).
        currency_column: Optional currency code column for per-currency breakdown.

    Returns:
        Dict with ``nonProcurableSpend``, ``spendColumnUsed``, and
        optionally ``nonProcurableSpendByCurrency`` (top 5 + Others).
    """
    if spend_column is None:
        return {
            "nonProcurableSpend": None,
            "spendColumnUsed": None,
            "nonProcurableSpendByCurrency": None,
        }

    tbl = quote_id(table_name)
    qd = quote_id(desc_column)
    qs = quote_id(spend_column)

    keyword_conditions = " OR ".join(
        f"INSTR(LOWER(TRIM({qd})), '{kw}') > 0"
        for kw in NON_PROCURABLE_KEYWORDS
    )
    numeric_check = (
        f"regexp_matches(TRIM({qs}), '[0-9]') "
        f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$')"
    )
    np_filter = f"({keyword_conditions}) AND ({numeric_check})"

    sql = (
        f"SELECT SUM(CASE WHEN {np_filter} "
        f"  THEN CAST({qs} AS REAL) ELSE 0 END"
        f") AS non_proc_spend "
        f"FROM {tbl} "
        f"WHERE {qd} IS NOT NULL AND TRIM({qd}) != ''"
    )
    row = conn.execute(sql).fetchone()
    total_np = round(float(row["non_proc_spend"] or 0))

    # Per-currency breakdown (top 5 + Others)
    by_currency: list[dict[str, Any]] | None = None
    if currency_column is not None:
        qc = quote_id(currency_column)
        rows = conn.execute(
            f"SELECT UPPER(TRIM({qc})) AS code, "
            f"  SUM(CASE WHEN {numeric_check} THEN CAST({qs} AS REAL) ELSE 0 END) AS spend "
            f"FROM {tbl} "
            f"WHERE {qd} IS NOT NULL AND TRIM({qd}) != '' "
            f"  AND ({keyword_conditions}) "
            f"  AND {qc} IS NOT NULL AND TRIM({qc}) != '' "
            f"GROUP BY UPPER(TRIM({qc})) "
            f"ORDER BY spend DESC"
        ).fetchall()
        top_5 = [
            {"code": str(r["code"]), "spend": round(float(r["spend"] or 0))}
            for r in rows[:5]
        ]
        if len(rows) > 5:
            others_spend = sum(float(r["spend"] or 0) for r in rows[5:])
            top_5.append({"code": "Others", "spend": round(others_spend)})
        by_currency = top_5

    return {
        "nonProcurableSpend": total_np,
        "spendColumnUsed": spend_column,
        "nonProcurableSpendByCurrency": by_currency,
    }


# ── alphanumeric spend ────────────────────────────────────────────────────

def compute_alphanumeric_spend(
    conn: DuckDBConnection,
    table_name: str,
    desc_column: str,
    spend_column: str | None,
    currency_column: str | None,
) -> dict[str, Any]:
    """Sum spend for rows with alphanumeric descriptions, optionally grouped by currency.

    Args:
        conn: SQLite connection.
        table_name: Target table.
        desc_column: Description column to check for alphanumeric content.
        spend_column: Spend column to sum (None if unavailable).
        currency_column: Currency code column for per-currency breakdown (None for single total).

    Returns:
        Dict with ``alphanumericSpendTotal``, ``alphanumericSpendByCurrency``,
        and ``alphanumericSpendColumn``.
    """
    if spend_column is None:
        return {
            "alphanumericSpendTotal": None,
            "alphanumericSpendByCurrency": None,
            "alphanumericSpendColumn": None,
        }

    tbl = quote_id(table_name)
    qd = quote_id(desc_column)
    qs = quote_id(spend_column)
    nn_d = _non_null_condition(qd)

    alphanumeric_cond = (
        f"(regexp_matches(TRIM({qd}), '[A-Za-z]') OR regexp_matches(TRIM({qd}), '[0-9]'))"
    )
    numeric_spend = (
        f"regexp_matches(TRIM({qs}), '[0-9]') "
        f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$')"
    )

    # Total alphanumeric spend
    total_row = conn.execute(
        f"SELECT SUM(CASE WHEN {numeric_spend} THEN CAST({qs} AS REAL) ELSE 0 END) AS alpha_spend "
        f"FROM {tbl} "
        f"WHERE {nn_d} AND {alphanumeric_cond}"
    ).fetchone()
    alpha_total = round(float(total_row["alpha_spend"] or 0))

    # Per-currency breakdown (top 5 + Others)
    by_currency: list[dict[str, Any]] | None = None
    if currency_column is not None:
        qc = quote_id(currency_column)
        rows = conn.execute(
            f"SELECT UPPER(TRIM({qc})) AS code, "
            f"  SUM(CASE WHEN {numeric_spend} THEN CAST({qs} AS REAL) ELSE 0 END) AS spend "
            f"FROM {tbl} "
            f"WHERE {nn_d} AND {alphanumeric_cond} "
            f"  AND {qc} IS NOT NULL AND TRIM({qc}) != '' "
            f"GROUP BY UPPER(TRIM({qc})) "
            f"ORDER BY spend DESC"
        ).fetchall()
        top_5 = [
            {"code": str(r["code"]), "spend": round(float(r["spend"] or 0))}
            for r in rows[:5]
        ]
        if len(rows) > 5:
            others_spend = sum(float(r["spend"] or 0) for r in rows[5:])
            top_5.append({"code": "Others", "spend": round(others_spend)})
        by_currency = top_5

    return {
        "alphanumericSpendTotal": alpha_total,
        "alphanumericSpendByCurrency": by_currency,
        "alphanumericSpendColumn": spend_column,
    }


# ── currency metrics ───────────────────────────────────────────────────────

def compute_currency_metrics(
    conn: DuckDBConnection,
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


def compute_currency_quality_analysis(
    conn: DuckDBConnection,
    table_name: str,
    currency_column: str,
    local_spend_col: str | None,
    reporting_spend_col: str | None,
    total_rows: int,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """Per-currency-code breakdown with row coverage and spend totals.

    Args:
        conn: SQLite connection.
        table_name: Target table.
        currency_column: Currency code column to group by.
        local_spend_col: Local currency spend column (None if unavailable).
        reporting_spend_col: Reporting currency spend column (None if unavailable).
        total_rows: Total rows in the table (denominator for row %).
        top_n: Show top N currencies by reporting spend; rest aggregated as 'Others'.

    Returns:
        List of dicts sorted by reporting spend descending, each with
        ``currencyCode``, ``rowCount``, ``rowPct``, ``localSpend``, ``reportingSpend``.
    """
    tbl = quote_id(table_name)
    qc = quote_id(currency_column)
    nn = _non_null_condition(qc)

    def _spend_expr(spend_col: str | None, alias: str) -> str:
        if spend_col is None:
            return f"NULL AS {alias}"
        qs = quote_id(spend_col)
        return (
            f"SUM(CASE WHEN regexp_matches(TRIM({qs}), '[0-9]') "
            f"AND regexp_matches(TRIM({qs}), '^[0-9eE.+-]+$') "
            f"THEN CAST({qs} AS REAL) ELSE 0 END) AS {alias}"
        )

    local_expr = _spend_expr(local_spend_col, "local_spend")
    reporting_expr = _spend_expr(reporting_spend_col, "reporting_spend")

    sql = (
        f"SELECT UPPER(TRIM({qc})) AS code, COUNT(*) AS row_ct, "
        f"{local_expr}, {reporting_expr} "
        f"FROM {tbl} WHERE {nn} "
        f"GROUP BY UPPER(TRIM({qc})) "
        f"ORDER BY reporting_spend DESC NULLS LAST, row_ct DESC"
    )
    rows = conn.execute(sql).fetchall()

    result: list[dict[str, Any]] = []
    others_row_ct = 0
    others_local = 0.0
    others_reporting = 0.0
    has_others = False

    for i, r in enumerate(rows):
        row_ct = int(r["row_ct"] or 0)
        local_val = float(r["local_spend"]) if r["local_spend"] is not None else None
        reporting_val = float(r["reporting_spend"]) if r["reporting_spend"] is not None else None

        if i < top_n:
            result.append({
                "currencyCode": str(r["code"]),
                "rowCount": row_ct,
                "rowPct": _safe_pct(row_ct, total_rows),
                "localSpend": round(local_val) if local_val is not None else None,
                "reportingSpend": round(reporting_val) if reporting_val is not None else None,
            })
        else:
            has_others = True
            others_row_ct += row_ct
            if local_val is not None:
                others_local += local_val
            if reporting_val is not None:
                others_reporting += reporting_val

    if has_others:
        result.append({
            "currencyCode": "Others",
            "rowCount": others_row_ct,
            "rowPct": _safe_pct(others_row_ct, total_rows),
            "localSpend": round(others_local) if local_spend_col else None,
            "reportingSpend": round(others_reporting) if reporting_spend_col else None,
        })

    return result


# ── fill rate summary (all columns) ──────────────────────────────────────

_SYSTEM_COLUMNS: set[str] = {"FILE_NAME", "RECORD_ID"}


def compute_fill_rate_summary(
    conn: DuckDBConnection,
    table_name: str,
    exclude: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Compute fill rate and unique-value count for every column in a table.

    Args:
        conn: SQLite connection.
        table_name: Target table (e.g. ``final_merged_v1``).
        exclude: Additional column names to skip (combined with system cols).

    Returns:
        List of dicts with ``columnName``, ``filledRows``, ``totalRows``,
        ``fillRate``, ``uniqueValues``.
    """
    all_cols = read_table_columns(conn, table_name)
    skip = _SYSTEM_COLUMNS | (exclude or set())
    columns = [c for c in all_cols if c not in skip]

    if not columns:
        return []

    tbl = quote_id(table_name)
    total_rows = table_row_count(conn, table_name)
    fill_rates = compute_fill_rates(conn, table_name, columns)

    summary: list[dict[str, Any]] = []
    for col in columns:
        qc = quote_id(col)
        unique_row = conn.execute(
            f"SELECT COUNT(DISTINCT {qc}) FROM {tbl} "
            f"WHERE {_non_null_condition(qc)}"
        ).fetchone()
        unique_count = int(unique_row[0] or 0)
        fr = fill_rates.get(col, {})

        summary.append({
            "columnName": col,
            "filledRows": fr.get("non_null", 0),
            "totalRows": total_rows,
            "fillRate": fr.get("fill_rate", 0.0),
            "uniqueValues": unique_count,
        })

    return summary
