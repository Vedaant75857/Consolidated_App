"""Executive Summary – Data Quality Assessment for the Spend Analyzer.

Self-contained service that computes fill rates, group-specific metrics,
and generates AI insights for a fixed set of target procurement columns.
Operates on the ``analysis_data`` table produced by column mapping.

This module is intentionally independent of Module-1's DQA implementation.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

from shared.ai_client import call_ai_json
from shared.db import get_meta

logger = logging.getLogger(__name__)

# Lookup table: fieldKey → displayName (populated from column_mapper at import time)
_FIELD_DISPLAY_NAMES: dict[str, str] = {}

def _load_field_display_names() -> dict[str, str]:
    """Load fieldKey→displayName from column_mapper.STANDARD_FIELDS (lazy, once)."""
    if _FIELD_DISPLAY_NAMES:
        return _FIELD_DISPLAY_NAMES
    try:
        from services.column_mapper import STANDARD_FIELDS
        for f in STANDARD_FIELDS:
            _FIELD_DISPLAY_NAMES[f["fieldKey"]] = f["displayName"]
    except ImportError:
        pass
    return _FIELD_DISPLAY_NAMES

# ═══════════════════════════════════════════════════════════════════════════
# A.  Column Registry
# ═══════════════════════════════════════════════════════════════════════════

COLUMN_GROUPS: list[dict[str, Any]] = [
    {
        "group": "Date",
        "columns": [
            {"fieldKey": "invoice_date", "displayName": "Invoice Date"},
        ],
    },
    {
        "group": "Spend",
        "columns": [
            {"fieldKey": "total_spend", "displayName": "Total Amount paid in Reporting Currency"},
            {"fieldKey": "local_spend", "displayName": "Total Amount paid in Local Currency"},
        ],
    },
    {
        "group": "Supplier",
        "columns": [
            {"fieldKey": "supplier", "displayName": "Vendor Name"},
        ],
    },
    {
        "group": "Currency",
        "columns": [
            {"fieldKey": "currency", "displayName": "Invoice Currency"},
        ],
    },
    {
        "group": "Description",
        "columns": [
            {"fieldKey": "po_material_description", "displayName": "PO Material Description"},
        ],
    },
]

PARETO_SPEND_FIELD = "total_spend"

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

_YEAR_RE = re.compile(r"((?:19|20)\d{2})")

# Date format detection patterns (adapted from Module 2 normalization agent)
_PAT_YEAR_FIRST  = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")
_PAT_DOT_SEP     = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")
_PAT_COMPACT8    = re.compile(r"^\d{8}$")
_PAT_YEAR_ONLY   = re.compile(r"^\d{4}$")
_PAT_SLASH_SEP   = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
_PAT_DASH_SEP    = re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$")
_TIME_SUFFIX_RE  = re.compile(r'\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$', re.IGNORECASE)
_ISO_T_RE        = re.compile(r'T\d{2}:\d{2}(:\d{2})?(\..*?)?(Z|[+-]\d{2}:\d{2})?$', re.IGNORECASE)
_MONTHS_RE       = re.compile(r'(?i)^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)')


def _profile_dmy_mdy(values: list[str]) -> str:
    """Score a list of date strings to determine DMY vs MDY order.

    Adapted from Module 2's ``_profile_date_series`` heuristic.
    """
    score_dmy = score_mdy = 0
    for raw in values:
        s = str(raw).strip()
        if _PAT_COMPACT8.match(s):
            continue
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
    if _PAT_YEAR_FIRST.match(v):
        return "YYYY-MM-DD"
    if _PAT_DOT_SEP.match(v):
        return "DD.MM.YYYY"
    if _PAT_COMPACT8.match(v):
        return "YYYYMMDD"
    if _PAT_YEAR_ONLY.match(v):
        return "YYYY"
    if _PAT_SLASH_SEP.match(v):
        return "DD/MM/YYYY" if order == "DMY" else "MM/DD/YYYY"
    if _PAT_DASH_SEP.match(v):
        return "DD-MM-YYYY" if order == "DMY" else "MM-DD-YYYY"
    return "other"


# ═══════════════════════════════════════════════════════════════════════════
# B.  SQL Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _quote_id(name: str) -> str:
    """Double-quote a SQLite identifier."""
    return f'"{name}"'


def _nn(qc: str) -> str:
    """SQL fragment: column is non-null and non-empty after trimming."""
    return f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"


def _safe_pct(num: int, den: int) -> float:
    if den == 0:
        return 0.0
    return round(num / den * 100, 2)


def _make_key(group: str, field_key: str) -> str:
    return f"{group}__{field_key}"


# ═══════════════════════════════════════════════════════════════════════════
# C.  Metrics Computation
# ═══════════════════════════════════════════════════════════════════════════

def _compute_fill_rates(
    conn: sqlite3.Connection,
    columns: list[str],
) -> dict[str, dict[str, Any]]:
    """Single-pass fill rate for multiple columns in ``analysis_data``."""
    if not columns:
        return {}
    parts = [
        f"COUNT(CASE WHEN {_nn(_quote_id(c))} THEN 1 END)"
        for c in columns
    ]
    sql = f'SELECT COUNT(*) AS total, {", ".join(parts)} FROM "analysis_data"'
    cur = conn.execute(sql)
    row = cur.fetchone()
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


def _compute_date_metrics(
    conn: sqlite3.Connection,
    column: str,
    sample_limit: int = 1000,
) -> dict[str, Any]:
    """Year range + global format consistency for a date column."""
    qc = _quote_id(column)
    rows = conn.execute(
        f'SELECT DISTINCT {qc} AS v FROM "analysis_data" WHERE {_nn(qc)} LIMIT 5000'
    ).fetchall()

    # Year range
    years: list[int] = []
    for r in rows:
        m = _YEAR_RE.search(str(r[0]))
        if m:
            years.append(int(m.group(1)))

    # Format consistency (global)
    values = [str(r[0]) for r in rows[:sample_limit]]
    order = _profile_dmy_mdy(values)
    counts: dict[str, int] = {}
    for v in values:
        fmt = _detect_format(v, order)
        counts[fmt] = counts.get(fmt, 0) + 1
    dominant = max(counts, key=counts.get) if counts else "unknown"

    return {
        "minYear": str(min(years)) if years else None,
        "maxYear": str(max(years)) if years else None,
        "formatConsistency": {
            "_global": {
                "dominantFormat": dominant,
                "formatCounts": counts,
                "consistent": len(counts) <= 1,
            }
        },
    }


def _compute_spend_metrics(
    conn: sqlite3.Connection,
    column: str,
) -> dict[str, Any]:
    """Total spend and numeric / non-numeric counts."""
    qc = _quote_id(column)
    row = conn.execute(
        f"SELECT "
        f"  SUM(CASE WHEN typeof({qc}) IN ('real','integer') OR "
        f"       (typeof({qc}) = 'text' AND TRIM({qc}) GLOB '*[0-9]*' "
        f"        AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*') "
        f"       THEN CAST({qc} AS REAL) ELSE 0 END) AS total_spend, "
        f"  COUNT(CASE WHEN {_nn(qc)} AND typeof({qc}) = 'text' "
        f"       AND (TRIM({qc}) NOT GLOB '*[0-9]*' "
        f"            OR TRIM({qc}) GLOB '*[^0-9.eE+-]*') "
        f"       THEN 1 END) AS non_numeric, "
        f"  COUNT(CASE WHEN {_nn(qc)} AND (typeof({qc}) IN ('real','integer') OR "
        f"       (typeof({qc}) = 'text' AND TRIM({qc}) GLOB '*[0-9]*' "
        f"        AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*')) "
        f"       THEN 1 END) AS numeric_ct "
        f'FROM "analysis_data"'
    ).fetchone()
    return {
        "totalSpend": float(row[0] or 0),
        "nonNumericCount": int(row[1] or 0),
        "numericCount": int(row[2] or 0),
    }


def _compute_supplier_metrics(
    conn: sqlite3.Connection,
    vendor_col: str,
    spend_col: str | None,
) -> dict[str, Any]:
    """Unique vendor count and Pareto 80 % analysis."""
    qv = _quote_id(vendor_col)

    unique_row = conn.execute(
        f'SELECT COUNT(DISTINCT {qv}) AS cnt FROM "analysis_data" WHERE {_nn(qv)}'
    ).fetchone()
    unique_vendors: int = int(unique_row[0] or 0)

    if not spend_col:
        return {
            "uniqueVendors": unique_vendors,
            "paretoFeasible": False,
            "paretoMessage": "Spend column not present — Pareto calculation not feasible.",
            "paretoVendorCount": None,
            "paretoVendorPct": None,
        }

    qs = _quote_id(spend_col)
    vendor_spend_rows = conn.execute(
        f"SELECT {qv} AS vendor, "
        f"  SUM(CAST(CASE WHEN {_nn(qs)} THEN {qs} ELSE '0' END AS REAL)) AS spend "
        f'FROM "analysis_data" WHERE {_nn(qv)} '
        f"GROUP BY {qv} ORDER BY spend DESC"
    ).fetchall()

    total_spend = sum(float(r[1] or 0) for r in vendor_spend_rows)
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
        cumulative += float(r[1] or 0)
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


def _compute_currency_metrics(
    conn: sqlite3.Connection,
    column: str,
) -> dict[str, Any]:
    """Distinct currency codes."""
    qc = _quote_id(column)
    rows = conn.execute(
        f'SELECT DISTINCT UPPER(TRIM(CAST({qc} AS TEXT))) AS code '
        f'FROM "analysis_data" WHERE {_nn(qc)} LIMIT 200'
    ).fetchall()
    codes = [str(r[0]) for r in rows]
    return {"distinctCount": len(codes), "codes": codes}


def _build_null_proxy_sql(qc: str) -> str:
    conditions = " OR ".join(
        f"LOWER(TRIM(CAST({qc} AS TEXT))) = '{v}'" for v in NULL_PROXY_VALUES
    )
    return f"COUNT(CASE WHEN ({conditions}) THEN 1 END)"


def _compute_description_metrics(
    conn: sqlite3.Connection,
    column: str,
) -> dict[str, Any]:
    """One-word / multi-word split, avg length, null-proxy count."""
    qc = _quote_id(column)
    nn = _nn(qc)
    proxy_expr = _build_null_proxy_sql(qc)

    sql = (
        f"SELECT "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM(CAST({qc} AS TEXT))) - LENGTH(REPLACE(TRIM(CAST({qc} AS TEXT)), ' ', '')) = 0 "
        f"       THEN 1 END) AS one_word, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND LENGTH(TRIM(CAST({qc} AS TEXT))) - LENGTH(REPLACE(TRIM(CAST({qc} AS TEXT)), ' ', '')) > 0 "
        f"       THEN 1 END) AS multi_word, "
        f"  AVG(CASE WHEN {nn} THEN LENGTH(TRIM(CAST({qc} AS TEXT))) END) AS avg_len, "
        f"  {proxy_expr} AS proxy_ct, "
        f"  COUNT(CASE WHEN {nn} "
        f"       AND (TRIM(CAST({qc} AS TEXT)) GLOB '*[A-Za-z]*' "
        f"            OR TRIM(CAST({qc} AS TEXT)) GLOB '*[0-9]*') "
        f"       THEN 1 END) AS alphanumeric_ct "
        f'FROM "analysis_data"'
    )
    row = conn.execute(sql).fetchone()
    return {
        "oneWordCount": int(row[0] or 0),
        "multiWordCount": int(row[1] or 0),
        "avgCharLength": round(float(row[2] or 0), 1),
        "nullProxyCount": int(row[3] or 0),
        "alphanumericCount": int(row[4] or 0),
    }


# ── non-procurable spend ──────────────────────────────────────────────────

def _compute_non_procurable_spend(
    conn: sqlite3.Connection,
    desc_column: str,
    spend_column: str | None,
) -> dict[str, Any]:
    """Sum spend for rows whose description matches non-procurable keywords."""
    if spend_column is None:
        return {"nonProcurableSpend": None, "spendColumnUsed": None}

    qd = _quote_id(desc_column)
    qs = _quote_id(spend_column)

    keyword_conditions = " OR ".join(
        f"INSTR(LOWER(TRIM(CAST({qd} AS TEXT))), '{kw}') > 0"
        for kw in NON_PROCURABLE_KEYWORDS
    )
    numeric_check = (
        f"(typeof({qs}) IN ('real','integer') OR "
        f" (typeof({qs}) = 'text' AND TRIM({qs}) GLOB '*[0-9]*' "
        f"  AND TRIM({qs}) NOT GLOB '*[^0-9.eE+-]*'))"
    )

    sql = (
        f"SELECT SUM(CASE "
        f"  WHEN ({keyword_conditions}) AND {numeric_check} "
        f"  THEN CAST({qs} AS REAL) ELSE 0 END"
        f") AS non_proc_spend "
        f'FROM "analysis_data" '
        f"WHERE {_nn(qd)}"
    )
    row = conn.execute(sql).fetchone()
    return {
        "nonProcurableSpend": round(float(row[0] or 0), 2),
        "spendColumnUsed": spend_column,
    }


# ── currency quality analysis ─────────────────────────────────────────────

def _compute_currency_quality_analysis(
    conn: sqlite3.Connection,
    currency_column: str,
    local_spend_col: str | None,
    reporting_spend_col: str | None,
    total_rows: int,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """Per-currency-code breakdown with row coverage and spend totals."""
    qc = _quote_id(currency_column)
    nn = _nn(qc)

    def _spend_expr(spend_col: str | None, alias: str) -> str:
        if spend_col is None:
            return f"NULL AS {alias}"
        qs = _quote_id(spend_col)
        return (
            f"SUM(CASE WHEN typeof({qs}) IN ('real','integer') OR "
            f"(typeof({qs}) = 'text' AND TRIM({qs}) GLOB '*[0-9]*' "
            f"AND TRIM({qs}) NOT GLOB '*[^0-9.eE+-]*') "
            f"THEN CAST({qs} AS REAL) ELSE 0 END) AS {alias}"
        )

    local_expr = _spend_expr(local_spend_col, "local_spend")
    reporting_expr = _spend_expr(reporting_spend_col, "reporting_spend")

    sql = (
        f"SELECT UPPER(TRIM(CAST({qc} AS TEXT))) AS code, COUNT(*) AS row_ct, "
        f"{local_expr}, {reporting_expr} "
        f'FROM "analysis_data" WHERE {nn} '
        f"GROUP BY UPPER(TRIM(CAST({qc} AS TEXT))) "
        f"ORDER BY reporting_spend DESC NULLS LAST, row_ct DESC"
    )
    rows = conn.execute(sql).fetchall()

    result: list[dict[str, Any]] = []
    others_row_ct = 0
    others_local = 0.0
    others_reporting = 0.0
    has_others = False

    for i, r in enumerate(rows):
        row_ct = int(r[1] or 0)
        local_val = float(r[2]) if r[2] is not None else None
        reporting_val = float(r[3]) if r[3] is not None else None

        if i < top_n:
            result.append({
                "currencyCode": str(r[0]),
                "rowCount": row_ct,
                "rowPct": _safe_pct(row_ct, total_rows),
                "localSpend": round(local_val, 2) if local_val is not None else None,
                "reportingSpend": round(reporting_val, 2) if reporting_val is not None else None,
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
            "localSpend": round(others_local, 2) if local_spend_col else None,
            "reportingSpend": round(others_reporting, 2) if reporting_spend_col else None,
        })

    return result


# ═══════════════════════════════════════════════════════════════════════════
# D.  AI Prompt & Insight Generation
# ═══════════════════════════════════════════════════════════════════════════

DQA_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant at a top-tier management
consulting firm.  You will receive a JSON payload containing computed metrics
for a client's procurement dataset, grouped by parameter category (Date,
Spend, Supplier, Currency, Description).

For **each** entry in the "columns" array of every parameter group, write a
concise, actionable insight (2–4 sentences).  Speak directly to the data
steward / analyst.  Reference the key metric numbers (fill rate, year span,
vendor concentration, etc.) using **bold** where impactful.

### Guidelines per parameter:

**Date columns**
- State the year range (e.g. "Data spans **2019–2024**").
- Flag any format inconsistencies across the data.  If all values use the
  same format, say so.
- Note if the fill rate is low and what downstream analyses may be affected.

**Spend columns**
- Report the total spend figure (use friendly formatting: $1.2B, €450M, etc.).
- Flag non-numeric values if present.
- If both reporting and local currency exist, note both; otherwise state which
  is available.

**Supplier (Vendor Name)**
- State unique vendor count.
- If Pareto data is available, state how many vendors (count and %) cover 80%
  of total spend—this is a key procurement insight.
- If Pareto is not feasible, explain why.

**Description columns**
- Summarise one-word vs multi-word split and what that implies for
  classification (spend cube) feasibility.
- Flag null-proxy values (unclassified, NA, etc.) if the count is material.
- Comment on average character length and what it says about description
  richness.
- Reference the alphanumeric value count — a high count relative to fill
  rate suggests clean, usable text; a low count may indicate codes or
  placeholder data.
- If non-procurable spend is material (relative to total spend), highlight
  it as a data-quality flag — these rows may need to be excluded from
  category analysis.

**Currency columns**
- List the currency codes found.
- If many currencies exist, note multi-currency complexity.
- If only one currency is present, note normalisation may not be needed.
- If a currency quality breakdown is provided, highlight the dominant
  currency by spend and note any long-tail currencies that cover a small
  share of rows.

### Output format

Return a JSON object:
```
{
  "parameterInsights": [
    {
      "parameterKey": "<group>__<column_name>",
      "insight": "<your insight>"
    }
  ]
}
```

``parameterKey`` must match **exactly** the keys provided in the input under
each column entry (``"parameterKey"``).  Do NOT invent keys or omit any.

Keep each insight to 2–4 sentences.  No bullet lists inside individual
insights.  Use markdown **bold** for numbers only.
"""


def _generate_insights(
    metrics_payload: dict[str, Any],
    api_key: str,
) -> dict[str, str]:
    """Send metrics to the LLM and return parameterKey → insight text."""
    logger.info("Generating executive-summary insights via LLM (rows=%s)",
                metrics_payload.get("totalRows"))
    result = call_ai_json(DQA_SYSTEM_PROMPT, metrics_payload, api_key=api_key)
    entries: list[dict[str, str]] = result.get("parameterInsights", [])
    return {
        str(e["parameterKey"]): str(e["insight"])
        for e in entries if "parameterKey" in e
    }


# ═══════════════════════════════════════════════════════════════════════════
# E.  Fill Rate Summary (target columns only)
# ═══════════════════════════════════════════════════════════════════════════

# Pre-compute the set of target field keys from COLUMN_GROUPS
_TARGET_FIELD_KEYS: set[str] = {
    col["fieldKey"]
    for grp in COLUMN_GROUPS
    for col in grp["columns"]
}


def _compute_fill_rate_summary(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Compute fill rate and unique-value count for target columns in analysis_data.

    Only includes field keys defined in COLUMN_GROUPS (the 6 target columns).
    """
    display_names = _load_field_display_names()
    # Also include displayNames from COLUMN_GROUPS for target columns
    for grp in COLUMN_GROUPS:
        for col in grp["columns"]:
            display_names.setdefault(col["fieldKey"], col["displayName"])

    # Verify which target fieldKeys actually exist in analysis_data
    pragma_rows = conn.execute('PRAGMA table_info("analysis_data")').fetchall()
    available: set[str] = {str(r[1]) for r in pragma_rows}
    present_keys = [fk for fk in _TARGET_FIELD_KEYS if fk in available]

    if not present_keys:
        return []

    # Total rows
    total_rows: int = conn.execute('SELECT COUNT(*) FROM "analysis_data"').fetchone()[0]

    # Batch fill rates
    fill_rates = _compute_fill_rates(conn, present_keys) if present_keys else {}

    # Unique counts (one query per column)
    summary: list[dict[str, Any]] = []
    for fk in present_keys:
        qc = _quote_id(fk)
        unique_row = conn.execute(
            f'SELECT COUNT(DISTINCT {qc}) FROM "analysis_data" WHERE {_nn(qc)}'
        ).fetchone()
        unique_count = int(unique_row[0] or 0)
        fr = fill_rates.get(fk, {})

        summary.append({
            "fieldKey": fk,
            "displayName": display_names.get(fk, fk),
            "filledRows": fr.get("non_null", 0),
            "totalRows": total_rows,
            "fillRate": fr.get("fill_rate", 0.0),
            "uniqueValues": unique_count,
        })

    return summary


# ═══════════════════════════════════════════════════════════════════════════
# F.  Public Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

# Metric-dispatch table keyed by group name
_METRIC_DISPATCH: dict[str, str] = {
    "Date": "date",
    "Spend": "spend",
    "Supplier": "supplier",
    "Currency": "currency",
    "Description": "description",
}


def run_executive_summary(
    conn: sqlite3.Connection,
    api_key: str,
) -> dict[str, Any]:
    """Run the full executive-summary DQA pipeline on ``analysis_data``.

    Returns:
        JSON-serialisable dict with ``totalRows`` and ``parameters`` list.
    """
    # 1. Check table exists
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_data'"
    ).fetchone()
    if not exists:
        raise ValueError("Table 'analysis_data' not found. Please confirm column mapping first.")

    # 2. Total rows
    total_rows: int = conn.execute('SELECT COUNT(*) FROM "analysis_data"').fetchone()[0]

    # 3. Resolve available columns
    pragma_rows = conn.execute('PRAGMA table_info("analysis_data")').fetchall()
    available: set[str] = {str(r[1]) for r in pragma_rows}

    # 4. Determine which target fieldKeys are present
    all_field_keys: list[str] = []
    for grp in COLUMN_GROUPS:
        for col in grp["columns"]:
            if col["fieldKey"] in available:
                all_field_keys.append(col["fieldKey"])

    # 5. Batch fill rates
    fill_rates = _compute_fill_rates(conn, all_field_keys) if all_field_keys else {}

    # Check if total_spend is available for Pareto
    pareto_spend_col = PARETO_SPEND_FIELD if PARETO_SPEND_FIELD in available else None

    # 6. Build per-group results
    parameters: list[dict[str, Any]] = []

    for grp in COLUMN_GROUPS:
        group_name = grp["group"]
        entries: list[dict[str, Any]] = []

        for col_def in grp["columns"]:
            fk = col_def["fieldKey"]
            display = col_def["displayName"]
            key = _make_key(group_name, fk)

            if fk not in available:
                entries.append({
                    "columnName": display,
                    "parameterKey": key,
                    "fillRate": 0.0,
                    "mapped": False,
                    "stats": {},
                })
                continue

            fr = fill_rates[fk]

            # Group-specific metrics
            if group_name == "Date":
                stats = _compute_date_metrics(conn, fk)
            elif group_name == "Spend":
                stats = _compute_spend_metrics(conn, fk)
            elif group_name == "Supplier":
                stats = _compute_supplier_metrics(conn, fk, pareto_spend_col)
            elif group_name == "Currency":
                stats = _compute_currency_metrics(conn, fk)
                # Currency quality analysis table
                local_col = "local_spend" if "local_spend" in available else None
                reporting_col = "total_spend" if "total_spend" in available else None
                stats["currencyQuality"] = _compute_currency_quality_analysis(
                    conn, fk, local_col, reporting_col, total_rows,
                )
                stats["hasLocalSpend"] = local_col is not None
                stats["hasReportingSpend"] = reporting_col is not None
            elif group_name == "Description":
                stats = _compute_description_metrics(conn, fk)
                # Non-procurable spend
                np_stats = _compute_non_procurable_spend(conn, fk, pareto_spend_col)
                stats.update(np_stats)
                stats["currencyLabel"] = PARETO_SPEND_FIELD if pareto_spend_col else None
            else:
                stats = {}

            entries.append({
                "columnName": display,
                "parameterKey": key,
                "fillRate": fr["fill_rate"],
                "mapped": True,
                "stats": stats,
            })

        parameters.append({"group": group_name, "columns": entries})

    # 7. Fill rate summary (all mapped columns)
    fill_rate_summary = _compute_fill_rate_summary(conn)

    # 8. AI insights
    ai_payload = {"totalRows": total_rows, "parameters": parameters}
    insights_map = _generate_insights(ai_payload, api_key)

    # 9. Merge insights
    for group in parameters:
        for entry in group["columns"]:
            pk = entry["parameterKey"]
            if not entry["mapped"]:
                entry["insight"] = "Column not present in data."
            else:
                entry["insight"] = insights_map.get(pk, "")

    return {
        "totalRows": total_rows,
        "parameters": parameters,
        "fillRateSummary": fill_rate_summary,
    }
