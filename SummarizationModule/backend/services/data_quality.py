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

_YEAR_RE = re.compile(r"((?:19|20)\d{2})")


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
) -> dict[str, Any]:
    """Year range for a date column stored as ISO strings."""
    qc = _quote_id(column)
    rows = conn.execute(
        f'SELECT DISTINCT {qc} AS v FROM "analysis_data" WHERE {_nn(qc)} LIMIT 5000'
    ).fetchall()
    years: list[int] = []
    for r in rows:
        m = _YEAR_RE.search(str(r[0]))
        if m:
            years.append(int(m.group(1)))
    return {
        "minYear": str(min(years)) if years else None,
        "maxYear": str(max(years)) if years else None,
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
        f"  {proxy_expr} AS proxy_ct "
        f'FROM "analysis_data"'
    )
    row = conn.execute(sql).fetchone()
    return {
        "oneWordCount": int(row[0] or 0),
        "multiWordCount": int(row[1] or 0),
        "avgCharLength": round(float(row[2] or 0), 1),
        "nullProxyCount": int(row[3] or 0),
    }


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

**Currency columns**
- List the currency codes found.
- If many currencies exist, note multi-currency complexity.
- If only one currency is present, note normalisation may not be needed.

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
# E.  Fill Rate Summary (all mapped columns)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_fill_rate_summary(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Compute fill rate and unique-value count for every mapped column in analysis_data.

    Uses the confirmed mapping stored in ``_meta`` to identify which fieldKeys
    were actually mapped, then runs a single SQL query for fill rates and
    individual queries for distinct counts.
    """
    display_names = _load_field_display_names()

    # Get confirmed mapping: {fieldKey: sourceColumnName | null}
    mapping: dict[str, str | None] = get_meta(conn, "mapping") or {}
    mapped_keys = [fk for fk, src in mapping.items() if src]

    if not mapped_keys:
        return []

    # Verify which fieldKeys actually exist in analysis_data
    pragma_rows = conn.execute('PRAGMA table_info("analysis_data")').fetchall()
    available: set[str] = {str(r[1]) for r in pragma_rows}
    present_keys = [fk for fk in mapped_keys if fk in available]

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
            elif group_name == "Description":
                stats = _compute_description_metrics(conn, fk)
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
