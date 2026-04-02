"""Compute the Executive Summary rows for the dashboard.

Each row corresponds to one of the five key columns and contains:
- fill-rate data (from the persisted cast report)
- an insight string computed freshly from *analysis_data*

All description-quality metrics are computed locally — nothing is imported
from the quality_analysis package.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

import pandas as pd

from shared.db import get_meta
from shared.formatting import format_spend

_DELIMITER_RE = re.compile(r"[-_/]")
_HAS_LETTER = re.compile(r"[a-zA-Z]")
_HAS_DIGIT = re.compile(r"\d")

_FIELD_DEFS: list[dict[str, str]] = [
    {"key": "invoice_date", "label": "Date"},
    {"key": "supplier", "label": "Supplier"},
    {"key": "total_spend", "label": "Amount in Reporting Currency"},
    {"key": "po_material_description", "label": "Description"},
    {"key": "currency", "label": "Currency"},
]


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)


def _is_coded_value(value: str) -> bool:
    """Return True when *value* looks like a part-number / code."""
    if _DELIMITER_RE.search(value):
        return True
    if _HAS_LETTER.search(value) and _HAS_DIGIT.search(value):
        return True
    stripped = value.strip()
    if stripped and stripped.isalnum() and " " not in stripped:
        return True
    return False


def _date_insight(df: pd.DataFrame) -> str | None:
    if "invoice_date" not in df.columns:
        return None
    dates = pd.to_datetime(df["invoice_date"], errors="coerce").dropna()
    if dates.empty:
        return None
    min_d = dates.min()
    max_d = dates.max()
    return f"{min_d.strftime('%b %Y')} \u2013 {max_d.strftime('%b %Y')}"


def _supplier_insight(df: pd.DataFrame) -> str | None:
    if "supplier" not in df.columns or "total_spend" not in df.columns:
        return None
    work = df.dropna(subset=["supplier", "total_spend"]).copy()
    work = work[work["supplier"].astype(str).str.strip() != ""]
    if work.empty:
        return None

    spend = (
        work.groupby("supplier")["total_spend"]
        .sum()
        .sort_values(ascending=False)
    )
    total_suppliers = len(spend)
    total_spend = spend.sum()
    if total_spend == 0:
        return f"{total_suppliers} total suppliers"

    cum_pct = (spend.cumsum() / total_spend * 100)
    cutoff = cum_pct[cum_pct >= 80.0]
    top_count = cutoff.index.get_loc(cutoff.index[0]) + 1 if len(cutoff) > 0 else total_suppliers
    return f"{top_count} suppliers cover 80% of spend, {total_suppliers:,} total"


def _amount_insight(df: pd.DataFrame) -> str | None:
    if "total_spend" not in df.columns:
        return None
    total = pd.to_numeric(df["total_spend"], errors="coerce").sum()
    if pd.isna(total) or total == 0:
        return None
    return f"{format_spend(total)} total spend"


def _description_insight(df: pd.DataFrame) -> str | None:
    if "po_material_description" not in df.columns:
        return None
    series = df["po_material_description"].fillna("").astype(str)
    non_null = series[series != ""]
    non_null_count = len(non_null)
    if non_null_count == 0:
        return None

    word_counts = non_null.str.split().str.len()
    multi_word_pct = _safe_pct(int((word_counts >= 2).sum()), non_null_count)

    coded_count = int(non_null.apply(_is_coded_value).sum())
    coded_pct = _safe_pct(coded_count, non_null_count)

    return f"{multi_word_pct}% multi-word, {coded_pct}% coded/alphanumeric"


def _currency_insight(df: pd.DataFrame) -> str | None:
    if "currency" not in df.columns:
        return None
    currencies = (
        df["currency"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    currencies = currencies[currencies != ""]
    unique = sorted(currencies.unique())
    if not unique:
        return None
    return ", ".join(unique)


_INSIGHT_FUNCS: dict[str, Any] = {
    "invoice_date": _date_insight,
    "supplier": _supplier_insight,
    "total_spend": _amount_insight,
    "po_material_description": _description_insight,
    "currency": _currency_insight,
}


def compute_executive_summary(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return the five executive-summary rows sorted by fill rate descending."""

    cast_report = get_meta(conn, "cast_report") or {}
    fields = cast_report.get("fields", {})
    total_rows = cast_report.get("total_rows", 0)

    tables = [
        t[0]
        for t in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_data'"
        ).fetchall()
    ]
    df = pd.read_sql("SELECT * FROM analysis_data", conn) if tables else pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for fdef in _FIELD_DEFS:
        key = fdef["key"]
        label = fdef["label"]
        field_info = fields.get(key, {})
        mapped = field_info.get("mapped", False)

        fill_rate = field_info.get("parseRate", 0.0) if mapped else 0.0
        valid_rows = field_info.get("validRows", 0) if mapped else 0
        t_rows = total_rows or (field_info.get("validRows", 0) + field_info.get("nullRows", 0))

        insight_fn = _INSIGHT_FUNCS.get(key)
        insight = insight_fn(df) if (mapped and insight_fn and not df.empty) else None

        rows.append({
            "key": key,
            "label": label,
            "mapped": mapped,
            "fillRate": fill_rate,
            "validRows": valid_rows,
            "totalRows": t_rows,
            "insight": insight,
        })

    rows.sort(key=lambda r: r["fillRate"], reverse=True)
    return rows
