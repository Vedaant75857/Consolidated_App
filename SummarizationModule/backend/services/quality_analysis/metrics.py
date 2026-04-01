"""Pure metric computation for description and supplier data quality."""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_DELIMITER_RE = re.compile(r"[-_/]")
_HAS_LETTER = re.compile(r"[a-zA-Z]")
_HAS_DIGIT = re.compile(r"\d")


def _is_coded_value(value: str) -> bool:
    """Return True when *value* looks like a part-number / code rather than
    natural language.

    Matches any of:
    - Contains both letters and digits mixed together
    - Consists only of alphanumeric characters with no spaces / special chars
    - Contains delimiters: hyphens, underscores, or slashes
    """
    if _DELIMITER_RE.search(value):
        return True
    if _HAS_LETTER.search(value) and _HAS_DIGIT.search(value):
        return True
    stripped = value.strip()
    if stripped and stripped.isalnum() and " " not in stripped:
        return True
    return False


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)


def _description_metrics(series: pd.Series) -> dict[str, Any]:
    """Compute all description-quality metrics on a pandas string Series."""
    total = len(series)
    non_null = series[series != ""]
    non_null_count = len(non_null)
    completion = _safe_pct(non_null_count, total)

    if non_null_count == 0:
        return {
            "completionRate": completion,
            "codedAlphanumericPct": 0.0,
            "multiWordPct": 0.0,
            "singleWordPct": 0.0,
            "longPct": 0.0,
            "shortPct": 0.0,
            "intersections": {"AC": 0.0, "AD": 0.0, "BC": 0.0, "BD": 0.0},
        }

    coded_mask = non_null.apply(_is_coded_value)
    coded_pct = _safe_pct(int(coded_mask.sum()), non_null_count)

    word_counts = non_null.str.split().str.len()
    multi_word = word_counts >= 2  # A
    single_word = ~multi_word       # B

    char_lengths = non_null.str.len()
    long_desc = char_lengths > 10   # C
    short_desc = ~long_desc          # D

    a_pct = _safe_pct(int(multi_word.sum()), non_null_count)
    b_pct = _safe_pct(int(single_word.sum()), non_null_count)
    c_pct = _safe_pct(int(long_desc.sum()), non_null_count)
    d_pct = _safe_pct(int(short_desc.sum()), non_null_count)

    ac = int((multi_word & long_desc).sum())
    ad = int((multi_word & short_desc).sum())
    bc = int((single_word & long_desc).sum())
    bd = int((single_word & short_desc).sum())

    return {
        "completionRate": completion,
        "codedAlphanumericPct": coded_pct,
        "multiWordPct": a_pct,
        "singleWordPct": b_pct,
        "longPct": c_pct,
        "shortPct": d_pct,
        "intersections": {
            "AC": _safe_pct(ac, non_null_count),
            "AD": _safe_pct(ad, non_null_count),
            "BC": _safe_pct(bc, non_null_count),
            "BD": _safe_pct(bd, non_null_count),
        },
    }


def _supplier_fill_rate(series: pd.Series) -> float:
    """Return the fill-rate for the supplier column (% non-null)."""
    total = len(series)
    non_null_count = int((series != "").sum())
    return _safe_pct(non_null_count, total)


def compute_quality_metrics(
    conn: sqlite3.Connection,
    mapping: dict[str, str | None],
) -> dict[str, Any]:
    """Compute all quality-analysis metrics from the *analysis_data* table.

    Returns a dict ready to be serialised as JSON and sent to the frontend /
    to the AI summary prompt.
    """
    df = pd.read_sql("SELECT * FROM analysis_data", conn)
    total_rows = len(df)

    desc_col = "po_material_description"
    supplier_col = "supplier"

    has_description = (
        mapping.get(desc_col) is not None and desc_col in df.columns
    )
    has_supplier = (
        mapping.get(supplier_col) is not None and supplier_col in df.columns
    )

    desc_metrics: dict[str, Any]
    if has_description:
        series = df[desc_col].fillna("").astype(str)
        desc_metrics = _description_metrics(series)
    else:
        desc_metrics = {
            "completionRate": 0.0,
            "codedAlphanumericPct": 0.0,
            "multiWordPct": 0.0,
            "singleWordPct": 0.0,
            "longPct": 0.0,
            "shortPct": 0.0,
            "intersections": {"AC": 0.0, "AD": 0.0, "BC": 0.0, "BD": 0.0},
        }

    supplier_fill = (
        _supplier_fill_rate(df[supplier_col].fillna("").astype(str))
        if has_supplier
        else 0.0
    )

    return {
        "description": desc_metrics,
        "supplierFillRate": supplier_fill,
        "totalRows": total_rows,
        "hasDescription": has_description,
        "hasSupplier": has_supplier,
    }
