"""Centralized column resolution for the Data Quality Assessment.

Provides a single ``resolve_column`` function that finds the best match for a
DQA role (e.g. ``"vendor_name"``, ``"payment_terms"``) among the columns
actually present in a table.  Uses a 3-tier strategy:

1. **Exact** — case-insensitive match against canonical standard field names.
2. **Alias** — check the header-normalisation alias dictionary.
3. **Fuzzy** — ``difflib.SequenceMatcher`` against the canonical names for the
   role, with a configurable threshold (default 0.75).

All candidate lists are derived from ``STANDARD_FIELDS`` in
``header-normalisation/schema_mapper.py`` so there is a single source of truth.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from difflib import SequenceMatcher
from typing import Any

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_HN_DIR = os.path.join(_THIS_DIR, os.pardir, "header-normalisation")


def _load_mod(name: str, path: str):
    """Import a module by file path (needed because the directory has a hyphen)."""
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_aliases_mod = _load_mod("_hn_aliases", os.path.join(_HN_DIR, "aliases.py"))

ALIAS_LOOKUP: dict[str, str] = _aliases_mod.ALIAS_LOOKUP
_norm = _aliases_mod._norm
STD_FIELD_NAMES: list[str] = _aliases_mod.STD_FIELD_NAMES

FUZZY_THRESHOLD = 0.75

# ── Role → canonical field names ──────────────────────────────────────────
# Each role lists the standard field names (in priority order) that satisfy it.

DQA_ROLE_MAP: dict[str, list[str]] = {
    "date": [
        "Invoice Date",
        "Goods Receipt Date",
        "Payment date",
        "PO Document Date",
        "Contract End Date",
        "Contract Start Date",
    ],
    "spend_reporting": [
        "Total Amount paid in Reporting Currency",
        "PO Total Amount in reporting currency",
    ],
    "spend_local": [
        "Total Amount paid in Local Currency",
        "PO Total Amount in Local Currency",
    ],
    "currency_code": [
        "Local Currency Code",
        "PO Local Currency Code",
    ],
    "vendor_name": [
        "Vendor Name",
    ],
    "payment_terms": [
        "Payment Terms",
    ],
    "country": [
        "Vendor Country",
        "Company Country",
        "Plant Country",
        "Country Code",
    ],
    "region": [
        "Region",
    ],
    "file_name": [
        "FILE_NAME",
    ],
}


def _case_insensitive_lookup(available: set[str]) -> dict[str, str]:
    """Build a lowered-key → real-column-name map from ``available``."""
    return {c.strip().lower(): c for c in available}


def resolve_column(
    available: set[str],
    role: str,
    *,
    fuzzy: bool = True,
    extra_candidates: list[str] | None = None,
) -> str | None:
    """Find the best column in *available* for *role*.

    Args:
        available: Column names actually present in the table.
        role: A key from ``DQA_ROLE_MAP`` (e.g. ``"vendor_name"``).
        fuzzy: Whether to attempt fuzzy matching as a last resort.
        extra_candidates: Additional candidate names to try before the standard
            role list (useful for user overrides or legacy names).

    Returns:
        The real column name from *available*, or ``None``.
    """
    candidates = list(extra_candidates or []) + DQA_ROLE_MAP.get(role, [])
    if not candidates:
        return None

    lookup = _case_insensitive_lookup(available)

    # Tier 1: exact (case-insensitive)
    for candidate in candidates:
        real = lookup.get(candidate.strip().lower())
        if real is not None:
            return real

    # Tier 2: alias dictionary
    for col_lower, real_col in lookup.items():
        normed = _norm(real_col)
        target = ALIAS_LOOKUP.get(normed)
        if target and target.strip().lower() in {c.strip().lower() for c in candidates}:
            return real_col

    # Tier 3: fuzzy
    if fuzzy:
        best_score = 0.0
        best_col: str | None = None
        for col_lower, real_col in lookup.items():
            for candidate in candidates:
                score = SequenceMatcher(None, col_lower, candidate.lower()).ratio()
                if score > best_score:
                    best_score = score
                    best_col = real_col
        if best_score >= FUZZY_THRESHOLD and best_col is not None:
            return best_col

    return None


def resolve_all_columns(
    available: set[str],
    role: str,
    *,
    fuzzy: bool = True,
    extra_candidates: list[str] | None = None,
) -> list[str]:
    """Return *all* columns in *available* that match *role* (not just the first).

    Useful for date columns where multiple may exist. Results are ordered:
    exact matches first (in candidate priority order), then alias matches,
    then fuzzy matches.
    """
    candidates = list(extra_candidates or []) + DQA_ROLE_MAP.get(role, [])
    if not candidates:
        return []

    lookup = _case_insensitive_lookup(available)
    found: list[str] = []
    seen: set[str] = set()

    # Tier 1: exact
    for candidate in candidates:
        real = lookup.get(candidate.strip().lower())
        if real is not None and real not in seen:
            found.append(real)
            seen.add(real)

    # Tier 2: alias
    for col_lower, real_col in lookup.items():
        if real_col in seen:
            continue
        normed = _norm(real_col)
        target = ALIAS_LOOKUP.get(normed)
        if target and target.strip().lower() in {c.strip().lower() for c in candidates}:
            found.append(real_col)
            seen.add(real_col)

    # Tier 3: fuzzy
    if fuzzy:
        scored: list[tuple[float, str]] = []
        for col_lower, real_col in lookup.items():
            if real_col in seen:
                continue
            for candidate in candidates:
                score = SequenceMatcher(None, col_lower, candidate.lower()).ratio()
                if score >= FUZZY_THRESHOLD:
                    scored.append((score, real_col))
                    break
        scored.sort(key=lambda t: -t[0])
        for _, col in scored:
            if col not in seen:
                found.append(col)
                seen.add(col)

    return found


def find_date_columns(available: set[str]) -> list[str]:
    """Return date columns: exact/alias/fuzzy matches first, then any column
    whose name contains ``'date'`` (legacy fallback).
    """
    resolved = resolve_all_columns(available, "date")
    resolved_set = set(resolved)
    extras = sorted(
        c for c in available
        if "date" in c.lower() and c not in resolved_set
    )
    return resolved + extras


def find_country_columns(available: set[str]) -> list[str]:
    """Return all country-type columns found in the table."""
    resolved = resolve_all_columns(available, "country")
    resolved_set = set(resolved)
    extras = sorted(
        c for c in available
        if "country" in c.lower() and c not in resolved_set
    )
    return resolved + extras


def find_currency_columns(available: set[str]) -> list[str]:
    """Return currency-type columns: exact/alias/fuzzy matches first, then any
    column whose name contains ``'curr'`` (keyword fallback).
    """
    resolved = resolve_all_columns(available, "currency_code")
    resolved_set = set(resolved)
    extras = sorted(
        c for c in available
        if "curr" in c.lower() and c not in resolved_set
    )
    return resolved + extras


def find_payment_terms_columns(available: set[str]) -> list[str]:
    """Return payment-terms-type columns: exact/alias/fuzzy matches first,
    then any column whose name contains ``'payment'`` or ``'term'`` (keyword
    fallback).
    """
    resolved = resolve_all_columns(available, "payment_terms")
    resolved_set = set(resolved)
    keywords = {"payment", "term"}
    extras = sorted(
        c for c in available
        if any(kw in c.lower() for kw in keywords) and c not in resolved_set
    )
    return resolved + extras


def find_supplier_columns(available: set[str]) -> list[str]:
    """Return supplier/vendor-type columns: exact/alias/fuzzy matches first,
    then any column whose name contains ``'vendor'`` or ``'supplier'`` (keyword
    fallback).
    """
    resolved = resolve_all_columns(available, "vendor_name")
    resolved_set = set(resolved)
    keywords = {"vendor", "supplier"}
    extras = sorted(
        c for c in available
        if any(kw in c.lower() for kw in keywords) and c not in resolved_set
    )
    return resolved + extras


# ── Shared spend/currency helpers (used by date, fill-rate, payment-terms) ──

_CURRENCY_CODE_FOR_SPEND: dict[str, str] = {
    "Total Amount paid in Local Currency": "Local Currency Code",
    "PO Total Amount in Local Currency": "PO Local Currency Code",
}


def pick_spend_column(available: set[str]) -> tuple[str | None, bool]:
    """Pick the best spend column (reporting first, then local).

    Returns:
        (column_name, is_reporting_currency)
    """
    reporting = resolve_column(available, "spend_reporting", fuzzy=False)
    if reporting is not None:
        return reporting, True
    local = resolve_column(available, "spend_local", fuzzy=False)
    if local is not None:
        return local, False
    return None, False


def pick_currency_code_column(
    spend_col: str | None, available: set[str],
) -> str | None:
    """Return the currency code column paired with a local spend column."""
    if spend_col is None:
        return None
    key = spend_col.strip().lower()
    for canonical, code_col in _CURRENCY_CODE_FOR_SPEND.items():
        if canonical.lower() == key:
            return resolve_column(available, "currency_code",
                                  extra_candidates=[code_col], fuzzy=False)
    return None
