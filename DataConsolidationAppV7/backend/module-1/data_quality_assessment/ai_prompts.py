"""Consolidated AI prompts for the Data Quality Assessment.

Two thematic LLM calls replace the original five per-panel calls:

1. **Financial Quality** — date, currency, payment terms
2. **Entity Quality** — country/region, supplier

Each panel's insight is returned as a JSON array of exactly 3 strings:
  [main finding, reasoning with numbers, examples from data]

Individual per-panel wrappers are kept for backward-compatible routes.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.ai import call_ai_json

logger = logging.getLogger(__name__)

# ── Shared instruction block (appended to every system prompt) ────────────

_INSIGHT_FORMAT_RULES = """\
### Output rules (apply to EVERY insight array you produce)

- Each insight is a JSON array of exactly **3** short strings (10-15 words max each).
- Line 1: the single most important quality observation (the finding).
- Line 2: why it matters, backed by numbers from the data (the reasoning).
- Line 3: concrete example values taken from the data (the evidence).
- Do NOT prefix lines with labels like "Finding:", "Why:", or "e.g.".
- Use markdown **bold** for key numbers and column names.
- If there is not enough data for a meaningful insight, return
  ["Insufficient data for analysis", "Column has too few non-null values", "N/A"].
"""

# ═══════════════════════════════════════════════════════════════════════════
# Prompt 1: Spend & Financial Quality (date + currency + payment terms)
# ═══════════════════════════════════════════════════════════════════════════

FINANCIAL_SYSTEM_PROMPT = f"""\
You are a senior procurement data-quality consultant. You will receive a JSON
payload containing analysis results for three financial panels from a client's
procurement dataset:

- **datePayload**: date column analysis (formatTable, consistent flag, pivotData,
  selectedColumn, columnMatchType, fillRate)
- **currencyPayload**: currency distribution (currencyTable, distinctCount, codes,
  columnMatchType, fillRate)
- **paymentTermsPayload**: payment terms breakdown (paymentTerms, totalSpend,
  uniqueCount, columnMatchType, fillRate)

Any payload may be null if the column was not found in the data.

### Instructions

For each non-null payload, produce an insight array.

**Date insight**: focus on format consistency across files; mention seasonality
or data gaps from pivotData when available. Reference the actual column name.

**Currency insight**: note the dominant currency, whether multi-currency
standardisation is needed, and any suspicious currency codes.

**Payment terms insight**: highlight standardisation opportunities (duplicates
like "Net 30" / "NET30"), dominant terms, and working-capital implications.

If a panel's column was found via fuzzy match (columnMatchType = "fuzzy"),
mention that the column name did not match exactly and may need review.

{_INSIGHT_FORMAT_RULES}

### Output format

Return JSON:
{{
  "dateInsight": ["...", "...", "..."] or null,
  "currencyInsight": ["...", "...", "..."] or null,
  "paymentTermsInsight": ["...", "...", "..."] or null
}}
"""

# ═══════════════════════════════════════════════════════════════════════════
# Prompt 2: Entity Quality (country/region + supplier)
# ═══════════════════════════════════════════════════════════════════════════

ENTITY_SYSTEM_PROMPT = f"""\
You are a senior procurement data-quality consultant. You will receive a JSON
payload containing analysis results for two entity panels from a client's
procurement dataset:

- **countryPayload**: unique country values, column name, columnMatchType, fillRate
- **regionPayload**: unique region values, column name (may be null)
- **supplierPayload**: supplier names sample, supplierCount, columnMatchType,
  fillRate, hasReportingSpend, top20 spend data

Any payload may be null if the column was not found.

### Instructions

**Country insight**: look for naming inconsistencies (e.g. "US" vs "United States"),
mixed formats, and non-standard codes. Reference the actual column name.

**Region insight**: same — naming inconsistencies, duplicates, non-standard entries.
Return null if no region data.

**Supplier insight**: assess whether supplier name normalisation is needed. Give
specific example pairs from the data (e.g. "Amazon Inc" / "AMAZON INC."). If
spend data is available, note concentration (e.g. top-N suppliers cover X% of spend).

If a column was found via fuzzy match, note the inexact match.

{_INSIGHT_FORMAT_RULES}

### Output format

Return JSON:
{{
  "countryInsight": ["...", "...", "..."] or null,
  "regionInsight": ["...", "...", "..."] or null,
  "supplierInsight": ["...", "...", "..."] or null
}}
"""


# ═══════════════════════════════════════════════════════════════════════════
# Consolidated callers
# ═══════════════════════════════════════════════════════════════════════════


def generate_financial_insights(
    date_payload: dict[str, Any] | None,
    currency_payload: dict[str, Any] | None,
    payment_terms_payload: dict[str, Any] | None,
    api_key: str,
) -> dict[str, list[str] | None]:
    """Call LLM once for date + currency + payment terms insights.

    Returns:
        Dict with keys ``dateInsight``, ``currencyInsight``,
        ``paymentTermsInsight`` — each a 3-element list or None.
    """
    logger.info("Generating consolidated financial AI insights")
    combined = {
        "datePayload": date_payload,
        "currencyPayload": currency_payload,
        "paymentTermsPayload": payment_terms_payload,
    }
    result = call_ai_json(FINANCIAL_SYSTEM_PROMPT, combined, api_key=api_key)
    return {
        "dateInsight": _normalise_insight(result.get("dateInsight")),
        "currencyInsight": _normalise_insight(result.get("currencyInsight")),
        "paymentTermsInsight": _normalise_insight(result.get("paymentTermsInsight")),
    }


def generate_entity_insights(
    country_payload: dict[str, Any] | None,
    region_payload: dict[str, Any] | None,
    supplier_payload: dict[str, Any] | None,
    api_key: str,
) -> dict[str, list[str] | None]:
    """Call LLM once for country + region + supplier insights.

    Returns:
        Dict with keys ``countryInsight``, ``regionInsight``,
        ``supplierInsight`` — each a 3-element list or None.
    """
    logger.info("Generating consolidated entity AI insights")
    combined = {
        "countryPayload": country_payload,
        "regionPayload": region_payload,
        "supplierPayload": supplier_payload,
    }
    result = call_ai_json(ENTITY_SYSTEM_PROMPT, combined, api_key=api_key)
    return {
        "countryInsight": _normalise_insight(result.get("countryInsight")),
        "regionInsight": _normalise_insight(result.get("regionInsight")),
        "supplierInsight": _normalise_insight(result.get("supplierInsight")),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Backward-compatible per-panel wrappers
# ═══════════════════════════════════════════════════════════════════════════


def generate_date_insight(payload: dict[str, Any], api_key: str) -> list[str] | None:
    """Date-only insight (uses consolidated prompt with null siblings)."""
    result = generate_financial_insights(payload, None, None, api_key)
    return result["dateInsight"]


def generate_currency_insight(payload: dict[str, Any], api_key: str) -> list[str] | None:
    """Currency-only insight."""
    result = generate_financial_insights(None, payload, None, api_key)
    return result["currencyInsight"]


def generate_payment_terms_insight(payload: dict[str, Any], api_key: str) -> list[str] | None:
    """Payment terms-only insight."""
    result = generate_financial_insights(None, None, payload, api_key)
    return result["paymentTermsInsight"]


def generate_country_region_insight(
    payload: dict[str, Any], api_key: str
) -> dict[str, list[str] | None]:
    """Country/Region insight (backward-compatible return shape)."""
    country_data = {
        "countryValues": payload.get("countryValues"),
        "countryColumn": payload.get("countryColumn"),
    }
    region_data = {
        "regionValues": payload.get("regionValues"),
        "regionColumn": payload.get("regionColumn"),
    }
    result = generate_entity_insights(
        country_data if payload.get("countryValues") else None,
        region_data if payload.get("regionValues") else None,
        None,
        api_key,
    )
    return {
        "countryInsight": result["countryInsight"],
        "regionInsight": result["regionInsight"],
    }


def generate_supplier_insight(payload: dict[str, Any], api_key: str) -> list[str] | None:
    """Supplier-only insight."""
    result = generate_entity_insights(None, None, payload, api_key)
    return result["supplierInsight"]


# ── Helpers ───────────────────────────────────────────────────────────────


def _normalise_insight(raw: Any) -> list[str] | None:
    """Ensure the insight is a list of strings, or None.

    Handles legacy string-based responses by splitting on newlines.
    """
    if raw is None:
        return None
    if isinstance(raw, list):
        return [str(item).strip().lstrip("- ") for item in raw if item]
    if isinstance(raw, str):
        lines = [ln.strip().lstrip("- ") for ln in raw.split("\n") if ln.strip()]
        return lines[:3] if lines else None
    return None
