"""Per-panel AI prompts for the redesigned Data Quality Assessment.

Each panel (Date, Currency, Payment Terms, Country/Region, Supplier) has its
own system prompt and a thin wrapper around ``shared.ai.call_ai_json``.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.ai import call_ai_json

logger = logging.getLogger(__name__)

# ── Date Analysis ─────────────────────────────────────────────────────────

DATE_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant. You will receive a JSON
payload describing a date column analysis from a client's procurement dataset.

The payload includes:
- `formatTable`: per-file date format detection (fileName, dominantFormat,
  formatPcts, examples, consistent flag)
- `consistent`: whether all files use the same dominant date format
- `pivotData`: a year x month spend matrix (may be null if unavailable)
- `selectedColumn`: the date column being analysed

### Instructions

1. If formats are **inconsistent** across files, clearly state that **date
   normalisation is required** before aggregation is reliable.  List which
   files use which formats.
2. If a `pivotData` table is provided, analyse it for:
   - Seasonality patterns (months with significantly higher/lower spend)
   - Year-over-year trends (growth, decline, stability)
   - Data gaps (months or years with zero spend that might indicate missing
     data)
   - Any anomalies worth flagging
3. Keep insights concise, actionable, and in bullet-point format.

### Output format

Return JSON:
```
{
  "insight": "- bullet 1\\n- bullet 2\\n- bullet 3"
}
```

Use `\\n` to separate bullets.  Use markdown **bold** for key numbers and
findings.
"""

# ── Currency Analysis ─────────────────────────────────────────────────────

CURRENCY_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant. You will receive a JSON
payload with a currency quality analysis table from a client's procurement
dataset.

The payload includes:
- `currencyTable`: per-currency breakdown with currencyCode, rowCount,
  rowPct, localSpend, reportingSpend
- `distinctCount`: number of distinct currency codes
- `codes`: list of all currency codes found

### Instructions

1. Summarise the currency distribution (dominant currency, long-tail
   currencies).
2. Flag that **currency standardisation is required** if multiple currencies
   exist.
3. Note if any currency codes look non-standard or potentially erroneous.
4. Comment on the availability of local vs reporting currency spend data.
5. Keep insights concise, actionable, and in bullet-point format.

### Output format

Return JSON:
```
{
  "insight": "- bullet 1\\n- bullet 2\\n- bullet 3"
}
```

Use `\\n` to separate bullets.  Use **bold** for key numbers.
"""

# ── Payment Terms Analysis ────────────────────────────────────────────────

PAYMENT_TERMS_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant. You will receive a JSON
payload with payment terms analysis from a client's procurement dataset.

The payload includes:
- `paymentTerms`: list of objects with term, spend, and pctOfTotal
- `totalSpend`: total spend across all payment terms
- `uniqueCount`: number of distinct payment terms

### Instructions

1. Summarise the payment terms distribution.
2. Identify if there is scope for **standardisation of payment terms** — look
   for terms that appear to be duplicates or variations (e.g. "Net 30",
   "NET30", "Net-30", "30 Days Net").
3. Flag dominant payment terms and any unusual or non-standard terms.
4. Comment on working capital implications if relevant.
5. Keep insights concise, actionable, and in bullet-point format.

### Output format

Return JSON:
```
{
  "insight": "- bullet 1\\n- bullet 2\\n- bullet 3"
}
```

Use `\\n` to separate bullets.  Use **bold** for key numbers.
"""

# ── Country / Region Analysis ─────────────────────────────────────────────

COUNTRY_REGION_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant. You will receive a JSON
payload with country and/or region data from a client's procurement dataset.

The payload includes:
- `countryValues`: list of unique country names/codes found (may be null)
- `regionValues`: list of unique region names found (may be null)
- `countryColumn`: the column name analysed for countries (may be null)
- `regionColumn`: the column name analysed for regions (may be null)

### Instructions

1. For **countries**: check for standardisation scope.  Look for:
   - Mixed formats (e.g. "US" vs "United States" vs "USA")
   - Inconsistent casing
   - Spelling variations or abbreviations
   - Non-standard or potentially erroneous entries
   Report findings and flag **country name standardisation is required** if
   issues exist.

2. For **regions**: same analysis — look for naming inconsistencies, duplicate
   entries with different spellings, non-standard entries.
   Flag **region standardisation is required** if issues exist.

3. Keep insights concise, actionable, and in bullet-point format.

### Output format

Return JSON:
```
{
  "countryInsight": "- bullet 1\\n- bullet 2" or null,
  "regionInsight": "- bullet 1\\n- bullet 2" or null
}
```

Set a key to null when no data was provided for that section.
Use `\\n` to separate bullets.  Use **bold** for key findings.
"""

# ── Supplier Analysis ─────────────────────────────────────────────────────

SUPPLIER_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant. You will receive a JSON
payload with a list of supplier names (sorted alphabetically) from a client's
procurement dataset.  These are the top suppliers by spend.

### Instructions

Analyse the supplier names for **name quality and normalisation
opportunities**.  Look for:

1. **Potential duplicates** — suppliers that appear to be the same entity
   under different names (e.g. "Amazon Inc", "Amazon Co", "AMAZON INC." →
   Amazon).
2. **Inconsistent formatting** — mixed case, abbreviation inconsistencies
   (e.g. "Corp" vs "Corporation", "Ltd" vs "Limited").
3. **Noise in names** — extra whitespace, special characters, numeric
   suffixes that may indicate data quality issues.
4. **Groups of related entities** that could be consolidated.

Provide a narrative assessment of supplier name quality.  Highlight specific
examples of normalisation opportunities.  State clearly whether **supplier
name normalisation is required** and estimate the scope (e.g. "approximately
X% of supplier names show potential normalisation opportunities").

### Output format

Return JSON:
```
{
  "insight": "- bullet 1\\n- bullet 2\\n- bullet 3"
}
```

Use `\\n` to separate bullets.  Use **bold** for key findings and examples.
"""


# ═══════════════════════════════════════════════════════════════════════════
# Public wrappers — one per panel
# ═══════════════════════════════════════════════════════════════════════════


def generate_date_insight(payload: dict[str, Any], api_key: str) -> str:
    """Generate AI insight for the date analysis panel."""
    logger.info("Generating date analysis AI insight")
    result = call_ai_json(DATE_SYSTEM_PROMPT, payload, api_key=api_key)
    return str(result.get("insight", ""))


def generate_currency_insight(payload: dict[str, Any], api_key: str) -> str:
    """Generate AI insight for the currency analysis panel."""
    logger.info("Generating currency analysis AI insight")
    result = call_ai_json(CURRENCY_SYSTEM_PROMPT, payload, api_key=api_key)
    return str(result.get("insight", ""))


def generate_payment_terms_insight(payload: dict[str, Any], api_key: str) -> str:
    """Generate AI insight for the payment terms panel."""
    logger.info("Generating payment terms AI insight")
    result = call_ai_json(PAYMENT_TERMS_SYSTEM_PROMPT, payload, api_key=api_key)
    return str(result.get("insight", ""))


def generate_country_region_insight(
    payload: dict[str, Any], api_key: str
) -> dict[str, str | None]:
    """Generate AI insights for the country/region panel.

    Returns:
        Dict with ``countryInsight`` and ``regionInsight`` (either may be None).
    """
    logger.info("Generating country/region AI insight")
    result = call_ai_json(COUNTRY_REGION_SYSTEM_PROMPT, payload, api_key=api_key)
    return {
        "countryInsight": result.get("countryInsight"),
        "regionInsight": result.get("regionInsight"),
    }


def generate_supplier_insight(payload: dict[str, Any], api_key: str) -> str:
    """Generate AI insight for the supplier analysis panel."""
    logger.info("Generating supplier analysis AI insight")
    result = call_ai_json(SUPPLIER_SYSTEM_PROMPT, payload, api_key=api_key)
    return str(result.get("insight", ""))
