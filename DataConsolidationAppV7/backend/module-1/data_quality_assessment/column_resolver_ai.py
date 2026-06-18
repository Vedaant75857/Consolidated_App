"""AI-assisted column suggestion for the Data Quality Assessment.

Sends column names + sample values to the LLM and asks it to identify all
columns that match each DQA analysis role. Used as an additional signal on top
of the keyword/alias/fuzzy resolution in ``column_resolver.py``.
"""

from __future__ import annotations

import logging

from shared.ai import call_ai_json

logger = logging.getLogger(__name__)

SUGGEST_COLUMNS_SYSTEM_PROMPT = """\
You are a procurement data analyst. You will receive a JSON object where each
key is a column name from a dataset and each value is a list of sample values
(up to 75 distinct non-null values) from that column.

For each of the following analysis roles, identify **all** column names that
are likely to contain the relevant data. Order them from most likely to least
likely. If no column matches a role, return an empty list.

Roles:
- **date**: columns containing dates (invoice date, payment date, PO date, etc.)
- **currency_code**: columns containing currency codes (USD, EUR, GBP, etc.)
  or currency symbols ($, €, £, ¥, etc.)
- **payment_terms**: columns containing payment terms (Net 30, Net 60, etc.)
- **country**: columns containing country names or codes
- **region**: columns containing region or geography values (EMEA, APAC, Americas, etc.)
- **vendor_name**: columns containing supplier or vendor names

Return JSON:
{
  "date": ["col_a", "col_b"],
  "currency_code": ["col_x", "col_y"],
  "payment_terms": ["col_y", "col_z"],
  "country": ["col_m"],
  "region": ["col_r"],
  "vendor_name": ["col_n", "col_o"]
}

Rules:
- Only use column names that appear in the input. Do NOT invent names.
- Judge by both the column name AND the sample values.
- A column can appear under multiple roles only if genuinely applicable.
- Return every plausible column for each role, not just the single best match.
"""

VALID_ROLES = {
    "date",
    "currency_code",
    "payment_terms",
    "country",
    "region",
    "vendor_name",
}


def suggest_columns_ai(
    column_samples: dict[str, list[str]],
    api_key: str,
) -> dict[str, list[str]]:
    """Ask the LLM to identify all columns for each DQA role.

    Args:
        column_samples: Mapping of column name -> list of sample values.
        api_key: API key for the LLM provider.

    Returns:
        Dict keyed by role name, each value a list of column names (best first).
        Missing/failed roles default to empty lists.
    """
    if not column_samples:
        return {role: [] for role in VALID_ROLES}

    try:
        raw = call_ai_json(SUGGEST_COLUMNS_SYSTEM_PROMPT, column_samples, api_key=api_key)
    except Exception as exc:
        logger.warning("AI column suggestion failed: %s", exc)
        return {role: [] for role in VALID_ROLES}

    available_names = set(column_samples.keys())
    result: dict[str, list[str]] = {}
    for role in VALID_ROLES:
        suggestions = raw.get(role, [])
        if not isinstance(suggestions, list):
            suggestions = []
        seen: set[str] = set()
        filtered: list[str] = []
        for s in suggestions:
            if isinstance(s, str) and s in available_names and s not in seen:
                seen.add(s)
                filtered.append(s)
        result[role] = filtered

    return result
