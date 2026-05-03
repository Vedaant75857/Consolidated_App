"""AI-assisted column suggestion for the Data Quality Assessment.

Sends column names + sample values to the LLM and asks it to pick the top-3
most likely columns for each DQA analysis role. Used as an additional signal
on top of the keyword/alias/fuzzy resolution in ``column_resolver.py``.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.ai import call_ai_json

logger = logging.getLogger(__name__)

SUGGEST_COLUMNS_SYSTEM_PROMPT = """\
You are a procurement data analyst. You will receive a JSON object where each
key is a column name from a dataset and each value is a list of up to 5 sample
values from that column.

For each of the following analysis roles, identify the **top 3** column names
that are most likely to contain the relevant data. Order them from most likely
to least likely. If fewer than 3 columns are plausible, return only the
plausible ones. If no column matches a role, return an empty list.

Roles:
- **date**: columns containing dates (invoice date, payment date, PO date, etc.)
- **currency_code**: columns containing currency codes (USD, EUR, GBP, etc.)
- **payment_terms**: columns containing payment terms (Net 30, Net 60, etc.)
- **country**: columns containing country names or codes
- **vendor_name**: columns containing supplier or vendor names

Return JSON:
{
  "date": ["col_a", "col_b", "col_c"],
  "currency_code": ["col_x"],
  "payment_terms": ["col_y", "col_z"],
  "country": ["col_m"],
  "vendor_name": ["col_n", "col_o"]
}

Rules:
- Only use column names that appear in the input. Do NOT invent names.
- Judge by both the column name AND the sample values.
- A column can appear under multiple roles only if genuinely applicable.
"""

VALID_ROLES = {"date", "currency_code", "payment_terms", "country", "vendor_name"}


def suggest_columns_ai(
    column_samples: dict[str, list[str]],
    api_key: str,
) -> dict[str, list[str]]:
    """Ask the LLM to suggest the top-3 columns for each DQA role.

    Args:
        column_samples: Mapping of column name -> list of up to 5 sample values.
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
        # Only keep names that actually exist in the table
        result[role] = [s for s in suggestions if isinstance(s, str) and s in available_names][:3]

    return result
