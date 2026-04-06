"""AI prompt and insight generation for the Data Quality Assessment feature.

A single LLM call processes all parameter metrics and returns one insight
string per parameter-column combination, framed from a procurement
consultant's perspective.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.ai import call_ai_json

logger = logging.getLogger(__name__)

DQA_SYSTEM_PROMPT = """\
You are a senior procurement data-quality consultant at a top-tier management
consulting firm.  You will receive a JSON payload containing computed metrics
for a client's procurement dataset, grouped by parameter category (Date,
Spend, Supplier, Description, Currency).

For **each** entry in the "columns" array of every parameter group, write a
concise, actionable insight (2–4 sentences).  Speak directly to the data
steward / analyst.  Reference the key metric numbers (fill rate, year span,
vendor concentration, etc.) using **bold** where impactful.

### Guidelines per parameter:

**Date columns**
- State the year range (e.g. "Data spans **2019–2024**").
- Flag any format inconsistencies across source files.  If all files use the
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


def generate_dqa_insights(
    metrics_payload: dict[str, Any],
    api_key: str,
) -> dict[str, str]:
    """Send all DQA metrics to the LLM and return a map of parameterKey → insight.

    Args:
        metrics_payload: Full metrics dict as assembled by the service layer.
        api_key: Portkey / OpenAI API key.

    Returns:
        ``{parameterKey: insight_text}`` for every column across all groups.

    Raises:
        ValueError: If the AI call fails after retries.
    """
    logger.info("Generating DQA insights via LLM (rows=%s)", metrics_payload.get("totalRows"))
    result = call_ai_json(DQA_SYSTEM_PROMPT, metrics_payload, api_key=api_key)
    entries: list[dict[str, str]] = result.get("parameterInsights", [])
    return {str(e["parameterKey"]): str(e["insight"]) for e in entries if "parameterKey" in e}
