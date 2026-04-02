"""AI prompt and summary generation for the quality-analysis feature."""

from __future__ import annotations

import logging
from typing import Any

from shared.ai_client import call_ai_json

logger = logging.getLogger(__name__)

QUALITY_ANALYSIS_PROMPT = """\
You are a senior procurement data-quality analyst at a global consulting firm.

You will receive a JSON object containing computed metrics about a client's
procurement dataset — specifically the quality of the **item description**
column and the **supplier (vendor name)** fill rate.

Your task is to write a professional, concise data-quality assessment in
**markdown** format.  The assessment must include:

1. An overall quality verdict on the description column.
2. A **primary recommendation** for which categorisation tool / approach is
   best suited for this dataset, and optionally one or two **secondary
   alternatives**.
3. Brief justification referencing the key metrics.

### Directional rules (use as guidance, not rigid thresholds — weigh all
metrics holistically and exercise professional judgement):

- **Row count > 400 000** → Creatives is recommended regardless of other
  metrics (highest-priority signal).
- **B∩D (single-word AND ≤10 chars) < 50 %** → descriptions are generally
  of good quality; an L3/L4-granularity spend cube is feasible.  Also
  consider A∩C (multi-word AND long), A∩D, and B∩C to form a nuanced view
  of overall description richness.
- **B > 50 % OR D > 50 %** → description quality is not great, but Map AI
  can still be run with VLOB as the categorisation priority.
- **Coded/alphanumeric % > 70 %** → Creatives is more suitable than
  Map AI for this dataset.
- **Description completion rate < 60 % BUT supplier fill rate > 90 %** →
  Map AI with VLOB as categorisation priority is preferred (supplier data
  compensates for weak descriptions).

When multiple signals conflict, state the primary recommendation clearly
and note the alternatives.  Avoid jargon explanations — the audience knows
what Creatives, Map AI, VLOB, and L3/L4 spend cubes are.

Keep the output to roughly 4–8 bullet points grouped under 2–3 section
headings.  Use **bold** for key numbers.

Return your answer as JSON:
{"summary": "<your markdown summary>"}
"""


def generate_quality_analysis_summary(
    metrics: dict[str, Any],
    api_key: str,
) -> str:
    """Send computed quality metrics to the LLM and return a markdown summary."""
    logger.info("Generating quality-analysis AI summary (rows=%s)", metrics.get("totalRows"))
    try:
        result = call_ai_json(QUALITY_ANALYSIS_PROMPT, metrics, api_key=api_key)
        summary: str = result.get("summary", "Summary generation failed.")
        logger.info("Quality-analysis summary generated successfully")
        return summary
    except Exception as exc:
        logger.error("Quality-analysis AI summary failed: %s", exc, exc_info=True)
        return f"AI summary unavailable: {exc}"
