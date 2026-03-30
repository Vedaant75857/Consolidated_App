import json
import logging
from typing import Any

from shared.ai_client import call_ai_json

logger = logging.getLogger(__name__)

_BASE_INTRO = (
    "You are a procurement analyst. Given table data and view metadata, "
    "write a concise analytical summary (bullet points) highlighting key "
    "findings, trends, concentrations, and anomalies. Be specific with "
    "numbers and percentages."
)

VIEW_PROMPTS: dict[str, str] = {
    "spend_over_time": f"""{_BASE_INTRO}

Focus on:
- Total spend
- Average monthly spend
- Highest and lowest spend months (with % vs average)
- Overall trend (increasing, stable, volatile)
- Spend spikes/dips and likely drivers (seasonality, one-off events, renewals)

Return JSON:
{{"summary": "your summary text here"}}""",

    "supplier_ranking": f"""{_BASE_INTRO}

Focus on:
- Total number of suppliers
- Spend share of top 5 and top 10 suppliers
- Contribution of the largest supplier
- Supplier concentration level and dependency risks

Return JSON:
{{"summary": "your summary text here"}}""",

    "pareto_analysis": f"""{_BASE_INTRO}

Focus on:
- Number of suppliers contributing to top 80% of total spend
- Number of suppliers in bottom 20% of total spend
- Long-tail size and contribution
- Whether spend is concentrated or fragmented

Return JSON:
{{"summary": "your summary text here"}}""",

    "currency_spend": f"""{_BASE_INTRO}

Focus on:
- Total currencies used
- Percentage of total spend by each currency
- Share of top currency and top 2 currencies
- Percentage of foreign currency spend
- Currency concentration and FX exposure risk

Return JSON:
{{"summary": "your summary text here"}}""",

    "country_spend": f"""{_BASE_INTRO}

Focus on:
- Total countries involved
- Spend share of top country and top 3 countries
- Number of countries accounting for the majority of total spend
- Geographic concentration and dependency risks

Return JSON:
{{"summary": "your summary text here"}}""",

    "l1_spend": f"""{_BASE_INTRO}

Focus on:
- Total L1 categories
- Spend share of top category and top 3 categories
- Whether spend is concentrated or diversified

Return JSON:
{{"summary": "your summary text here"}}""",

    "l1_vs_l2_mekko": f"""{_BASE_INTRO}

Focus on:
- Top L2 categories and their contribution to total spend
- Contribution of L2s within major L1 categories
- Category concentration vs fragmentation

Return JSON:
{{"summary": "your summary text here"}}""",

    "l2_vs_l3_mekko": f"""{_BASE_INTRO}

Focus on:
- Top L3 categories and their contribution
- Number of L3s driving majority of spend
- Long-tail size and fragmentation

Return JSON:
{{"summary": "your summary text here"}}""",
}

SKIP_SUMMARY_VIEWS = {"category_drilldown"}

_FALLBACK_PROMPT = f"""{_BASE_INTRO}

Return JSON:
{{"summary": "your summary text here"}}"""


def _extract_first_50_rows(view_result: dict[str, Any]) -> list[dict]:
    table_data = view_result.get("tableData")
    if table_data is None:
        return []
    if isinstance(table_data, list):
        return table_data[:50]
    if isinstance(table_data, dict):
        for key in ["monthly", "last12", "yearly"]:
            if key in table_data and isinstance(table_data[key], list):
                return table_data[key][:50]
        first_key = next(iter(table_data), None)
        if first_key and isinstance(table_data[first_key], list):
            return table_data[first_key][:50]
    return []


def generate_summary_for_view(
    view_result: dict[str, Any], api_key: str
) -> str:
    view_id = view_result.get("viewId", "")
    if view_id in SKIP_SUMMARY_VIEWS:
        logger.info("Skipping AI summary for view '%s' (in SKIP_SUMMARY_VIEWS)", view_id)
        return ""

    rows = _extract_first_50_rows(view_result)
    if not rows:
        logger.warning("No rows extracted for view '%s' — skipping summary", view_result.get("title"))
        return "No data available for analysis."

    payload = {
        "viewTitle": view_result.get("title", ""),
        "chartType": view_result.get("chartType", ""),
        "rowCount": len(rows),
        "columns": list(rows[0].keys()) if rows else [],
        "data": rows,
    }

    if "excludedRows" in view_result:
        payload["excludedRows"] = view_result["excludedRows"]
    if "totalSuppliers" in view_result:
        payload["totalSuppliers"] = view_result["totalSuppliers"]
    if "suppliersInGroup" in view_result:
        payload["suppliersInGroup"] = view_result["suppliersInGroup"]
    if "threshold" in view_result:
        payload["paretoThreshold"] = view_result["threshold"]

    prompt = VIEW_PROMPTS.get(view_id, _FALLBACK_PROMPT)

    logger.info(
        "Generating AI summary for view '%s' (%d rows, %d columns)",
        view_result.get("title"),
        len(rows),
        len(payload.get("columns", [])),
    )

    try:
        result = call_ai_json(prompt, payload, api_key=api_key)
        summary = result.get("summary", "Summary generation failed.")
        logger.info("Summary generated successfully for view '%s'", view_result.get("title"))
        return summary
    except Exception as exc:
        logger.error("AI summary failed for view '%s': %s", view_result.get("title"), exc, exc_info=True)
        return f"AI summary unavailable: {exc}"


def generate_summaries(
    view_results: list[dict[str, Any]], api_key: str
) -> list[dict[str, Any]]:
    logger.info("Generating summaries for %d view(s)", len(view_results))
    for view in view_results:
        view_id = view.get("viewId", "")
        if view_id in SKIP_SUMMARY_VIEWS:
            continue
        if "error" not in view:
            view["aiSummary"] = generate_summary_for_view(view, api_key)
        else:
            view["aiSummary"] = "Summary unavailable due to computation error."
    return view_results
