import json
from typing import Any

from shared.ai_client import call_ai_json

SUMMARY_SYSTEM_PROMPT = """You are a procurement analyst. Given table data and view metadata,
write a concise analytical summary (4-6 sentences) highlighting key findings, trends,
concentrations, and anomalies. Be specific with numbers and percentages.
Return JSON: {"summary": "your summary text here"}"""


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
    rows = _extract_first_50_rows(view_result)
    if not rows:
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

    try:
        result = call_ai_json(SUMMARY_SYSTEM_PROMPT, payload, api_key=api_key)
        return result.get("summary", "Summary generation failed.")
    except Exception as exc:
        return f"AI summary unavailable: {exc}"


def generate_summaries(
    view_results: list[dict[str, Any]], api_key: str
) -> list[dict[str, Any]]:
    for view in view_results:
        if "error" not in view:
            view["aiSummary"] = generate_summary_for_view(view, api_key)
        else:
            view["aiSummary"] = "Summary unavailable due to computation error."
    return view_results
