import json
import logging
from typing import Any

from shared.ai_client import call_ai_json
from shared.formatting import format_spend, format_pct

logger = logging.getLogger(__name__)

VIEW_ID_TO_JSON_KEY = {
    "spend_over_time": "spend_vs_time",
    "currency_spend": "spend_vs_currency",
    "country_spend": "spend_vs_country",
    "supplier_ranking": "supplier_ranking",
    "pareto_analysis": "pareto",
    "l1_spend": "spend_vs_l1",
    "l1_vs_l2_mekko": "l1_vs_l2",
    "l2_vs_l3_mekko": "l2_vs_l3",
}

_EMPTY_VIEWS_JSON: dict[str, dict] = {
    "spend_vs_time": {
        "total_spend": "", "date_range": "", "avg_monthly_spend": "",
        "highest_month": "", "lowest_month": "", "trend": "", "anomalies": "",
    },
    "spend_vs_currency": {
        "total_currencies": "", "top_currency": "", "top_currency_pct": "",
        "foreign_currency_pct": "", "fx_risk_note": "",
    },
    "spend_vs_country": {
        "total_countries": "", "top_country": "", "top_country_pct": "",
        "top_3_countries_pct": "", "geo_risk_note": "",
    },
    "supplier_ranking": {
        "total_suppliers": "", "top_5_pct": "", "top_10_pct": "",
        "largest_supplier": "", "largest_supplier_pct": "", "concentration_note": "",
    },
    "pareto": {
        "suppliers_top_80_pct": "", "long_tail_count": "",
        "long_tail_spend_pct": "", "fragmentation": "",
    },
    "spend_vs_l1": {
        "total_l1_categories": "", "top_l1": "", "top_l1_pct": "",
        "top_3_l1_pct": "", "concentration": "",
    },
    "l1_vs_l2": {
        "top_l2_categories": "", "key_l2_within_top_l1": "",
        "fragmentation_note": "",
    },
    "l2_vs_l3": {
        "top_l3_categories": "", "l3_long_tail_note": "",
    },
}

GENERATION_PROMPT = """You are a senior procurement consultant at a top-tier consulting firm.

Using the view summaries below, write a professional client-facing \
spend data review email. Follow this structure — skip any section \
where the summary has no relevant data:

1. Opening — acknowledge data, state scope/date range
2. Spend Overview — total spend, geography, currency
3. Spend Trend — trend direction, highs/lows, spikes
4. Supplier Landscape — concentration, top suppliers, pareto
5. Category Breakdown — L1, L2, L3 observations
6. Callouts — anomalies, risks, data gaps
7. Next Steps — use the provided next_steps if available, max 4 actions with owner and timeline

Tone: consulting style. Lead with numbers. No filler.
Auto-scale spend values (e.g. $1.2M, $450K, $3.5B).
Return JSON: {"email": "the full email text", "subject": "the subject line"}"""

EMAIL_TEMPLATE = """Subject: [Client Name] – Spend Data Review: Initial Observations

Hi [Recipient Name],

Hope you're doing well. Thank you for sharing the data. [One line if there's a scope boundary.]

We have completed an initial review of the [date range] spend data. Below are our key observations.

Spend Overview
- Total spend of [$ X] across [date range], averaging [$ X] per month
- Spend is spread across [X] countries, with [top country] accounting for [X%] of total
- [X] currencies in use; [top currency] dominates at [X%] of spend, with [X%] in foreign currencies

Spend Trend
- Overall trend is [increasing / stable / volatile] over the period
- Highest spend month: [month] at [$ X] ([+X%] vs monthly average); lowest: [month] at [$ X] ([-X%] vs average)
- [One line on notable spike or dip and likely driver]

Supplier Landscape
- [X] unique suppliers identified across the dataset
- Top 5 suppliers account for [X%] of total spend; top 10 for [X%]
- Largest supplier — [Supplier Name] — contributes [X%] individually
- [X suppliers] cover 80% of total spend; remaining [X suppliers] form a long tail contributing [X%]

Category Breakdown
- Spend is distributed across [X] L1 categories; top category [L1 name] accounts for [X%], top 3 combined at [X%]
- Within [dominant L1], key sub-categories are [L2 names]
- At L3 level, top sub-categories are [L3 names]; remaining form a fragmented long tail

Data Observations & Callouts
- [Any anomalies, risks, or data gaps inferred from the data]

Next Steps
Action | Owner | Timeline
[action] | [owner] | [timeline]

Best regards,
[Sender Name]
[Sender Role]
Procurement CoE"""


def _assemble_email_json(
    view_results: list[dict[str, Any]], context: dict[str, Any]
) -> dict[str, Any]:
    views_json: dict[str, dict] = {}
    for key, empty in _EMPTY_VIEWS_JSON.items():
        views_json[key] = dict(empty)

    missing_views = []
    view_ids_present = {v.get("viewId") for v in view_results if not v.get("error")}

    for view in view_results:
        view_id = view.get("viewId", "")
        json_key = VIEW_ID_TO_JSON_KEY.get(view_id)
        if not json_key:
            continue
        metrics = view.get("metrics")
        if metrics and not view.get("error"):
            views_json[json_key] = {**views_json[json_key], **metrics}
        else:
            missing_views.append(view.get("title", view_id))

    for view_id, json_key in VIEW_ID_TO_JSON_KEY.items():
        if view_id not in view_ids_present:
            missing_views.append(json_key)

    return {
        "context": {
            "recipient_name": context.get("recipient_name", ""),
            "client_name": context.get("client_name", ""),
            "sender_name": context.get("sender_name", ""),
            "sender_role": context.get("sender_role", ""),
            "scope_note": context.get("scope_note", ""),
            "next_steps": context.get("next_steps", []),
        },
        "views": views_json,
        "missing_views_note": (
            f"Note: The following views had no data and should be skipped: {', '.join(missing_views)}"
            if missing_views else ""
        ),
    }


def generate_email(
    view_results: list[dict[str, Any]],
    context: dict[str, Any],
    api_key: str,
) -> dict[str, str]:
    email_json = _assemble_email_json(view_results, context)
    user_content = (
        f"--- INPUT DATA ---\n{json.dumps(email_json, indent=2)}\n\n"
        f"--- EMAIL TEMPLATE (use as a guide, generate naturally) ---\n{EMAIL_TEMPLATE}"
    )
    logger.info("Generating email (payload %.1f KB)", len(user_content) / 1024)

    result = call_ai_json(GENERATION_PROMPT, user_content, api_key=api_key)
    return {
        "email": result.get("email", ""),
        "subject": result.get("subject", ""),
    }


def build_fallback_email(
    view_results: list[dict[str, Any]],
    context: dict[str, Any],
) -> str:
    """Build a template-filled email without AI, using stored metrics."""
    email_json = _assemble_email_json(view_results, context)
    ctx = email_json["context"]
    v = email_json["views"]

    lines = []
    subject = f"{ctx['client_name'] or '[Client Name]'} \u2013 Spend Data Review: Initial Observations"
    lines.append(f"Subject: {subject}")
    lines.append("")
    lines.append(f"Hi {ctx['recipient_name'] or '[Recipient Name]'},")
    lines.append("")
    lines.append("Hope you're doing well. Thank you for sharing the data.")
    if ctx["scope_note"]:
        lines.append(ctx["scope_note"])
    lines.append("")

    sot = v.get("spend_vs_time", {})
    if sot.get("total_spend"):
        lines.append(
            f"We have completed an initial review of the {sot.get('date_range', '')} "
            f"spend data. Below are our key observations."
        )
        lines.append("")
        lines.append("Spend Overview")

        overview_parts = [f"Total spend of {sot['total_spend']}"]
        if sot.get("date_range"):
            overview_parts[0] += f" across {sot['date_range']}"
        if sot.get("avg_monthly_spend"):
            overview_parts[0] += f", averaging {sot['avg_monthly_spend']} per month"
        lines.append(f"- {overview_parts[0]}")

        sc = v.get("spend_vs_country", {})
        if sc.get("total_countries"):
            lines.append(
                f"- Spend across {sc['total_countries']} countries, "
                f"with {sc.get('top_country', '')} accounting for {sc.get('top_country_pct', '')} of total"
            )

        scur = v.get("spend_vs_currency", {})
        if scur.get("total_currencies"):
            lines.append(
                f"- {scur['total_currencies']} currencies in use; "
                f"{scur.get('top_currency', '')} dominates at {scur.get('top_currency_pct', '')}, "
                f"with {scur.get('foreign_currency_pct', '')} in foreign currencies"
            )
        lines.append("")

    if sot.get("highest_month"):
        lines.append("Spend Trend")
        lines.append(
            f"- Highest spend month: {sot['highest_month']} at {sot.get('highest_month_spend', '')} "
            f"({sot.get('highest_vs_avg_pct', '')} vs monthly average)"
        )
        lines.append(
            f"- Lowest spend month: {sot['lowest_month']} at {sot.get('lowest_month_spend', '')} "
            f"({sot.get('lowest_vs_avg_pct', '')} vs average)"
        )
        lines.append("")

    sr = v.get("supplier_ranking", {})
    par = v.get("pareto", {})
    if sr.get("total_suppliers"):
        lines.append("Supplier Landscape")
        lines.append(f"- {sr['total_suppliers']} unique suppliers identified")
        if sr.get("top_5_pct"):
            lines.append(
                f"- Top 5 suppliers account for {sr['top_5_pct']} of total spend; "
                f"top 10 for {sr.get('top_10_pct', '')}"
            )
        if sr.get("largest_supplier"):
            lines.append(
                f"- Largest supplier: {sr['largest_supplier']} at {sr.get('largest_supplier_pct', '')}"
            )
        if par.get("suppliers_top_80_pct"):
            lines.append(
                f"- {par['suppliers_top_80_pct']} suppliers cover 80% of spend; "
                f"remaining {par.get('long_tail_count', '')} form a long tail "
                f"contributing {par.get('long_tail_spend_pct', '')}"
            )
        lines.append("")

    l1 = v.get("spend_vs_l1", {})
    if l1.get("total_l1_categories"):
        lines.append("Category Breakdown")
        lines.append(
            f"- {l1['total_l1_categories']} L1 categories; "
            f"top category {l1.get('top_l1', '')} accounts for {l1.get('top_l1_pct', '')}, "
            f"top 3 combined at {l1.get('top_3_l1_pct', '')}"
        )
        l1l2 = v.get("l1_vs_l2", {})
        if l1l2.get("key_l2_within_top_l1"):
            lines.append(f"- Key L2 sub-categories: {l1l2['key_l2_within_top_l1']}")
        l2l3 = v.get("l2_vs_l3", {})
        if l2l3.get("top_l3_categories"):
            lines.append(f"- Top L3 sub-categories: {l2l3['top_l3_categories']}")
        lines.append("")

    next_steps = ctx.get("next_steps", [])
    if next_steps:
        lines.append("Next Steps")
        lines.append("Action | Owner | Timeline")
        for step in next_steps:
            lines.append(f"{step.get('action', '')} | {step.get('owner', '')} | {step.get('timeline', '')}")
        lines.append("")

    lines.append("Best regards,")
    lines.append(ctx.get("sender_name", "[Your Name]"))
    lines.append(ctx.get("sender_role", "[Role]"))
    lines.append("Procurement CoE")

    return "\n".join(lines)
