"""Column metadata for guided merge — 73 standard procurement columns with classification."""

from __future__ import annotations

import difflib
from typing import Any

COLUMN_METADATA: dict[str, dict[str, str]] = {
    # Identifiers (30)
    "invoice_id":           {"category": "identifier", "eligibility": "high"},
    "invoice_number":       {"category": "identifier", "eligibility": "high"},
    "po_number":            {"category": "identifier", "eligibility": "high"},
    "purchase_order_id":    {"category": "identifier", "eligibility": "high"},
    "vendor_id":            {"category": "identifier", "eligibility": "high"},
    "vendor_code":          {"category": "identifier", "eligibility": "high"},
    "customer_id":          {"category": "identifier", "eligibility": "high"},
    "customer_code":        {"category": "identifier", "eligibility": "high"},
    "line_number":          {"category": "identifier", "eligibility": "high"},
    "line_item_id":         {"category": "identifier", "eligibility": "high"},
    "order_id":             {"category": "identifier", "eligibility": "high"},
    "shipment_id":          {"category": "identifier", "eligibility": "high"},
    "contract_id":          {"category": "identifier", "eligibility": "high"},
    "contract_number":      {"category": "identifier", "eligibility": "high"},
    "agreement_id":         {"category": "identifier", "eligibility": "high"},
    "sku":                  {"category": "identifier", "eligibility": "high"},
    "item_code":            {"category": "identifier", "eligibility": "high"},
    "product_id":           {"category": "identifier", "eligibility": "high"},
    "category_code":        {"category": "identifier", "eligibility": "high"},
    "cost_center_code":     {"category": "identifier", "eligibility": "high"},
    "gl_account":           {"category": "identifier", "eligibility": "high"},
    "department_code":      {"category": "identifier", "eligibility": "high"},
    "project_code":         {"category": "identifier", "eligibility": "high"},
    "batch_id":             {"category": "identifier", "eligibility": "high"},
    "transaction_id":       {"category": "identifier", "eligibility": "high"},
    "receipt_number":       {"category": "identifier", "eligibility": "high"},
    "payment_id":           {"category": "identifier", "eligibility": "high"},
    "payment_reference":    {"category": "identifier", "eligibility": "high"},
    "employee_id":          {"category": "identifier", "eligibility": "high"},
    "grn_number":           {"category": "identifier", "eligibility": "high"},
    # Descriptors (22)
    "vendor_name":          {"category": "descriptor", "eligibility": "medium"},
    "customer_name":        {"category": "descriptor", "eligibility": "medium"},
    "product_name":         {"category": "descriptor", "eligibility": "medium"},
    "product_description":  {"category": "descriptor", "eligibility": "medium"},
    "category_name":        {"category": "descriptor", "eligibility": "medium"},
    "department_name":      {"category": "descriptor", "eligibility": "medium"},
    "country":              {"category": "descriptor", "eligibility": "medium"},
    "region":               {"category": "descriptor", "eligibility": "medium"},
    "city":                 {"category": "descriptor", "eligibility": "medium"},
    "currency":             {"category": "descriptor", "eligibility": "medium"},
    "payment_terms":        {"category": "descriptor", "eligibility": "medium"},
    "payment_method":       {"category": "descriptor", "eligibility": "medium"},
    "invoice_status":       {"category": "descriptor", "eligibility": "medium"},
    "order_status":         {"category": "descriptor", "eligibility": "medium"},
    "approval_status":      {"category": "descriptor", "eligibility": "medium"},
    "uom":                  {"category": "descriptor", "eligibility": "medium"},
    "period":               {"category": "descriptor", "eligibility": "medium"},
    "fiscal_year":          {"category": "descriptor", "eligibility": "medium"},
    "fiscal_quarter":       {"category": "descriptor", "eligibility": "medium"},
    "plant":                {"category": "descriptor", "eligibility": "medium"},
    "warehouse":            {"category": "descriptor", "eligibility": "medium"},
    "incoterms":            {"category": "descriptor", "eligibility": "medium"},
    # Metrics (11) — never join on these
    "quantity":             {"category": "metric", "eligibility": "never"},
    "unit_price":           {"category": "metric", "eligibility": "never"},
    "line_amount":          {"category": "metric", "eligibility": "never"},
    "tax_amount":           {"category": "metric", "eligibility": "never"},
    "discount_amount":      {"category": "metric", "eligibility": "never"},
    "total_amount":         {"category": "metric", "eligibility": "never"},
    "net_amount":           {"category": "metric", "eligibility": "never"},
    "gross_amount":         {"category": "metric", "eligibility": "never"},
    "exchange_rate":        {"category": "metric", "eligibility": "never"},
    "budget_amount":        {"category": "metric", "eligibility": "never"},
    "actual_amount":        {"category": "metric", "eligibility": "never"},
    # Weak / Unstable (10)
    "comments":             {"category": "weak", "eligibility": "low"},
    "notes":                {"category": "weak", "eligibility": "low"},
    "description":          {"category": "weak", "eligibility": "low"},
    "free_text":            {"category": "weak", "eligibility": "low"},
    "remarks":              {"category": "weak", "eligibility": "low"},
    "invoice_date":         {"category": "weak", "eligibility": "low"},
    "due_date":             {"category": "weak", "eligibility": "low"},
    "posting_date":         {"category": "weak", "eligibility": "low"},
    "created_by":           {"category": "weak", "eligibility": "low"},
    "approved_by":          {"category": "weak", "eligibility": "low"},
}

_ALL_KEYS = list(COLUMN_METADATA.keys())


def _normalize(name: str) -> str:
    return name.lower().strip().replace(" ", "_").replace("-", "_")


def fuzzy_match_column(col_name: str) -> dict[str, Any] | None:
    """Fuzzy-match a column name against COLUMN_METADATA keys.
    Returns {'matched_key': str, **meta} or None if confidence is too low.
    """
    norm = _normalize(col_name)
    if norm in COLUMN_METADATA:
        return {"matched_key": norm, **COLUMN_METADATA[norm]}
    matches = difflib.get_close_matches(norm, _ALL_KEYS, n=1, cutoff=0.75)
    if matches:
        key = matches[0]
        return {"matched_key": key, **COLUMN_METADATA[key]}
    return None


def get_color_for_eligibility(eligibility: str) -> str:
    """Map eligibility level to a CSS-friendly color name."""
    return {
        "high": "green",
        "medium": "orange",
        "low": "red",
        "never": "red",
    }.get(eligibility, "grey")
