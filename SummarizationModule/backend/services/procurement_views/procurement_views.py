"""Analysis feasibility registries.

Two registries check which advanced analyses the data can support:
  1. Spend X-ray dashboards
  2. Category Navigator levers

Each entry maps a dashboard/lever name to the fieldKeys it requires.
Feasibility is purely informational — no computation happens here.
"""

from typing import Any

from services.mapping.column_mapper import STANDARD_FIELDS

# ---------------------------------------------------------------------------
# Spend X-ray dashboard registry
# ---------------------------------------------------------------------------

SPEND_XRAY_REGISTRY: list[dict[str, Any]] = [
    {
        "viewId": "contract_status",
        "title": "Contract Status",
        "requiredFields": [
            "contract_end_date", "contract_id", "contract_start_date",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "dynamic_spend_view",
        "title": "Dynamic Spend View",
        "requiredFields": [
            "business_unit", "contract_end_date", "contract_indicator",
            "contract_start_date", "invoice_po_number", "plant_code",
            "plant_name", "local_spend", "supplier", "contract_status",
            "total_spend",
        ],
    },
    {
        "viewId": "fy_spend_overview",
        "title": "FY20 Spend Overview",
        "requiredFields": [
            "business_unit", "plant_code", "plant_name",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "lcc_sourcing",
        "title": "Low Cost Country (LCC) Sourcing",
        "requiredFields": [
            "local_spend", "vendor_country", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "material_deep_dive",
        "title": "Material wise deep dive",
        "requiredFields": [
            "quantity", "uom", "description", "po_material_number",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "maverick_spend",
        "title": "Maverick Spend",
        "requiredFields": [
            "goods_receipt_date", "invoice_po_number", "po_document_date",
            "local_spend", "total_spend",
        ],
    },
    {
        "viewId": "mega_supplier_sourcing",
        "title": "Mega Supplier Sourcing",
        "requiredFields": [
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "payment_terms_rationalization",
        "title": "Payment terms rationalization",
        "requiredFields": [
            "payment_terms", "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "peer_benchmarking",
        "title": "Peer to peer/ Industry benchmarking analysis",
        "requiredFields": [
            "local_spend", "total_spend",
        ],
    },
    {
        "viewId": "price_rationalization",
        "title": "Price Rationalization",
        "requiredFields": [
            "contract_indicator", "invoice_date", "quantity",
            "plant_code", "plant_name", "description", "po_material_number",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "savings_potential_price",
        "title": "Savings Potential - Price rationalization",
        "requiredFields": [
            "quantity", "uom", "plant_code", "plant_name",
            "description", "po_material_number", "price_per_uom",
            "local_spend", "total_spend",
        ],
    },
    {
        "viewId": "size_of_prize",
        "title": "Size of prize",
        "requiredFields": [
            "local_spend", "total_spend",
        ],
    },
    {
        "viewId": "specification_rationalization",
        "title": "Specification rationalization",
        "requiredFields": [
            "contract_indicator", "quantity", "description",
            "po_material_number", "price_per_uom",
            "local_spend", "total_spend",
        ],
    },
    {
        "viewId": "spend_by_supplier_country",
        "title": "Spend by supplier country",
        "requiredFields": [
            "local_spend", "vendor_country", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "spend_distribution_summary",
        "title": "Spend Distribution Summary",
        "requiredFields": [
            "business_unit", "plant_code", "plant_name",
            "local_spend", "supplier", "country", "total_spend",
        ],
    },
    {
        "viewId": "spend_profile_monthly",
        "title": "Spend Profile - Monthly",
        "requiredFields": [
            "business_unit", "invoice_date", "invoice_number",
            "plant_name", "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "spend_type_analysis",
        "title": "Spend type analysis",
        "requiredFields": [
            "contract_indicator", "invoice_po_number", "description",
            "po_material_number", "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "supplier_rationalization",
        "title": "Supplier rationalization",
        "requiredFields": [
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "supplier_spend_payment_terms",
        "title": "Supplier spend distribution by payment terms",
        "requiredFields": [
            "payment_terms", "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "transaction_intensity",
        "title": "Transaction Intensity",
        "requiredFields": [
            "contract_indicator", "invoice_po_number",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "centralized_buying",
        "title": "Centralized buying across plants/BUs",
        "requiredFields": [
            "business_unit", "plant_code", "plant_name",
            "local_spend", "supplier", "total_spend",
        ],
    },
    {
        "viewId": "dynamic_category_savings",
        "title": "Dynamic Category Savings",
        "requiredFields": [
            "local_spend", "total_spend",
        ],
    },
]

# ---------------------------------------------------------------------------
# Category Navigator lever registry
# ---------------------------------------------------------------------------

CATEGORY_NAVIGATOR_REGISTRY: list[dict[str, Any]] = [
    {
        "viewId": "bundling_aggregation",
        "title": "Bundling/ Aggregation of Spend",
        "requiredFields": [
            "total_spend", "business_unit", "country", "region",
            "l1", "l2", "l3", "l4", "invoice_date",
        ],
    },
    {
        "viewId": "bypass_intermediaries",
        "title": "By-pass Intermediaries",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "po_material_description", "quantity",
            "l1", "l2", "l3", "l4", "uom", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "complexity_reduction",
        "title": "Complexity Reduction",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "po_material_description", "region",
            "l1", "l2", "l3", "l4", "invoice_date",
        ],
    },
    {
        "viewId": "leverage_purchasing_timing",
        "title": "Leverage Appropriate Purchasing Timing",
        "requiredFields": [
            "total_spend", "po_material_description", "quantity",
            "invoice_date", "l1", "l2", "l3", "l4",
            "uom", "supplier",
        ],
    },
    {
        "viewId": "mega_supplier_strategy",
        "title": "Mega Supplier Strategy",
        "requiredFields": [
            "total_spend", "business_unit", "country", "region",
            "l1", "l2", "l3", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "on_time_payment",
        "title": "On-Time Payment",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "invoice_po_number", "invoice_due_date", "payment_date",
            "l1", "l2", "l3", "l4",
            "vendor_country", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "optimize_payment_terms",
        "title": "Optimize Payment Terms",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "po_document_date", "invoice_date",
            "l1", "l2", "l3", "l4",
            "vendor_country", "supplier",
        ],
    },
    {
        "viewId": "order_quantity_optimization",
        "title": "Order Quantity Optimization",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "po_material_description", "invoice_po_number", "quantity",
            "region", "l1", "l2", "l3", "l4", "uom", "invoice_date",
        ],
    },
    {
        "viewId": "penalty_avoidance",
        "title": "Penalty Avoidance",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "description", "po_material_description",
            "l1", "l2", "l3", "l4", "invoice_date",
        ],
    },
    {
        "viewId": "shift_volumes_best_price",
        "title": "Shift volumes to best price supplier",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "po_material_description", "quantity",
            "l1", "l2", "l3", "l4", "uom", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "supplier_consolidation",
        "title": "Supplier Consolidation",
        "requiredFields": [
            "total_spend", "business_unit", "country", "region",
            "l1", "l2", "l3", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "tail_spend_management",
        "title": "Tail Spend Management",
        "requiredFields": [
            "total_spend", "business_unit", "country", "region",
            "l1", "l2", "l3", "l4", "supplier", "invoice_date",
        ],
    },
    {
        "viewId": "sole_source_management",
        "title": "Sole Source Management",
        "requiredFields": [
            "total_spend", "business_unit", "country",
            "l1", "l2", "l3", "l4", "supplier", "invoice_date",
        ],
    },
]

# ---------------------------------------------------------------------------
# Feasibility check
# ---------------------------------------------------------------------------

_FIELD_DISPLAY_NAMES: dict[str, str] = {
    f["fieldKey"]: f["displayName"] for f in STANDARD_FIELDS
}


def _check_registry(
    registry: list[dict[str, Any]],
    mapped_keys: set[str],
) -> list[dict[str, Any]]:
    """Check feasibility for every entry in a registry.

    Args:
        registry: List of view/lever dicts with ``requiredFields``.
        mapped_keys: Set of fieldKeys that have been mapped.

    Returns:
        List of result dicts with ``available`` and ``missingFields``.
    """
    results = []
    for entry in registry:
        missing_keys = [f for f in entry["requiredFields"] if f not in mapped_keys]
        missing_display = [_FIELD_DISPLAY_NAMES.get(k, k) for k in missing_keys]
        results.append({
            "viewId": entry["viewId"],
            "title": entry["title"],
            "requiredFields": entry["requiredFields"],
            "available": len(missing_keys) == 0,
            "missingFields": missing_display,
        })
    return results


def get_procurement_view_availability(
    mapping: dict[str, str | None],
) -> dict[str, list[dict[str, Any]]]:
    """Check which analyses are feasible given the current mapping.

    Args:
        mapping: {fieldKey: sourceColumnName | None}.

    Returns:
        Dict with ``spendXray`` and ``categoryNavigator`` lists.
    """
    mapped_keys = {k for k, v in mapping.items() if v}
    return {
        "spendXray": _check_registry(SPEND_XRAY_REGISTRY, mapped_keys),
        "categoryNavigator": _check_registry(CATEGORY_NAVIGATOR_REGISTRY, mapped_keys),
    }
