"""Procurement X-ray view feasibility registry.

Each entry maps a Spend X-ray dashboard name to the fieldKeys it requires.
This is used purely for informational feasibility checks -- no computation.
"""

from typing import Any

from services.column_mapper import STANDARD_FIELDS

PROCUREMENT_VIEW_REGISTRY: list[dict[str, Any]] = [
    {
        "viewId": "contract_status",
        "title": "Contract Status",
        "requiredFields": [
            "contract_end_date",
            "contract_id",
            "contract_start_date",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "dynamic_spend_view",
        "title": "Dynamic Spend View",
        "requiredFields": [
            "business_unit",
            "contract_end_date",
            "contract_indicator",
            "contract_start_date",
            "invoice_po_number",
            "plant_code",
            "plant_name",
            "local_spend",
            "supplier",
            "contract_status",
            "total_spend",
        ],
    },
    {
        "viewId": "fy_spend_overview",
        "title": "FY20 Spend Overview",
        "requiredFields": [
            "business_unit",
            "payment_terms",
            "plant_code",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "lcc_sourcing",
        "title": "Low Cost Country (LCC) Sourcing",
        "requiredFields": [
            "local_spend",
            "vendor_country",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "material_deep_dive",
        "title": "Material wise deep dive",
        "requiredFields": [
            "invoice_line_qty",
            "invoice_line_qty_uom",
            "po_material_description",
            "po_material_number",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "maverick_spend",
        "title": "Maverick Spend",
        "requiredFields": [
            "goods_receipt_date",
            "invoice_po_number",
            "po_document_date",
            "local_spend",
            "total_spend",
        ],
    },
    {
        "viewId": "mega_supplier_sourcing",
        "title": "Mega Supplier Sourcing",
        "requiredFields": [
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "payment_terms_rationalization",
        "title": "Payment terms rationalization",
        "requiredFields": [
            "payment_terms",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "peer_benchmarking",
        "title": "Peer to peer / Industry benchmarking analysis",
        "requiredFields": [
            "local_spend",
            "total_spend",
        ],
    },
    {
        "viewId": "price_rationalization",
        "title": "Price Rationalization",
        "requiredFields": [
            "contract_indicator",
            "invoice_date",
            "invoice_line_qty",
            "plant_code",
            "plant_name",
            "po_material_description",
            "po_material_number",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "savings_potential_price",
        "title": "Savings Potential - Price rationalization",
        "requiredFields": [
            "invoice_line_qty",
            "invoice_line_qty_uom",
            "payment_terms",
            "plant_code",
            "po_material_description",
            "po_material_number",
            "price_per_uom",
            "local_spend",
            "total_spend",
        ],
    },
    {
        "viewId": "size_of_prize",
        "title": "Size of prize",
        "requiredFields": [
            "local_spend",
            "total_spend",
        ],
    },
    {
        "viewId": "specification_rationalization",
        "title": "Specification rationalization",
        "requiredFields": [
            "contract_indicator",
            "invoice_line_qty",
            "po_material_description",
            "po_material_number",
            "price_per_uom",
            "local_spend",
            "total_spend",
        ],
    },
    {
        "viewId": "spend_by_supplier_country",
        "title": "Spend by supplier country",
        "requiredFields": [
            "local_spend",
            "vendor_country",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "spend_distribution_summary",
        "title": "Spend Distribution Summary",
        "requiredFields": [
            "business_unit",
            "payment_terms",
            "plant_code",
            "local_spend",
            "supplier",
            "country",
            "total_spend",
        ],
    },
    {
        "viewId": "spend_profile_monthly",
        "title": "Spend Profile - Monthly",
        "requiredFields": [
            "business_unit",
            "invoice_date",
            "invoice_number",
            "plant_code",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "spend_type_analysis",
        "title": "Spend type analysis",
        "requiredFields": [
            "contract_indicator",
            "invoice_po_number",
            "po_material_description",
            "po_material_number",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "supplier_rationalization",
        "title": "Supplier rationalization",
        "requiredFields": [
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "supplier_spend_payment_terms",
        "title": "Supplier spend distribution by payment terms",
        "requiredFields": [
            "payment_terms",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "transaction_intensity",
        "title": "Transaction Intensity",
        "requiredFields": [
            "contract_indicator",
            "invoice_po_number",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "centralized_buying",
        "title": "Centralized buying across plants/BUs",
        "requiredFields": [
            "business_unit",
            "payment_terms",
            "plant_code",
            "local_spend",
            "supplier",
            "total_spend",
        ],
    },
    {
        "viewId": "dynamic_category_savings",
        "title": "Dynamic Category Savings",
        "requiredFields": [
            "local_spend",
            "total_spend",
        ],
    },
]

_FIELD_DISPLAY_NAMES: dict[str, str] = {
    f["fieldKey"]: f["displayName"] for f in STANDARD_FIELDS
}


def get_procurement_view_availability(
    mapping: dict[str, str | None],
) -> list[dict[str, Any]]:
    """Check which procurement views are feasible given the current mapping."""
    mapped_keys = {k for k, v in mapping.items() if v}
    results = []
    for view in PROCUREMENT_VIEW_REGISTRY:
        missing_keys = [f for f in view["requiredFields"] if f not in mapped_keys]
        missing_display = [
            _FIELD_DISPLAY_NAMES.get(k, k) for k in missing_keys
        ]
        results.append({
            "viewId": view["viewId"],
            "title": view["title"],
            "requiredFields": view["requiredFields"],
            "available": len(missing_keys) == 0,
            "missingFields": missing_display,
        })
    return results
