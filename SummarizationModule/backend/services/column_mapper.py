import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from shared.ai_client import call_ai_json
from shared.db import get_meta, set_meta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 28 Standard Fields (sorted alphabetically by displayName)
# ---------------------------------------------------------------------------

STANDARD_FIELDS: list[dict[str, Any]] = [
    {
        "fieldKey": "business_unit",
        "displayName": "Business Unit",
        "expectedType": "string",
        "description": "Organisational business unit or division",
        "aliases": ["BU", "Division", "Business Area"],
    },
    {
        "fieldKey": "l1",
        "displayName": "Category Level 1",
        "expectedType": "string",
        "description": "Top-level procurement category (broadest, fewest distinct values)",
        "aliases": ["Spend Classification Level 1", "L1", "Category 1"],
    },
    {
        "fieldKey": "l2",
        "displayName": "Category Level 2",
        "expectedType": "string",
        "description": "Second-level procurement category",
        "aliases": ["Spend Classification Level 2", "L2", "Category 2"],
    },
    {
        "fieldKey": "l3",
        "displayName": "Category Level 3",
        "expectedType": "string",
        "description": "Third-level procurement category (most granular)",
        "aliases": ["Spend Classification Level 3", "L3", "Category 3"],
    },
    {
        "fieldKey": "contract_end_date",
        "displayName": "Contract End Date",
        "expectedType": "datetime",
        "description": "Date the contract expires or ends",
        "aliases": ["Contract Expiry Date", "Contract Expiration"],
    },
    {
        "fieldKey": "contract_id",
        "displayName": "Contract ID",
        "expectedType": "string",
        "description": "Unique identifier for the contract",
        "aliases": ["Contract Number", "Contract Ref", "Agreement ID"],
    },
    {
        "fieldKey": "contract_indicator",
        "displayName": "Contract indicator",
        "expectedType": "string",
        "description": "Flag or code indicating whether a contract exists",
        "aliases": ["Contract Flag", "Has Contract", "Contract Y/N"],
    },
    {
        "fieldKey": "contract_start_date",
        "displayName": "Contract Start Date",
        "expectedType": "datetime",
        "description": "Date the contract became effective",
        "aliases": ["Contract Effective Date", "Contract Begin Date"],
    },
    {
        "fieldKey": "contract_status",
        "displayName": "Contract Status",
        "expectedType": "string",
        "description": "Current status of the contract (active, expired, etc.)",
        "aliases": ["Contract State", "Agreement Status"],
    },
    {
        "fieldKey": "goods_receipt_date",
        "displayName": "Goods Receipt Date",
        "expectedType": "datetime",
        "description": "Date goods were received or delivery was confirmed",
        "aliases": ["GR Date", "Delivery Date", "Receipt Date"],
    },
    {
        "fieldKey": "invoice_date",
        "displayName": "Invoice Date",
        "expectedType": "datetime",
        "description": "Date of invoice or transaction",
        "aliases": ["Inv Date", "Billing Date", "Transaction Date"],
    },
    {
        "fieldKey": "invoice_line_qty",
        "displayName": "Invoice Line Number Quantity",
        "expectedType": "numeric",
        "description": "Quantity on the invoice line item",
        "aliases": ["Invoice Quantity", "Line Qty", "Billed Quantity"],
    },
    {
        "fieldKey": "invoice_line_qty_uom",
        "displayName": "Invoice Line Number Quantity UOM",
        "expectedType": "string",
        "description": "Unit of measure for the invoice line quantity",
        "aliases": ["Invoice UOM", "Line UOM", "Quantity Unit"],
    },
    {
        "fieldKey": "currency",
        "displayName": "Invoice Currency",
        "expectedType": "string",
        "description": "ISO 4217 currency code of the local/original invoice",
        "aliases": ["Local Currency Code", "Currency Code", "CCY"],
    },
    {
        "fieldKey": "invoice_number",
        "displayName": "Invoice Number",
        "expectedType": "string",
        "description": "Unique invoice document identifier",
        "aliases": ["Invoice No", "Invoice #", "Invoice ID", "Inv Number"],
    },
    {
        "fieldKey": "invoice_po_number",
        "displayName": "Invoice PO Number",
        "expectedType": "string",
        "description": "Purchase order number referenced on the invoice",
        "aliases": ["PO Number", "PO #", "Purchase Order Number", "PO Ref"],
    },
    {
        "fieldKey": "payment_terms",
        "displayName": "Payment Terms",
        "expectedType": "string",
        "description": "Payment terms code or description (e.g. NET30, NET60)",
        "aliases": ["Pay Terms", "Terms of Payment", "Payment Condition"],
    },
    {
        "fieldKey": "plant_code",
        "displayName": "Plant Code",
        "expectedType": "string",
        "description": "Code identifying the plant or facility",
        "aliases": ["Plant ID", "Plant No", "Facility Code", "Site Code"],
    },
    {
        "fieldKey": "country",
        "displayName": "Plant Country",
        "expectedType": "string",
        "description": "Country of the company or plant",
        "aliases": ["Country", "Company Country"],
    },
    {
        "fieldKey": "plant_name",
        "displayName": "Plant Name",
        "expectedType": "string",
        "description": "Name of the plant or facility",
        "aliases": ["Plant Description", "Facility Name", "Site Name"],
    },
    {
        "fieldKey": "po_document_date",
        "displayName": "PO Document Date",
        "expectedType": "datetime",
        "description": "Date the purchase order document was created",
        "aliases": ["PO Date", "Order Date", "Purchase Order Date"],
    },
    {
        "fieldKey": "po_material_description",
        "displayName": "PO Material Description",
        "expectedType": "string",
        "description": "Description of the material or item on the purchase order",
        "aliases": ["Material Description", "Item Description", "PO Description"],
    },
    {
        "fieldKey": "po_material_number",
        "displayName": "PO Material Number",
        "expectedType": "string",
        "description": "Material or item number on the purchase order",
        "aliases": ["Material Number", "Material Code", "Item Number", "Material ID"],
    },
    {
        "fieldKey": "price_per_uom",
        "displayName": "Price per UOM",
        "expectedType": "numeric",
        "description": "Unit price per unit of measure",
        "aliases": ["Unit Price", "Price per Unit", "PO Unit Price"],
    },
    {
        "fieldKey": "total_spend",
        "displayName": "Total Amount paid in Reporting Currency",
        "expectedType": "numeric",
        "description": "Monetary spend amount converted/normalized to a single reporting currency",
        "aliases": ["Total Spend (USD)", "Total Spend", "Reporting Currency Amount"],
    },
    {
        "fieldKey": "local_spend",
        "displayName": "Total Amount paid in Local Currency",
        "expectedType": "numeric",
        "description": "Spend amount in the original/local invoice currency",
        "aliases": ["Local Spend Amount", "Local Spend", "Invoice Amount"],
    },
    {
        "fieldKey": "vendor_country",
        "displayName": "Vendor Country",
        "expectedType": "string",
        "description": "Country of the vendor/supplier",
        "aliases": ["Supplier Country", "Vendor Nation"],
    },
    {
        "fieldKey": "supplier",
        "displayName": "Vendor Name",
        "expectedType": "string",
        "description": "Vendor/supplier entity name",
        "aliases": ["Supplier Name", "Supplier", "Vendor"],
    },
]

# ---------------------------------------------------------------------------
# Per-field disambiguation hints for AI mapping
# ---------------------------------------------------------------------------

PER_FIELD_HINTS: dict[str, str] = {
    "total_spend": """TOTAL SPEND (REPORTING CURRENCY) vs LOCAL SPEND:
- This is the CONVERTED/NORMALIZED amount in a single reporting currency (usually USD).
  Look for columns with "USD", "dollar", "converted", "normalized", "standard",
  "reporting currency", or "Total Amount paid in Reporting Currency" in the name.
- Sample values are typically all in the same magnitude/scale since they share one currency.
- If only ONE numeric spend column exists, assign it here.
- This should NEVER be the same column as Local Spend.""",

    "local_spend": """LOCAL SPEND vs TOTAL SPEND:
- This is the ORIGINAL invoice amount in the vendor's local currency.
  Look for columns with "local", "original", "invoice value", "native",
  "LC", "LCY", or "Total Amount paid in Local Currency" in the name.
- Sample values may vary wildly in magnitude (e.g. 100 USD vs 10,000 JPY vs 85 EUR).
- This should NEVER be the same column as Total Spend / Reporting Currency Amount.""",

    "invoice_date": """INVOICE DATE vs OTHER DATES:
- The date the invoice was issued or received. Look for "invoice date", "inv date",
  "billing date", "transaction date".
- DO NOT confuse with: "payment date", "PO date", "delivery date", "receipt date",
  "due date", "posting date", "contract date", "goods receipt date".
- If ambiguous, prefer the column closest to "invoice" or "transaction" in name.""",

    "supplier": """VENDOR/SUPPLIER NAME:
- The vendor/seller/provider entity. Look for "vendor", "supplier", "seller",
  "provider", "payee", "creditor", "Vendor Name" in the name.
- Samples look like company names: "Inc.", "Ltd.", "LLC", "GmbH", "Corp.".
- DO NOT confuse with buyer/requestor, cost center, plant, material description.""",

    "country": """PLANT/COMPANY COUNTRY:
- The country of the company or plant (NOT the vendor/supplier country).
  Look for "plant country", "company country", or unqualified "country".
- Sample values are country names or ISO codes ("US", "DE", "USA").
- DO NOT confuse with "vendor country" / "supplier country" (that's a separate field).""",

    "vendor_country": """VENDOR/SUPPLIER COUNTRY:
- The country of the vendor or supplier. Look for "vendor country",
  "supplier country" in the name.
- Sample values are country names or ISO codes.
- DO NOT confuse with "plant country" / "company country" (that's a separate field).""",

    "l1": """CATEGORY LEVEL 1 (HIERARCHY):
- Broadest procurement category with fewest distinct values (typically 5-20).
  Look for "L1", "level 1", "category 1", "segment", "Spend Classification Level 1".
- Examples: "Direct Materials", "Indirect Spend", "Services", "IT".
- Count distinct sample values: fewer = L1, more = L2/L3.""",

    "l2": """CATEGORY LEVEL 2 (HIERARCHY):
- Second-level category with moderate distinct values (typically 20-80).
  Look for "L2", "level 2", "category 2", "family", "sub-category".
- Sits between L1 (broad) and L3 (granular).""",

    "l3": """CATEGORY LEVEL 3 (HIERARCHY):
- Most granular procurement category with most distinct values.
  Look for "L3", "level 3", "category 3", "class", "commodity", "sub-class".
- More distinct values than L1 or L2.""",

    "currency": """INVOICE CURRENCY:
- ISO 4217 currency code of the local/original invoice. Look for "currency",
  "ccy", "curr", "FX", "invoice currency", "local currency".
- Sample values are 3-letter codes: "USD", "EUR", "GBP", "JPY".
- DO NOT confuse with country codes, UOM codes, or payment terms.""",

    "business_unit": """BUSINESS UNIT:
- The organisational business unit or division. Look for "business unit",
  "BU", "division", "business area", "segment".
- DO NOT confuse with: plant name/code (facility), cost center, department.""",

    "contract_end_date": """CONTRACT END DATE:
- The date the contract expires. Look for "contract end", "contract expiry",
  "contract expiration", "agreement end date".
- DO NOT confuse with: invoice date, PO date, goods receipt date, payment date.""",

    "contract_id": """CONTRACT ID:
- Unique identifier for the contract. Look for "contract ID", "contract number",
  "contract ref", "agreement ID", "contract #".
- Usually alphanumeric codes. DO NOT confuse with PO number or invoice number.""",

    "contract_indicator": """CONTRACT INDICATOR:
- A flag or code indicating whether a contract exists. Look for "contract indicator",
  "contract flag", "has contract", "contract Y/N".
- Values might be "Y"/"N", "Yes"/"No", or short codes.""",

    "contract_start_date": """CONTRACT START DATE:
- The date the contract became effective. Look for "contract start",
  "contract effective date", "agreement start", "contract begin".
- DO NOT confuse with: PO date, invoice date, goods receipt date.""",

    "contract_status": """CONTRACT STATUS:
- Current status of the contract. Look for "contract status", "agreement status",
  "contract state".
- Values like "Active", "Expired", "Pending", "Terminated".""",

    "goods_receipt_date": """GOODS RECEIPT DATE:
- The date goods were received. Look for "goods receipt date", "GR date",
  "delivery date", "receipt date".
- DO NOT confuse with: invoice date, PO date, contract date, payment date.""",

    "invoice_line_qty": """INVOICE LINE QUANTITY:
- The quantity on the invoice line item. Look for "invoice line quantity",
  "line qty", "billed quantity", "invoice quantity".
- This is numeric. DO NOT confuse with PO quantity or price.""",

    "invoice_line_qty_uom": """INVOICE LINE QUANTITY UOM:
- Unit of measure for the invoice line quantity. Look for "UOM", "unit of measure",
  "quantity unit", "invoice UOM".
- Values like "EA", "KG", "LB", "PC", "L", "M". Short codes, NOT currency codes.""",

    "invoice_number": """INVOICE NUMBER:
- Unique invoice document identifier. Look for "invoice number", "invoice no",
  "invoice #", "invoice ID", "inv number".
- Alphanumeric codes. DO NOT confuse with PO number or contract ID.""",

    "invoice_po_number": """INVOICE PO NUMBER:
- The purchase order number referenced on the invoice. Look for "PO number",
  "purchase order number", "PO #", "PO ref", "invoice PO number".
- DO NOT confuse with invoice number or contract ID.""",

    "payment_terms": """PAYMENT TERMS:
- Payment terms code or description. Look for "payment terms", "pay terms",
  "terms of payment", "payment condition".
- Values like "NET30", "NET60", "2/10 NET 30", "Due on Receipt".""",

    "plant_code": """PLANT CODE:
- Code identifying the plant or facility. Look for "plant code", "plant ID",
  "plant no", "facility code", "site code".
- Short alphanumeric codes. DO NOT confuse with plant name or company code.""",

    "plant_name": """PLANT NAME:
- Name of the plant or facility. Look for "plant name", "plant description",
  "facility name", "site name".
- Free-text names. DO NOT confuse with plant code, company name, or vendor name.""",

    "po_document_date": """PO DOCUMENT DATE:
- The date the purchase order was created. Look for "PO date", "PO document date",
  "order date", "purchase order date".
- DO NOT confuse with: invoice date, goods receipt date, contract date.""",

    "po_material_description": """PO MATERIAL DESCRIPTION:
- Description of the material/item on the PO. Look for "material description",
  "item description", "PO description", "PO line item description".
- Free-text. DO NOT confuse with material number/code or vendor name.""",

    "po_material_number": """PO MATERIAL NUMBER:
- Material or item number on the PO. Look for "material number", "material code",
  "item number", "material ID", "PO material number".
- Alphanumeric codes. DO NOT confuse with PO number or invoice number.""",

    "price_per_uom": """PRICE PER UOM:
- Unit price per unit of measure. Look for "unit price", "price per unit",
  "price per UOM", "PO unit price".
- Numeric. DO NOT confuse with total spend or local spend (those are line totals).""",
}

# ---------------------------------------------------------------------------
# Per-field AI prompt template
# ---------------------------------------------------------------------------

_SINGLE_FIELD_SYSTEM_PROMPT = """You are a senior procurement data analyst. You are mapping ONE specific field
from a procurement dataset.

TARGET FIELD:
- Name: {display_name}
- Type: {expected_type}
- Description: {description}

DISAMBIGUATION HINTS:
{hints}

TYPE INFERENCE RULES (infer from sample values):
- numeric: sample values are numbers, possibly with currency symbols/commas
- datetime: sample values look like dates
- string: sample values are free-text, codes, or names

TYPE COMPATIBILITY:
- numeric fields: only map columns whose samples are parseable as numbers
- datetime fields: only map columns whose samples are parseable as dates
- string fields: can map any column

Given the list of available columns with sample values, find the best match for this field.
Return a JSON object with keys: "bestMatch" (column name or null), "alternatives" (array of
up to 2 alternative column names), "reasoning" (brief explanation)."""

# ---------------------------------------------------------------------------
# Deterministic exact-match pass
# ---------------------------------------------------------------------------


def deterministic_match(
    columns: list[dict[str, Any]],
) -> tuple[dict[str, str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Match uploaded columns to standard fields by exact name (case-insensitive).

    Returns:
        (matched, unmatched_fields, unmatched_columns) where matched is
        {fieldKey: columnName}, unmatched_fields is a list of STANDARD_FIELDS
        entries that had no match, and unmatched_columns is columns not consumed.
    """
    lookup: dict[str, str] = {}  # lowercase name -> fieldKey
    for field in STANDARD_FIELDS:
        lookup[field["displayName"].lower()] = field["fieldKey"]
        for alias in field.get("aliases", []):
            lower_alias = alias.lower()
            if lower_alias not in lookup:
                lookup[lower_alias] = field["fieldKey"]

    matched: dict[str, str] = {}  # fieldKey -> column name
    consumed_columns: set[str] = set()

    for col in columns:
        col_lower = col["name"].strip().lower()
        if col_lower in lookup:
            fk = lookup[col_lower]
            if fk not in matched:
                matched[fk] = col["name"]
                consumed_columns.add(col["name"])

    matched_keys = set(matched.keys())
    unmatched_fields = [f for f in STANDARD_FIELDS if f["fieldKey"] not in matched_keys]
    unmatched_columns = [c for c in columns if c["name"] not in consumed_columns]

    return matched, unmatched_fields, unmatched_columns


# ---------------------------------------------------------------------------
# Parallel per-field AI mapping
# ---------------------------------------------------------------------------


def _ai_map_single_field(
    field: dict[str, Any],
    columns: list[dict[str, Any]],
    api_key: str,
) -> dict[str, Any]:
    """Send a focused AI call for a single standard field."""
    hints = PER_FIELD_HINTS.get(field["fieldKey"], "No specific hints for this field.")
    system_prompt = _SINGLE_FIELD_SYSTEM_PROMPT.format(
        display_name=field["displayName"],
        expected_type=field["expectedType"],
        description=field["description"],
        hints=hints,
    )
    user_payload = {
        "columns": [
            {"name": c["name"], "samples": c["sampleValues"][:30]}
            for c in columns
        ],
    }
    try:
        result = call_ai_json(system_prompt, user_payload, api_key=api_key)
    except Exception as exc:
        logger.warning("AI mapping failed for %s: %s", field["fieldKey"], exc)
        return {
            "fieldKey": field["fieldKey"],
            "bestMatch": None,
            "alternatives": [],
            "reasoning": f"AI call failed: {exc}",
            "expectedType": field["expectedType"],
        }

    bm = result.get("bestMatch")
    if isinstance(bm, dict):
        bm = bm.get("column") or bm.get("name") or None

    alts = result.get("alternatives", [])
    if not isinstance(alts, list):
        alts = []
    alts = [
        (a.get("column") or a.get("name") or "") if isinstance(a, dict) else str(a)
        for a in alts
    ]

    return {
        "fieldKey": field["fieldKey"],
        "bestMatch": bm,
        "alternatives": alts,
        "reasoning": result.get("reasoning", ""),
        "expectedType": field["expectedType"],
    }


def ai_map_columns(
    unmatched_fields: list[dict[str, Any]],
    unmatched_columns: list[dict[str, Any]],
    api_key: str,
) -> list[dict[str, Any]]:
    """Map unmatched fields to columns using parallel per-field AI calls."""
    if not unmatched_fields or not unmatched_columns:
        return [
            {
                "fieldKey": f["fieldKey"],
                "bestMatch": None,
                "alternatives": [],
                "reasoning": "No columns available for AI mapping",
                "expectedType": f["expectedType"],
            }
            for f in unmatched_fields
        ]

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, len(unmatched_fields))) as executor:
        futures = {
            executor.submit(
                _ai_map_single_field, field, unmatched_columns, api_key
            ): field["fieldKey"]
            for field in unmatched_fields
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                fk = futures[future]
                logger.error("AI mapping thread failed for %s: %s", fk, exc)
                results.append({
                    "fieldKey": fk,
                    "bestMatch": None,
                    "alternatives": [],
                    "reasoning": f"Thread error: {exc}",
                    "expectedType": "string",
                })

    return results


# ---------------------------------------------------------------------------
# build_typed_table
# ---------------------------------------------------------------------------

_DATETIME_FIELD_KEYS = {
    f["fieldKey"] for f in STANDARD_FIELDS if f["expectedType"] == "datetime"
}


def build_typed_table(
    conn: sqlite3.Connection, mapping: dict[str, str | None]
) -> dict[str, Any]:
    """Build the ``analysis_data`` table with enforced types.

    Args:
        conn: SQLite connection for the session
        mapping: dict of fieldKey -> sourceColumnName (or None if unmapped)

    Returns:
        Cast report with per-field stats.
    """
    from services.file_loader import _get_registry

    registry = _get_registry(conn)
    data_tables = [r["data_table"] for r in registry]

    frames = []
    for tname in data_tables:
        try:
            df = pd.read_sql(f'SELECT * FROM "{tname}"', conn)
            if "RECORD_ID" in df.columns:
                df = df.drop(columns=["RECORD_ID"])
            frames.append(df)
        except Exception:
            continue

    if not frames:
        raise ValueError("No data tables found in session")

    raw_df = pd.concat(frames, ignore_index=True)
    total_rows = len(raw_df)

    typed_df = pd.DataFrame(index=raw_df.index)
    cast_report: dict[str, Any] = {"total_rows": total_rows, "fields": {}}

    for field in STANDARD_FIELDS:
        fk = field["fieldKey"]
        source_col = mapping.get(fk)
        expected_type = field["expectedType"]

        if not source_col or source_col not in raw_df.columns:
            typed_df[fk] = None
            cast_report["fields"][fk] = {
                "mapped": False,
                "sourceColumn": None,
                "validRows": 0,
                "nullRows": total_rows,
                "parseRate": 0.0,
                "sampleFailures": [],
            }
            continue

        raw_col = raw_df[source_col].copy()

        if expected_type == "numeric":
            cleaned = raw_col.astype(str).str.replace(r"[,$€£¥\s]", "", regex=True)
            typed_col = pd.to_numeric(cleaned, errors="coerce")
            failures = raw_col[typed_col.isna() & raw_col.notna() & (raw_col.astype(str).str.strip() != "")]
            valid = int(typed_col.notna().sum())
            typed_df[fk] = typed_col

        elif expected_type == "datetime":
            typed_col = pd.to_datetime(raw_col, errors="coerce", format="mixed")
            failures = raw_col[typed_col.isna() & raw_col.notna() & (raw_col.astype(str).str.strip() != "")]
            valid = int(typed_col.notna().sum())
            typed_df[fk] = typed_col

        else:  # string
            typed_col = raw_col.astype(str).str.strip()
            typed_col = typed_col.replace({"nan": "", "None": "", "NULL": "", "<NA>": ""})
            failures = pd.Series([], dtype=str)
            valid = int((typed_col != "").sum())
            typed_df[fk] = typed_col

        null_count = total_rows - valid
        parse_rate = round(valid / total_rows * 100, 2) if total_rows > 0 else 0.0
        sample_fails = failures.head(5).tolist() if len(failures) > 0 else []

        cast_report["fields"][fk] = {
            "mapped": True,
            "sourceColumn": source_col,
            "validRows": valid,
            "nullRows": null_count,
            "parseRate": parse_rate,
            "sampleFailures": [str(s) for s in sample_fails],
        }

    # Store ALL datetime columns as ISO strings for SQLite compatibility
    for dt_fk in _DATETIME_FIELD_KEYS:
        if dt_fk in typed_df.columns:
            dt_col = typed_df[dt_fk]
            if hasattr(dt_col, "dt"):
                nat_mask = dt_col.isna()
                typed_df[dt_fk] = dt_col.dt.strftime("%Y-%m-%dT%H:%M:%S")
                typed_df.loc[nat_mask, dt_fk] = None

    typed_df.to_sql("analysis_data", conn, if_exists="replace", index=False)

    # Critical null audit (only total_spend + invoice_date)
    mask = pd.Series(False, index=typed_df.index)
    if "total_spend" in typed_df.columns:
        mask = mask | typed_df["total_spend"].isna()
    if "invoice_date" in typed_df.columns:
        mask = mask | typed_df["invoice_date"].isna()
    critical_nulls = typed_df[mask] if mask.any() else pd.DataFrame()
    if len(critical_nulls) > 0:
        critical_nulls.to_sql("_null_rows", conn, if_exists="replace", index=False)

    conn.commit()
    set_meta(conn, "cast_report", cast_report)
    return cast_report
