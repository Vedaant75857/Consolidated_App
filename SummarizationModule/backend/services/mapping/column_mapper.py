"""Column mapping service for Module 3 (Spend Summarizer).

Maps uploaded column headers to 32 standard procurement fields using:
  1. Deterministic exact / alias matching (fast, no AI)
  2. Single-batch AI call for any remaining unmatched fields

Then builds a typed ``analysis_data`` table with enforced column types.
"""

import json
import logging
import re
from typing import Any

import pandas as pd

from shared.ai_client import call_ai_json
from shared.db import set_meta
from shared.duckdb_compat import DuckDBConnection
from services.upload.file_loader import _get_registry
from services.mapping.date_parser import parse_date_column

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 32 Standard Procurement Fields
# ---------------------------------------------------------------------------

STANDARD_FIELDS: list[dict[str, Any]] = [
    {
        "fieldKey": "business_unit",
        "displayName": "Business Unit",
        "expectedType": "string",
        "description": "Organisational business unit or division",
        "aliases": ["BU", "Division", "Business Area"],
        "hint": "Look for BU, division, business area. Not plant or cost center.",
    },
    {
        "fieldKey": "contract_end_date",
        "displayName": "Contract End Date",
        "expectedType": "datetime",
        "description": "Date the contract expires or ends",
        "aliases": ["Contract Expiry Date", "Contract Expiration"],
        "hint": "Contract expiry/end. Not invoice date, PO date, or payment date.",
    },
    {
        "fieldKey": "contract_id",
        "displayName": "Contract ID",
        "expectedType": "string",
        "description": "Unique identifier for the contract",
        "aliases": ["Contract Number", "Contract Ref", "Agreement ID"],
        "hint": "Alphanumeric contract identifier. Not PO number or invoice number.",
    },
    {
        "fieldKey": "contract_indicator",
        "displayName": "Contract indicator",
        "expectedType": "string",
        "description": "Flag or code indicating whether a contract exists",
        "aliases": ["Contract Flag", "Has Contract", "Contract Y/N"],
        "hint": "Y/N or short code flag for contract existence.",
    },
    {
        "fieldKey": "contract_start_date",
        "displayName": "Contract Start Date",
        "expectedType": "datetime",
        "description": "Date the contract became effective",
        "aliases": ["Contract Effective Date", "Contract Begin Date"],
        "hint": "Contract start/effective date. Not PO date or invoice date.",
    },
    {
        "fieldKey": "contract_status",
        "displayName": "Contract Status",
        "expectedType": "string",
        "description": "Current status of the contract (active, expired, etc.)",
        "aliases": ["Contract State", "Agreement Status"],
        "hint": "Values like Active, Expired, Pending, Terminated.",
    },
    {
        "fieldKey": "description",
        "displayName": "Description",
        "expectedType": "string",
        "description": "Free-text description of the line item, material, or service",
        "aliases": [
            "Invoice Description", "Material Description",
            "GL Account Description", "Item Description", "Line Description",
            "PO Material Description", "Product Description", "Service Description",
        ],
        "hint": "Any free-text description column describing what was purchased.",
    },
    {
        "fieldKey": "goods_receipt_date",
        "displayName": "Goods Receipt Date",
        "expectedType": "datetime",
        "description": "Date goods were received or delivery was confirmed",
        "aliases": ["GR Date", "Delivery Date", "Receipt Date"],
        "hint": "Goods receipt/delivery date. Not invoice or PO date.",
    },
    {
        "fieldKey": "invoice_date",
        "displayName": "Invoice Date",
        "expectedType": "datetime",
        "description": "Date of invoice or transaction",
        "aliases": ["Inv Date", "Billing Date", "Transaction Date"],
        "hint": "Invoice/billing/transaction date. Not payment, PO, delivery, or due date.",
    },
    {
        "fieldKey": "invoice_due_date",
        "displayName": "Invoice Due Date",
        "expectedType": "datetime",
        "description": "Date by which the invoice must be paid",
        "aliases": ["Due Date", "Payment Due Date", "Inv Due Date"],
        "hint": "When payment is due. Not invoice date or payment date.",
    },
    {
        "fieldKey": "invoice_number",
        "displayName": "Invoice Number",
        "expectedType": "string",
        "description": "Unique invoice document identifier",
        "aliases": ["Invoice No", "Invoice #", "Invoice ID", "Inv Number"],
        "hint": "Invoice identifier. Not PO number or contract ID.",
    },
    {
        "fieldKey": "invoice_po_number",
        "displayName": "Invoice PO Number",
        "expectedType": "string",
        "description": "Purchase order number referenced on the invoice",
        "aliases": ["PO Number", "PO #", "Purchase Order Number", "PO Ref"],
        "hint": "PO number on the invoice. Not invoice number or contract ID.",
    },
    {
        "fieldKey": "payment_date",
        "displayName": "Payment Date",
        "expectedType": "datetime",
        "description": "Date the payment was made",
        "aliases": ["Pay Date", "Date Paid", "Clearing Date"],
        "hint": "Actual payment/clearing date. Not invoice date or due date.",
    },
    {
        "fieldKey": "payment_terms",
        "displayName": "Payment Terms",
        "expectedType": "string",
        "description": "Payment terms code or description (e.g. NET30, NET60)",
        "aliases": ["Pay Terms", "Terms of Payment", "Payment Condition"],
        "hint": "Values like NET30, NET60, 2/10 NET 30.",
    },
    {
        "fieldKey": "plant_code",
        "displayName": "Plant Code",
        "expectedType": "string",
        "description": "Code identifying the plant or facility",
        "aliases": ["Plant ID", "Plant No", "Facility Code", "Site Code"],
        "hint": "Short code for plant/facility. Not plant name.",
    },
    {
        "fieldKey": "country",
        "displayName": "Plant Country",
        "expectedType": "string",
        "description": "Country of the company or plant",
        "aliases": ["Country", "Company Country", "Plant Country"],
        "hint": "Plant/company country. NOT vendor/supplier country.",
    },
    {
        "fieldKey": "plant_name",
        "displayName": "Plant Name",
        "expectedType": "string",
        "description": "Name of the plant or facility",
        "aliases": ["Plant Description", "Facility Name", "Site Name"],
        "hint": "Free-text plant/facility name. Not plant code or vendor name.",
    },
    {
        "fieldKey": "po_document_date",
        "displayName": "PO Document Date",
        "expectedType": "datetime",
        "description": "Date the purchase order document was created",
        "aliases": ["PO Date", "Order Date", "Purchase Order Date"],
        "hint": "PO creation date. Not invoice date or goods receipt date.",
    },
    {
        "fieldKey": "po_material_number",
        "displayName": "PO Material Number",
        "expectedType": "string",
        "description": "Material or item number on the purchase order",
        "aliases": ["Material Number", "Material Code", "Item Number", "Material ID"],
        "hint": "Alphanumeric material/item code. Not PO number or invoice number.",
    },
    {
        "fieldKey": "po_material_description",
        "displayName": "PO Material Description",
        "expectedType": "string",
        "description": "Description of the material on the purchase order",
        "aliases": [
            "PO Description", "Purchase Order Description",
            "PO Line Description", "PO Text",
        ],
        "hint": "PO-level material/item description. Distinct from generic Description.",
    },
    {
        "fieldKey": "price_per_uom",
        "displayName": "Price per UOM",
        "expectedType": "numeric",
        "description": "Unit price per unit of measure",
        "aliases": ["Unit Price", "Price per Unit", "PO Unit Price"],
        "hint": "Per-unit price. Not total spend or local spend (those are line totals).",
    },
    {
        "fieldKey": "quantity",
        "displayName": "Quantity",
        "expectedType": "numeric",
        "description": "Quantity ordered or invoiced",
        "aliases": [
            "Invoice Line Number Quantity", "Invoice Quantity",
            "Line Qty", "Billed Quantity", "Order Quantity", "Qty",
        ],
        "hint": "Numeric quantity. Not price or spend amount.",
    },
    {
        "fieldKey": "region",
        "displayName": "Region",
        "expectedType": "string",
        "description": "Geographic region (e.g. NA, EMEA, APAC, LATAM)",
        "aliases": ["Geographic Region", "Sales Region", "Market Region"],
        "hint": "High-level region. Values like NA, EMEA, APAC, LATAM.",
    },
    {
        "fieldKey": "l1",
        "displayName": "Spend Classification Level 1",
        "expectedType": "string",
        "description": "Top-level procurement category (broadest, fewest distinct values)",
        "aliases": ["Category Level 1", "L1", "Category 1", "Segment"],
        "hint": "Broadest category, typically 5-20 distinct values. Fewer than L2/L3.",
    },
    {
        "fieldKey": "l2",
        "displayName": "Spend Classification Level 2",
        "expectedType": "string",
        "description": "Second-level procurement category",
        "aliases": ["Category Level 2", "L2", "Category 2", "Family", "Sub-category"],
        "hint": "Mid-level category, typically 20-80 distinct values.",
    },
    {
        "fieldKey": "l3",
        "displayName": "Spend Classification Level 3",
        "expectedType": "string",
        "description": "Third-level procurement category",
        "aliases": ["Category Level 3", "L3", "Category 3", "Class", "Commodity"],
        "hint": "Granular category, typically 100+ distinct values. More than L1/L2.",
    },
    {
        "fieldKey": "l4",
        "displayName": "Spend Classification Level 4",
        "expectedType": "string",
        "description": "Fourth-level procurement category (most granular)",
        "aliases": ["Category Level 4", "L4", "Category 4", "Sub-class"],
        "hint": "Most granular category level. More distinct values than L3.",
    },
    {
        "fieldKey": "local_spend",
        "displayName": "Total Amount paid in Local Currency",
        "expectedType": "numeric",
        "description": "Spend amount in the original/local invoice currency",
        "aliases": ["Local Spend Amount", "Local Spend", "Invoice Amount"],
        "hint": "ORIGINAL invoice amount in vendor's local currency. Values may vary wildly in magnitude. Never same as Total Spend.",
    },
    {
        "fieldKey": "currency",
        "displayName": "Local Currency Code",
        "expectedType": "string",
        "description": "ISO currency code of the invoice/local spend (e.g. USD, EUR, GBP)",
        "aliases": ["Currency", "Invoice Currency", "Currency Code", "Local Currency"],
        "hint": "Short 3-letter ISO codes like USD, EUR, GBP, INR. NOT country names.",
    },
    {
        "fieldKey": "total_spend",
        "displayName": "Total Amount paid in Reporting Currency",
        "expectedType": "numeric",
        "description": "Monetary spend amount converted/normalized to a single reporting currency",
        "aliases": ["Total Spend (USD)", "Total Spend", "Reporting Currency Amount"],
        "hint": "CONVERTED/NORMALIZED amount, usually USD. If only one spend column, assign here. Never same as Local Spend.",
    },
    {
        "fieldKey": "uom",
        "displayName": "Unit of Measurement",
        "expectedType": "string",
        "description": "Unit of measure for the quantity",
        "aliases": [
            "Invoice Line Number Quantity UOM", "UOM", "Invoice UOM",
            "Line UOM", "Quantity Unit", "Unit of Measure",
        ],
        "hint": "Short codes like EA, KG, LB, PC, L, M. NOT currency codes.",
    },
    {
        "fieldKey": "vendor_country",
        "displayName": "Vendor Country",
        "expectedType": "string",
        "description": "Country of the vendor/supplier",
        "aliases": ["Supplier Country", "Vendor Nation"],
        "hint": "Vendor/supplier country. NOT plant/company country.",
    },
    {
        "fieldKey": "supplier",
        "displayName": "Vendor Name",
        "expectedType": "string",
        "description": "Vendor/supplier entity name",
        "aliases": ["Supplier Name", "Supplier", "Vendor"],
        "hint": "Company names (Inc., Ltd., GmbH). Not buyer, cost center, or plant name.",
    },
]

# Lookup helpers built once at import time
_FIELD_BY_KEY: dict[str, dict[str, Any]] = {
    f["fieldKey"]: f for f in STANDARD_FIELDS
}
_DATETIME_FIELD_KEYS: set[str] = {
    f["fieldKey"] for f in STANDARD_FIELDS if f["expectedType"] == "datetime"
}

# ---------------------------------------------------------------------------
# Deterministic exact / alias matching
# ---------------------------------------------------------------------------


def deterministic_match(
    columns: list[dict[str, Any]],
) -> tuple[dict[str, str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Match uploaded columns to standard fields by exact name or alias.

    Case-insensitive comparison against each field's displayName and aliases.

    Args:
        columns: List of column info dicts with at least a ``name`` key.

    Returns:
        (matched, unmatched_fields, unmatched_columns) where matched is
        {fieldKey: columnName}.
    """
    lookup: dict[str, str] = {}
    for field in STANDARD_FIELDS:
        lookup[field["displayName"].strip().lower()] = field["fieldKey"]
        for alias in field.get("aliases", []):
            key = alias.strip().lower()
            if key not in lookup:
                lookup[key] = field["fieldKey"]

    matched: dict[str, str] = {}
    consumed: set[str] = set()

    for col in columns:
        col_lower = col["name"].strip().lower()
        if col_lower in lookup:
            fk = lookup[col_lower]
            if fk not in matched:
                matched[fk] = col["name"]
                consumed.add(col["name"])

    matched_keys = set(matched.keys())
    unmatched_fields = [f for f in STANDARD_FIELDS if f["fieldKey"] not in matched_keys]
    unmatched_columns = [c for c in columns if c["name"] not in consumed]
    return matched, unmatched_fields, unmatched_columns


# ---------------------------------------------------------------------------
# Single-batch AI mapping
# ---------------------------------------------------------------------------

_BATCH_SYSTEM_PROMPT = """You are a senior procurement data analyst. Map uploaded dataset columns to standard procurement fields.

STANDARD FIELDS TO MAP (only map these — do NOT invent new fields):
{fields_block}

MATCHING RULES:
1. TYPE COMPATIBILITY IS MANDATORY:
   - "numeric" fields must map to columns with inferredType "numeric"
   - "datetime" fields must map to columns with inferredType "datetime"
   - "string" fields accept any inferredType
2. Use column NAME as the primary signal, SAMPLE VALUES to confirm.
3. Use distinctCount to disambiguate category levels: L1 ~5-20, L2 ~20-80, L3 ~100+, L4 even more.
4. Each column can be assigned to AT MOST ONE field. Never map the same column to two fields.
5. If no column is a confident match for a field, map it to null.
6. A wrong match is worse than no match.

RESPONSE FORMAT (strict JSON, no markdown fences):
Return a JSON object mapping each fieldKey to the exact column name string or null:
{{"fieldKey1": "exact column name", "fieldKey2": null, ...}}"""


def _build_fields_block(fields: list[dict[str, Any]]) -> str:
    """Build the fields description for the AI prompt."""
    lines = []
    for f in fields:
        hint = f.get("hint", "")
        lines.append(
            f'- {f["fieldKey"]} | "{f["displayName"]}" | {f["expectedType"]} | '
            f'{f["description"]}. {hint}'
        )
    return "\n".join(lines)


def _build_columns_payload(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the column info payload for the AI prompt."""
    return [
        {
            "name": c["name"],
            "samples": c.get("sampleValues", [])[:20],
            "inferredType": c.get("inferredType", "string"),
            "distinctCount": c.get("distinctCount", 0),
            "nullRate": round(c.get("nullRate", 0.0), 3),
        }
        for c in columns
    ]


def ai_map_columns(
    unmatched_fields: list[dict[str, Any]],
    unmatched_columns: list[dict[str, Any]],
    api_key: str,
) -> list[dict[str, Any]]:
    """Map unmatched fields to columns with a single batch AI call.

    Args:
        unmatched_fields: Standard field dicts that weren't matched deterministically.
        unmatched_columns: Column info dicts that weren't consumed by exact matching.
        api_key: Portkey API key.

    Returns:
        List of mapping result dicts (one per unmatched field) with keys:
        fieldKey, bestMatch, alternatives, confidence, reasoning, expectedType.
    """
    if not unmatched_fields or not unmatched_columns:
        return [
            {
                "fieldKey": f["fieldKey"],
                "bestMatch": None,
                "alternatives": [],
                "confidence": "low",
                "reasoning": "No columns available for AI mapping",
                "expectedType": f["expectedType"],
            }
            for f in unmatched_fields
        ]

    system_prompt = _BATCH_SYSTEM_PROMPT.format(
        fields_block=_build_fields_block(unmatched_fields),
    )
    user_payload = {"columns": _build_columns_payload(unmatched_columns)}

    try:
        raw_result = call_ai_json(system_prompt, user_payload, api_key=api_key)
    except Exception as exc:
        logger.warning("Batch AI mapping failed: %s", exc)
        return [
            {
                "fieldKey": f["fieldKey"],
                "bestMatch": None,
                "alternatives": [],
                "confidence": "low",
                "reasoning": f"AI call failed: {exc}",
                "expectedType": f["expectedType"],
            }
            for f in unmatched_fields
        ]

    if not isinstance(raw_result, dict):
        logger.warning("AI returned non-dict result: %s", type(raw_result))
        raw_result = {}

    # Case-insensitive column name resolution
    col_name_lower: dict[str, str] = {
        c["name"].strip().lower(): c["name"] for c in unmatched_columns
    }
    col_info_by_name: dict[str, dict[str, Any]] = {
        c["name"]: c for c in unmatched_columns
    }

    # Resolve AI results and validate types
    resolved: dict[str, str | None] = {}
    for f in unmatched_fields:
        fk = f["fieldKey"]
        ai_pick = raw_result.get(fk)

        if not ai_pick or not isinstance(ai_pick, str):
            resolved[fk] = None
            continue

        actual = col_name_lower.get(ai_pick.strip().lower())
        if actual is None:
            logger.warning("AI returned non-existent column '%s' for %s", ai_pick, fk)
            resolved[fk] = None
            continue

        # Type compatibility check
        col_type = col_info_by_name.get(actual, {}).get("inferredType", "string")
        field_type = f["expectedType"]
        if field_type == "numeric" and col_type != "numeric":
            logger.warning("Type mismatch for %s: need numeric, got %s", fk, col_type)
            resolved[fk] = None
            continue
        if field_type == "datetime" and col_type != "datetime":
            logger.warning("Type mismatch for %s: need datetime, got %s", fk, col_type)
            resolved[fk] = None
            continue

        resolved[fk] = actual

    # Resolve duplicate claims: if two fields map to the same column, keep the first
    claimed: dict[str, str] = {}  # column_lower -> fieldKey that claimed it
    for fk, col in resolved.items():
        if col is None:
            continue
        col_key = col.strip().lower()
        if col_key in claimed:
            logger.info(
                "Conflict: %s and %s both claim '%s' — keeping %s",
                claimed[col_key], fk, col, claimed[col_key],
            )
            resolved[fk] = None
        else:
            claimed[col_key] = fk

    # Build result list
    results = []
    for f in unmatched_fields:
        fk = f["fieldKey"]
        bm = resolved.get(fk)
        results.append({
            "fieldKey": fk,
            "bestMatch": bm,
            "alternatives": [],
            "confidence": "medium" if bm else "low",
            "reasoning": raw_result.get(f"{fk}_reasoning", "AI batch mapping"),
            "expectedType": f["expectedType"],
        })
    return results


# ---------------------------------------------------------------------------
# Type casting helpers
# ---------------------------------------------------------------------------


def _cast_numeric(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast a series to numeric, stripping currency symbols.

    Returns:
        (typed_series, failure_series) where failure_series contains original
        values that couldn't be parsed.
    """
    cleaned = series.astype(str).str.replace(r"[,$€£¥\s]", "", regex=True)
    typed = pd.to_numeric(cleaned, errors="coerce")
    non_blank = series.notna() & (series.astype(str).str.strip() != "")
    failures = series[typed.isna() & non_blank]
    return typed, failures


def _cast_datetime(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast a series to datetime using the date_parser module.

    Returns:
        (typed_series, failure_series).
    """
    typed = parse_date_column(series)
    non_blank = series.notna() & (series.astype(str).str.strip() != "")
    failures = series[typed.isna() & non_blank]
    return typed, failures


def _cast_string(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast a series to clean strings.

    Returns:
        (typed_series, empty failure_series).
    """
    typed = series.astype(str).str.strip()
    typed = typed.replace({"nan": "", "None": "", "NULL": "", "<NA>": ""})
    return typed, pd.Series([], dtype=str)


# ---------------------------------------------------------------------------
# build_typed_table
# ---------------------------------------------------------------------------


def build_typed_table(
    conn: DuckDBConnection, mapping: dict[str, str | None]
) -> dict[str, Any]:
    """Build the ``analysis_data`` table with enforced types.

    Concatenates all session data tables, maps columns according to the
    confirmed mapping, casts each to its expected type, and stores the
    result as ``analysis_data`` in the session database.

    Args:
        conn: Session database connection.
        mapping: {fieldKey: sourceColumnName | None}.

    Returns:
        Cast report with per-field stats.
    """
    registry = _get_registry(conn)
    data_tables = [r["data_table"] for r in registry]

    frames = []
    for tname in data_tables:
        try:
            df = conn._conn.execute(f'SELECT * FROM "{tname}"').df()
            if "RECORD_ID" in df.columns:
                df = df.drop(columns=["RECORD_ID"])
            frames.append(df)
        except Exception as exc:
            logger.warning("Failed to load table '%s': %s", tname, exc)

    if not frames:
        raise ValueError("No data tables found in session")

    raw_df = pd.concat(frames, ignore_index=True)
    total_rows = len(raw_df)

    # Case-insensitive column lookup
    col_lower_map: dict[str, str] = {
        c.strip().lower(): c for c in raw_df.columns
    }

    typed_df = pd.DataFrame(index=raw_df.index)
    cast_report: dict[str, Any] = {"total_rows": total_rows, "fields": {}}

    casters = {
        "numeric": _cast_numeric,
        "datetime": _cast_datetime,
        "string": _cast_string,
    }

    for field in STANDARD_FIELDS:
        fk = field["fieldKey"]
        source_col = mapping.get(fk)
        expected_type = field["expectedType"]

        actual_col = (
            col_lower_map.get(source_col.strip().lower())
            if source_col else None
        )

        if not actual_col:
            typed_df[fk] = None
            cast_report["fields"][fk] = {
                "mapped": False, "sourceColumn": None,
                "validRows": 0, "nullRows": total_rows,
                "parseRate": 0.0, "sampleFailures": [],
            }
            continue

        caster = casters.get(expected_type, _cast_string)
        typed_col, failures = caster(raw_df[actual_col].copy())

        if expected_type == "string":
            valid = int((typed_col != "").sum())
        else:
            valid = int(typed_col.notna().sum())

        typed_df[fk] = typed_col
        cast_report["fields"][fk] = {
            "mapped": True,
            "sourceColumn": source_col,
            "validRows": valid,
            "nullRows": total_rows - valid,
            "parseRate": round(valid / total_rows * 100, 2) if total_rows else 0.0,
            "sampleFailures": [str(s) for s in failures.head(5).tolist()],
        }

    # Store datetime columns as ISO strings for DB portability
    for dt_fk in _DATETIME_FIELD_KEYS:
        if dt_fk in typed_df.columns:
            dt_col = typed_df[dt_fk]
            if hasattr(dt_col, "dt"):
                nat_mask = dt_col.isna()
                typed_df[dt_fk] = dt_col.dt.strftime("%Y-%m-%dT%H:%M:%S")
                typed_df.loc[nat_mask, dt_fk] = None

    # Persist analysis_data table
    conn._conn.register("_temp_df", typed_df)
    try:
        conn.execute('DROP TABLE IF EXISTS "analysis_data"')
        conn._conn.execute('CREATE TABLE "analysis_data" AS SELECT * FROM _temp_df')
    finally:
        try:
            conn._conn.unregister("_temp_df")
        except Exception:
            pass

    # Critical null audit (total_spend + invoice_date)
    mask = pd.Series(False, index=typed_df.index)
    if "total_spend" in typed_df.columns:
        mask = mask | typed_df["total_spend"].isna()
    if "invoice_date" in typed_df.columns:
        mask = mask | typed_df["invoice_date"].isna()
    critical_nulls = typed_df[mask] if mask.any() else pd.DataFrame()
    if len(critical_nulls) > 0:
        conn._conn.register("_temp_df", critical_nulls)
        try:
            conn.execute('DROP TABLE IF EXISTS "_null_rows"')
            conn._conn.execute('CREATE TABLE "_null_rows" AS SELECT * FROM _temp_df')
        finally:
            try:
                conn._conn.unregister("_temp_df")
            except Exception:
                pass

    conn.commit()
    set_meta(conn, "cast_report", cast_report)
    return cast_report
