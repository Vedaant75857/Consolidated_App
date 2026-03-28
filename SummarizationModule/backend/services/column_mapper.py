import re
import sqlite3
from typing import Any

import pandas as pd

from shared.ai_client import call_ai_json
from shared.db import get_meta, set_meta

STANDARD_FIELDS = [
    {
        "fieldKey": "total_spend",
        "displayName": "Total Spend (USD)",
        "expectedType": "numeric",
        "description": "Monetary spend amount converted/normalized to USD",
    },
    {
        "fieldKey": "invoice_date",
        "displayName": "Invoice Date",
        "expectedType": "datetime",
        "description": "Date of invoice or transaction",
    },
    {
        "fieldKey": "country",
        "displayName": "Country",
        "expectedType": "string",
        "description": "Country of origin or supplier country",
    },
    {
        "fieldKey": "supplier",
        "displayName": "Supplier Name",
        "expectedType": "string",
        "description": "Vendor/supplier entity name",
    },
    {
        "fieldKey": "l1",
        "displayName": "Category Level 1",
        "expectedType": "string",
        "description": "Top-level procurement category (broadest, fewest distinct values)",
    },
    {
        "fieldKey": "l2",
        "displayName": "Category Level 2",
        "expectedType": "string",
        "description": "Second-level procurement category",
    },
    {
        "fieldKey": "l3",
        "displayName": "Category Level 3",
        "expectedType": "string",
        "description": "Third-level procurement category",
    },
    {
        "fieldKey": "l4",
        "displayName": "Category Level 4",
        "expectedType": "string",
        "description": "Fourth-level procurement category (most granular, most distinct values)",
    },
    {
        "fieldKey": "currency",
        "displayName": "Invoice Currency",
        "expectedType": "string",
        "description": "ISO 4217 currency code of the local/original invoice",
    },
    {
        "fieldKey": "local_spend",
        "displayName": "Local Spend Amount",
        "expectedType": "numeric",
        "description": "Spend amount in the original/local invoice currency",
    },
]

COLUMN_MAPPING_SYSTEM_PROMPT = """You are a senior procurement data analyst. Given a list of column names with sample
values and their detected data types from a procurement/spend dataset, map each
required field to the most suitable column. Each required field has an expected data
type (numeric, datetime, or string) — you MUST only map columns whose detected type
matches or is coercible to the expected type.

For each field, provide the best match and 2 alternatives. If no suitable column
exists for a field, set bestMatch to null.

Return a JSON object with a single key "mappings" containing an array.

TYPE COMPATIBILITY RULES:
- numeric fields: only match columns detected as numeric
- datetime fields: only match columns detected as datetime, or string columns where
  sample values look like dates (e.g. "2023-01-15", "01/15/2023", "Jan 2023")
- string fields: can match any detected type

DISAMBIGUATION HINTS — read carefully before mapping:

1. TOTAL SPEND (USD) vs LOCAL SPEND:
   - Total Spend (USD) is the CONVERTED/NORMALIZED amount in a single reference
     currency (usually USD). Look for columns with "USD", "dollar", "converted",
     "normalized", "standard", or "reporting currency" in the name. Sample values
     are typically all in the same magnitude/scale since they share one currency.
   - Local Spend is the ORIGINAL invoice amount in the vendor's local currency.
     Look for columns with "local", "original", "invoice value", "native", or
     "LC"/"LCY" in the name. Sample values may vary wildly in magnitude (e.g.
     100 USD vs 10,000 JPY vs 85 EUR) because they span multiple currencies.
   - If only ONE numeric spend column exists, assign it to Total Spend (USD) and
     leave Local Spend unmapped (bestMatch: null).
   - These two columns should NEVER be the same column.

2. INVOICE DATE vs OTHER DATE COLUMNS:
   - Invoice Date is the date the invoice was issued or received. Look for
     "invoice date", "inv date", "billing date", "transaction date".
   - DO NOT confuse with: "payment date" / "pay date" (when payment was made),
     "PO date" / "order date" (when the purchase order was created), "delivery
     date" / "receipt date" (when goods arrived), "due date" (payment deadline),
     "posting date" / "created date" (ERP system dates), "fiscal period".
   - If ambiguous, prefer the column closest to "invoice" or "transaction" in name.

3. SUPPLIER NAME vs OTHER NAME/TEXT COLUMNS:
   - Supplier Name is the vendor/seller/provider entity. Look for "vendor",
     "supplier", "seller", "provider", "payee", "creditor" in the name.
   - Sample values typically look like company names: contain "Inc.", "Ltd.",
     "LLC", "GmbH", "Corp.", "S.A.", "Co.", "PLC", "Pvt" or are well-known
     brand/company names.
   - DO NOT confuse with: "buyer" / "requestor" / "approver" (internal people),
     "cost center" / "department" / "business unit" (organizational units),
     "plant" / "site" / "location" (facilities), "material" / "description"
     / "item" (what was purchased), "contract" / "PO number" (document refs).

4. CATEGORY L1 vs L2 vs L3 vs L4 (HIERARCHY):
   - These form a taxonomy hierarchy: L1 is the broadest (fewest distinct values),
     L4 is the most granular (most distinct values).
   - L1 examples: "Direct Materials", "Indirect Spend", "Services", "IT",
     "Marketing", "Logistics". Typically 5-20 distinct values.
   - L2 examples: "Raw Materials", "Packaging", "MRO", "Professional Services".
     Typically 20-80 distinct values.
   - L3/L4 are increasingly specific subcategories with more distinct values.
   - To distinguish levels: COUNT THE DISTINCT VALUES in the samples — fewer
     distinct values = higher level (L1), more distinct values = lower level (L4).
   - Look for "L1", "L2", "level 1", "level 2", "category 1", "cat1", "segment",
     "family", "class", "commodity", "sub-category", "sub-class" in column names.
   - DO NOT confuse with: "GL account" / "account code" (financial codes),
     "UNSPSC" / "HS code" (standard classification codes — these are numeric
     codes, not category names), "material group" (could be a category but verify
     via sample values).
   - If the dataset has columns like "Category" and "Sub-Category" (only 2 levels),
     map Category -> L1 and Sub-Category -> L2. Leave L3/L4 unmapped.

5. COUNTRY vs OTHER GEOGRAPHIC COLUMNS:
   - Country is the country associated with the supplier or transaction. Look for
     "country", "nation", "supplier country", "vendor country".
   - Sample values are country names ("United States", "Germany") or ISO codes
     ("US", "DE", "USA", "GBR").
   - DO NOT confuse with: "city", "state" / "province" / "region" (sub-country),
     "plant country" / "ship-to country" / "bill-to country" (if multiple country
     columns exist, prefer "supplier country" or the unqualified "country"),
     "continent", "market", "zone".

6. CURRENCY vs OTHER SHORT-CODE COLUMNS:
   - Currency is the ISO 4217 currency code of the local/original invoice. Look
     for "currency", "ccy", "curr", "FX", "invoice currency", "local currency".
   - Sample values are 3-letter codes: "USD", "EUR", "GBP", "JPY", "INR", "CNY".
   - DO NOT confuse with: "country code" (2-letter ISO 3166: "US", "DE"),
     "unit of measure" / "UOM" ("EA", "KG", "LB", "PC"), "language code"
     ("EN", "FR"), "payment terms" ("NET30", "NET60").
   - If multiple currency columns exist (e.g. "Invoice Currency" vs "Payment
     Currency"), prefer the one tied to the invoice/transaction."""


def ai_map_columns(
    columns: list[dict[str, Any]], api_key: str
) -> list[dict[str, Any]]:
    user_payload = {
        "requiredFields": STANDARD_FIELDS,
        "columns": [
            {
                "name": c["name"],
                "detectedType": c["detectedType"],
                "distinctCount": c["distinctCount"],
                "samples": c["sampleValues"][:50],
            }
            for c in columns
        ],
    }
    result = call_ai_json(COLUMN_MAPPING_SYSTEM_PROMPT, user_payload, api_key=api_key)
    return result.get("mappings", [])


def build_typed_table(
    conn: sqlite3.Connection, mapping: dict[str, str | None]
) -> dict[str, Any]:
    """Build the `analysis_data` table with enforced types.

    Args:
        conn: SQLite connection for the session
        mapping: dict of fieldKey -> sourceColumnName (or None if unmapped)

    Returns:
        Cast report with per-field stats.
    """
    table_names = get_meta(conn, "table_names") or []

    frames = []
    for tname in table_names:
        try:
            df = pd.read_sql(f'SELECT * FROM "{tname}"', conn)
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

    # Store datetime as ISO strings for SQLite compatibility
    if "invoice_date" in typed_df.columns:
        dt_col = typed_df["invoice_date"]
        if hasattr(dt_col, "dt"):
            nat_mask = dt_col.isna()
            typed_df["invoice_date"] = dt_col.dt.strftime("%Y-%m-%dT%H:%M:%S")
            typed_df.loc[nat_mask, "invoice_date"] = None

    typed_df.to_sql("analysis_data", conn, if_exists="replace", index=False)

    # Store null audit rows (where total_spend or invoice_date failed)
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
