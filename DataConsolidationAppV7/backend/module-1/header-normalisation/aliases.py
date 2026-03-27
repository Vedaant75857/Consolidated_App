"""Alias mappings, expected types, semantic patterns, ERP codes, multilingual
dictionaries, and abbreviation expansion for standard procurement fields."""

from __future__ import annotations

import importlib.util
import os
import re
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _norm(s: str) -> str:
    """Lowercase, strip, collapse whitespace/underscores/hyphens/slashes."""
    if not s:
        return ""
    return re.sub(r"[\s_\-/]+", " ", str(s).strip().lower())


# ---------------------------------------------------------------------------
# 1. Field aliases  (normalised at load time via _norm)
# ---------------------------------------------------------------------------

_RAW_ALIASES: dict[str, list[str]] = {
    "Invoice Number": [
        "inv no", "inv_no", "invoice_no", "invoice_num", "inv_number", "bill_number",
        "bill_no", "inv num", "invoice number", "invoicenumber", "inv_nbr", "bill number",
        "invoice_id", "inv_id", "rechnungsnummer", "facture", "invoiceid", "fattura",
        "num fattura", "numero fattura", "doc fornitore", "fatt data doc", "billing document",
    ],
    "Invoice Line Number": [
        "line_no", "line_num", "inv_line", "invoice_line", "line_number", "line item",
        "item_no", "inv_line_no", "invoice line", "lineitem", "invoicelinenumber",
        "extrainvoicelinekey", "splitaccountingnumber",
    ],
    "Invoice Date": [
        "inv_date", "invoice_date", "inv_dt", "bill_date", "billing_date",
        "invoice date", "invoicedate", "inv date", "rechnungsdatum",
        "calendar day", "calendarday", "data di registrazione", "data documento",
        "accounting date", "accountingdate",
    ],
    "Goods Receipt Date": [
        "gr_date", "goods_receipt_date", "receipt_date", "grn_date", "delivery_date",
        "goods receipt", "gr date", "receiving_date",
    ],
    "Invoice Line Description": [
        "line_description", "inv_desc", "item_description", "description",
        "invoice_description", "line desc", "item desc", "line_desc", "text",
        "invoice line description", "material_text", "short_text",
        "product description", "productdescription",
    ],
    "Invoice Line Number Quantity": [
        "qty", "quantity", "inv_qty", "line_qty", "invoice_qty", "inv quantity",
        "billed_qty", "billed quantity",
    ],
    "Invoice Line Number Quantity UOM": [
        "uom", "unit", "unit_of_measure", "measure", "inv_uom", "order_unit",
        "base_uom", "unit of measure", "unitofmeasure", "base unit", "baseunit",
    ],
    "Local Currency Code": [
        "currency", "curr_code", "currency_code", "local_currency", "curr",
        "doc_currency", "document_currency", "transaction_currency", "local currency",
        "amountcurrency", "divisa documento", "divisa locale", "divisa",
    ],
    "Total Amount paid in Local Currency": [
        "amount", "local_amount", "inv_amount", "net_amount", "total_amount",
        "line_amount", "amount_lc", "spend", "value", "net_value",
        "amt", "total amount local", "amount local currency",
        "billing amount in lc", "billingamountinlc", " amount",
        "importo in divisa documento", "importo in divisa locale",
    ],
    "Total Amount paid in Reporting Currency": [
        "reporting_amount", "amount_rc", "usd_amount", "converted_amount",
        "reporting_currency_amount", "amount reporting", "spend_usd",
        "total amount reporting", "importo in divisa di gruppo",
    ],
    "Price per UOM": [
        "unit_price", "price", "price_per_unit", "net_price", "unit price",
    ],
    "Contract indicator": [
        "contract_indicator", "on_contract", "contract_flag", "contract indicator",
        "under_contract",
    ],
    "Fiscal Year": [
        "fiscal_year", "fy", "fin_year", "financial_year", "fisc_year",
        "fiscal year", "year",
    ],
    "Payment date": [
        "payment_date", "pay_date", "pmt_date", "clearing_date",
        "payment date", "paid_date", "paiddate",
    ],
    "Debit/ Credit Indicator": [
        "debit_credit", "dc_indicator", "db_cr", "dr_cr", "debit credit",
        "posting_type", "linetype", "line type",
    ],
    "PO Indicator": [
        "po_indicator", "po_flag", "po indicator", "po_ind",
    ],
    "Invoice PO Number": [
        "po_number", "po_no", "po_num", "purchase_order", "po number",
        "purchase order", "po", "poid", "documento acquisti", "oda",
    ],
    "Invoice PO Line Number": [
        "po_line", "po_line_no", "po_item", "po line", "po_line_number",
        "polinenumber", "extrapoline", "extrapolinekey",
    ],
    "PO Document Date": [
        "po_date", "po_doc_date", "purchase_order_date", "po date", "oda data",
    ],
    "PO Line Item Description 1": [
        "po_description", "po_desc", "po_text", "po_item_text",
        "po line description", "po description",
    ],
    "PO Material Group Description": [
        "material_group_desc", "commodity_desc", "mat_group_desc",
        "material group description", "commodity_group_desc", "category_description",
    ],
    "PO Material Number": [
        "material_number", "material_no", "mat_no", "item_number", "material number",
        "part_number", "sku", "partnumber", "partrevision",
    ],
    "PO Material Description": [
        "material_description", "material_desc", "mat_desc",
        "material description", "item_description_po",
    ],
    "PO Material Group Code": [
        "material_group", "mat_group", "commodity_code", "commodity_group",
        "material group code", "category_code", "material_group_code", "erpcommodityid",
    ],
    "PO Line Item Quantity": [
        "po_qty", "po_quantity", "order_qty", "order_quantity", "po quantity",
    ],
    "PO Line Item Quantity UOM": [
        "po_uom", "order_uom", "po_unit", "po uom",
    ],
    "PO Local Currency Code": [
        "po_currency", "po_curr", "po currency",
    ],
    "PO Line Item Unit Price": [
        "po_unit_price", "po_price", "order_price", "po unit price",
    ],
    "PO Manufacturer part number": [
        "mfr_part", "manufacturer_part", "mfg_part_no", "manufacturer part number",
        "partrevision", "partrevisionnumber",
    ],
    "PO Manufacturer name": [
        "manufacturer", "mfr_name", "mfg_name", "manufacturer name",
    ],
    "PO Line Item Description 2": [
        "po_desc_2", "po_description_2", "po text 2", "secondary_po_desc",
    ],
    "PO Total Amount in Local Currency": [
        "po_amount", "po_value", "order_amount", "po_net_value",
        "po amount local", "po total amount local",
    ],
    "PO Total Amount in reporting currency": [
        "po_amount_rc", "po_reporting_amount", "po_value_usd",
        "po amount reporting", "po total amount reporting",
    ],
    "Vendor Code": [
        "vendor_code", "supplier_code", "vendor_no", "supplier_no",
        "vendor_id", "supplier_id", "vendor_number", "supplier_number",
        "vendor code", "supplier code", "lifnr", "supplierid", "supplierlocationid",
    ],
    "Vendor Name": [
        "vendor_name", "supplier_name", "vendor", "supplier",
        "vendor name", "supplier name", "vendor_nm", "supplier_nm", "fornitore",
    ],
    "Vendor Country": [
        "vendor_country", "supplier_country", "vendor_ctry", "supp_country",
        "vendor country", "supplier country",
    ],
    "Vendor State": [
        "vendor_state", "supplier_state", "vendor state", "supplier state",
        "vendor_region",
    ],
    "Vendor Preferred Status": [
        "preferred", "preferred_vendor", "preferred_supplier",
        "vendor preferred", "preferred status",
    ],
    "Vendor Address": [
        "vendor_address", "supplier_address", "vendor address", "supplier address",
    ],
    "Vendor City": [
        "vendor_city", "supplier_city", "vendor city", "supplier city",
    ],
    "Vendor Zip/Postal Code": [
        "vendor_zip", "vendor_postal", "supplier_zip", "postal_code",
        "vendor zip", "zip_code",
    ],
    "Vendor Diversity": [
        "diversity", "vendor_diversity", "supplier_diversity", "minority",
    ],
    "Business Unit": [
        "business_unit", "bu", "biz_unit", "business unit", "segment",
        "b.u.", "business area", "businessarea",
    ],
    "Company Code": [
        "company_code", "comp_code", "bukrs", "company code", "entity_code",
        "accountcompanycode", "societa", "societ\u00e0",
    ],
    "Company Name": [
        "company_name", "company", "legal_entity", "entity_name",
        "company name",
    ],
    "Company Country": [
        "company_country", "entity_country", "company country",
    ],
    "Plant Name": [
        "plant_name", "plant", "facility_name", "site_name", "plant name",
        "facility", "site", "werks_name", "companysiteid", "company site",
    ],
    "Plant Code": [
        "plant_code", "werks", "facility_code", "site_code", "plant code",
    ],
    "Plant Country": [
        "plant_country", "facility_country", "site_country", "plant country",
    ],
    "Plant State": [
        "plant_state", "facility_state", "site_state", "plant state",
    ],
    "Plant City": [
        "plant_city", "facility_city", "site_city", "plant city",
    ],
    "Business Division": [
        "division", "business_division", "bus_division", "business division",
    ],
    "Contract ID": [
        "contract_id", "contract_no", "contract_number", "agreement_no",
        "contract id", "contract number", "contractid",
    ],
    "Contract party": [
        "contract_party", "contracting_party", "contract party",
    ],
    "Contract End Date": [
        "contract_end", "contract_end_date", "agreement_end", "contract end date",
        "expiry_date",
    ],
    "Contract Start Date": [
        "contract_start", "contract_start_date", "agreement_start",
        "contract start date", "effective_date",
    ],
    "Payment Terms": [
        "payment_terms", "pay_terms", "terms", "payment terms", "pmt_terms",
        "incoterms", "appaymentterms", "ap payment terms",
    ],
    "Contract Status": [
        "contract_status", "agreement_status", "contract status",
    ],
    "Contract Description": [
        "contract_desc", "contract_description", "agreement_desc",
        "contract description",
    ],
    "Spend Classification Level 1": [
        "category_l1", "spend_l1", "classification_l1", "cat_level_1",
        "level_1", "l1_category",
    ],
    "Spend Classification Level 2": [
        "category_l2", "spend_l2", "classification_l2", "cat_level_2",
        "level_2", "l2_category",
    ],
    "Spend Classification Level 3": [
        "category_l3", "spend_l3", "classification_l3", "cat_level_3",
        "level_3", "l3_category",
    ],
    "Spend Classification Level 4": [
        "category_l4", "spend_l4", "classification_l4", "cat_level_4",
        "level_4", "l4_category",
    ],
    "Procurement Contract Owner": [
        "contract_owner", "procurement_owner", "buyer", "buyer_name",
    ],
    "Cost Center Code": [
        "cost_center", "kostl", "cost_center_code", "cc_code",
        "cost center code", "cost center", "costcenterid",
        "centro di costo", "centro costo",
    ],
    "Cost Center Description": [
        "cost_center_desc", "cost_center_name", "cc_desc",
        "cost center description", "cost center name", "costcenterdesc",
    ],
    "GL Account": [
        "gl_account", "gl_code", "gl", "general_ledger", "saknr",
        "gl account", "gl code", "account_code", "accountid",
        "conto co.ge.", "conto coge", "conto ge",
    ],
    "GL Account Description": [
        "gl_desc", "gl_account_desc", "gl_description", "account_description",
        "gl account description",
    ],
    "GL Account Hierarchy Level 1": [
        "gl_l1", "gl_hierarchy_1", "gl hierarchy l1",
    ],
    "GL Account Hierarchy Level 2": [
        "gl_l2", "gl_hierarchy_2", "gl hierarchy l2",
    ],
    "Currency Conversion rate": [
        "fx_rate", "exchange_rate", "conversion_rate", "currency_rate",
        "currency conversion", "exch_rate",
    ],
    "Data Source System": [
        "source_system", "erp_system", "data_source", "system", "source",
        "data source", "erp",
    ],
    "Transaction ID": [
        "transaction_id", "txn_id", "trans_id", "doc_number", "document_number",
        "transaction id", "reference_number", "ref_no",
        "numero documento", "extrainvoicekey",
    ],
}

# Pre-built normalised alias sets (used by deterministic_matcher backward compat)
FIELD_ALIASES: dict[str, set[str]] = {
    field: {_norm(a) for a in aliases}
    for field, aliases in _RAW_ALIASES.items()
}

FIELD_ALIASES_LIST: dict[str, list[str]] = {
    field: sorted(dict.fromkeys(aliases))
    for field, aliases in _RAW_ALIASES.items()
}


# ---------------------------------------------------------------------------
# 2. Flat O(1) alias lookup  (norm_string -> canonical field)
# ---------------------------------------------------------------------------

_schema_mod = _load_mod("hn_aliases_schema", os.path.join(_this_dir, "schema_mapper.py"))
STD_FIELD_NAMES: list[str] = _schema_mod.STD_FIELD_NAMES

ALIAS_LOOKUP: dict[str, str] = {}
for _field, _aliases in _RAW_ALIASES.items():
    ALIAS_LOOKUP[_norm(_field)] = _field
    for _alias in _aliases:
        ALIAS_LOOKUP[_norm(_alias)] = _field
for _f in STD_FIELD_NAMES:
    ALIAS_LOOKUP[_norm(_f)] = _f

# Targeted patches for ambiguous / incorrect default resolutions
_DIRECT_PATCHES: dict[str, str] = {
    "numero documento": "Invoice Number",
    "num documento": "Invoice Number",
    "g l account": "GL Account",
    "g l acct": "GL Account",
    "g l code": "GL Account",
    "g l": "GL Account",
    "document date": "Invoice Date",
    "doc date": "Invoice Date",
    "posting date": "Invoice Date",
    "posting dt": "Invoice Date",
    "book date": "Invoice Date",
    "booking date": "Invoice Date",
    "transaction date": "Invoice Date",
    "value date": "Invoice Date",
    "order number": "Invoice PO Number",
    "order num": "Invoice PO Number",
    "order no": "Invoice PO Number",
    "order no.": "Invoice PO Number",
    "purch order": "Invoice PO Number",
    "purchase doc": "Invoice PO Number",
    "purchasing document": "Invoice PO Number",
    "material": "PO Material Number",
    "mat": "PO Material Number",
    "article": "PO Material Number",
    "category": "PO Material Group Code",
    "commodity": "PO Material Group Code",
    "spend category": "PO Material Group Code",
    "costcentercompanycode": "Cost Center Description",
    "cost center company code": "Cost Center Description",
    "g l account description": "GL Account Description",
    "g l acct desc": "GL Account Description",
    "g l account desc": "GL Account Description",
    "purch doc": "Invoice PO Number",
    "purch. doc.": "Invoice PO Number",
    "purch. doc": "Invoice PO Number",
    "purch document": "Invoice PO Number",
}
for _k, _v in _DIRECT_PATCHES.items():
    ALIAS_LOOKUP[_norm(_k)] = _v


# ---------------------------------------------------------------------------
# 3. SAP / ERP field code dictionary
# ---------------------------------------------------------------------------

ERP_CODES: dict[str, str] = {
    "lifnr": "Vendor Code", "lifnr1": "Vendor Name",
    "bukrs": "Company Code", "bukrs1": "Company Name",
    "kostl": "Cost Center Code",
    "saknr": "GL Account", "hkont": "GL Account",
    "werks": "Plant Code", "name1": "Plant Name",
    "matnr": "PO Material Number",
    "ebeln": "Invoice PO Number", "bstnr": "Invoice PO Number",
    "ebelp": "Invoice PO Line Number",
    "belnr": "Invoice Number",
    "bldat": "Invoice Date", "budat": "Invoice Date",
    "zfbdt": "Payment date", "augdt": "Payment date",
    "waers": "Local Currency Code", "hwaer": "Local Currency Code",
    "wrbtr": "Total Amount paid in Local Currency",
    "dmbtr": "Total Amount paid in Local Currency",
    "kursf": "Currency Conversion rate",
    "mblnr": "Goods Receipt Date",
    "maktx": "PO Material Description",
    "matkl": "PO Material Group Code",
    "gjahr": "Fiscal Year",
    "shkzg": "Debit/ Credit Indicator",
    "zterm": "Payment Terms",
    "txz01": "PO Line Item Description 1",
    "netpr": "Price per UOM",
    "menge": "Invoice Line Number Quantity",
    "meins": "Invoice Line Number Quantity UOM",
    "pstyp": "PO Indicator",
    "lifnr2": "Vendor Name",
    "kunnr": "Company Code",
    "vendor_id": "Vendor Code", "vendor_site_id": "Vendor Code",
    "org_id": "Company Code", "invoice_id": "Invoice Number",
    "invoice_line_id": "Invoice Line Number",
    "erp_number": "Invoice Number", "erp_order_id": "Invoice PO Number",
    "commodity_id": "PO Material Group Code",
    "account_type": "GL Account",
}


# ---------------------------------------------------------------------------
# 4. Multilingual dictionary
# ---------------------------------------------------------------------------

MULTILINGUAL: dict[str, str] = {
    # Italian
    "numero documento": "Invoice Number",
    "num documento": "Invoice Number",
    "data di registrazione": "Invoice Date",
    "data documento": "Invoice Date",
    "fatt data doc": "Invoice Date",
    "doc fornitore": "Invoice Number",
    "fornitore": "Vendor Name",
    "documento acquisti": "Invoice PO Number",
    "oda": "Invoice PO Number",
    "oda data": "PO Document Date",
    "profit center": "Business Unit",
    "profit centre": "Business Unit",
    "b u": "Business Unit",
    "b.u.": "Business Unit",
    "business area": "Business Unit",
    "centro di costo": "Cost Center Code",
    "centro costo": "Cost Center Code",
    "conto co ge": "GL Account",
    "conto coge": "GL Account",
    "conto ge": "GL Account",
    "wbe": "GL Account",
    "wbs": "GL Account",
    "wbs element": "GL Account",
    "divisa documento": "Local Currency Code",
    "divisa locale": "Local Currency Code",
    "divisa di gruppo": "Total Amount paid in Reporting Currency",
    "importo in divisa documento": "Total Amount paid in Local Currency",
    "importo in divisa locale": "Total Amount paid in Local Currency",
    "importo in divisa di gruppo": "Total Amount paid in Reporting Currency",
    "societa": "Company Code",
    "societ\u00e0": "Company Code",
    # German
    "rechnungsnummer": "Invoice Number",
    "rechnungsdatum": "Invoice Date",
    "lieferant": "Vendor Name", "lieferantenname": "Vendor Name",
    "lieferantennummer": "Vendor Code",
    "kostenstelle": "Cost Center Code",
    "buchungskreis": "Company Code",
    "buchungsdatum": "Invoice Date",
    "belegdatum": "Invoice Date",
    "werksname": "Plant Name",
    "bestellnummer": "Invoice PO Number",
    "zahlungsbedingung": "Payment Terms",
    "gesch\u00e4ftsjahr": "Fiscal Year",
    "sachkonto": "GL Account",
    "betrag": "Total Amount paid in Local Currency",
    "waehrung": "Local Currency Code",
    # Spanish
    "proveedor": "Vendor Name", "nombre proveedor": "Vendor Name",
    "codigo proveedor": "Vendor Code",
    "factura": "Invoice Number", "numero factura": "Invoice Number",
    "fecha factura": "Invoice Date", "fecha documento": "Invoice Date",
    "importe": "Total Amount paid in Local Currency",
    "moneda": "Local Currency Code",
    "centro coste": "Cost Center Code",
    "orden compra": "Invoice PO Number",
    # French
    "fournisseur": "Vendor Name", "nom fournisseur": "Vendor Name",
    "numero facture": "Invoice Number",
    "date facture": "Invoice Date",
    "montant": "Total Amount paid in Local Currency",
    "devise": "Local Currency Code",
    "centre cout": "Cost Center Code",
    "bon commande": "Invoice PO Number",
}


# ---------------------------------------------------------------------------
# 5. CamelCase splitter + abbreviation expander
# ---------------------------------------------------------------------------

def split_camel(s: str) -> str:
    """APPaymentTerms -> ap payment terms  |  InvoiceId -> invoice id"""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1 \2", s)
    return s.lower().strip()


_ABBREVS: dict[str, str] = {
    r"\bamt\b": "amount", r"\bamts\b": "amounts",
    r"\bdt\b": "date", r"\bdte\b": "date", r"\bdts\b": "dates",
    r"\bno\b": "number", r"\bno\.\b": "number",
    r"\bnum\b": "number", r"\b#\b": "number",
    r"\bid\b": "id",
    r"\bnm\b": "name", r"\bnme\b": "name",
    r"\bdesc\b": "description", r"\bdescr\b": "description",
    r"\bcd\b": "code", r"\bcd\.\b": "code",
    r"\bctr\b": "center", r"\bctr\.\b": "center",
    r"\bctry\b": "country", r"\bctry\.\b": "country",
    r"\bccy\b": "currency", r"\bcurr\b": "currency",
    r"\bvend\b": "vendor", r"\bvend\.\b": "vendor",
    r"\bsupp\b": "supplier", r"\bsupp\.\b": "supplier",
    r"\binv\b": "invoice", r"\binv\.\b": "invoice",
    r"\bpurch\b": "purchase",
    r"\bmat\b": "material", r"\bmatl\b": "material",
    r"\bgrp\b": "group",
    r"\bqty\b": "quantity",
    r"\buom\b": "unit of measure",
    r"\bgr\b": "goods receipt",
    r"\bgl\b": "gl account",
    r"\bpo\b": "purchase order",
    r"\bcc\b": "cost center",
    r"\bbu\b": "business unit",
    r"\bfy\b": "fiscal year",
    r"\brc\b": "reporting currency",
    r"\blc\b": "local currency",
    r"\bpc\b": "profit center",
    r"\bref\b": "reference",
    r"\bpay\b": "payment",
    r"\bbal\b": "balance",
    r"\bfx\b": "currency",
    r"\bexch\b": "exchange",
}


def expand_abbrevs(s: str) -> str:
    """Expand abbreviations: 'Vend Nm' -> 'vendor name', 'Pay Dt' -> 'payment date'"""
    result = s.lower()
    for pattern, replacement in _ABBREVS.items():
        result = re.sub(pattern, replacement, result)
    return re.sub(r"\s+", " ", result).strip()


# ---------------------------------------------------------------------------
# 6. Expected data types per field
# ---------------------------------------------------------------------------

EXPECTED_DTYPE: dict[str, str] = {
    "Invoice Number": "text",
    "Invoice Line Number": "text",
    "Invoice Date": "date",
    "Goods Receipt Date": "date",
    "Invoice Line Description": "text",
    "Invoice Line Number Quantity": "numeric",
    "Invoice Line Number Quantity UOM": "text",
    "Local Currency Code": "text",
    "Total Amount paid in Local Currency": "numeric",
    "Total Amount paid in Reporting Currency": "numeric",
    "Price per UOM": "numeric",
    "Contract indicator": "text",
    "Fiscal Year": "text",
    "Payment date": "date",
    "Debit/ Credit Indicator": "text",
    "PO Indicator": "text",
    "Invoice PO Number": "text",
    "Invoice PO Line Number": "text",
    "PO Document Date": "date",
    "PO Line Item Description 1": "text",
    "PO Material Group Description": "text",
    "PO Material Number": "text",
    "PO Material Description": "text",
    "PO Material Group Code": "text",
    "PO Line Item Quantity": "numeric",
    "PO Line Item Quantity UOM": "text",
    "PO Local Currency Code": "text",
    "PO Line Item Unit Price": "numeric",
    "PO Manufacturer part number": "text",
    "PO Manufacturer name": "text",
    "PO Line Item Description 2": "text",
    "PO Total Amount in Local Currency": "numeric",
    "PO Total Amount in reporting currency": "numeric",
    "Vendor Code": "text",
    "Vendor Name": "text",
    "Vendor Country": "text",
    "Vendor State": "text",
    "Vendor Preferred Status": "text",
    "Vendor Address": "text",
    "Vendor City": "text",
    "Vendor Zip/Postal Code": "text",
    "Vendor Diversity": "text",
    "Business Unit": "text",
    "Company Code": "text",
    "Company Name": "text",
    "Company Country": "text",
    "Plant Name": "text",
    "Plant Code": "text",
    "Plant Country": "text",
    "Plant State": "text",
    "Plant City": "text",
    "Business Division": "text",
    "Contract ID": "text",
    "Contract party": "text",
    "Contract End Date": "date",
    "Contract Start Date": "date",
    "Payment Terms": "text",
    "Contract Status": "text",
    "Contract Description": "text",
    "Spend Classification Level 1": "text",
    "Spend Classification Level 2": "text",
    "Spend Classification Level 3": "text",
    "Spend Classification Level 4": "text",
    "Procurement Contract Owner": "text",
    "Cost Center Code": "text",
    "Cost Center Description": "text",
    "GL Account": "text",
    "GL Account Description": "text",
    "GL Account Hierarchy Level 1": "text",
    "GL Account Hierarchy Level 2": "text",
    "Currency Conversion rate": "numeric",
    "Data Source System": "text",
    "Transaction ID": "text",
}


# ---------------------------------------------------------------------------
# 7. Semantic pattern detectors  (applied to sample values)
# ---------------------------------------------------------------------------

_ISO_CURRENCY = re.compile(r"^[A-Z]{3}$")
_ISO_COUNTRY_2 = re.compile(r"^[A-Z]{2}$")
_DATE_LIKE = re.compile(r"^\d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{1,4}$")
_NUMERIC_LIKE = re.compile(r"^[\-+]?\d[\d,]*\.?\d*$")
_YEAR_LIKE = re.compile(r"^(19|20)\d{2}$")
_ZIP_LIKE = re.compile(r"^\d{4,6}(-\d{4})?$")

COUNTRY_NAMES = {
    "united states", "germany", "france", "china", "india", "japan",
    "united kingdom", "brazil", "canada", "mexico", "australia",
    "italy", "spain", "south korea", "netherlands", "switzerland",
    "sweden", "belgium", "austria", "norway", "denmark", "finland",
    "ireland", "singapore", "taiwan", "thailand", "malaysia",
    "indonesia", "vietnam", "philippines", "poland", "czech republic",
    "hungary", "romania", "turkey", "south africa", "egypt", "nigeria",
    "saudi arabia", "uae", "israel", "new zealand", "argentina",
    "chile", "colombia", "peru", "portugal", "greece",
}

UOM_KEYWORDS = {
    "ea", "each", "pc", "pcs", "kg", "lb", "lbs", "ton", "mt",
    "l", "liter", "litre", "gal", "gallon", "m", "ft", "in",
    "box", "case", "pallet", "lot", "set", "pack", "roll", "sheet",
}

DEBIT_CREDIT_KEYWORDS = {"h", "s", "d", "c", "debit", "credit", "dr", "cr"}


def infer_value_type(samples: list[str]) -> str:
    """Classify a list of sample values as 'date', 'numeric', or 'text'."""
    if not samples:
        return "text"
    date_count = sum(1 for s in samples if _DATE_LIKE.match(s.strip()))
    num_count = sum(1 for s in samples if _NUMERIC_LIKE.match(s.strip().replace(",", "")))
    n = len(samples)
    if date_count / n >= 0.5:
        return "date"
    if num_count / n >= 0.5:
        return "numeric"
    return "text"


def semantic_hints(samples: list[str]) -> list[str]:
    """Return a list of semantic tags detected from sample values."""
    if not samples:
        return []
    tags: list[str] = []
    lowers = [s.strip().lower() for s in samples if s.strip()]
    uppers = [s.strip() for s in samples if s.strip()]

    currency_hits = sum(1 for s in uppers if _ISO_CURRENCY.match(s))
    if currency_hits / max(len(uppers), 1) >= 0.3:
        tags.append("currency_code")

    iso2_hits = sum(1 for s in uppers if _ISO_COUNTRY_2.match(s))
    if iso2_hits / max(len(uppers), 1) >= 0.3:
        tags.append("country_iso2")

    country_hits = sum(1 for s in lowers if s in COUNTRY_NAMES)
    if country_hits / max(len(lowers), 1) >= 0.2:
        tags.append("country_name")

    year_hits = sum(1 for s in uppers if _YEAR_LIKE.match(s))
    if year_hits / max(len(uppers), 1) >= 0.3:
        tags.append("year")

    uom_hits = sum(1 for s in lowers if s in UOM_KEYWORDS)
    if uom_hits / max(len(lowers), 1) >= 0.2:
        tags.append("uom")

    zip_hits = sum(1 for s in uppers if _ZIP_LIKE.match(s))
    if zip_hits / max(len(uppers), 1) >= 0.3:
        tags.append("postal_code")

    dc_hits = sum(1 for s in lowers if s in DEBIT_CREDIT_KEYWORDS)
    if dc_hits / max(len(lowers), 1) >= 0.3:
        tags.append("debit_credit")

    return tags


SEMANTIC_TAG_TO_FIELDS: dict[str, list[str]] = {
    "currency_code": ["Local Currency Code", "PO Local Currency Code"],
    "country_iso2": ["Vendor Country", "Company Country", "Plant Country"],
    "country_name": ["Vendor Country", "Company Country", "Plant Country"],
    "year": ["Fiscal Year"],
    "uom": ["Invoice Line Number Quantity UOM", "PO Line Item Quantity UOM"],
    "postal_code": ["Vendor Zip/Postal Code"],
    "debit_credit": ["Debit/ Credit Indicator"],
}

FIELD_TO_SEMANTIC_TAGS: dict[str, list[str]] = {}
for _tag, _fields in SEMANTIC_TAG_TO_FIELDS.items():
    for _field in _fields:
        FIELD_TO_SEMANTIC_TAGS.setdefault(_field, []).append(_tag)
