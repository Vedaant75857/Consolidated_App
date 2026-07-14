"""Procurement standard fields, analytics view categories, and per-view field requirements."""

STANDARD_FIELDS: list[dict] = [
    {"id": 1, "type": "Invoice Details", "name": "Invoice Number", "description": "Invoice number"},
    {"id": 2, "type": "Invoice Details", "name": "Invoice Line Number", "description": "Invoice Line Number"},
    {"id": 3, "type": "Invoice Details", "name": "Invoice Date", "description": "Date the invoice was created"},
    {"id": 4, "type": "Invoice Details", "name": "Goods Receipt Date", "description": "Date when goods are received"},
    {"id": 5, "type": "Invoice Details", "name": "Invoice Line Description", "description": "Description of transaction explaining product/service bought"},
    {"id": 6, "type": "Invoice Details", "name": "Invoice Line Number Quantity", "description": "Quantity purchased for invoice line number"},
    {"id": 7, "type": "Invoice Details", "name": "Invoice Line Number Quantity UOM", "description": "Unit of measure for invoice line number quantity"},
    {"id": 8, "type": "Invoice Details", "name": "Local Currency Code", "description": "Currency of Total Amount in Local Currency"},
    {"id": 9, "type": "Invoice Details", "name": "Total Amount paid in Local Currency", "description": "Amount of Invoice Line in local currency"},
    {"id": 10, "type": "Invoice Details", "name": "Total Amount paid in Reporting Currency", "description": "Amount of Invoice Line in reporting currency"},
    {"id": 11, "type": "Invoice Details", "name": "Price per UOM", "description": "Price associated with invoice line item quantity"},
    {"id": 12, "type": "Invoice Details", "name": "Contract indicator", "description": "ON/OFF Contract spend indicator"},
    {"id": 13, "type": "Invoice Details", "name": "Fiscal Year", "description": "Fiscal year when the invoice was created"},
    {"id": 14, "type": "Invoice Details", "name": "Payment date", "description": "Date when Payment is made"},
    {"id": 15, "type": "Invoice Details", "name": "Debit/ Credit Indicator", "description": "Debit/Credit Indicator of amount"},
    {"id": 16, "type": "Invoice Details", "name": "PO Indicator", "description": "ON PO or NON PO spend indicator"},
    {"id": 17, "type": "PO Details", "name": "Invoice PO Number", "description": "PO Number linking to invoice"},
    {"id": 18, "type": "PO Details", "name": "Invoice PO Line Number", "description": "PO line item number"},
    {"id": 19, "type": "PO Details", "name": "PO Document Date", "description": "Date PO created"},
    {"id": 20, "type": "PO Details", "name": "PO Line Item Description 1", "description": "Primary description available in PO"},
    {"id": 21, "type": "PO Details", "name": "PO Material Group Description", "description": "Material/commodity group description"},
    {"id": 22, "type": "PO Details", "name": "PO Material Number", "description": "Material or Item number"},
    {"id": 23, "type": "PO Details", "name": "PO Material Description", "description": "Material description"},
    {"id": 24, "type": "PO Details", "name": "PO Material Group Code", "description": "Material/commodity group code"},
    {"id": 25, "type": "PO Details", "name": "PO Line Item Quantity", "description": "PO quantity"},
    {"id": 26, "type": "PO Details", "name": "PO Line Item Quantity UOM", "description": "PO UOM"},
    {"id": 27, "type": "PO Details", "name": "PO Local Currency Code", "description": "PO local currency"},
    {"id": 28, "type": "PO Details", "name": "PO Line Item Unit Price", "description": "PO unit price"},
    {"id": 29, "type": "PO Details", "name": "PO Manufacturer part number", "description": "Manufacturer part number"},
    {"id": 30, "type": "PO Details", "name": "PO Manufacturer name", "description": "Manufacturer name"},
    {"id": 31, "type": "PO Details", "name": "PO Line Item Description 2", "description": "Secondary PO description"},
    {"id": 32, "type": "PO Details", "name": "PO Total Amount in Local Currency", "description": "PO amount in local currency"},
    {"id": 33, "type": "PO Details", "name": "PO Total Amount in reporting currency", "description": "PO amount in reporting currency"},
    {"id": 34, "type": "Vendor Details", "name": "Vendor Code", "description": "Supplier code"},
    {"id": 35, "type": "Vendor Details", "name": "Vendor Name", "description": "Supplier name"},
    {"id": 36, "type": "Vendor Details", "name": "Vendor Country", "description": "Supplier country"},
    {"id": 37, "type": "Vendor Details", "name": "Vendor State", "description": "Supplier state"},
    {"id": 38, "type": "Vendor Details", "name": "Vendor Preferred Status", "description": "Preferred supplier indicator"},
    {"id": 39, "type": "Vendor Details", "name": "Vendor Address", "description": "Supplier address"},
    {"id": 40, "type": "Vendor Details", "name": "Vendor City", "description": "Supplier city"},
    {"id": 41, "type": "Vendor Details", "name": "Vendor Zip/Postal Code", "description": "Supplier postal code"},
    {"id": 42, "type": "Vendor Details", "name": "Vendor Diversity", "description": "Diversity indicator"},
    {"id": 43, "type": "Buyer Details", "name": "Business Unit", "description": "Business unit name"},
    {"id": 44, "type": "Buyer Details", "name": "Company Code", "description": "Legal entity code"},
    {"id": 45, "type": "Buyer Details", "name": "Company Name", "description": "Legal entity name"},
    {"id": 46, "type": "Buyer Details", "name": "Company Country", "description": "Legal entity country"},
    {"id": 47, "type": "Buyer Details", "name": "Plant Name", "description": "Facility name"},
    {"id": 48, "type": "Buyer Details", "name": "Plant Code", "description": "Facility code"},
    {"id": 49, "type": "Buyer Details", "name": "Plant Country", "description": "Facility country"},
    {"id": 50, "type": "Buyer Details", "name": "Plant State", "description": "Facility state"},
    {"id": 51, "type": "Buyer Details", "name": "Plant City", "description": "Facility city"},
    {"id": 52, "type": "Buyer Details", "name": "Business Division", "description": "Business division"},
    {"id": 53, "type": "Contract Details", "name": "Contract ID", "description": "Contract number"},
    {"id": 54, "type": "Contract Details", "name": "Contract party", "description": "Contract party name"},
    {"id": 55, "type": "Contract Details", "name": "Contract End Date", "description": "Contract end date"},
    {"id": 56, "type": "Contract Details", "name": "Contract Start Date", "description": "Contract start date"},
    {"id": 57, "type": "Contract Details", "name": "Payment Terms", "description": "Payment terms details"},
    {"id": 58, "type": "Contract Details", "name": "Contract Status", "description": "Contract status"},
    {"id": 59, "type": "Contract Details", "name": "Contract Description", "description": "Contract description"},
    {"id": 60, "type": "Procurement Details", "name": "Spend Classification Level 1", "description": "Category level 1"},
    {"id": 61, "type": "Procurement Details", "name": "Spend Classification Level 2", "description": "Category level 2"},
    {"id": 62, "type": "Procurement Details", "name": "Spend Classification Level 3", "description": "Category level 3"},
    {"id": 63, "type": "Procurement Details", "name": "Spend Classification Level 4", "description": "Category level 4"},
    {"id": 64, "type": "Procurement Details", "name": "Procurement Contract Owner", "description": "Procurement contract owner"},
    {"id": 65, "type": "Accounting Data", "name": "Cost Center Code", "description": "Cost center code"},
    {"id": 66, "type": "Accounting Data", "name": "Cost Center Description", "description": "Cost center name"},
    {"id": 67, "type": "Accounting Data", "name": "GL Account", "description": "GL code"},
    {"id": 68, "type": "Accounting Data", "name": "GL Account Description", "description": "GL description"},
    {"id": 69, "type": "Accounting Data", "name": "GL Account Hierarchy Level 1", "description": "GL hierarchy L1"},
    {"id": 70, "type": "Accounting Data", "name": "GL Account Hierarchy Level 2", "description": "GL hierarchy L2"},
    {"id": 71, "type": "Others", "name": "Currency Conversion rate", "description": "Currency conversion rate"},
    {"id": 72, "type": "Others", "name": "Data Source System", "description": "ERP system name"},
    {"id": 73, "type": "Others", "name": "Transaction ID", "description": "Unique transaction identifier"},
]

STD_FIELD_NAMES: list[str] = [f["name"] for f in STANDARD_FIELDS]

STD_FIELD_DESCRIPTIONS: dict[str, str] = {
    "Invoice Number":
        "Unique identifier assigned to a supplier invoice by the vendor or ERP. "
        "Alphanumeric, e.g. INV-2024-001, 5100012345. Links all line items on the same bill.",
    "Invoice Line Number":
        "Sequential line item number within a single invoice. Numeric, e.g. 1, 2, 10. "
        "Combined with Invoice Number it uniquely identifies one charge.",
    "Invoice Date":
        "Date the supplier issued the invoice. Format varies: YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY. "
        "Used for aging, fiscal-year bucketing, and payment-term calculation.",
    "Goods Receipt Date":
        "Date the buying organisation physically received the goods or confirmed service delivery. "
        "Marks the start of the payment obligation. Often called GR date or delivery date.",
    "Invoice Line Description":
        "Free-text description of the product or service charged on this invoice line. "
        "E.g. 'Office Supplies Q3', 'Consulting - Strategy Review'. Used for spend classification.",
    "Invoice Line Number Quantity":
        "Number of units billed on this invoice line. Numeric, e.g. 100, 2.5. "
        "Multiplied by Price per UOM to derive line-item spend.",
    "Invoice Line Number Quantity UOM":
        "Unit of measure for the invoice line quantity. E.g. EA (each), KG, L, HR (hours), MT. "
        "Must match the UOM used in the corresponding PO line.",
    "Local Currency Code":
        "ISO 4217 three-letter currency code of the invoice amount in the country of transaction. "
        "E.g. USD, EUR, GBP, INR. Distinct from the reporting/group currency.",
    "Total Amount paid in Local Currency":
        "Net invoice line amount expressed in the local transaction currency. "
        "Numeric, can be negative for credit notes. Core field for spend analytics.",
    "Total Amount paid in Reporting Currency":
        "Invoice line amount converted to the company's single reporting / group currency (e.g. USD or EUR). "
        "Used for cross-entity spend comparisons and dashboards.",
    "Price per UOM":
        "Unit price charged by the supplier for one unit of measure. "
        "= Total Amount / Quantity. Used for price benchmarking and rationalization.",
    "Contract indicator":
        "Binary flag showing whether this invoice was covered by a formal contract. "
        "Values: ON CONTRACT / OFF CONTRACT, Y/N, 1/0, Contracted/Non-contracted.",
    "Fiscal Year":
        "The company's fiscal year in which the invoice falls. Numeric, e.g. 2023, FY2024. "
        "May differ from calendar year depending on company's fiscal calendar.",
    "Payment date":
        "Date the invoice was actually paid / cleared by accounts payable. "
        "Used to measure payment-term compliance and Days Payable Outstanding (DPO).",
    "Debit/ Credit Indicator":
        "Indicates whether the posting increases (Debit/D/H) or decreases (Credit/C/S) the expense. "
        "Credit entries represent reversals or credit notes from suppliers.",
    "PO Indicator":
        "Flag indicating whether a purchase order was raised for this transaction. "
        "Values: PO / Non-PO, ON PO / OFF PO, Y/N. Drives maverick-spend analysis.",
    "Invoice PO Number":
        "Purchase Order number referenced on the invoice. Alphanumeric, e.g. PO-45001234, 4500012345. "
        "Links invoice spend back to approved procurement documents.",
    "Invoice PO Line Number":
        "Specific line item within the referenced Purchase Order. Numeric, e.g. 1, 10, 20. "
        "Together with PO Number it pinpoints the exact approved commitment.",
    "PO Document Date":
        "Date the Purchase Order was created / approved in the ERP system. "
        "Used to measure procurement lead times and contract coverage.",
    "PO Line Item Description 1":
        "Primary free-text description from the PO line. E.g. 'Raw Material - Steel Coil HRC'. "
        "May differ from the invoice description if goods were substituted.",
    "PO Material Group Description":
        "Human-readable label for the material/commodity group assigned to the PO line. "
        "E.g. 'Office Supplies', 'MRO', 'IT Hardware'. Used for spend categorisation.",
    "PO Material Number":
        "Internal material or item number in the ERP/catalogue. Alphanumeric, e.g. MAT-00012, 100-200. "
        "Used for price benchmarking and specification rationalization.",
    "PO Material Description":
        "Descriptive text for the ERP material master record linked to the PO line. "
        "More standardised than free-text descriptions; used for like-for-like comparison.",
    "PO Material Group Code":
        "Alphanumeric code for the commodity/material group. E.g. L001, 00300, MRO-IT. "
        "Used to roll up spend to category hierarchies.",
    "PO Line Item Quantity":
        "Quantity ordered on the PO line. Numeric. May differ from the invoiced quantity "
        "if partial deliveries or over-deliveries occurred.",
    "PO Line Item Quantity UOM":
        "Unit of measure for the PO line quantity. E.g. EA, KG, MT, L, HR. "
        "Should match Invoice Line Number Quantity UOM for three-way matching.",
    "PO Local Currency Code":
        "ISO currency code used when the PO was raised. E.g. EUR, USD. "
        "May differ from invoice currency if FX rates changed between PO and invoice.",
    "PO Line Item Unit Price":
        "Agreed price per unit on the PO. Numeric. Benchmark for invoice price validation "
        "and savings tracking (PO price vs invoice price).",
    "PO Manufacturer part number":
        "OEM or manufacturer's own part number for the item. Used in indirect/MRO procurement "
        "to validate substitutions and support specification rationalization.",
    "PO Manufacturer name":
        "Name of the original equipment manufacturer (OEM). Distinct from the vendor/distributor. "
        "Used in LCC sourcing and mega-supplier analysis.",
    "PO Line Item Description 2":
        "Secondary or supplemental description field on the PO line. "
        "Some ERPs split long descriptions across two text fields.",
    "PO Total Amount in Local Currency":
        "Total committed value of the PO line in local currency. "
        "Compared to invoiced amount to detect over-invoicing or under-delivery.",
    "PO Total Amount in reporting currency":
        "Total PO line commitment converted to reporting currency. "
        "Used for budget tracking and category spend commitments.",
    "Vendor Code":
        "Unique supplier identifier in the ERP (e.g. SAP LIFNR). Alphanumeric, e.g. V10001, SUP-4532. "
        "Primary key for all vendor master data lookups.",
    "Vendor Name":
        "Legal or trading name of the supplier. E.g. 'Acme Corp', 'Siemens AG'. "
        "Used in supplier rationalization, mega-supplier, and benchmarking analyses.",
    "Vendor Country":
        "Country where the supplier is registered or where the supply originates. ISO or full name. "
        "E.g. DE, India, United States. Core field for LCC sourcing analysis.",
    "Vendor State":
        "State or province of the supplier. E.g. Bavaria, California, Maharashtra. "
        "Used for regional sourcing and tax compliance.",
    "Vendor Preferred Status":
        "Flag indicating whether the supplier has been approved as a preferred/strategic partner. "
        "Values: Preferred / Non-preferred, Y/N, Strategic/Approved/Unapproved.",
    "Vendor Address":
        "Street address of the supplier. Used for logistics, tax and compliance purposes.",
    "Vendor City":
        "City of the supplier's registered or billing address.",
    "Vendor Zip/Postal Code":
        "Postal or ZIP code of the supplier's address. Used for geographic spend clustering.",
    "Vendor Diversity":
        "Indicates whether the supplier qualifies as a diverse/minority/women-owned business. "
        "Values: MBE, WBE, MWBE, Yes/No, certified diversity category codes.",
    "Business Unit":
        "Internal business unit, profit center, or segment that owns the spend. "
        "E.g. 'Marketing', 'Manufacturing - Plant A', 'IT'. Used for spend allocation.",
    "Company Code":
        "Legal entity code in the ERP (e.g. SAP BUKRS). Short alphanumeric, e.g. 1000, US01. "
        "Identifies which legal company incurred the spend.",
    "Company Name":
        "Full legal name of the buying entity. E.g. 'Acme Inc.', 'XYZ GmbH'. "
        "Used alongside Company Code for multi-entity reporting.",
    "Company Country":
        "Country of incorporation of the buying legal entity. E.g. USA, Germany, India.",
    "Plant Name":
        "Name of the manufacturing site, warehouse, or facility that ordered/received the goods. "
        "E.g. 'Stuttgart Plant', 'Mumbai Warehouse'.",
    "Plant Code":
        "ERP code for the plant or facility (e.g. SAP WERKS). E.g. P001, IN01. "
        "Used for centralized buying and plant-level spend analysis.",
    "Plant Country":
        "Country where the plant or facility is located. Used for cross-border procurement analysis.",
    "Plant State":
        "State or province where the plant is located.",
    "Plant City":
        "City where the plant is located.",
    "Business Division":
        "Higher-level grouping above Business Unit. E.g. 'EMEA Division', 'Consumer Products Division'. "
        "Used for divisional roll-up reporting.",
    "Contract ID":
        "Unique identifier for the contract in the contract management system. "
        "E.g. CTR-2024-001, AGR-5500012345. Links spend to negotiated agreements.",
    "Contract party":
        "Name of the counterparty (usually the supplier) in the contract. "
        "May differ from invoicing vendor name if invoicing entity differs from contracted entity.",
    "Contract End Date":
        "Date on which the contract expires. Used to flag at-risk spend and drive renewals. "
        "Critical for contract status and dynamic spend views.",
    "Contract Start Date":
        "Date the contract became effective. Used with end date to assess active coverage window.",
    "Payment Terms":
        "Agreed payment terms between buyer and supplier. E.g. Net 30, 2/10 Net 30, Net 60, "
        "Immediate. Used for DPO optimisation and payment-terms rationalization.",
    "Contract Status":
        "Current lifecycle state of the contract. E.g. Active, Expired, Pending Renewal, "
        "Terminated. Used to flag off-contract spend.",
    "Contract Description":
        "Free-text description of the contract scope. E.g. 'IT Services MSA 2024-2026'. "
        "Helps map invoices to the correct contract.",
    "Spend Classification Level 1":
        "Top-level spend taxonomy category. E.g. 'Direct', 'Indirect', 'Services', 'Capex'. "
        "Broadest bucketing for spend cube.",
    "Spend Classification Level 2":
        "Second-level spend taxonomy. E.g. 'Raw Materials', 'MRO', 'Professional Services'. "
        "Narrows L1 into manageable sub-categories.",
    "Spend Classification Level 3":
        "Third-level spend taxonomy. E.g. 'Steel', 'Lubricants', 'Legal Services'. "
        "Operationally actionable category level for sourcing.",
    "Spend Classification Level 4":
        "Most granular spend taxonomy level. E.g. 'Hot Rolled Coil', 'Hydraulic Oil 46'. "
        "Used for specification rationalization and price benchmarking.",
    "Procurement Contract Owner":
        "Name or ID of the procurement professional responsible for managing the contract. "
        "E.g. 'Jane Smith', 'Category Manager - IT'. Used for accountability reporting.",
    "Cost Center Code":
        "Accounting cost center code (e.g. SAP KOSTL). Alphanumeric, e.g. CC1001, 4100. "
        "Defines which internal department bears the cost.",
    "Cost Center Description":
        "Human-readable name of the cost center. E.g. 'Finance - AP', 'R&D - Lab 3'. "
        "Used alongside Cost Center Code for financial reporting.",
    "GL Account":
        "General Ledger account code (e.g. SAP SAKNR/HKONT). Numeric, e.g. 400000, 613100. "
        "Classifies the type of expense (capex, opex, materials, services).",
    "GL Account Description":
        "Text label of the GL account. E.g. 'Raw Material Consumption', 'Travel & Entertainment'. "
        "Used for finance-to-procurement spend alignment.",
    "GL Account Hierarchy Level 1":
        "Top node of the GL account hierarchy tree. Groups accounts into broad P&L or BS categories. "
        "E.g. 'Operating Expenses', 'Cost of Goods Sold'.",
    "GL Account Hierarchy Level 2":
        "Second node of the GL hierarchy. Narrows the L1 grouping. "
        "E.g. 'Selling & Distribution', 'Manufacturing Overhead'.",
    "Currency Conversion rate":
        "FX rate used to convert local currency to reporting currency at the time of posting. "
        "Numeric decimal, e.g. 1.0823, 82.5. Critical for multi-currency reporting accuracy.",
    "Data Source System":
        "Name or code of the ERP or source system the record originated from. "
        "E.g. SAP ECC, SAP S/4HANA, Oracle R12, Coupa, Ariba. Used for data lineage.",
    "Transaction ID":
        "Unique identifier for the financial posting or document in the source ERP. "
        "E.g. FI document number, journal entry ID. Used for audit trail and deduplication.",
}

VIEW_CATEGORIES: dict[str, list[str]] = {
    "Contract": ["Contract Status", "Dynamic Spend View"],
    "Spend": [
        "FY20 Spend Overview",
        "Spend Distribution Summary",
        "Spend Profile - Monthly",
        "Spend type analysis",
        "Size of prize",
    ],
    "Supplier": [
        "Low Cost Country (LCC) Sourcing",
        "Mega Supplier Sourcing",
        "Supplier rationalization",
        "Spend by supplier country",
        "Peer to peer/ Industry benchmarking analysis",
        "Dynamic Category Savings",
    ],
    "Price": [
        "Price Rationalization",
        "Savings Potential - Price rationalization",
        "Specification rationalization",
        "Material wise deep dive ",
    ],
    "Compliance": [
        "Maverick Spend",
        "Transaction Intensity",
        "Centralized buying across plants/BUs",
    ],
    "Finance": ["Payment terms rationalization", "Supplier spend distribution by payment terms"],
}

VIEW_REQUIREMENTS: dict[str, list[str]] = {
    "Contract Status": [
        "Contract End Date",
        "Contract ID",
        "Contract indicator",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Contract Status",
        "Total Amount paid in Reporting Currency",
    ],
    "Dynamic Spend View": [
        "Business Unit",
        "Contract End Date",
        "Contract indicator",
        "Contract Start Date",
        "Invoice Date",
        "Invoice Number",
        "Invoice PO Number",
        "Vendor Name",
        "Contract Status",
        "Total Amount paid in Reporting Currency",
    ],
    "FY20 Spend Overview": [
        "Business Unit",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Low Cost Country (LCC) Sourcing": [
        "Invoice Date",
        "Invoice Number",
        "Vendor Country",
        "Total Amount paid in Reporting Currency",
    ],
    "Material wise deep dive ": [
        "Contract End Date",
        "Contract ID",
        "Invoice Date",
        "Invoice Number",
        "PO Material Description",
        "PO Material Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Maverick Spend": [
        "Contract ID",
        "Contract indicator",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
    ],
    "Mega Supplier Sourcing ": [
        "Invoice Date",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Payment terms rationalization ": [
        "Invoice Date",
        "Invoice Number",
        "Payment Terms",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Peer to peer/ Industry benchmarking analysis ": [
        "Invoice Date",
        "Total Amount paid in Reporting Currency",
    ],
    "Price Rationalization ": [
        "Business Unit",
        "Contract ID",
        "Contract indicator",
        "Invoice Date",
        "Invoice Line Number Quantity",
        "Invoice Number",
        "PO Material Description",
        "PO Material Number",
        "Price per UOM",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Savings Potential - Price rationalization": [
        "Contract ID",
        "Contract indicator",
        "Invoice Date",
        "Invoice Line Number Quantity",
        "Invoice Number",
        "PO Material Description",
        "PO Material Number",
        "Price per UOM",
        "Total Amount paid in Local Currency",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Size of prize": ["Invoice Date", "Total Amount paid in Reporting Currency"],
    "Specification rationalization ": [
        "Business Unit",
        "Contract indicator",
        "Invoice Date",
        "Invoice Line Number Quantity",
        "Invoice Number",
        "PO Material Description",
        "PO Material Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Spend by supplier country": [
        "Invoice Date",
        "Invoice Number",
        "Vendor Country",
        "Total Amount paid in Reporting Currency",
    ],
    "Spend Distribution Summary": [
        "Business Unit",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Plant Country",
        "Total Amount paid in Reporting Currency",
    ],
    "Spend Profile - Monthly ": [
        "Business Unit",
        "Contract ID",
        "Contract indicator",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Spend type analysis ": [
        "Business Unit",
        "Contract indicator",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Supplier rationalization": [
        "Invoice Date",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Supplier spend distribution by payment terms": [
        "Invoice Date",
        "Payment Terms",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Transaction Intensity": [
        "Business Unit",
        "Contract indicator",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Centralized buying across plants/BUs": [
        "Business Unit",
        "Invoice Date",
        "Invoice Number",
        "Vendor Name",
        "Total Amount paid in Reporting Currency",
    ],
    "Dynamic Category Savings": ["Invoice Date", "Total Amount paid in Reporting Currency"],
}
