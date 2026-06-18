# Durable Decisions

## 2026-06-18 - SummarizationModule Column Mapping Grouping

For the Step 3 map-column UI, visual grouping of standard target fields should be owned by the frontend and keyed by stable `fieldKey` values.

Do not add backend `group` or `category` metadata for this request unless the grouping becomes shared domain taxonomy used by multiple clients, exports, downstream APIs, saved preferences, or backend-owned ordering.

## 2026-06-18 - Spend Quality Assessment Date Fallback

Spend Quality Assessment date-dependent calculations should use a single backend-resolved date source in this order: `invoice_date`, `invoice_due_date`, `payment_date`, `goods_receipt_date`, `po_document_date`.

Fallback is allowed only when higher-priority candidates are unmapped or have zero valid parsed date rows. `contract_start_date` and `contract_end_date` are excluded because they represent contract lifecycle dates, not transaction timing.

The backend executive-summary response should be authoritative for final date-source provenance and fallback warnings; Step 3 may show only provisional client-side guidance.

## 2026-06-18 - Browser-Session API Key Handoff

The suite-wide user API key should be stored only in browser `sessionStorage` under the canonical key `procip_api_key`.

Because the landing page and modules can run on separate origins/ports, module launches and cross-module transfers should use a URL fragment handoff (`#apiKey=...`) when a key must cross origins. Modules should read the fragment once, store the key in their own session storage, then immediately strip secret URL data with `history.replaceState`.

Do not introduce new `apiKey` query-string handoffs or server-side key persistence. Existing backend request bodies using `{ apiKey }` remain the compatibility contract.
