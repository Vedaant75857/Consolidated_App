# Active Plan — Spend Quality Assessment Excel Export

Status: planned; application code unchanged.
Date: 2026-07-31

## Objective

Add one export action to Module 3 Step 4 (Spend Quality Assessment) that downloads
one valid `.xlsx` workbook containing every cut represented by the current
Executive Summary assessment, with one worksheet per cut. The planned scope is
the canonical `ExecutiveSummaryResult`, not the separately triggered
Not Procurable, CAPEX/OPEX, or Intercompany workflows.

## Repository evidence and affected areas

- `frontend/module3/src/components/spend_quality_assessment/DataQualityStep.tsx`
  owns the Step 4 tabs and knows when the Executive Summary has loaded.
- `frontend/module3/src/components/spend_quality_assessment/ExecutiveSummary.tsx`
  owns the authoritative in-memory result and renders the four analytical cuts:
  column fill rate, spend bifurcation, monthly/date pivot, and Pareto cuts.
- `frontend/module3/src/api/client.ts` defines the Executive Summary DTOs and
  existing blob-based CSV export pattern.
- `backend/module3/routes/views_routes.py` computes and caches the completed
  assessment as `_meta["executive_summary"]`; mapping confirmation already
  invalidates that cache.
- `backend/module3/services/spend_quality_assessment/data_quality.py` owns the
  calculation functions and the current result schema.
- `backend/module3/routes/export_routes.py` owns the existing format-specific
  export route. `openpyxl` is already present in
  `backend/module3/requirements.txt`, so no dependency change is planned.

## Contract and workbook design

Use:

```text
POST /api/export/xlsx/spend-quality-assessment
Request: { "sessionId": "<non-empty string>" }
Success: raw XLSX bytes
```

Success headers should include the XLSX MIME type,
`Content-Disposition: attachment; filename="spend-quality-assessment.xlsx"`,
`Cache-Control: no-store`, and `X-Content-Type-Options: nosniff`.
Failures should be JSON `{ error, code }` with stable statuses/codes for invalid
requests, unknown sessions, an assessment that is not ready, and generation
failures.

Build from a lock-protected snapshot of the current completed
`_meta["executive_summary"]`. Release the session lock before workbook creation;
do not rerun SQL or AI during export. Serialize only the public export fields,
not internal description samples or other implementation-only payload members.

Create these stable sheets, retaining a sheet even when its cut is infeasible:

1. `Executive Summary` — total rows, key-point/summary rows, date-source
   provenance, and warnings.
2. `Fill Rate Summary` — original/display column, source column, order, fill
   rate, and spend coverage.
3. `Spend Bifurcation` — positive spend and percentage, negative spend and
   percentage, and net spend.
4. `Monthly Spend Pivot` — month rows, returned year columns, annual totals,
   and date fallback provenance.
5. `Pareto Cuts` — every returned threshold and the total spend, transaction,
   unique transaction, and supplier metrics.

Numeric values must remain numeric; null/unavailable values remain distinguishable.
Sanitize untrusted cell text for spreadsheet formula injection and enforce Excel
sheet, cell-text, row, and column limits. Existing dashboard CSV export remains
unchanged.

## Implementation phases

### 1. Backend workbook service and route

- Add a focused workbook builder under
  `backend/module3/services/spend_quality_assessment/` that maps the explicit
  current DTO sections to the five sheets, applies stable headers/order/number
  formats, writes infeasible messages, and returns an in-memory byte stream.
- Extend `backend/module3/routes/export_routes.py` with the XLSX endpoint,
  shared-session-lock snapshotting, current-cache validation, stable JSON errors,
  and response headers.
- Keep the export independent of `apiKey`; a completed cached assessment is the
  input. Mapping cache invalidation remains the stale-data boundary.

### 2. Frontend export action and binary client contract

- Add a dedicated binary export helper in
  `frontend/module3/src/api/client.ts` that sends only `{ sessionId }`, parses
  JSON error bodies into the existing `ApiClientError` shape, and returns a Blob
  plus the server filename when available.
- Add an `Export all cuts (Excel)` action to
  `DataQualityStep.tsx`, available after the Executive Summary is loaded and
  outside the hidden panel so it remains usable when the user switches tabs.
- Add pending/disabled state, duplicate-click protection, accessible status/error
  feedback, and safe object-URL download cleanup. Export contents must not depend
  on active tab, panel expansion, or selected Pareto threshold.
- Reuse the existing download behavior; extract the small `downloadBlob` helper
  only if needed to avoid duplicating object-URL lifecycle code.

### 3. Focused validation

- Add backend workbook/service and route coverage in a focused
  `backend/module3/tests/test_spend_quality_export.py`, including sheet order and
  headers, numeric/negative/zero/null values, totals, date fallback and warnings,
  infeasible cuts, formula-injection protection, omission of internal samples,
  stale/missing cache, session errors, and response headers.
- Add focused frontend contract coverage for the exact route, one request with
  `{ sessionId }`, Blob handling, filename fallback, loading/error accessibility,
  duplicate prevention, and independence from tab/panel state. Wire it into the
  existing Module 3 test command without changing unrelated behavior.
- Run the narrow backend tests, existing SQA regression tests, frontend contract
  tests, and Module 3 production build. Where available, open the generated bytes
  with `openpyxl` and perform a browser/manual smoke check for download behavior.

## Acceptance criteria

- A loaded assessment downloads one workbook in one user action.
- The workbook always contains the five named sheets, including explanatory
  content for infeasible sections.
- Workbook values match the same completed assessment shown in the UI, including
  all returned Pareto thresholds and authoritative date-source fallback data.
- Export works with collapsed panels and after switching among Step 4 tabs.
- The button prevents duplicate requests, exposes pending/error state, and never
  downloads a partial file on failure.
- Missing/expired sessions and absent/stale assessments return stable JSON errors;
  no AI call or recalculation occurs during export.
- Existing CSV exports and other Step 4 workflows continue to behave as before.

## Scope assumption and remaining product risk

The codebase uses “cuts” explicitly for the Pareto 80/85/90/95 thresholds, while
the other visible panels are sections of the Executive Summary response. This
plan treats “all cuts/data cuts” as the complete Executive Summary result and
keeps the three separately parameterized Step 4 tabs out of the workbook. If the
product request instead means those tabs too, that is a separate scope expansion:
their user-selected columns/keywords/client name and on-demand results need a
persisted/export contract before they can be included faithfully.
