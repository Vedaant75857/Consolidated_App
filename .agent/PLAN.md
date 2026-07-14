# Active Plan - Repo-Level Python Venv Consolidation

Mode: execution completed for consolidating Python virtual environments across the repository. Prior SummarizationModule playground context remains below as historical project context.

## 2026-07-14 Planning Slice - Repo-Level Python Venv Consolidation

User goal: replace multiple module-local Python virtual environments with one overarching repo-level venv for `C:\Users\75857\Downloads\Consolidated_App`, without editing application code in this planning turn.

### Current Venv Inventory

1. `DataConsolidationAppV7/backend/.venv`
   - Python 3.13.x.
   - Has `pytest` available.
2. `ProcIP_Module2-main/backend/.venv`
   - Python 3.13.x.
   - Does not have `pytest` available.
3. `SummarizationModule/.venv`
   - Python 3.13.x.
   - Does not have `pytest` available.
4. `SummarizationModule/backend/.venv`
   - Stale/broken.
   - `pyvenv.cfg` points at missing `C:\Users\78464\AppData\Local\Programs\Python\Python310\python.exe`.

Recommended target: create and standardize on root `.venv` at `Consolidated_App/.venv`. Root `.gitignore` already ignores `.venv/`.

### Affected Areas For Execution

1. `setup.ps1`
   - Currently creates `.venv` inside each backend folder and installs each backend's local `requirements.txt`.
   - Should create root `.venv` once, upgrade pip once, and install unified backend/dev dependency manifests.
2. Root `package.json`
   - `dev:stitcher-be` currently uses `DataConsolidationAppV7/backend/.venv`.
   - `dev:normalizer-be` currently uses `ProcIP_Module2-main/backend/.venv`.
   - `dev:summarizer-be` currently uses `SummarizationModule/.venv`.
   - All backend scripts should call root `.venv/Scripts/python`.
3. `DataConsolidationAppV7/package.json`
   - `dev:mod1` currently uses `backend/.venv/Scripts/python`.
   - Should call the repo root interpreter from within the module path.
4. Backend dependency manifests:
   - `DataConsolidationAppV7/backend/requirements.txt`
   - `ProcIP_Module2-main/backend/requirements.txt`
   - `SummarizationModule/backend/requirements.txt`
   - These should be reconciled into one root backend runtime manifest, plus a root dev/test manifest containing `pytest`.
5. Documentation/setup notes:
   - `ProcIP_Module2-main/README.md` still documents plain `pip install -r requirements.txt`.
   - Living context command examples that point to nested venvs should be updated after implementation.

### Dependency Resolution Notes

Do not blindly install all three backend `requirements.txt` files sequentially. Current constraints conflict:

1. Flask:
   - DataConsolidation pins `Flask==3.1.3`.
   - ProcIP pins `flask==3.0.0`.
   - Summarization requires `Flask>=3.1,<4`.
2. Flask-CORS:
   - DataConsolidation pins `flask-cors==6.0.2`.
   - Summarization requires `flask-cors>=5.0,<6`.
   - ProcIP allows `flask-cors>=4.0.0`.
3. Werkzeug/python-dotenv:
   - DataConsolidation pins newer versions.
   - ProcIP pins older versions.

Proposed first target for execution: Python 3.13, `Flask>=3.1,<4`, `Werkzeug>=3.1,<4`, explicitly resolved `flask-cors`, and all unique runtime packages used by the three backends: `duckdb`, `pandas`, `requests`, `python-dotenv`, `portkey-ai`, `pydantic`, `openpyxl`, `python-calamine`, `reportlab`, `Pillow`, `httpx`, and `langdetect`.

### Execution Checklist

1. [x] Add root backend dependency manifests: `requirements-backend.txt` and `requirements-dev.txt`.
2. [x] Resolve dependency conflicts explicitly: use `Flask>=3.1,<4`, `Werkzeug>=3.1,<4`, and `flask-cors>=6,<7`, plus the union of runtime packages.
3. [x] Create root `.venv` with Python 3.13.
4. [x] Install root runtime and dev dependencies into root `.venv`.
5. [x] Update `setup.ps1` to manage only root `.venv`, with Python 3.13 launcher/per-user-install fallback and no Python 3.12 fallback.
6. [x] Update backend dev scripts in root `package.json` and `DataConsolidationAppV7/package.json`.
7. [x] Update ProcIP setup documentation for root `.venv` use.
8. [x] Run backend validation from root `.venv`:
   - `cd SummarizationModule/backend; ..\..\.venv\Scripts\python.exe -B -m pytest tests -q`
   - `cd DataConsolidationAppV7/backend/module-1; ..\..\..\.venv\Scripts\python.exe -B -m pytest tests -q`
   - `cd ProcIP_Module2-main/backend; ..\..\.venv\Scripts\python.exe -B -m pytest tests -q`
   - `.\.venv\Scripts\python.exe -m pip check`
   - Results: Summarization 72 passed; ProcIP 1 passed; Module 1 52 passed and 2 existing business-rule test failures in `tests/test_value_distribution.py`.
   - `pip check`: passed.
9. [x] Run a static dev-script smoke check after script updates; all backend scripts point to root `.venv` and no nested-venv script references remain.
10. [x] After explicit user approval, remove obsolete nested venv folders: `DataConsolidationAppV7/backend/.venv`, `ProcIP_Module2-main/backend/.venv`, `SummarizationModule/.venv`, and `SummarizationModule/backend/.venv`.

### Risks And Open Questions

1. The new root environment resolves and imports all planned wheels, and `pip check` is clean; Module 1 has two unrelated currency-column test failures that need separate triage.
2. ProcIP passed its focused backend suite after lifting its older Flask/Werkzeug pins.
3. The four obsolete nested venv folders were removed after explicit user approval; root `.venv` remains the sole supported environment.
4. Module-local `requirements.txt` files remain as historical/standalone manifests; the root manifests are the consolidated setup path.

### Visible Agents Created In This Planning Slice

1. `backend_architect` (`Einstein`): inspected backend environment architecture, venv locations, dependency conflicts, script/docs impact, and backend risks.
2. `test_engineer` (`McClintock`): identified existing test commands, dependency manifests, validation commands, pytest gaps, and checks for stale nested venv references.

### Agents Considered But Skipped

1. `frontend_architect`: skipped because no frontend source behavior changes are planned; frontend impact is limited to npm script commands.
2. `api_contract_reviewer`: skipped because no frontend/backend API request or response contracts change.
3. `performance_reviewer`: skipped because venv consolidation does not change runtime code paths.

## Historical Context - SummarizationModule Playground Feature Parity

## 2026-06-19 Planning Slice - Module 1 Playground UI Parity Polish

User goal: keep the SummarizationModule theme and overall layout, but make the playground controls match Module 1 where users create pivots, filter/sort columns, use filter buttons, and read/change column types. Do not change calculated fields UI in this slice.

### Affected Areas

1. `SummarizationModule/frontend/src/components/playground/ExcelPreviewOverlay.tsx`
   - `toggleSort`
   - header render loop for sort/filter/type/action controls
   - `ColumnFilterPopover`
   - `ColumnMenu`
   - `PivotDialog` / `PivotZone`
   - preserve `CalculatedColumnDialog`
2. `SummarizationModule/frontend/test/playground-overlay.contract.test.mjs`
   - extend source-contract assertions for the parity controls and guardrails.
3. Likely unchanged:
   - `SummarizationModule/frontend/src/api/client.ts`
   - `SummarizationModule/frontend/src/types/excelPreview.ts`
   - backend preview routes/services.

### Implementation Plan For Next Execution

1. [x] Header/filter/sort/type parity:
   - Replace the current split header affordance with a Module 1-like compact header control pattern.
   - Header click should cycle sort state and show active sort clearly.
   - Filter affordance should look like Module 1's header-menu/filter control: compact inactive state, red active state, `aria-pressed`, hover/focus states.
   - Column type should be displayed as a compact type icon/badge instead of raw uppercase subtext under every header.
   - Keep horizontal column virtualization and stable dimensions so dense headers do not overlap.
2. [x] Filter popover parity:
   - Keep stable-key requests, especially `columnKey` and `excludeColumnFilter(filters, column.key)`.
   - Match Module 1's compact value picker behavior: search values, Select All, Clear, blanks, selected count/distinct count, OK/Cancel-style apply behavior.
   - Preserve include/exclude support already present in SummarizationModule unless it conflicts visually; if retained, style it as a compact segmented control.
3. [x] Pivot creation parity:
   - Replace checkbox-only pivot selection with Module 1's Available Fields plus Row Fields, Column Fields, and Values zones.
   - Add drag/drop placement where practical, with click/keyboard alternatives so drag is not the only path.
   - Show display names and type badges to users, but submit stable-key payloads:
     - `rowFields: columnKey[]`
     - `columnFields: columnKey[]`
     - `valueFields: [{ columnKey, aggregation }]`
   - Use numeric-aware default aggregations and disable/label number-only aggregations for non-numeric fields.
4. [x] Preserve non-goals and contracts:
   - Do not alter calculated fields UI or calculated-column payloads.
   - Do not add Module 1 Apply UI, `/api/preview/apply`, or apply target kinds.
   - Do not port `@glideapps/glide-data-grid`; keep SummarizationModule's target-local table and virtualization.
   - Do not change backend contracts unless implementation discovers a real frontend/backend mismatch.
5. [x] Validation:
   - Update `SummarizationModule/frontend/test/playground-overlay.contract.test.mjs` for:
     - pivot Available Fields / Rows / Columns / Values zones
     - stable-key pivot payloads and no Module 1 `field` payloads
     - filter active/inactive button styling path and `aria-pressed`
     - blanks, include/exclude, clear, selected count in filter popover
     - clear-sort/status behavior
     - compact type badge/helper
     - calculated dialog remains present and unchanged
     - no Apply UI/endpoint/targets.
   - Run:
     - `cd SummarizationModule/frontend; npm test`
     - `cd SummarizationModule/frontend; npm run build`
   - Run backend guardrails if contracts are touched or as final confidence:
     - `cd SummarizationModule/backend; python -B -m pytest tests/test_playground_contract.py tests/test_playground_preview_routes.py tests/test_playground_operations.py tests/test_playground_column_metadata.py -q`
   - Manual/browser check remains required for true UI parity because current frontend tests are source-contract tests, not rendered component tests.

### 2026-06-19 Execution Result

- Header controls now use compact sort/type/filter/action affordances while preserving horizontal column virtualization and stable column widths.
- Filter popover keeps stable-key `columnKey` requests and `excludeColumnFilter(filters, column.key)`, with search, Select All/Select loaded, Clear, blanks, selected/distinct count, include/exclude segmented controls, capped-result warning, and Cancel/OK staging.
- Pivot creation now uses Available Fields plus Row Fields, Column Fields, and Values zones with drag/drop and click alternatives; payloads remain stable-key based and the UI allows backend-supported values-only aggregate pivots.
- Calculated-column UI/payloads, backend contracts, and no-Apply boundaries were not changed.
- Frontend review follow-ups fixed the misleading capped-value `Select All` label and aligned pivot submit enablement with backend validation.
- Validation:
  - `npm test` from `SummarizationModule/frontend`: passed, `7 passed`.
  - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Dl3x98Zc.js` at `1,011.70 kB`.
  - `python -B -m pytest tests/test_playground_contract.py tests/test_playground_preview_routes.py tests/test_playground_operations.py -q` from `SummarizationModule/backend`: passed, `31 passed, 6 subtests passed`.
  - Runtime guardrail grep found no `/api/preview/apply`, no Apply target leakage, and no `motion/react` imports in runtime source paths.
  - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.

### Visible Agents Created In This Execution Slice

1. `frontend_implementer` (`James`): implemented the header/filter/pivot UI parity polish and initial frontend validation.
2. `contract_sync_agent` (`Curie`): verified stable-key frontend/backend preview contracts and no Apply leakage.
3. `frontend_reviewer` (`Turing`): reviewed UI/accessibility behavior and identified capped-value and pivot-submit follow-ups.
4. `test_engineer` (`Linnaeus`): ran frontend validation and runtime guardrail checks.

### Open Product Boundary

The current SummarizationModule overlay uses a left table sidebar, while Module 1 uses horizontal tabs. Because the user said the theme and layouts are liked, keep the left sidebar unless the next execution request explicitly asks to make table navigation match Module 1 too.

### Visible Agents Created In This Planning Slice

1. `product_planner` (`Russell`): clarified acceptance criteria, non-goals, edge cases, and the layout/theme boundary.
2. `frontend_architect` (`Plato`): mapped Module 1 pivot/filter/sort/type UI patterns to the SummarizationModule frontend implementation areas.
3. `test_engineer` (`Raman`): planned frontend source-contract updates, backend guardrail checks, and manual/browser validation needs.

Target: recreate the DataConsolidationAppV7 preview/playground feature set inside `SummarizationModule` as target-local files, including a Module 1-style top icon entry point.

## Remaining-Parity Execution Plan

Implement the remaining playground features in phases. Do not start with the rich UI before the backend identity and mutation model are safe.

### Phase 1 - Column Metadata And Contract Cleanup

1. [x] Persist per-table column metadata during upload and header-row changes:
   - stable `columnKey`
   - physical SQL column name
   - exact original `displayName`
   - `dataType`
   - `order`
   - `hidden`
2. [x] Backfill metadata for existing sessions from `raw_table` when possible and from current `data_table` names as a fallback.
3. [x] Update `/api/preview/state` to return metadata-backed `columns[].displayName` and `columns[].dataType`.
4. [x] Keep one canonical response shape: `columns[].dataType`; do not add a parallel `columnTypes` response map.
5. [x] Require persistent hidden row identity for every preview table, including pivot outputs. Do not rely on `offset + idx` except as a temporary fallback in legacy data.
6. [x] Normalize request contracts to stable-key DTOs only:
   - filters/sort use `columnKey`
   - rows use `__row_id`
   - row values use `values[columnKey]`
   - operations use `columnKey`, `columnKeys`, and `displayName`
7. [x] Remove or normalize non-canonical filter aliases before service logic. Canonical filter ops are `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `startswith`, `endswith`, `is_null`, `is_not_null`, `in`, `not_in`.

Current Phase 1 backend slice completed for metadata-backed preview state:

- Added `_column_metadata` with stable `COL_n` keys, physical SQL names, exact display labels, type/order/hidden flags.
- New upload/finalization/header-row paths persist metadata where this slice owns table creation.
- Legacy sessions backfill metadata from `raw_table` first-row headers when present and data-table column names as fallback.
- `/api/preview/state` returns metadata-backed public columns and row values keyed by stable keys, while preserving `RECORD_ID` only as hidden row identity exposed through `__row_id`.
- Filters/sort are stable-key only; supported UI aliases normalize at the route boundary before service logic. Display-name-only and physical-name refs are rejected.
- Backend review follow-up: `/api/set-header-row` now locks the shared session DB and rechecks session existence inside the lock; legacy metadata backfill filters uploaded `RECORD_ID` headers and falls back to current physical columns when raw first-row headers no longer match the data-table shape.

Phase 1 follow-ups now covered:

- Pivot outputs now get hidden `RECORD_ID` identity and playground-side stable column metadata.
- Operation DTOs are stable-key only at the route boundary; display-name and physical-name refs are rejected.

### Phase 2 - Backend Playground Boundary And Read APIs

1. [x] Add a copy-on-write playground working table model. First mutation clones the source `data_table` into a `pg__*` table so playground edits do not silently mutate Step 3+ pipeline inputs.
2. [x] Track `tableVersion`, dirty state, undo/redo stack metadata, source table key, and playground table name per table.
3. [x] Implement `POST /api/preview/column-values`:
   - filters exclude the queried column's own filter
   - supports dropdown search
   - returns `{ columnKey, values, hasBlanks, totalDistinct }`
   - implements `includeBlanks` semantics for `in` / `not_in`
   - caps returned values at 500.
4. [x] Add versioned cache keys for expensive reads: `{ tableVersion, tableKey, search, filters, sort }` and `{ tableVersion, columnKey, filtersWithoutSelf, search }`.
5. [x] Keep backend preview reads paginated with the hard max 1000; the overlay now presents one virtualized giant scroll and fetches 1000-row chunks behind the scenes. Stale/approximate counts for broad large-table searches remain a Phase 7 performance follow-up.

### Phase 3 - Backend Mutating Operations

1. [x] Implement `POST /api/preview/operation` with key-based params:
   - `cell_edit`: `{ rowId, columnKey, value }`
   - `rows_delete`: `{ rowIds }`
   - `column_rename`: `{ columnKey, displayName }`
   - `column_delete`: `{ columnKey }`
   - `column_reorder`: `{ columnKeys }`
   - `column_change_type`: `{ columnKey, newType }`
   - `calculated_column`: `{ name, expression, dataType }`
   - `pivot`: `{ rowFields, columnFields, valueFields }`
2. [x] Prefer metadata-only operations where possible:
   - rename changes `displayName`
   - reorder changes metadata order
   - type override changes metadata and sort/cast behavior unless a real cast is required.
3. [x] Validate operation-specific params strictly and reject display-name-only column refs.
4. [x] Calculated column expressions must be allowlisted, length-capped, and resolve references only through metadata.
5. [x] Row delete should cap request size, for example 5,000 row IDs, or reject with a clear validation error.

### Phase 4 - Pivot And Inventory Refresh

1. [x] Pivot output is playground-only and must not be registered as a normal Step 2/mapping source table.
2. [x] Register pivot output in playground metadata with stable keys/display names/data types/order and persistent row identity.
3. [x] Add hard pivot caps before CTAS:
   - generated columns: 100-200
   - row fields: max 5
   - value fields: max 5
4. [x] Add row-cardinality/output-shape estimation and reject oversized pivots before materializing high-cardinality row fields.
5. [x] Implement `POST /api/preview/refresh-inventory` returning `{ ok, fileInventory, columns, previews }` for overlay state only.
6. [x] Do not add `/api/preview/apply` or Apply-style state patches.

### Phase 5 - Undo/Redo

1. [x] Choose and implement one undo/redo model before enabling UI controls:
   - operation inverses for small operations, or
   - bounded copy-on-write snapshots for table/schema operations.
2. [x] Implemented model:
   - bounded copy-on-write snapshots for enabled mutating operations
   - snapshot depth capped at 5
   - review follow-up drops consumed undo/redo snapshots after restore
   - operation-specific inverses remain a Phase 7 performance improvement.
3. [x] Expose `undoDepth`, `redoDepth`, and `dirty` in every state/operation response.
4. [x] `POST /api/preview/undo` and `/redo` return updated preview state plus `{ ok: true }`.
5. [x] If an operation cannot be safely undone, keep its UI disabled or return a validation error before exposing it as enabled.

### Phase 6 - Frontend Full Overlay Parity

1. [x] Do not add `@glideapps/glide-data-grid` in this slice because it is not already installed in Module 3; use a target-local editable table instead.
2. [x] Replace the current read-only table with a target-local editable playground table.
3. [x] Add frontend client wrappers for the planned backend routes:
   - `previewColumnValues`
   - `previewOperation`
   - `previewUndo`
   - `previewRedo`
   - `previewRefreshInventory`
4. [x] Add per-column filter dropdowns:
   - lazy-load values on open
   - debounce search
   - multi-select values
   - blank handling
   - filter chips and clear-all.
5. [x] Add cell editing, row selection/delete, column menu, column rename/delete/reorder/type override.
6. [x] Add calculated-column and pivot dialogs adapted from Module 1, but using `{ key, displayName }` and stable `columnKey` payloads.
7. [x] Add undo/redo toolbar controls driven by backend `undoDepth` / `redoDepth`.
8. [x] Add `onInventoryRefresh` from `App.tsx` to patch overlay inventory after pivot/shape changes.
9. [x] Preserve existing top icon, Step 2 inline preview, and no Apply UI.
10. [x] Accessibility: labelled icon buttons, dialog/popover Escape handling, outside-click close, and practical focus handling.

### Phase 7 - Performance And Validation Hardening

1. [x] Add synthetic large-table benchmarks or focused tests for preview state, column-values, pivot, edit/delete, undo/redo, and virtualized grid rendering.
   - Backend focused regressions now cover versioned preview-state caching, column-values distinct-query caching, tableVersion cache invalidation after mutation, and pivot high-cardinality preflight.
   - Preview-service focused tests now cover paginated state reads, hidden row identity, original headers, stable-key search/filter/sort, typed sort, and large-table search guardrails.
   - Performance validation covered synthetic 100k x 25, 1M x 25, and read-only 100k x 100/250 paths; frontend source-contract tests continue to guard virtualized grid wiring.
2. [ ] Validate at 100k, 1M, and optionally 5M rows with 25/100/250 columns.
   - Completed feasible synthetic validation for 100k x 25, 100k x 100, 100k x 250, and 1M x 25.
   - Deferred: 1M x 100/250, all 5M shapes, and browser-level virtualized rendering validation until projection/search and snapshot byte/disk guardrails are expanded.
3. [x] Cap `in` / `not_in` filter lists at 500 values.
4. [x] Debounce global search around 300 ms is implemented; backend preview-state reads now enforce minimum search length and searchable-column caps for very large tables.
5. [x] Use typed sort expressions from metadata when type is known.
6. [x] Verify no repeated identical distinct-value SQL on dropdown reopen/search.
7. [x] Harden the frontend giant-scroll grid:
   - row rendering is one continuous virtualized scroll backed by 1000-row chunks
   - row chunk cache is bounded to a moving viewport window
   - stale preview/filter requests are abortable and generation-guarded
   - very wide tables use horizontal column virtualization
   - filter/column menus position from the clicked trigger instead of a fixed overlay corner.

## User Constraints

1. Recreate/copy/adapt files into `SummarizationModule`; do not link or import from `DataConsolidationAppV7`.
2. Module 3 should get the features of the Module 1 preview/playground, not only a read-only data preview.
3. The playground should show from the top as an icon, similar to the `Raw Data Preview` `Table2` icon in Module 1 Data Stitcher.
4. Remove the Apply flow from Module 3; no Apply button, apply endpoint, or apply-to-pipeline behavior.
5. Uploaded/finalized data should still preserve original uploaded column-header display.
6. Previous planning turn was read-only; approved execution has completed the top-icon/read-state slice and the Phase 1 metadata-backed preview-state slice.

## Source Pattern To Mirror

Module 1 Data Stitcher:

1. Shows a top-right `Table2` icon when `inventory.length > 0`.
2. Icon title is `Raw Data Preview`.
3. Click calls `openRawDataPreview`.
4. `openRawDataPreview` lazy-fetches missing previews and opens `ExcelPreviewOverlay`.
5. `ExcelPreviewOverlay` is a full-screen overlay with:
   - table tabs
   - search
   - filters and column value dropdowns
   - sorting
   - paginated grid
   - cell editing
   - row delete
   - column rename/delete/reorder/type change
   - calculated columns
   - pivot creation
   - undo/redo controls
   - apply-to-pipeline flow, which Module 3 should omit
6. Source API clients are in `DataConsolidationAppV7/frontend/src/components/module-1/services/stitchingApi.ts`.
7. Source backend contracts are the `/api/preview/*` routes in `DataConsolidationAppV7/backend/module-1/routes/preview_routes.py`.

## Current Target State

`SummarizationModule` currently has:

1. Upload and lazy first-page preview through:
   - `SummarizationModule/backend/routes/upload_routes.py`
   - `SummarizationModule/backend/services/upload/file_loader.py`
   - `SummarizationModule/frontend/src/api/client.ts`
   - `SummarizationModule/frontend/src/components/data_preview/DataLoading.tsx`
2. Step 2 inline preview already supports table delete, header-row edit, and row delete, but not the richer Module 1 overlay workflow.
3. Top-right app chrome currently has theme toggle only.
4. `/api/get-preview` returns `{ columns, rows }` only and cannot support playground feature parity.
5. Upload uses normalized physical SQL names, while `_column_metadata` now preserves exact uploaded/header-row display labels for preview state.
6. Several upload preview routes access cached DuckDB without the shared session lock; new playground routes must not repeat that risk.

## Recommended Implementation Scope

1. [x] Keep Step 2 as the inline upload/data-preview workflow so users can proceed to mapping.
2. [x] Add a Module 1-style top-right `Table2` icon in `SummarizationModule/frontend/src/App.tsx`.
   - Show it when `sessionId && inventory.length > 0`.
   - Place it beside the theme toggle.
   - Title/label: `Raw Data Preview` or `Playground`.
3. [x] Add `showDataPreview` state and an `openRawDataPreview` handler in `SummarizationModule/frontend/src/App.tsx`.
4. [x] Create target-local frontend playground files:
   - `SummarizationModule/frontend/src/components/playground/ExcelPreviewOverlay.tsx`
   - `SummarizationModule/frontend/src/types/excelPreview.ts`
5. [x] Adapt, do not blindly copy, Module 1 `ExcelPreviewOverlay`.
   - Use `framer-motion`, not `motion/react`.
   - Use `SummarizationModule/frontend/src/api/client.ts` for API calls.
   - Use `FileInventoryItem` / Module 3 state names.
   - Remove the Apply button, apply dialog, and Module 1-specific apply targets from the UI.
6. [x] Add typed playground API client calls in `SummarizationModule/frontend/src/api/client.ts`:
   - [x] `previewState`
   - [x] `previewColumnValues`
   - [x] `previewOperation`
   - [x] `previewUndo`
   - [x] `previewRedo`
   - [x] `previewRefreshInventory`
7. [x] Do not add `@glideapps/glide-data-grid` in this slice; current implementation uses a target-local editable table and existing `framer-motion`.
8. [x] Keep existing `/api/get-preview` temporarily for current Step 2 compatibility, but the new top-icon playground should use `/api/preview/state`.
9. [x] Add target-local backend preview/playground files:
   - `SummarizationModule/backend/routes/preview_routes.py`
   - `SummarizationModule/backend/services/playground/service.py`
   - register the blueprint in `SummarizationModule/backend/app.py`
10. [x] Port compatible backend logic from `DataConsolidationAppV7/backend/module-1/preview_operations/service.py` while adapting it to Module 3 stable-key DTOs and no-Apply boundaries.
11. [x] Replace source table lookup/registration with `SummarizationModule` `_table_registry` access.
12. [x] Read/write the finalized uploaded `data_table`; use `raw_table` only where explicitly needed for upload/header provenance.
13. [x] Preserve display-header metadata:
   - stable internal column key
   - original display name
   - data type
   - order
14. [x] Use key/display DTOs instead of relying on display header strings as object keys, because original headers may be duplicate or blank.
15. [x] Hide `RECORD_ID` from visible columns; expose row identity as `__row_id` only.
16. [x] Do not add Apply/Save to pipeline for Module 3. Playground edits should remain playground-scope unless a later execution decision explicitly commits them.

## Feature Set To Port

Port these features into Module 3 playground:

1. [x] Paginated preview state.
2. [x] Global search.
3. [x] Column filters, including blank handling and `in` / `not_in` frontend UI.
4. [x] Type-aware sort through metadata-backed numeric/date/boolean sort expressions.
5. [x] Distinct column values for filter dropdowns via planned `previewColumnValues` client call.
6. [x] Cell edit UI wired to stable-key `cell_edit`.
7. [x] Row delete UI wired to `rows_delete`.
8. [x] Column rename UI wired to `column_rename`.
9. [x] Column delete UI wired to `column_delete`.
10. [x] Column reorder UI wired to `column_reorder`.
11. [x] Column type override UI wired to `column_change_type`.
12. [x] Calculated column dialog wired to `calculated_column`.
13. [x] Pivot table dialog wired to `pivot`.
14. [x] Undo/redo toolbar wired to `previewUndo` / `previewRedo`.
15. [x] Refresh inventory after operations that alter playground table shape or create playground tables.
16. [x] No Apply/Save to Module 3 pipeline.

Do not copy Module 1 source apply target kinds or apply UI into Module 3:

1. `raw`
2. `group`
3. `merge_version`
4. `merge_group`

## Contract Plan

Use `/api/preview/*` in Module 3 for parity with the source frontend pattern.

### `POST /api/preview/state`

Request:

```json
{
  "sessionId": "string",
  "tableKey": "string",
  "offset": 0,
  "limit": 200,
  "search": "optional string",
  "filters": [{ "columnKey": "COL_1", "op": "contains", "value": "abc" }],
  "sort": [{ "columnKey": "COL_1", "dir": "asc" }]
}
```

Response:

```json
{
  "tableKey": "string",
  "columns": [
    { "key": "COL_1", "displayName": "Original Header", "dataType": "TEXT" }
  ],
  "rows": [
    { "__row_id": 1, "values": { "COL_1": "value" } }
  ],
  "totalRows": 123,
  "offset": 0,
  "limit": 200,
  "undoDepth": 0,
  "redoDepth": 0,
  "dirty": false
}
```

### `POST /api/preview/column-values`

Request:

```json
{
  "sessionId": "string",
  "tableKey": "string",
  "columnKey": "COL_1",
  "search": "optional string",
  "filters": [],
  "limit": 500
}
```

Response:

```json
{
  "columnKey": "COL_1",
  "values": ["A", "B"],
  "hasBlanks": true,
  "totalDistinct": 25
}
```

### `POST /api/preview/operation`

Supported operations:

1. `cell_edit`: `{ rowId, columnKey, value }`
2. `column_rename`: `{ columnKey, displayName }`
3. `column_delete`: `{ columnKey }`
4. `column_reorder`: `{ columnKeys }`
5. `column_change_type`: `{ columnKey, newType }`
6. `rows_delete`: `{ rowIds }`
7. `calculated_column`: `{ name, expression, dataType }`
8. `pivot`: `{ rowFields, columnFields, valueFields }`

Response: updated preview state plus `{ ok: true, message?, newTableKey? }`.

### `POST /api/preview/undo` / `POST /api/preview/redo`

Request:

```json
{ "sessionId": "string", "tableKey": "string" }
```

Response: updated preview state plus `{ "ok": true }`.

Important: do not blindly copy source undo behavior. Module 1's service only truly restores some operations. Module 3 should either implement undo/redo for every enabled mutating operation or disable undo for unsupported operations.

### Apply Flow

Do not implement `POST /api/preview/apply` for this Module 3 scope.

Remove the Apply button/dialog from the copied overlay. Module 3 playground operations should behave like Module 1 playground operations inside the overlay, but without committing changes back into the Module 3 workflow through an apply-to-pipeline action.

### `POST /api/preview/refresh-inventory`

Needed when pivot creates a new table or operations alter visible table shape.

Response:

```json
{
  "ok": true,
  "fileInventory": [],
  "columns": [],
  "previews": {}
}
```

## Validation Rules

1. `sessionId` is required and must exist.
2. Recheck `session_exists(sessionId)` inside `get_session_lock(sessionId)` before `get_session_db(sessionId)`.
3. `tableKey` must resolve through `_table_registry`; never accept raw SQL table names.
4. `offset >= 0`; `1 <= limit <= 1000`.
5. `columnKey` must exist in stored metadata.
6. Sort direction must be `asc` or `desc`.
7. Filter op must be from an approved enum.
8. Calculated-column expressions must reject unsafe SQL.
9. Pivot must cap generated columns and reject invalid/empty configs.
10. Duplicate/blank original headers must get stable unique keys while preserving display labels.
11. Error shape should be consistent:

```json
{ "error": "Human readable message", "code": "VALIDATION_ERROR", "details": {} }
```

## Validation Plan

Focused validation refresh for completing playground parity:

1. Phase 0 - Tooling gate:
   - Must pass with system Python until venvs are fixed:
     `cd SummarizationModule\backend; python -B -m pytest tests\test_playground_preview_routes.py tests\test_upload_file_types.py`
   - Current blocker:
     `& ..\.venv\Scripts\python.exe -B -m pytest --version` finds Python 3.13 but reports `No module named pytest`.
   - Secondary blocker:
     `& .\.venv\Scripts\python.exe --version` under `SummarizationModule\backend` points at missing `C:\Users\78464\AppData\Local\Programs\Python\Python310\python.exe`.
2. Phase 1 - Existing read-only contract:
   - Keep `tests\test_playground_preview_routes.py` green for `/api/preview/state`, `_table_registry` table-key resolution, hidden `RECORD_ID`, search/filter/sort, pagination validation, and in-lock session recheck.
3. Phase 2 - Backend service/route parity:
   - Add focused tests for `column-values`, `operation`, `undo`, `redo`, and `refresh-inventory` as those routes are implemented.
   - Use Module 1 `test_preview_service.py` and `test_preview_routes.py` as behavior references, but assert no `/api/preview/apply` route/client/UI exists in Module 3.
4. Phase 3 - Header metadata and stable keys:
   - Add upload/playground fixtures with mixed-case headers, duplicate headers, blank headers, special characters, and normalized DuckDB names.
   - Must assert `columns[].key` is stable/unique, `columns[].displayName` preserves original uploaded labels, filters/sorts/operations use keys, and row `values` are keyed by stable keys.
5. Phase 4 - Operations and undo/redo:
   - Cover cell edit, row delete, column rename/delete/reorder/type change, calculated column, pivot, invalid operation validation, and undo/redo depth/redo clearing.
   - Must either prove undo/redo restores every enabled mutating operation or prove unsupported operations disable undo rather than exposing broken controls.
6. Phase 5 - No Apply/downstream regression:
   - Assert no Apply route/client/UI contract is present.
   - After playground mutations, Step 3+ mapping/procurement artifacts must not silently change unless a future explicit commit feature is added.
7. Phase 6 - Cleanup/concurrency:
   - Add regression coverage for preview reads/operations racing `delete_session`.
   - Must not recreate deleted session DBs, crash DuckDB, or bypass `get_session_lock(sessionId)` plus in-lock `session_exists(sessionId)`.
8. Phase 7 - Frontend build/component/manual:
   - Build gate: `cd SummarizationModule\frontend; npm run build`.
   - If a frontend test runner is later added, cover App top icon visibility, overlay open/close, API payload shapes, no Apply controls, original display headers, operations, undo/redo, pivot, and error states.
   - Manual gate: upload CSV/XLSX/ZIP, confirm Step 2 still loads, top `Raw Data Preview` icon appears, overlay tabs/search/filter/sort/pagination work, operations behave, pivot creates expected table/inventory state, and no Apply/downstream mutation occurs.

Execution validation completed for the first slice on 2026-06-18:

1. [x] Backend playground performance hardening:
   - `python -B -m pytest tests/test_playground_operations.py tests/test_playground_preview_routes.py tests/test_playground_contract.py` from `SummarizationModule/backend`: passed, `27 passed`.
   - `python -B -m pytest tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `10 passed`.
   - `python -B -m pytest tests/test_playground_operations.py -q` from `SummarizationModule/backend`: passed, `14 passed`.
1. [x] Backend review follow-up hardening:
   - Failed calculated-column updates now roll back working table/state/snapshot artifacts.
   - Pivot creation now cleans temp/pivot tables, registry rows, metadata rows, and state/cache metadata if a post-materialization failure occurs.
   - Pivot undo/redo inventory behavior and all preview route deleted-session checks are covered.
   - Upload route now treats `.xlsx`/Excel extensions as Excel files even though XLSX files are ZIP containers.
   - `python -B -m pytest tests/test_playground_operations.py tests/test_playground_preview_routes.py tests/test_upload_file_types.py -q` from `SummarizationModule/backend`: passed, `33 passed, 6 subtests passed`.
1. [x] Frontend playground overlay contract coverage:
   - `npm test` from `SummarizationModule/frontend`: passed, `6 passed`.
   - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Day_2Nna.js` at `998.97 kB`.
   - No jsdom/component runner exists in the module and no new test dependencies were installed; coverage uses dependency-free Node source-contract tests.
   - Frontend types now include backend `tableVersion`, `sourceTableKey`, `playgroundTableName`, and pivot undo `dropped`; the API client preserves backend error `code` and `details`.
1. [x] Final integrated validation refresh:
   - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `42 passed`.
   - `python -B -m pytest tests/test_summary_email_concurrency.py tests/test_spend_quality_assessment.py` from `SummarizationModule/backend`: passed, `24 passed`.
   - `npm test` from `SummarizationModule/frontend`: passed, `6 passed`.
   - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-DIOEFBA4.js` at `999.35 kB`.
1. [x] Frontend giant-scroll overlay update:
   - Module 3 playground overlay now uses `@tanstack/react-virtual` and 1000-row `/api/preview/state` chunk requests for one continuous scroll over the filtered/sorted dataset.
   - Previous/next page controls were removed; status now reports loaded rows vs total rows.
   - Selection is retained when selected rows scroll out of the rendered viewport and is cleared on table/query resets and mutating operation refreshes.
   - `npm test` from `SummarizationModule/frontend`: passed, `7 passed`.
   - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-BDC4T9N-.js` at `1,001.04 kB`.
   - Disposable Flask test-client smoke passed for CSV, XLSX, and ZIP uploads, Step 2 preview, preview state, column-values, edit, delete, rename, calculated column, pivot, undo/redo, and refresh-inventory.
   - Guardrail grep found no `/api/preview/apply`, no Apply target leakage, and no `motion/react` imports in runtime source paths.
   - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
1. [x] Frontend giant-scroll hardening:
   - Bounded the row chunk cache to a moving viewport window and aborts stale chunk/filter requests.
   - Added horizontal column virtualization for wide tables while preserving sticky row selector/identity columns.
   - Moved filter and column menus to clicked-trigger positioning with viewport clamping.
   - `npm test` from `SummarizationModule/frontend`: passed, `7 passed`.
   - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-DWpVDGW_.js` at `1,004.78 kB`.
   - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_contract.py -q` from `SummarizationModule/backend`: passed, `14 passed, 6 subtests passed`.
   - Runtime guardrail grep found no `/api/preview/apply`, no old page buttons, and no `motion/react` imports.
   - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
1. [x] Integrated playground validation refresh:
   - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `33 passed`.
   - `python -B -m pytest tests/test_summary_email_concurrency.py tests/test_spend_quality_assessment.py` from `SummarizationModule/backend`: passed, `24 passed`.
   - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Day_2Nna.js` at `998.97 kB`.
   - Guardrail grep found no `/api/preview/apply` in backend route/client/UI source paths and no `motion/react` imports.
   - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
1. [x] Metadata review follow-up: `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `21 passed`.
1. [x] Backend remaining-parity routes: `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `30 passed`.
2. [x] Phase 1 metadata slice: `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `17 passed`.
3. [x] `python -B -m pytest tests/test_playground_preview_routes.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `11 passed`.
4. [x] `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning.
5. [x] Contract grep found no `/api/preview/apply` client/UI/backend route and no unimplemented operation client exports.
6. [x] Automated API-smoke upload and overlay checks passed for CSV/XLSX/ZIP. True browser click-through remains pending because no browser automation/manual UI session was run.
7. [ ] Planned `..\.venv\Scripts\python.exe -B -m pytest ...` commands remain blocked because the target venv lacks `pytest`; system Python ran the focused backend tests successfully.
   - Rechecked on 2026-06-19: `& ..\.venv\Scripts\python.exe -B -m pytest --version` still fails with `No module named pytest`.

Add backend tests:

1. [x] `test_playground_preview_service.py`
   - paginated state reads uploaded `data_table`, not `raw_table`
   - hides `RECORD_ID`
   - preserves original header display text
   - search/filter/sort through stable column keys
   - numeric/date-aware sort
   - large-table minimum search length and searchable-column caps
2. [x] `test_playground_operations.py`
   - cell edit, row delete, column rename/delete/reorder/type change
   - calculated column
   - pivot
   - undo/redo
   - invalid operation validation
   - invalid calculated expressions do not dirty state
   - failed calculated-column full-table update rollback
   - pivot partial-failure cleanup
   - pivot undo/redo inventory restore/drop behavior
   - missing row IDs do not create undo history
   - noncanonical pivot aliases are rejected
3. [x] Route-level preview coverage:
   - route registration
   - all routes use shared session lock and in-lock session recheck
   - clean 400/404 JSON errors
4. [x] `test_playground_contract.py`
   - frontend/backend operation names match
   - response includes `columns[].dataType`, `rows`, `totalRows`, `offset`, `limit`, `undoDepth`, `redoDepth`, `dirty`
   - no Apply route/client/UI contract is present
5. [x] Downstream invalidation regression:
   - playground operations do not silently mutate Step 3+ pipeline artifacts when Apply is absent.
6. [x] Cleanup/concurrency regression:
   - preview reads/operations racing cleanup do not recreate deleted sessions or crash DuckDB.

Frontend validation:

1. [x] `npm run build --prefix SummarizationModule/frontend`
2. [x] `npm test --prefix SummarizationModule/frontend`
3. [x] Automated Flask smoke uploaded CSV/XLSX/ZIP and confirmed Step 2 preview loads.
4. [x] Source-contract test confirms top `Table2` icon wiring after upload/session state.
5. [x] Automated API/source-contract checks cover overlay state, tabs/search/filter/sort/pagination wiring; true browser rendering remains pending.
6. [x] Automated API smoke exercised edit, row delete, column rename, calc column, pivot, undo/redo, and refresh-inventory; type change/column reorder/delete remain covered by backend tests.
7. [x] Backend metadata tests and smoke verify original headers display, including non-uppercase, duplicates/blanks in focused fixtures.
8. [x] Source guardrails verify no Apply button/dialog appears in runtime overlay source.
9. [x] Source guardrails verify no Module 1-only raw/group/merge apply targets appear.

Commands after implementation:

```powershell
cd SummarizationModule\frontend
npm run build
```

```powershell
cd SummarizationModule\backend
..\.venv\Scripts\python.exe -B -m pytest tests\test_playground_preview_service.py tests\test_playground_operations.py tests\test_playground_routes.py tests\test_playground_contract.py
```

```powershell
cd SummarizationModule\backend
..\.venv\Scripts\python.exe -B -m pytest tests\test_upload_file_types.py tests\test_summary_email_concurrency.py tests\test_spend_quality_assessment.py
```

Optional source-reference checks:

```powershell
cd DataConsolidationAppV7\backend\module-1
..\..\.venv\Scripts\python.exe -B -m pytest tests\test_preview_service.py tests\test_preview_routes.py
```

## Open Decisions For Execution

1. Undo/redo model:
   - implement for all enabled operations, or disable for operations that cannot be restored safely.
2. Playground persistence model:
   - use source-like playground working-copy behavior, but omit Apply/commit to Module 3 workflow.
   - if a later request asks to commit playground changes to mapping, add that as a separate explicit feature.
3. Pivot output:
   - register pivot output as a new uploaded/playground table, or keep it temporary until applied.
4. Duplicate/blank header display:
   - preserve exact display labels while using stable unique keys internally.

## Visible Agents Created In Latest Backend Execution Slice

1. `backend_implementer` (`Turing`): implemented backend copy-on-write routes, operations, pivot, undo/redo, refresh inventory, and tests.
2. `frontend_implementer` (`Dirac`): implemented the richer target-local overlay controls, client usage, and frontend validation.
3. `contract_sync_agent` (`Hilbert`): reviewed frontend/backend DTO alignment and found post-implementation contract cleanup items.
4. `backend_reviewer` (`Franklin`): reviewed backend correctness and found calculated-expression/state and row-mutation cleanup issues.
5. `frontend_reviewer` (`Banach`): reviewed frontend behavior and found pivot/table-switch/filter/cell-cancel issues.
6. `test_engineer` (`Boyle`): ran integrated backend/frontend validation and guardrail checks.
7. `performance_reviewer` (`Anscombe`): reviewed large-table risks and identified cache/benchmark/cardinality follow-ups.

## Visible Agents Created In Prior Execution Slice

1. `backend_implementer` (`Erdos`): implemented Phase 1 metadata-backed preview state.
2. `contract_sync_agent` (`Dewey`): verified frontend/backend stable-key preview-state contract alignment and no Apply leakage.
3. `backend_reviewer` (`Faraday`): found set-header locking and legacy backfill issues.
4. `test_engineer` (`Herschel`): ran focused metadata/backend validation and no-Apply checks.
5. `backend_implementer` (`Gibbs`): fixed the review findings.
6. `backend_reviewer` (`Mendel`): confirmed the fixes had no blocking findings.
7. `test_engineer` (`Rawls`): ran final focused validation and `git diff --check`.

## Visible Agents Created In Latest Planning Revision

1. `frontend_architect` (`Euclid`): planned editable grid/filter/dialog/undo-redo/frontend refresh work.
2. `backend_architect` (`Planck`): planned metadata, copy-on-write playground tables, operations, undo/redo, and pivot.
3. `api_contract_reviewer` (`McClintock`): refined stable-key API contracts and route sequencing.
4. `test_engineer` (`Rawls`): planned backend/contract/manual validation and noted venv pytest blockers.
5. `performance_reviewer` (`Peirce`): added caps/caching/materialization guardrails.

## Visible Agents Created In Earlier Plan Revision

1. `frontend_architect` (`Copernicus`): revised frontend plan for top icon and full overlay.
2. `backend_architect` (`Dirac`): revised backend plan for rich playground operations and Module 3-specific persistence boundaries.
3. `api_contract_reviewer` (`Boole`): revised `/api/preview/*` contract with stable column keys and Module 3 state patching.
4. `test_engineer` (`Faraday`): revised validation plan for route/service/contract/downstream invalidation coverage.

## Agents Considered But Skipped

1. `product_planner`: skipped because the user clarified the product direction directly.
2. `frontend_implementer` and `frontend_reviewer`: skipped because this slice changed backend metadata/read contracts only; contract sync inspected frontend types/client usage.
3. `performance_reviewer`: skipped because this slice added metadata/backfill behavior and did not introduce large-table read APIs, pivot materialization, or grid rendering changes.
