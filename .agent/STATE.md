# Project State - Repo-Level Python Venv Consolidation

Last updated: 2026-07-14

## Current Focus

Execution completed the repository-wide consolidation to one root-level Python environment. No application-code behavior or frontend/backend API contracts changed.

Recommended target:

- `Consolidated_App/.venv` as the single repo-level Python virtual environment.
- Root `.gitignore` already ignores `.venv/`.

Root environment status:

- `Consolidated_App/.venv` now exists with Python 3.13.0.
- Root `requirements-backend.txt` contains the reconciled runtime dependency union; `requirements-dev.txt` layers in `pytest`.
- Root runtime/dev dependencies installed successfully and `pip check` reports no broken requirements.
- `setup.ps1` now creates/uses only root `.venv`, preferring `py -3.13` and safely falling back to a verified standard per-user Python 3.13 installation; it rejects Python 3.12.
- Root and Module 1 npm backend scripts now use the root interpreter; ProcIP README documents the root setup path.

Removed obsolete nested venv folders after explicit user approval:

- `DataConsolidationAppV7/backend/.venv`
- `ProcIP_Module2-main/backend/.venv`
- `SummarizationModule/.venv`
- `SummarizationModule/backend/.venv` (previously stale/broken)

Affected setup/script areas:

- `setup.ps1` creates backend-local `.venv` folders and installs each backend's local `requirements.txt`.
- Root `package.json` hard-codes backend-local venv interpreters for `dev:stitcher-be`, `dev:normalizer-be`, and `dev:summarizer-be`.
- `DataConsolidationAppV7/package.json` hard-codes `backend/.venv/Scripts/python` for `dev:mod1`.
- `ProcIP_Module2-main/README.md` still documents local `pip install -r requirements.txt`.

Dependency consolidation must be deliberate. The current backend manifests conflict:

- Flask pins/ranges differ between `Flask==3.1.3`, `flask==3.0.0`, and `Flask>=3.1,<4`.
- `flask-cors==6.0.2` conflicts with Summarization's `flask-cors>=5.0,<6`.
- ProcIP pins older `werkzeug==3.0.1` and `python-dotenv==1.0.0` while DataConsolidation pins newer versions.

Validation from root `.venv`:

- `SummarizationModule/backend`: passed, 72 tests.
- `ProcIP_Module2-main/backend`: passed, 1 test.
- `DataConsolidationAppV7/backend/module-1`: 52 passed, 2 failed in `tests/test_value_distribution.py` because business logic adds `PO Local Currency Code` when tests supplied only `Local Currency Code`; this is not an environment/dependency failure and was not changed in this scope.
- `pip check` and `git diff --check` passed; static dev-script smoke found no nested `.venv` references.

Post-removal verification confirms all four paths are absent, root `.venv` remains Python 3.13.0 with a clean `pip check`, and referenced backend scripts use the root environment.

Visible project-local subagents created in this planning slice:

- `backend_architect` (`Einstein`) inspected backend environment architecture, dependency conflicts, script/docs impact, and risks.
- `test_engineer` (`McClintock`) identified test commands, validation strategy, pytest gaps, and stale nested-venv checks.

## Previous Focus - SummarizationModule Playground Feature Parity

Execution mode completed a backend-only preview-service hardening slice for `SummarizationModule`. Preview-state global search now enforces a minimum search length and caps the searched visible columns only when the table row count crosses the large-table threshold, preserving small/medium-table all-column search behavior. Focused service tests now cover paginated reads from `data_table`, hidden `RECORD_ID`, original header display text, stable-key filter/sort, numeric/date-aware sort, and the new large-table search guardrails.

Files changed in this slice:

- `SummarizationModule/backend/services/playground/service.py`
- `SummarizationModule/backend/tests/test_playground_preview_service.py`
- `.agent/PLAN.md`
- `.agent/STATE.md`
- `.agent/CHANGELOG_AGENT.md`

Validation completed:

- `python -B -m pytest tests/test_playground_preview_service.py -q` from `SummarizationModule/backend`: passed, `6 passed`.
- `python -B -m pytest tests/test_playground_preview_service.py tests/test_playground_preview_routes.py tests/test_playground_operations.py tests/test_playground_contract.py -q` from `SummarizationModule/backend`: passed, `37 passed, 6 subtests passed`.
- `python -B -m pytest tests/test_playground_preview_service.py tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py -q` from `SummarizationModule/backend`: passed, `48 passed, 6 subtests passed`.
- Synthetic large-table validation covered `100k x 25`, `1M x 25`, and read-only `100k x 100/250` paths.
- `& ..\.venv\Scripts\python.exe -B -m pytest --version` from `SummarizationModule/backend`: failed because `SummarizationModule\.venv` still lacks `pytest`.

Visible project-local subagents created in this execution slice:

- `backend_implementer` (`Hubble`) implemented preview-service search guardrails and focused service tests.
- `performance_reviewer` (`Epicurus`) ran synthetic large-table validation and identified remaining benchmark/performance risks.

Remaining benchmark scope:

- Feasible automated coverage is now complete for focused service behavior, cache guardrails, and representative synthetic large-table paths.
- Deferred: `1M x 100/250`, all `5M` shapes, browser-level virtualized rendering validation, backend column projection for wide chunks, and undo/redo snapshot byte/disk caps.

Execution mode completed the frontend UI parity polish inside `SummarizationModule` playground. Module 3/Summarization playground controls now match Module 1 more closely for pivot creation, filter/sort UI, filter button appearance, and column type display, while preserving the current SummarizationModule theme/layout and leaving calculated fields UI unchanged.

Files changed in this slice:

- `SummarizationModule/frontend/src/components/playground/ExcelPreviewOverlay.tsx`
- `SummarizationModule/frontend/test/playground-overlay.contract.test.mjs`
- `.agent/PLAN.md`
- `.agent/STATE.md`
- `.agent/CHANGELOG_AGENT.md`

Backend stable-key/no-Apply contracts remain the boundary:

- filters/sort use `columnKey`
- pivot payloads use `rowFields: columnKey[]`, `columnFields: columnKey[]`, and `valueFields: [{ columnKey, aggregation }]`
- calculated column UI/payload should remain unchanged
- do not add `/api/preview/apply` or Module 1 apply targets.

Validation completed:

- `npm test` from `SummarizationModule/frontend`: passed, `7 passed`.
- `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Dl3x98Zc.js` at `1,011.70 kB`.
- `python -B -m pytest tests/test_playground_contract.py tests/test_playground_preview_routes.py tests/test_playground_operations.py -q` from `SummarizationModule/backend`: passed, `31 passed, 6 subtests passed`.
- Runtime guardrail grep found no `/api/preview/apply`, no Apply target leakage, and no `motion/react` imports in runtime source paths.
- `git diff --check`: passed with LF-to-CRLF working-copy warnings only.

Visible project-local subagents created in this execution slice:

- `frontend_implementer` (`James`) implemented the UI parity patch.
- `contract_sync_agent` (`Curie`) verified stable-key/no-Apply contract alignment and ran backend guardrails.
- `frontend_reviewer` (`Turing`) reviewed UI behavior and found capped-value and pivot-submit follow-ups.
- `test_engineer` (`Linnaeus`) ran frontend validation and runtime guardrail checks.

Follow-up fixes from review:

- Filter popover now relabels capped value selection as `Select loaded` and warns that only loaded values will be selected when `totalDistinct` exceeds the returned values.
- Pivot submit enablement now allows backend-supported values-only aggregate pivots while still requiring value fields.

Previous execution context:

Execution mode had completed the approved DataConsolidationAppV7 preview/playground parity slice inside `SummarizationModule`.

Implemented the target-local top `Raw Data Preview` icon, metadata-backed preview state, backend copy-on-write playground routes, mutating operations, pivot, undo/redo, refresh inventory, and the frontend remaining-parity overlay controls. The overlay now uses stable-key payloads for filters, cell edits, row delete, column operations, calculated columns, pivot, undo/redo, inventory refresh, and a continuous virtualized giant-scroll grid backed by paginated `/api/preview/state` chunks. The frontend grid now keeps only a bounded moving chunk window, aborts stale preview/filter requests, and virtualizes columns horizontally for wide tables.

Post-review fixes are complete: pivot activation waits for inventory refresh, stale operation results cannot overwrite a switched table, Escape cancels cell edits without committing, clearing a filter removes it, pivot value fields no longer send display names, calculated expressions reject raw quoted identifiers before mutation, missing row IDs fail without dirtying playground state, and consumed undo/redo snapshots are dropped after restore.

Backend playground performance hardening now adds bounded versioned read caches for preview state and column-values reads, keyed by `tableVersion` plus request shape. Pivot creation now estimates distinct row-field cardinality and output cells before CTAS and rejects oversized high-cardinality pivots without materializing pivot tables.

Final review follow-ups are complete: failed calculated-column updates roll back playground working state and snapshots; pivot partial failures clean temp/pivot tables, registry rows, metadata rows, and state/cache metadata; pivot undo/redo inventory behavior is explicitly covered; all preview endpoints have deleted-session route coverage; `.xlsx` uploads are routed as Excel even though XLSX files are ZIP containers.

## Revised Target Behavior

- Add a top-right `Table2`/playground icon in `SummarizationModule/frontend/src/App.tsx`, visible after upload/session restore when inventory exists.
- Clicking the icon opens a target-local full-screen playground overlay.
- Keep current Step 2 inline data preview/upload review as the workflow step.
- Add rich overlay features: table tabs, search, filters, sort, continuous virtualized row scrolling, cell edit, row delete, column rename/delete/reorder/type change, calculated columns, pivot, undo/redo, and inventory refresh.
- Preserve original uploaded header display with stable internal column keys.
- The Apply flow is explicitly removed for Module 3; playground changes should not be committed into the Module 3 workflow unless a later request adds that feature.

## Source Reference Areas

- `DataConsolidationAppV7/frontend/src/App.tsx`
- `DataConsolidationAppV7/frontend/src/components/module-1/ExcelPreviewOverlay.tsx`
- `DataConsolidationAppV7/frontend/src/components/module-1/services/stitchingApi.ts`
- `DataConsolidationAppV7/frontend/src/types/excelPreview.ts`
- `DataConsolidationAppV7/backend/module-1/routes/preview_routes.py`
- `DataConsolidationAppV7/backend/module-1/preview_operations/service.py`
- `DataConsolidationAppV7/backend/module-1/tests/test_preview_service.py`
- `DataConsolidationAppV7/backend/module-1/tests/test_preview_routes.py`

## Target Areas For Later Implementation

- `SummarizationModule/frontend/src/types/index.ts`
- browser/manual large-table validation for the virtualized playground grid
- `SummarizationModule/backend/routes/upload_routes.py`
- `SummarizationModule/backend/services/upload/file_loader.py`
- broader backend tests for cleanup/concurrency and large-table performance

## Key Findings

- 2026-06-19 UI parity planning found the remaining mismatch is concentrated in `SummarizationModule/frontend/src/components/playground/ExcelPreviewOverlay.tsx`.
- Module 1's relevant source is `DataConsolidationAppV7/frontend/src/components/module-1/ExcelPreviewOverlay.tsx`.
- Module 1 pivot creation uses an Available Fields list plus Row Fields, Column Fields, and Values drop zones with aggregation selectors and type labels; SummarizationModule currently uses simpler checkbox zones.
- Module 1 header/filter/sort/type behavior is more compact: filter/sort/type state is represented through header icons/menus and status text; SummarizationModule currently uses separate sort, filter, and column-action buttons plus raw uppercase type subtext in each header.
- SummarizationModule should match Module 1 control behavior but keep Module 3 contracts stable-key based; do not copy Module 1 display-name-based DTOs.
- Current frontend tests are source-contract tests in `SummarizationModule/frontend/test/playground-overlay.contract.test.mjs`; there is no rendered jsdom/browser component test runner for this overlay.
- Module 1 top icon pattern is a `Table2` button shown when inventory exists, beside the theme toggle, opening `ExcelPreviewOverlay`.
- Module 3 now has a matching `Table2` top icon and target-local overlay with filter, edit, column, calculated-column, pivot, undo/redo, and inventory-refresh controls.
- Module 3 now registers `POST /api/preview/state`, using `get_session_lock(sessionId)`, in-lock `session_exists(sessionId)`, and `_table_registry` table resolution.
- Module 3 now registers `POST /api/preview/column-values`, `/operation`, `/undo`, `/redo`, and `/refresh-inventory`, all using the same session lock plus in-lock session existence recheck.
- First mutating operation clones a source `data_table` into a `pg__*` working table and stores playground-side metadata/state so Step 3+ source inputs are not silently mutated.
- Operation contracts are stable-key only. Display-name and physical-name refs are rejected at the route boundary.
- Calculated column expressions are allowlisted, must reference columns as `[COL_n]`, reject raw quoted identifiers, and are validated before any mutation snapshot is created.
- Pivot outputs are registered only in `_playground_registry`, not `_table_registry`, and are returned through overlay `refresh-inventory`.
- Preview state and column-values reads use bounded `_meta` read caches. Mutations naturally bypass stale entries by incrementing `tableVersion`; old cache entries remain bounded and age out.
- Pivot preflight preserves the existing row-field/value-field/generated-column caps and additionally rejects excessive distinct row tuples or estimated output cells before creating `pg__pivot__*` tables.
- Frontend preview DTOs include backend `tableVersion`, `sourceTableKey`, `playgroundTableName`, and pivot undo `dropped`; the API client preserves backend error `code` and `details`.
- Automated Flask smoke passed for CSV, XLSX, and ZIP uploads, Step 2 preview, preview state, column-values, edit, row delete, column rename, calculated column, pivot, undo/redo, and refresh-inventory. This is an API/manual-equivalent smoke, not a true browser click-through.
- Remaining parity should not copy Module 1 DTOs directly because Module 1 is display-name based; Module 3 must stay stable-key based.
- Mutations should use a copy-on-write `pg__*` playground table so the no-Apply model cannot silently mutate Step 3+ pipeline data.
- Full frontend parity uses the existing table pattern and `@tanstack/react-virtual` for giant row scrolling; the overlay fetches backend chunks up to the existing 1,000-row max as the user scrolls.
- The giant-scroll overlay is no longer page-button based: it renders one continuous scroll surface, prefetches visible row chunks, bounds cached row chunks around the current viewport, and renders only visible columns plus spacer cells for wide datasets.
- Preview state and filter-value client calls now accept `AbortSignal`; table/search/filter resets abort old chunk reads before clearing current state.
- Module 1 uses `motion/react`; Module 3 already uses `framer-motion`, so copied overlay imports must be adapted.
- `/api/get-preview` remains for Step 2 compatibility; the new top-icon overlay uses `/api/preview/state`.
- Source apply target kinds are Module 1-specific and should be omitted from Module 3.
- No Apply route/client/UI was added in this slice.
- Current Module 3 upload persists `_column_metadata` for new uploads and header-row changes. `/api/preview/state` backfills metadata for legacy sessions from `raw_table` first-row headers only when they align with the current data table, otherwise from current `data_table` names.
- New preview routes must follow shared DuckDB session-lock discipline.
- Review follow-up fixed `/api/set-header-row` to perform header-row mutation, inventory/preview rebuild, metadata writes, and column collection inside `get_session_lock(session_id)` with an in-lock session-existence recheck.
- Legacy metadata backfill now trusts raw first-row display headers only when the cleaned/deduped/`RECORD_ID`-filtered raw headers align with the current visible data-table physical columns; stale raw headers fall back to current physical column names.

## Planned Contract Direction

Use DTOs with stable column keys:

- `columns`: array of `{ key, displayName, dataType }`
- `rows`: array of `{ __row_id, values }`, where `values` is keyed by column key

Required routes:

- `POST /api/preview/state` implemented
- `POST /api/preview/column-values`
- `POST /api/preview/operation`
- `POST /api/preview/undo`
- `POST /api/preview/redo`
- `POST /api/preview/refresh-inventory`

Contract direction:

- Use `columns[].dataType`; do not add a separate `columnTypes` response map.
- Use canonical filter ops only at the external contract: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `startswith`, `endswith`, `is_null`, `is_not_null`, `in`, `not_in`.
- Preview rows use `values[columnKey]` with stable metadata keys such as `COL_0`; physical SQL column names and display-name-only refs are rejected for preview filters/sort.
- `refresh-inventory` should return Module 3 naming: `{ ok, fileInventory, columns, previews }`.
- `/api/preview/apply` remains intentionally absent.

## Durable Decisions / Constraints

- Do not link/import source Module 1 files.
- Copy/adapt into target-local Module 3 files.
- Preserve original uploaded header display explicitly with metadata; do not rely on normalized DuckDB names.
- Do not implement Module 1 `raw/group/merge_*` apply targets or any Module 3 Apply button/endpoint for this scope.
- Playground operations should remain playground-scope and must not silently mutate Step 3+ artifacts.
- Every new preview route must use `get_session_lock(session_id)` and recheck session existence inside the lock.

## Visible Subagents This Planning Slice

- `product_planner` (`Russell`) clarified acceptance criteria, non-goals, edge cases, and product boundary.
- `frontend_architect` (`Plato`) mapped Module 1 UI patterns to SummarizationModule frontend areas and implementation sequence.
- `test_engineer` (`Raman`) identified source-contract and backend guardrail validation for the planned UI parity fix.

## Visible Subagents This Execution Slice

- `backend_implementer` (`Turing`) implemented backend copy-on-write routes, operations, pivot, undo/redo, refresh inventory, and tests.
- `frontend_implementer` (`Dirac`) implemented the richer target-local overlay controls, client usage, and frontend validation.
- `contract_sync_agent` (`Hilbert`) reviewed frontend/backend DTO alignment.
- `backend_reviewer` (`Franklin`) reviewed backend correctness and identified post-implementation fixes.
- `frontend_reviewer` (`Banach`) reviewed frontend behavior and identified post-implementation fixes.
- `test_engineer` (`Boyle`) ran integrated backend/frontend validation and guardrail checks.
- `performance_reviewer` (`Anscombe`) reviewed large-table risks and identified cache/benchmark/cardinality follow-ups.

## Validation Status

Latest validation:

- Backend preview-service hardening on 2026-06-19:
  - `python -B -m pytest tests/test_playground_preview_service.py -q` from `SummarizationModule/backend`: passed, `6 passed`.
  - `python -B -m pytest tests/test_playground_preview_service.py tests/test_playground_preview_routes.py tests/test_playground_operations.py tests/test_playground_contract.py -q` from `SummarizationModule/backend`: passed, `37 passed, 6 subtests passed`.
- Frontend giant-scroll hardening on 2026-06-18:
  - `npm test` from `SummarizationModule/frontend`: passed, `7 passed`.
  - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-DWpVDGW_.js` at `1,004.78 kB`.
  - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_contract.py -q` from `SummarizationModule/backend`: passed, `14 passed, 6 subtests passed`.
  - Runtime guardrail grep found no `/api/preview/apply`, no old page buttons, and no `motion/react` imports.
  - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
- Final playground validation on 2026-06-18:
  - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `42 passed`.
  - `python -B -m pytest tests/test_summary_email_concurrency.py tests/test_spend_quality_assessment.py` from `SummarizationModule/backend`: passed, `24 passed`.
  - `npm test` from `SummarizationModule/frontend`: passed, `6 passed`.
  - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-DIOEFBA4.js` at `999.35 kB`.
  - Disposable Flask test-client smoke passed for CSV/XLSX/ZIP upload, Step 2 preview, and overlay API operations.
  - Runtime guardrails found no `/api/preview/apply`, no Apply target leakage, and no `motion/react` imports.
  - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
- Backend playground performance hardening on 2026-06-18:
  - `python -B -m pytest tests/test_playground_operations.py tests/test_playground_preview_routes.py tests/test_playground_contract.py` from `SummarizationModule/backend`: passed, `27 passed`.
  - `python -B -m pytest tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `10 passed`.
  - `python -B -m pytest tests/test_playground_operations.py -q` from `SummarizationModule/backend`: passed, `14 passed`.
- Frontend playground overlay contract coverage on 2026-06-18:
  - `npm test` from `SummarizationModule/frontend`: passed, `6 passed`.
  - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Day_2Nna.js` at `998.97 kB`.
  - No jsdom/component runner exists and no new dependencies were installed; current frontend coverage is source-contract tests for top icon visibility/open wiring, overlay controls, stable-key payload wiring, blank filter handling, no Apply UI, and undo/redo controls.
- Integrated playground validation on 2026-06-18:
  - `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `33 passed`.
  - `python -B -m pytest tests/test_summary_email_concurrency.py tests/test_spend_quality_assessment.py` from `SummarizationModule/backend`: passed, `24 passed`.
  - `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted the existing large chunk warning for `assets/index-Day_2Nna.js` at `998.97 kB`.
  - Guardrail grep found no `/api/preview/apply` in backend route/client/UI source paths and no `motion/react` imports.
  - `git diff --check`: passed with LF-to-CRLF working-copy warnings only.
- `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `21 passed` after metadata review fixes.
- `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_playground_operations.py tests/test_playground_contract.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `30 passed`.
- `python -B -m pytest tests/test_playground_preview_routes.py tests/test_playground_column_metadata.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `17 passed`.
- `python -B -m pytest tests/test_playground_preview_routes.py tests/test_upload_file_types.py` from `SummarizationModule/backend`: passed, `11 passed`.
- `npm run build` from `SummarizationModule/frontend`: passed; Vite emitted a large chunk warning around `970 kB`.
- `npm run build` from `SummarizationModule/frontend`: passed after the frontend remaining-parity slice; Vite emitted a large chunk warning for `assets/index-D-SxoILB.js` at `998.53 kB`.
- Grep check found no `/api/preview/apply` route/client/UI and no unimplemented preview operation client exports.
- Plan-mode validation refresh on 2026-06-18 re-ran the focused backend tests with system Python: `11 passed`.

Validation environment note:

- `SummarizationModule\.venv\Scripts\python.exe` exists and reports Python 3.13, but lacks `pytest`.
- `SummarizationModule\backend\.venv\Scripts\python.exe` is broken and points at missing `C:\Users\78464\AppData\Local\Programs\Python\Python310\python.exe`.
- In PowerShell, invoke relative venv executables with the call operator, for example `& ..\.venv\Scripts\python.exe -B -m pytest ...`; without `&`, PowerShell does not execute the relative path.

## Remaining Risks

- Current UI parity can only be partially guarded by source-contract tests; a manual/browser comparison is still needed for filter button appearance, pivot drag/drop behavior, popover placement, dense header fit, and responsive layout.
- Matching Module 1 too literally would break SummarizationModule contracts because Module 1 uses display-name fields in places.
- Pivot drag/drop must retain click/keyboard alternatives; drag-only interaction would be a regression.
- Header preservation for preview state is now covered by backend metadata tests for exact, duplicate, blank, and reserved `RECORD_ID` headers; automated CSV/XLSX/ZIP upload smoke passed, but true browser upload validation remains pending.
- Undo/redo is implemented with bounded snapshots for enabled mutating operations; performance review recommends operation-specific inverses before very large-table workloads.
- Pivot/operation semantics, row-cardinality/output-shape preflight, pivot undo/redo, and partial-failure cleanup are covered by focused backend tests; true large-table/materialization benchmarks still need validation.
- Module 3 now has dependency-free source-contract frontend tests for the top icon and overlay contracts, including bounded giant-scroll wiring, but no jsdom/browser component tests or click-level interaction tests exist.
- Performance guardrails are present as backend caps/read caches, large-table search length/column caps, and frontend bounded row/column virtualization, but true 100k/1M/5M benchmark coverage and undo snapshot byte/disk caps remain pending.
