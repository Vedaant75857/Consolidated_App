# Durable Decisions

## 2026-07-14 - Portable Bundle Dependency Exclusion Boundary

The portable sharing archive must be built from an explicit staging allowlist, not
by compressing the repository root. Root and application development dependency
trees and caches are excluded, including frontend `node_modules`, `.venv`, logs,
and agent metadata.

`bin/node-portable/node_modules` is retained as part of the portable Node/npm
runtime because `bin/node-portable/npm.cmd` loads npm from that tree. It is not
treated as an installed application development cache. If policy later requires
excluding every path component named `node_modules` without exception, the Node
runtime must first be repackaged so npm no longer depends on that directory.

The guarded `start.bat` should explicitly run setup only when root `node_modules`
or `.venv` is missing, then invoke the root `dev` script with npm lifecycle scripts
disabled for that call. This preserves `npm run dev` orchestration without allowing
the existing `predev` hook to run setup unconditionally a second time.

## 2026-07-14 - Unified Parent Folders With Independent Applications

The suite should be physically consolidated under `backend/` and `frontend/` while preserving independent application boundaries:

- `backend/module1`, `backend/module2`, `backend/module3`
- `frontend/landing`, `frontend/module1`, `frontend/module2`, `frontend/module3`

The relocation phase must not combine the three Flask apps into one Python process or merge the four Vite/React apps into one source/dependency tree. Existing endpoints, DTOs, ports, app-local imports, manifests, and lockfiles should remain stable until relocation parity is proven.

The root `.venv`, `requirements-backend.txt`, and `requirements-dev.txt` remain the canonical combined local Python environment. Each backend may retain a module-scoped requirements manifest for independent deployment/build purposes.

Vercel deployment readiness is a separate phase from folder relocation. Frontends may be deployed as separate Vercel projects from the monorepo. The filesystem-backed DuckDB backends require persistent hosting or an explicit external persistence, upload, and concurrency redesign before they are suitable for Vercel Functions.

## 2026-06-18 - SummarizationModule Excel Upload Classification

When classifying uploaded files, explicit Excel extensions such as `.xlsx`, `.xlsm`, `.xlsb`, `.xltx`, and `.xltm` should route to the Excel parser before generic ZIP-container detection.

Modern Excel files are ZIP containers internally; treating any ZIP-shaped bytes as a ZIP archive causes valid XLSX uploads to bypass Excel parsing and fail as empty/invalid archives.

## 2026-06-18 - SummarizationModule DuckDB Session Locking

Cached DuckDB session access in the SummarizationModule backend should be serialized with `shared.db.get_session_lock(session_id)` whenever a route uses the shared cached connection returned by `get_session_db(session_id)`.

Route-local locks should not be used to protect the same cached DuckDB session connection because they can leave other routes unsynchronized.

`delete_session(session_id)` should keep the per-session lock object stable while closing/removing session files so waiting requests cannot split onto a new lock and recreate a deleted session DB.

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

## 2026-06-18 - SummarizationModule Playground Port Scope

The playground/preview feature copied from DataConsolidationAppV7 into SummarizationModule should be recreated as target-local files, not linked or imported from the source module.

The scope is feature parity with the Module 1 preview/playground, exposed in Module 3 from a top icon like Data Stitcher's `Raw Data Preview` icon. It should include the rich playground overlay and compatible operations rather than a read-only-only preview.

Exact original header display should be preserved explicitly with metadata during upload/preview handling instead of relying on normalized DuckDB column names. Stable internal column keys should be used for filtering, sorting, editing, and operations so duplicate or blank original headers remain safe.

Module 1 apply target kinds such as `raw`, `group`, `merge_version`, and `merge_group` should not be copied into Module 3 for this scope. The Module 3 playground should omit the Apply button/dialog, omit `POST /api/preview/apply`, and keep playground operations from silently committing into the Step 3+ workflow.

## 2026-06-18 - SummarizationModule Playground Remaining-Parity Architecture

Module 3 playground contracts should use stable `columnKey` values everywhere. Module 1 can be used as a feature reference, but its display-name-based payloads must not be copied directly.

The canonical preview column DTO is `{ key, displayName, dataType }`; do not add a separate `columnTypes` response map unless a later cross-client need appears.

Per-table preview metadata is persisted in `_column_metadata`. Stable public keys use generated `COL_n` values for visible uploaded columns, while `physical_name` stores the actual DuckDB column name and `display_name` stores the exact uploaded/header-row label. `RECORD_ID` is reserved as hidden row identity and should only be exposed to clients as row `__row_id`.

Mutating playground operations should use a copy-on-write working table, preferably `pg__*`, created from the uploaded `data_table` on first mutation. This keeps the no-Apply model honest: playground edits must not silently mutate Step 3+ pipeline inputs.

`column_rename`, `column_reorder`, and type override should be metadata-first operations where possible. Physical SQL column names should stay stable unless a real table-shape change requires a migration.

Pivot outputs are playground-only tables unless a later explicit feature adds commit/apply behavior. They should be surfaced through `refresh-inventory` using Module 3 naming: `{ ok, fileInventory, columns, previews }`.

Undo/redo should not be exposed for operations unless the backend can restore them safely. Use bounded history: before-images for cell edits, bounded deleted-row payloads, metadata snapshots for metadata-only operations, and small capped table snapshots only for heavy table-shape operations.

Performance guardrails are part of the contract for remaining parity: cap distinct filter values, `in` filter lists, row-delete batches, pivot-generated columns/value fields/row fields, and undo snapshot depth/disk usage.

## 2026-06-18 - SummarizationModule Playground Giant Scroll UX

The Module 3 playground overlay should present the dataset as one continuous scroll surface instead of exposing page navigation.

Backend preview reads remain paginated with the existing 1000-row maximum, but pagination is an implementation detail hidden behind frontend virtualization and chunk prefetching.

For large uploads, the frontend should bound cached row chunks around the current viewport, abort stale preview/filter reads on table/search/filter changes, and virtualize wide columns horizontally while keeping row identity controls sticky.
