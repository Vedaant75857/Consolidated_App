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

## 2026-07-31 - Spend Quality Assessment XLSX Export Boundary

The Spend Quality Assessment workbook export should be generated server-side at
`POST /api/export/xlsx/spend-quality-assessment` from a lock-protected snapshot
of the completed `_meta["executive_summary"]` result. Export must not rerun SQL or
AI, must not require an API key, and must preserve the existing mapping-driven
cache invalidation boundary. `openpyxl` is already an approved backend
dependency.

The initial workbook scope is the canonical Executive Summary result: narrative
summary, fill-rate summary, spend bifurcation, monthly/date pivot, and Pareto
cuts. The separate Not Procurable, CAPEX/OPEX, and Intercompany Step 4 tabs are
out of scope until their interactive inputs and on-demand results have a durable
export contract.

## 2026-08-01 - One-Process Namespaced Backend Host

The earlier decision to keep the three Flask applications in independent Python
processes is superseded for local-suite operation. Keep the existing
`backend/module1`, `backend/module2`, and `backend/module3` folders and their
domain ownership, but compose their applications in one host process with
module-specific API namespaces: `/api/module1/*`, `/api/module2/*`, and
`/api/module3/*`.

The host file is a deliberately thin composition/launcher layer. It may mount
applications, provide host health/readiness, and own process lifecycle, but must
not become a large merged `app.py` containing module routes, business logic,
services, or data access.

Do not merge the incompatible module endpoints under one bare `/api/*` route
surface. Per-module frontend proxy rewrites preserve current relative client
calls and DTOs. Module session/runtime/upload boundaries remain isolated, and
existing cross-module transfers use the unified host URL plus the destination
namespace. The three frontend dev servers are outside this decision.

## 2026-08-01 - Unified Frontend Suite Boundary

The frontend consolidation target is one small suite host with one React root
and lazy route features, not a giant merged React application. Keep the current
landing and three module folders as owned feature areas. The suite host owns
routing, shell navigation, runtime/API-base configuration, and asset delivery;
module Apps retain their state, providers, components, and workflows.

All browser API calls must use explicit module bases (`/api/module1`,
`/api/module2`, `/api/module3`) when served from the shared origin. Endpoint
suffixes and DTOs remain unchanged. In built mode, the thin unified Python host
may serve suite static assets and SPA fallback so the complete local suite can
run as one process; during HMR development, one Vite process remains intentional.

## 2026-08-01 - Interim Suite Theme Policy

The unified suite mounts each module's existing theme provider inside its lazy
route wrapper. Modules 1 and 2 intentionally continue to share
`datastitcher_theme`; Module 3 keeps its existing independent theme key. This is
an interim compatibility policy, not CSS isolation: module global styles must be
scoped before frontend cutover.

## 2026-08-01 - Repository TLS Trust Boundary

Do not commit Bain/Zscaler certificates or disable TLS verification. The unified
Python process may discover an existing standard Windows Bain PEM path and may
accept an explicit `PROCIP_CA_BUNDLE` override, mapping it to Python's standard
CA environment variables. External tools such as Claude Code, AWS CLI, pip,
and npm must be configured separately because application code cannot control
their trust stores or install-time connections.

## 2026-08-01 - Shared Ingestion Ownership and Fidelity Boundary

The unified backend owns one package-qualified ingestion core at
`backend/ingestion/`. Use Module 1's dispatch, progress, raw-preview,
DuckDB-native loading and batched-transaction patterns as the operational base,
but do not preserve its current all-`VARCHAR` storage policy.

Ingestion uses an immutable raw representation plus a typed working
representation, ordered schema/stable column identity, source/workbook
provenance, and structured issues. Ambiguous or unsupported values remain exact
text with visible diagnostics rather than being guessed, replaced, dropped or
coerced to null. Dates require workbook date-system and number-format provenance;
formulas and merged ranges have explicit non-recalculation/non-expansion policies.

Module routes, public DTOs, session databases, registries and downstream append,
merge, normalization, mapping and playground logic remain module-owned behind
thin ingestion adapters. Module 3 retains its stable `COL_n` metadata contract.
Cross-module data movement should use a versioned typed artifact and schema/
provenance manifest; CSV remains a temporary, explicitly lossy fallback.

## 2026-08-01 - Ingestion cutover authorization

Module ingestion cutover is fail-closed and ordered: Module 1, then Module 2,
then Module 3. A feature flag is necessary but not sufficient. Evidence must be
bound to the exact payload, session/operation where applicable, ingestion
contract version, compatibility postconditions, and verified predecessor state.
Evidence from one upload cannot authorize a different upload, and environment
booleans alone cannot stand in for predecessor parity.

Transfer parity requires verification of actual Arrow/Parquet bytes plus the
manifest contract version and encoding. Metadata-only checksums or current CSV
handoffs cannot authorize typed cutover. Until destination routes accept and
verify typed payloads and route/workflow parity passes, all three module adapters
remain legacy by default.
