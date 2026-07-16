# Agent Changelog

## 2026-07-15 - Module 2 stale Module 1 component cleanup

- Mode: execution. Visible project-local agents: `frontend_cleanup`,
  `frontend_cleanup_review`, `module2_cleanup_tests`, `frontend_cleanup_final`,
  and `module2_final_validation`.
- Rehomed Module 2's active DataLoading, DataInventory, and required
  VirtualPreviewTable into `components/module-2`; updated App imports.
- Deleted eleven orphaned Module 1-era Module 2 UI/API files, removed the stale
  Merging test assertion, and deleted the now-empty `components/module-1` tree.
- Validation: no old-path/helper references; Module 2 feedback contracts 2/2 and
  production build passed. Existing Vite large-chunk advisory remains.

## 2026-07-15 - Pivot parity and merge-export boundary execution

- Mode: execution. Visible project-local agents: `frontend_implementer`,
  `backend_implementer`, `test_engineer`, `frontend_reviewer`, and
  `backend_reviewer`.
- Module 3 Pivot now mirrors the Module 1 drag-only, always-two-column UI, filters
  identity fields from field selection/state/submission, and shows mapped Column
  Fill Rate destinations in accessible red. Module 3 stable-key and no-Apply
  contracts remain unchanged.
- Module 1 merge CSV/XLSX/ZIP/version/step/analyzer export routes share a filtered
  public-column list. New regression coverage verifies `__row_id` and provenance
  remain absent from files and the analyzer transfer payload.
- Validation: Module 1 export test 1 passed; combined provenance/export tests 23
  passed; Module 3 contracts 11/11; Module 3 build passed; diff checks passed.
- Remaining risk: no browser drag/drop/restored-state/light-dark smoke or live
  cross-process transfer was available. Existing Vite large-chunk advisory remains.

## 2026-07-15 - Module 3 Pivot parity and mapped-name color plan

- Mode: plan. Visible project-local agents: `frontend_architect` and
  `product_planner`.
- Read-only comparison confirmed that Module 3's Pivot dialog is a custom variant:
  it exposes its identity `Record ID` and adds per-field quick-add controls that
  Module 1 does not have. The plan restores Module 1's drag-only presentation,
  keeps Module 3's stable-key/safety contracts, and makes Column Fill Rate mapped
  destinations red. No application files or API contracts changed.

## 2026-07-15 - Merge output transfer diagnosis

- Mode: debug. Visible project-local agent: `debug_triage_agent`.
- Read-only diagnosis confirmed that Module 1 merge exports query public data but
  serialize physical `__row_id`, causing the exact displayed KeyError before any
  request reaches Module 3. No application files were changed and no tests ran.

## 2026-07-15 - Validation, runtime smoke, and TypeScript hardening

- Mode: execution. Visible project-local agents: `test_engineer`,
  `performance_reviewer`, `frontend_implementer`, and `frontend_reviewer`.
- Fixed the documented Module 1/2 TypeScript blockers in the two Error Boundaries
  and Module 1 DataCleaning without changing emitted behavior or adding packages;
  focused review found no findings.
- Fresh validation passed: frontend contracts 32/32; focused backend 175/175;
  all four production builds; Module 1/2 lint/type checks; Module 3 type check;
  `pip check`; and diff checks. Full Module 1 backend is now 102/102 after two
  stale value-distribution expectations were corrected to assert the documented
  AI-first plus fallback-column union and exact three-row result shape.
- Portable `start.bat` opened all seven listeners, all localhost HTTP probes
  returned 200, and process cleanup left no listener residue. Bundle atomic
  replacement/cleanup was statically verified; no safe failure-injection hook
  exists, so the user archive was not mutated for that test.
- Hardened only the four frontend lockfiles with non-forced patch/minor audit
  resolutions. Clean installs, all four Vite builds, and audits now pass with
  zero vulnerabilities; lockfile review found no manifest changes or integrity
  gaps.
- Measured a generated 100k-row Module 1 preview: first page 46.75ms, warm p95
  30.96ms; global search warm p95 206.44ms; and legacy row-ID first-read 85.78ms.
  The page cache is bounded to three 200-row pages. Browser-level performance and
  interaction smoke remains unavailable because the repository has no browser
  harness or browser executable.
- Remaining risks: globally cast text search remains the primary preview hotspot;
  split lock/query/serialization metrics are not exposed; and browser interaction
  validation still requires a browser harness or manual run.

## 2026-07-15 - Module 1 preview/DQA recovery and Module 3 panel arrow

- Mode: execution. Visible project-local agents: `frontend_implementer`,
  `backend_implementer`, `contract_sync_agent`, `test_engineer`,
  `frontend_reviewer`, `backend_reviewer`, `api_contract_reviewer`, and
  `performance_reviewer`.
- Implemented active-plan Phases 1-4 and 6. Phase 5 automated validation is
  complete; browser interaction/responsive smoke and measured performance targets
  remain pending.
- Made every Module 1 DQA panel honor the shared public-column boundary, added
  type-safe predicates and sanitized errors, and stopped deterministic 4xx
  retries. Added typed/public preview-to-fill/all regressions.
- Reworked Module 1 preview for immediate opening, bounded three-page caching,
  dedupe/prefetch, cancellation/latest-wins, bounded in-flight reads, mutation
  view consumption, stable cross-page selection IDs, and nonblocking background
  loading. Added concrete preview DTOs and synchronized optional view contracts.
- Added bounded backend preview validation, deterministic identity tie-breaking,
  revision-aware metadata/count caches, precise invalidation, immutable survivor
  IDs, protected identity/reserved boundaries, upfront identity on normal
  materialization paths, schema-safe post-mutation views, and generic 500
  sanitization.
- Replaced Module 3's labeled inventory control with an accessible panel-edge
  chevron rail and rebuilt Module 1 preview chrome as a 64px identity strip plus
  responsive 36px action row.
- Final independent validation: Module 1 frontend contracts 12/12, Module 3
  contracts 10/10, focused Module 1 backend 86/86, both production builds, Python
  compilation, and diff/whitespace checks passed. Module 1 type-check reports only
  the 10 known unrelated `ErrorBoundary.tsx`/`DataCleaning.tsx` diagnostics.
- Remaining risks: legacy/uncommon intermediate tables may use lazy row-ID
  fallback; browser throttling, breakpoint/zoom interaction, and baseline-vs-after
  p95/lock/query/payload/cache measurements were not run. Existing large-chunk
  advisories remain non-failing.

## 2026-07-15 - Module 3 mapping indicators, Mekko labels, and preview collapse

- Mode: execution. Visible project-local agents: `frontend_implementer`,
  `test_engineer`, and `frontend_reviewer`.
- Threaded confirmed mapping and standard-field metadata into Spend Quality fill
  rates and added a shared locale-independent reverse mapping helper supporting
  ordered duplicate destinations and exact original display headers.
- Removed visible segment and bottom-axis text from the shared Mekko chart while
  retaining hover tooltips and adding focus/keyboard tooltip and ARIA parity.
- Added an accessible Show/Hide tables inventory toggle to Raw Data Preview;
  collapse preserves working state, releases the fixed sidebar width, and
  remeasures the column virtualizer.
- Extended Module 3 contract/helper coverage, including locale-independent
  `INVOICE DATE` matching. Final validation: `npm test` 10/10, `npm run build`
  passed, and `git diff --check -- frontend/module3` passed.
- No backend files, endpoints, or DTOs changed, so contract synchronization was
  not required. Remaining risks are the missing real-browser interaction/layout
  smoke and the existing Vite large-chunk advisory.

## 2026-07-15 - Module 1 header normalization and provenance execution

- Mode: execution. Visible project-local agents: `frontend_implementer`,
  `backend_implementer`, `contract_sync_agent`, `test_engineer`,
  `frontend_reviewer`, and `backend_reviewer`.
- Removed five orphan frontend reset calls and separated response-known Apply
  success from best-effort local cleanup so Step 4 advances without a misleading
  API failure.
- Added exact shared backend/frontend reserved provenance predicates and applied
  defensive projections/sanitization to append, preview/playground, Header
  Normalization, stats, merge, export, and transfer paths.
- Stopped creating provenance columns; added transactional all-output append
  staging/swaps with retained audit contribution metadata and rollback coverage.
- Added locked, transactional, idempotent legacy cleanup that preserves
  `__row_id`, refreshes restore metadata, and removes only-reserved artifact ghosts.
- Hardened Header Apply with complete prevalidation, one transaction, deferred
  alias learning, legacy stale-decision tolerance, and Excel route filtering.
- Added the accessible bulk KEEP checkbox with deep baseline restoration; completed
  native-control/theme styling and related labels/theme-toggle state.
- Added `test_provenance_boundary.py`; final focused suite passed 19 tests, adjacent
  preview/stats/data-loading coverage passed 40 tests, and full Module 1 backend
  validation reported 71 passed plus the same 2 known value-distribution failures.
  Module 1 production build passed; lint showed only known errors in unchanged
  `ErrorBoundary.tsx` and `DataCleaning.tsx`.
- Remaining risks: real-browser native-control/fullscreen/focus/contrast/theme and
  checkbox smoke was not available; cross-process cleanup relies on DuckDB file
  locking pending a production session-ownership design. Existing Rollup annotation
  and large-chunk warnings remain.

## 2026-07-15 - Module 2 legacy component cleanup review

- Read-only reachability review confirmed that ten legacy Module 1-named Module 2
  components/services are not mounted by the application. `Merging.tsx` remains
  referenced solely by `frontend/module2/test/feedback.contract.test.mjs`.
- No application files were changed and no validation commands were run; a future
  cleanup must update/remove that stale test, preserve the existing `Merging.tsx`
  worktree edits deliberately, then run the focused test, lint, and production build.
