# Project State

Last updated: 2026-08-02

## Current mode and objective

Execution mode handoff. Shared readers, optimized dual DuckDB sinks, recursive
archive orchestration, typed HTTP transfers, byte-bound contracts and guarded
module adapters are implemented. Production cutover remains disabled. The next
chat should resume with Module 1 runner review/parity, then Module 2 and Module 3
shared upload writers and ordered workflow cutover.

## Key findings

- Module 1 is the best orchestration base, but its current CSV/Excel paths read
  whole files and intentionally cast all source cells to `VARCHAR`. Module 2 and
  Module 3 duplicate ingestion and also stringify or re-infer values.
- The target is `backend/ingestion/` with safe detection/archive traversal,
  pluggable readers, dual immutable-raw/typed storage, stable provenance,
  structured issues, chunked DuckDB writes, progress and cancellation.
- Thin per-module adapters must preserve incompatible upload/preview/header/
  inventory/session DTOs. Routes, sessions and downstream business logic remain
  module-owned; Module 3 retains stable `COL_n` metadata and copy-on-write
  playground behavior.
- M1→M2, M1→M3 and M2→M3 currently round-trip through CSV and lose types and
  metadata. Plan a versioned Parquet/Arrow-style typed artifact plus manifest;
  retain CSV temporarily as an explicit warned fallback.
- Duplicate loader code must not be deleted until shared-reader tests, adapter
  contract parity, raw/typed hash checks, downstream workflow tests, browser
  transfer smoke tests and million-row performance gates pass.

## Validation status

No tests were run. Existing coverage is strongest in Module 1 ingestion/preview/
append/merge and Module 3 upload/playground, minimal in Module 2, and lacks
browser end-to-end transfers and native source-type preservation. The active plan
contains a 38-scenario adversarial matrix and measurable memory, throughput,
preview, cancellation, transfer and concurrency gates.

## Visible planning delegation

- `backend_architect`: shared package boundary, readers/storage/provenance and
  safe migration/deletion sequence.
- `frontend_architect`: Module 1 raw/rich preview, Module 2 preview, Module 3
  stable-key playground, restore and UI compatibility.
- `api_contract_reviewer`: exact public DTO differences, session/table identity
  and lossless cross-module transfer contract.
- `performance_reviewer`: million-row buffering/scanning/locking hotspots and
  measurable resource/latency gates.
- `test_engineer`: current test inventory, gaps and 45-case source/workflow
  validation matrix consolidated into the active plan.

## Next action

Finish and validate the interrupted real Module 1 shared runner in
`backend/module1/ingestion_adapter.py`. Add focused CSV/ZIP/XLSX route and
workflow parity tests, run the full suite, and leave the production flag off
unless canonical legacy/shared evidence passes. Then implement real Module 2 and
Module 3 shared upload writers and cut over in that order.

## Execution result — shared ingestion foundation

- Added typed ingestion contracts, deterministic serialization, stable issue
  semantics and conservative default policies.
- Added Excel-first format detection, safe bounded archive utilities, an atomic
  reader capability registry and typed direct-transfer manifest contracts.
- Reviewer blockers for ZIP-bomb bypass, Windows traversal, non-seekable stream
  consumption, duplicate archive paths, serialization, registry atomicity,
  transfer invariants and invalid limits were fixed.
- Passed: 30 focused shared-ingestion tests and 2 unified-host tests.
- No module/backend route or frontend file was changed in this slice; existing
  uploads remain active.
- Remaining future risks: actual reader/sink fidelity is not implemented;
  recursive nested-ZIP depth belongs in the future service; transfer verification
  must bind to actual Arrow/Parquet payload bytes.

## Execution result - readers, sinks, adapters, and cutover safety

- Real CSV/delimited and OOXML Excel readers preserve row zero and raw values,
  with conservative typing and structured diagnostics.
- DuckDB sink creates immutable raw/typed tables with batch writes, stable row
  identity, collision-resistant names, hashes, and failure cleanup.
- Recursive nested archive depth is enforced by `IngestionService` at the nested
  open boundary.
- Module 1 shadow and Module 2/3 compatibility adapters are disabled/legacy by
  default. Evidence is source/session/version-bound and predecessor ordering is
  verified rather than inferred from environment flags.
- Transfer manifests verify actual Arrow/Parquet payload bytes and bind contract
  version/encoding. Existing cross-module routes remain CSV-only and are not
  considered cut over.
- Validation passed: backend shared/unified tests 50; combined adapter tests 23;
  Module 1 suite 116; Module 2 suite 8; Module 3 suite 84 (plus 6 subtests).

## Current handoff state - 2026-08-02

- Typed Parquet is now the default cross-module transport for M1->M2, M1->M3
  and M2->M3. A versioned manifest binds the actual bytes, schema, row count,
  order, provenance and hashes. Destination verification occurs before session
  mutation. Explicit unsupported-typed fallback to CSV is retained and warned
  in backend responses and frontend UI.
- The optimized delimited path uses bounded spooling plus DuckDB-native loading,
  profiling and hashing. Full-stream validation catches late encodings, ragged
  rows and malformed quotes; cancellation/progress/rollback and temp cleanup are
  covered. Archive member traversal is linear and cumulative nested expansion is
  bounded.
- Legacy Excel-family dispatch exists through python-calamine for `.xls`,
  `.xlsb` and `.ods`; genuine representative binary fixtures are still missing.
- Latest focused regression result: 10/10 optimized-ingestion regression tests
  passed. Latest known full backend result before those additions: 70 passed.
  Earlier known module results: Module 1 121, Module 2 12, Module 3 87;
  unified/typed end-to-end 7. Frontend suite build, Module 1/2 lint and Module 3
  contract tests (12/12) passed. Re-run all after the interrupted M1 edits.
- Performance evidence: 1M x 20 native CSV 88.938s (time gate passed); 250k x 20
  peak RSS about 445.5 MB; progress max gap 0.284s; cancellation acknowledgement
  about 0.251s; 100/500/1000-entry archive traversal was linear. Valid 1M RSS,
  large workbook, 5M transfer/preview and concurrency gates remain unverified.
- An interrupted backend specialist added `run_module1_shared` and
  `run_module1_shared_shadow` to `backend/module1/ingestion_adapter.py`. Treat
  this as unreviewed/incomplete: no new runner-specific route/workflow tests were
  completed, and the production cutover flag must remain disabled.
- Module 2 and Module 3 typed import boundaries and cutover gates are hardened,
  but their actual production upload writers have not completed shared-pipeline
  workflow parity. Ordered production cutover therefore remains: M1, then M2,
  then M3.
- Browser automation was unavailable. Browser-equivalent typed-transfer,
  fallback-warning and session-restore smoke tests remain open.
- Temporary `bench_*` outputs were removed. The worktree contains broad ongoing
  project changes; preserve unrelated/user-owned edits and review diffs before
  committing.

## Resume checklist

1. Inspect and test the new Module 1 runner; add CSV/ZIP/XLSX route parity tests.
2. Run all backend/module/frontend validation listed in `.agent/PLAN.md`.
3. Review Module 1 publication/rollback/SSE behavior; enable only on real parity.
4. Implement and validate Module 2 shared upload publication; cut over second.
5. Implement and validate Module 3 shared upload publication; cut over third.
6. Close genuine legacy-format, browser and remaining scale/concurrency gates.
7. Remove legacy code only after rollback sign-off for all three modules.
