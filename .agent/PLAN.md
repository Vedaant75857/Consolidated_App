# Active Plan — Shared Data Ingestion Pipeline

Status: in progress; shared ingestion and typed transfers implemented, production cutover still disabled.
Date: 2026-08-02

## Objective

Build one shared ingestion pipeline under `backend/`, using Module 1's dispatch,
progress, raw-preview, DuckDB-native loading, and batched-transaction patterns as
the operational base. Extend that base so source values, types, dates, formulas,
headers, row order, and provenance are not silently lost or mutated. Preserve
Module 1 raw preview, Module 2 normalization, Module 3 data preview/playground,
append/merge/header-normalization behavior, and all cross-module transfers.

This is not a direct move of Module 1's loader. Its current CSV and Excel paths
read whole files and coerce cells to `VARCHAR`; Module 2 and Module 3 also coerce
or re-infer values. Those behaviors do not meet the requested fidelity or scale.

## Affected areas

- Shared host and session routing: `backend/unified_app.py`, module-scoped session
  databases/locks, and the namespaced `/api/module1|2|3` surfaces.
- Module 1: `backend/module1/data_loading/`,
  `routes/data_loading_routes.py`, `routes/preview_routes.py`,
  `routes/merging_routes.py`, `shared/db/table_ops.py`, append/merge/header and
  provenance tests, plus raw/rich preview frontend clients and stores.
- Module 2: inline source parsing, upload/import/preview/header selection and
  transfer code in `backend/module2/app.py`, `backend/module2/db/`, normalization
  logic, and Module 2 upload/inventory/preview frontend components.
- Module 3: `backend/module3/services/upload/file_loader.py`, upload/import routes,
  `_column_metadata`, mapping and playground services, Step 2 preview and rich
  playground clients/types.
- Cross-module handoffs: M1→M2, M1→M3, and M2→M3 currently serialize complete
  datasets as CSV, losing types, null/empty distinctions, precision, dates, and
  workbook/source metadata.

## Chosen architecture

Add a package-qualified `backend/ingestion/` package:

- `models.py`: request, source asset, table artifact, ordered column schema,
  workbook/cell provenance, issue, statistics, and result models.
- `detect.py`: extension, MIME and magic/container classification; known Excel
  extensions take precedence over generic ZIP detection.
- `archives.py`: safe, bounded, streaming archive traversal.
- `readers/csv_reader.py` and `readers/excel_reader.py`; add a typed
  Parquet/Arrow reader for cross-module transfer.
- `schema.py`: source-aware type profiling, ambiguity reporting, explicit
  overrides, and compatibility views.
- `duckdb_sink.py`: chunked/native writes, immutable raw representation, typed
  working tables, stable row identity, atomic staging/swap and rollback.
- `service.py`: Module-1-derived dispatch, progress, cancellation, batching and
  final publication.
- `errors.py` and `provenance.py`: stable diagnostic codes and source lineage.

Keep module routes, sessions, registries, DTO builders and downstream business
logic in their existing modules. Add thin `ingestion_adapter.py` files per
module. Module 3's adapter alone owns its stable `COL_n` keys and
`_column_metadata`; Module 1 retains raw/processed preview and Apply targets;
Module 2 retains caller-created session IDs and normalization tables.

## Fidelity and error policy

- Store an immutable raw representation and a typed working representation.
  Do not uppercase, trim, collapse blanks, infer header rows, or normalize values
  during ingestion. Existing explicit downstream cleaning remains explicit.
- Preserve integers, decimal precision/scale, floating values, booleans, text,
  blanks, errors, dates, times and timestamps when the source identifies them.
  Values outside safe typed ranges remain exact strings with diagnostics.
- CSV is lexically typed by nature: decode reversibly, preserve lexical values,
  and create typed working columns only through deterministic full-column or
  bounded two-phase profiling. Ambiguous/mixed columns remain text with an
  explicit warning and override path. Never use silent replacement decoding.
- Excel date interpretation must use the cell kind, number format and workbook's
  1900/1904 date system. Store the typed value plus original serial/format/date
  system provenance. Never guess ambiguous date strings without reporting it.
- For formulas, retain formula text in sparse provenance and use a cached result
  only when present and trustworthy. Missing/stale caches, formula errors and
  external links produce visible diagnostics; the backend does not recalculate
  workbooks. Macros are never executed.
- Preserve merged-range geometry. The anchor retains the source value and other
  cells remain blank unless a documented module adapter explicitly expands it.
- Internal results include tables, ordered schema/stable keys, raw/typed table
  names, source/archive/sheet identity, row/column counts, issues, timing/bytes,
  and `partial`. Recoverable file/sheet problems may yield explicit partial
  success; archive, transaction or integrity failures roll back. No exception is
  logged and discarded without a response issue.

## Compatibility contracts to preserve

- Module 1 `/upload` remains SSE with reading/parsing/file/committed/inventory and
  terminal done/error events; its final result remains
  `{sessionId,inventory,previews,warnings}`. Raw preview, header selection,
  bulk delete, paginated rich preview, stable `__row_id`, Apply targets, page
  cache and stale-request cancellation remain compatible.
- Module 2 `/upload` retains the caller-supplied `sessionId` and JSON
  `{message,inventory}`. Its plain preview DTO, `rowIndex/customNames` header DTO,
  table selection and normalization behavior remain compatible.
- Module 3 `/upload` remains JSON
  `{sessionId,columns,fileInventory,previews,warnings}`. Its playground continues
  to use stable `{key,displayName,dataType}` columns, value maps keyed by
  `columnKey`, copy-on-write tables, bounded history, pagination/virtualization,
  and no Apply into Step 3+.
- Table keys are opaque stable identities across inventory, preview, mutation,
  restore and transfer. Display headers never become operation identity.
- Preserve namespaced public paths, module-scoped sessions and current new-tab
  restore contracts. Add backward-compatible `issues`, `partial` and diagnostic
  codes; do not atomically replace all public DTOs.

## Phased implementation plan

### 0. Characterize and freeze current behavior

- Create golden fixtures and exact contract snapshots for all three upload,
  inventory, raw/processed preview, header, delete, restore and error responses.
- Record source-cell hashes, row/column order, table keys, DuckDB schemas, exact
  display headers and existing warnings for representative formats.
- Cover Module 1 append/merge/header normalization and Apply targets, Module 2
  normalization, Module 3 mapping/playground/refresh-inventory, and all transfers.
- Define a versioned issue catalogue and explicit product policies for hidden
  data, merged cells, formulas/caches, dates, partial success and resource limits.

### 1. Build shared core behind a feature flag

- Implement spooled-file upload, safe classification/archive traversal, reader
  plugins, dual raw/typed artifacts, provenance and structured issues.
- Parse outside the session lock where safe; use short writer critical sections,
  per-file staging, atomic metadata publication, cleanup on failure, retry
  idempotency and cancellation checked per batch.
- Shadow-run against Module 1 and compare raw hashes, typed schema, row counts,
  table identity, diagnostics, time and peak RSS. Do not delete legacy code.

### 2. Cut over Module 1 first

- Route Module 1 upload through its adapter while preserving SSE and all DTOs.
- Retain header-row rebuilding over immutable raw data and compatibility views for
  downstream SQL that currently expects text.
- Validate raw/step/rich preview, append, merge, header normalization, data
  quality, downloads, Apply/undo/redo and session restore before defaulting on.

### 3. Cut over Module 2, then Module 3

- Migrate Module 2 upload/import through its adapter; preserve its session
  ownership, plain preview/header DTOs and active-table normalization semantics.
- Migrate Module 3 upload/import after its stable metadata adapter is complete;
  validate mapping, Step 2 preview, stable-key playground, copy-on-write
  operations, downstream invalidation and spend-quality calculations.
- Keep each legacy loader selectable for rollback until contract and workflow
  parity passes on representative real files.

### 4. Make transfers lossless

- Add a versioned typed artifact protocol, preferably Parquet plus a schema and
  provenance manifest; Arrow streaming may be used where it is operationally
  simpler. The destination imports through the shared pipeline.
- Dual-accept typed artifacts and legacy CSV, but label CSV fallback as lossy and
  return warnings. Do not make in-process direct table copy the public contract.
- Validate M1→M2, M1→M3 and M2→M3 schema/type/value hashes, row counts, session
  restoration and downstream workflows before preferring the typed path.

### 5. Optimize previews and large-data operations

- Persist ingest-time row counts, column profiles and bounded reservoir samples;
  avoid per-column/full-table rescans in Module 3 inventory and state restore.
- Use keyset pagination on stable row IDs where possible, bounded payloads,
  cached stats and one-scan distinct/filter queries. Keep frontend row/column
  virtualization and chunk eviction.
- Bound session connections, file descriptors, temp disk, WAL growth and cached
  preview data; close evicted Module 1 connections and add active-operation
  accounting.

### 6. Remove redundant ingestion implementations

- Search all imports and verify the rollback flag is no longer needed.
- Remove only loader-owned code: Module 1 `data_loading/file_loader.py` after its
  reusable DTO service has been separated, Module 3
  `services/upload/file_loader.py` and the empty upload folder, and Module 2's
  `SOURCE_*`/`_load_source_file_to_session` logic.
- Retain module route files, DTO adapters, registry/session code and downstream
  append/merge/normalization/playground/mapping features. Remove old tests only
  after equivalent shared-reader and adapter contract tests exist.

## Adversarial validation matrix

Every case must assert raw-cell/order fidelity, typed schema/value behavior,
structured warnings/errors, rollback/partial policy and relevant previews:

1. Empty, zero-byte, header-only and all-blank files.
2. Wrong extension/MIME/magic and XLSX misclassified as ZIP.
3. `.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xltx`, `.xltm`, CSV and mixed archives.
4. Corrupt/truncated workbooks, CRC failures and password/encrypted files.
5. ZIP traversal, absolute paths, symlinks, bombs, high compression ratios,
   excessive entries, duplicate paths and nested-depth overflow.
6. Empty, hidden and very-hidden sheets; hidden/filtered rows and columns.
7. Merged header/data cells and huge/sparse merged ranges.
8. Formula cells with valid cached results, no cache and stale cache.
9. Formula errors, shared/dynamic-array formulas, volatile formulas and external
   links; macros are retained only as source provenance and never executed.
10. 1900 serial system including serial 60, 1904 system and pre-1900/out-of-range
    values.
11. Ambiguous DMY/MDY, locale dates, leap days, DST gaps/folds, time zones,
    date-only/time-only and mixed date/text columns.
12. Numeric-looking identifiers, leading zeros, date-looking IDs and strings
    beginning with `=`.
13. Integers beyond JavaScript-safe and 64-bit ranges, decimals beyond DuckDB
    precision, scientific notation, negative zero and infinity/NaN.
14. Locale decimal/thousands separators, currency/percentage formats and
    parenthesized negatives.
15. Native booleans versus `TRUE`/`FALSE` strings.
16. Null, blank, empty quoted string, whitespace and common NA-token distinctions.
17. Duplicate, blank, whitespace-only, case-colliding, Unicode/RTL/emoji, very
    long, reserved and SQL-metacharacter headers.
18. Multi-row titles/preambles and zero-based custom header selection.
19. CSV UTF-8 BOM, UTF-16 LE/BE, Windows-1252 and Latin-1.
20. Invalid byte sequences; no silent replacement characters.
21. Delimiter/quote/escape/CRLF variants, embedded delimiters/newlines and
    malformed quotes.
22. Ragged rows, unexpected extra columns, embedded NUL and huge single fields;
    never truncate without a row/column issue.
23. Duplicate file/sheet names and case collisions producing deterministic keys.
24. Unsupported charts, images, rich text, comments, notes and hyperlinks with
    an explicit retained/ignored policy.
25. Sparse sheets near Excel's maximum dimensions and CSV/Excel with millions of
    rows or thousands of columns.
26. Partial multi-file success, transaction failure and disk exhaustion.
27. Client disconnect, cancellation, retry/idempotency and terminal SSE errors.
28. Concurrent uploads/deletes/previews in the same and different sessions.
29. Schema drift across appended/merged files: missing/new/case/type changes.
30. Append with disjoint/overlapping/duplicate columns, nulls, numbers and dates.
31. Merge version/group CSV/XLSX/ZIP round-trips and atomic rollback.
32. Module 1 raw preview/header mutation and rich preview Apply/undo/redo.
33. Module 2 normalization, reset, select-table and normalized transfer behavior.
34. Module 3 Step 2 preview, stable-key filters/sorts/edits/delete/type/pivot and
    bounded undo/redo without mutating the source table.
35. Calculation divide-by-zero, invalid casts, missing/reserved identifiers and
    cleanup of partial artifacts.
36. M1→M2, M1→M3 and M2→M3 typed round trips, session restore and namespace
    routing with no bare route collisions.
37. Expired/invalid sessions, unsupported input and per-table lazy preview errors
    remain visible and retryable.
38. Deterministic final hashes after ingest→preview/header→append/merge or
    normalization→transfer→download.

## Performance and reliability gates

- 1M×20 CSV completes within 120 seconds on the documented reference host;
  large XLSX/XLSB completes within 180 seconds, with format-specific exceptions
  documented rather than hidden.
- Peak RSS is bounded by `2 × largest active entry + 512 MB`; no archive-sized
  duplicate Python object and no Python per-cell loop above 100k rows.
- ZIP expansion ratio is capped at 100×, nesting at 2, and entry/expanded-byte
  limits are configurable and reported with stable codes.
- Progress is emitted at least every 2 seconds; cancellation is acknowledged
  within 1 second and leaves no visible partial table.
- A 5M-row first preview/filter response has p95 ≤500 ms and p99 ≤1 s, payload
  ≤1 MB, and no more than one data scan per request under DuckDB profiling.
- A 100-column/5M-row profile uses at most two full scans per table.
- A 5M-row transfer stays within 512 MB process RSS, is no slower than 2× local
  export, and preserves row/schema/type hashes.
- Eight concurrent sessions remain bounded in RSS/file descriptors, produce no
  DuckDB lock errors and add no more than 20% p95 slowdown on the reference host.

## Cutover acceptance criteria

- All supported inputs either load with exact documented fidelity or return a
  visible, actionable, location-aware diagnostic; no parse error is swallowed.
- Raw values, typed values, exact display headers, row/column order, table keys
  and provenance survive upload and transfer under the documented policy.
- All current public module endpoints and frontend DTOs remain compatible during
  migration; Module 1 raw preview, Module 3 playground and module restoration
  behave unchanged except for approved typed fidelity improvements.
- Append, merge, header normalization, Module 2 normalization, Module 3 mapping
  and other downstream calculations pass characterization and regression suites.
- Cross-module transfers use typed artifacts by default and preserve schema/value
  hashes; CSV remains only a warned rollback path.
- Legacy ingestion folders/functions are removed only after import scans, feature
  parity, performance gates, browser transfer smoke tests and rollback sign-off.

## Principal risks and open decisions

- Calamine may not expose all formula, cached-value, merged-range, style and date
  metadata. Benchmark targeted openpyxl OOXML enrichment and document legacy
  XLS/XLSB limitations before selecting reader fallbacks.
- Typed working tables can break existing SQL written for `VARCHAR`. Use
  compatibility views/adapters and migrate queries incrementally rather than
  changing all downstream tables at once.
- Exact memory/time limits need a documented reference machine and representative
  production files before they become hard defaults.
- Whether hidden rows/sheets are included, formula caches are accepted when
  workbook calculation is stale, and partial multi-file success is the default
  are product policies that must be finalized in Phase 0.

## Planned validation commands

- `.venv\Scripts\python.exe -B -m pytest backend/module1/tests -q`
- `.venv\Scripts\python.exe -B -m pytest backend/module2/tests -q`
- `.venv\Scripts\python.exe -B -m pytest backend/module3/tests -q`
- Focused shared-ingestion, adapter-contract, unified-host, benchmark and browser
  transfer suites added during execution; run narrow suites before full builds.

## Execution update — 2026-08-01

Completed the smallest safe Phase 0/1 foundation slice without changing any
module route, frontend contract, or existing loader:

- Added `backend/ingestion/` contracts for source/table/schema/provenance/issues,
  deterministic JSON serialization, policy defaults, statistics and results.
- Added Excel-first extension/magic classification for delimited text, supported
  Excel families, ODS, ZIP and Parquet.
- Added bounded archive inspection and reads with traversal, Windows path,
  symlink, duplicate/case-collision, encryption, entry-count, expanded-size,
  compression-ratio and finite-limit protection.
- Added an atomic reader capability registry. Readers remain capability metadata
  in this slice and return structured unsupported errors until real parsers are
  wired.
- Added a typed direct-transfer manifest with invariants, deterministic digest,
  artifact checksum fields and tamper verification. No CSV/HTTP transfer was
  added to the shared path and existing transfer routes remain unchanged.
- Added 30 focused characterization/security/contract tests. The shared suite
  and unified-host regression suite pass.

The shared platform is the union of generic capabilities from all modules; Module
1 is its engineering base, not an upload gateway. Every module remains an upload
entry point after adapters are introduced.

### Next executable item

- Implement actual CSV/delimited and Excel readers plus chunked DuckDB raw/typed
  sink, then add the Module 1 adapter and shadow comparison behind a disabled-by-
  default feature flag.
- Bind typed transfer verification to actual Arrow/Parquet payload bytes when the
  transfer sink is implemented.
- Enforce recursive nested-archive depth in the service that opens nested
  archives; the current `max_depth` protects member path depth only.

## Execution update - 2026-08-01 (readers, sinks, adapters, and gates)

Completed the safe implementation portion of the next slice:

- Added real delimited and OOXML Excel readers that retain the first source row,
  preserve immutable raw rows, infer typed columns conservatively, and surface
  formula, hidden-data, merge, decoding, and ambiguity diagnostics.
- Added collision-resistant immutable raw and typed DuckDB tables, stable row
  IDs, batch writes, source/typed hashes, validation, and service cleanup on
  later sink, archive, or cancellation failure.
- Added recursive archive service traversal with nested-archive depth enforced
  where nested payloads are opened.
- Added disabled-by-default Module 1 shadow routing and legacy-default Module 2
  and Module 3 adapters. Parity evidence is bound to the exact payload/session
  and verified predecessor state; environment flags alone cannot authorize an
  ordered cutover.
- Bound transfer manifests to actual Parquet/Arrow bytes and to contract version
  and encoding. Tampered payloads and metadata downgrades fail verification.

Production cutover is intentionally not complete. Existing M1->M2, M1->M3, and
M2->M3 HTTP routes still serialize CSV and destination routes do not yet accept
and verify a typed manifest/payload. The adapters therefore remain legacy by
default and cannot be declared parity-complete.

### Next executable item

- Add destination typed-transfer endpoints/adapters that verify the manifest
  and Parquet/Arrow bytes before publication, with CSV retained only as a warned
  rollback path.
- Build canonical route/workflow parity evidence covering raw/typed hashes,
  schemas, row counts, stable identities, Module 1 SSE, Module 2 normalization,
  Module 3 `COL_n` metadata/playground, and all cross-module transfers.
- Run browser transfer smoke tests and representative performance/RSS gates;
  only then record passing evidence and enable Module 1, Module 2, then Module 3.

## Handoff checkpoint - 2026-08-02

### Completed

- Real delimited readers now stream/spool input, validate the full file for
  encoding, malformed quotes and late ragged rows, preserve lexical raw values,
  and use conservative typed inference. Real OOXML readers and calamine-backed
  `.xls`/`.xlsb`/`.ods` dispatch are present with explicit format limitations.
- DuckDB ingestion now publishes collision-resistant immutable raw and typed
  tables, uses native CSV loading/profiling/hashing, reports heartbeat progress,
  supports prompt cancellation, rolls back staging state, and carries artifact
  hashes/provenance through both native and materialized paths.
- Archive traversal is single-pass/linear, bounded and spooled. Nested archive
  depth and cumulative expanded-byte limits are enforced by the service that
  recursively opens members.
- Typed Parquet transfer with a versioned, byte-bound manifest is the preferred
  path for M1->M2, M1->M3 and M2->M3. Destinations verify payload, schema,
  row count, ordering and provenance before mutation. CSV remains an explicit,
  warned compatibility fallback.
- Module adapters and central cutover gates remain legacy-default/disabled.
  Frontends expose lossy CSV fallback warnings.
- Permanent optimized-ingestion regressions were added in
  `backend/tests/test_optimized_ingestion_regressions.py`; its 10 focused tests
  passed before handoff.
- Performance evidence: 1M x 20 native CSV completed in 88.938 seconds; 250k x
  20 peak RSS was about 445.5 MB; progress gap was 0.284 seconds; cancellation
  acknowledgement gap was about 0.251 seconds; archive traversal scaled
  linearly. The 1M-row RSS gate was not measured correctly and remains open.
- The unified frontend build, Module 1/2 lint, Module 3 contract tests, and the
  previously recorded backend/module suites passed. Temporary benchmark files
  were removed.

### Remaining work, in order

1. **Finish and validate the real Module 1 runner.** An interrupted specialist
   added `run_module1_shared` and `run_module1_shared_shadow` in
   `backend/module1/ingestion_adapter.py`, but did not finish route/workflow
   parity tests. Review the publication semantics (registry, raw table,
   `rawArray`, headers, warnings, SSE progress, rollback) and add CSV, ZIP and
   XLSX route tests before using its evidence.
2. **Keep Module 1 disabled until canonical parity passes.** Exercise raw and
   processed previews, custom header selection, append, merge, header
   normalization, data-quality actions, downloads, Apply/undo/redo and session
   restoration against legacy and shared sessions. Only then enable Module 1's
   flag in a controlled environment; retain rollback.
3. **Implement real shared upload writers for Module 2 and Module 3.** Existing
   adapters/import boundaries and gates are hardened, but their production
   upload paths still require full shared-session publication and workflow
   parity. Cut over Module 2 only after normalization/reset/select/transfer
   parity, then Module 3 only after `COL_n` metadata, mapping, playground,
   copy-on-write/history and downstream calculation parity.
4. **Run the final complete validation after the interrupted M1 edits.** At a
   minimum run `backend/tests`, all three module test suites, unified/typed
   transfer end-to-end tests, frontend suite build, Module 1/2 lint and Module 3
   contract tests. Add focused tests for the new M1 runner first.
5. **Close remaining scale evidence.** Measure valid 1M-row peak RSS, large
   XLSX/XLSB time/RSS, 5M-row typed-transfer RSS/latency, preview/filter p95/p99,
   profiling scan counts, disk/WAL behavior and eight-session concurrency.
6. **Use representative real legacy-binary fixtures.** Current legacy-family
   coverage exercises calamine dispatch using OOXML-compatible bytes renamed to
   `.xls`, `.xlsb` and `.ods`; add genuine files for each format and document
   formula/cache/hidden-row fidelity.
7. **Run browser-equivalent transfer/workflow smoke tests.** Browser automation
   was unavailable in this session; verify all three typed handoffs, visible CSV
   fallback warnings and restored sessions through the built frontend.
8. **Remove legacy loaders only after all three ordered cutovers and rollback
   sign-off.** Do not enable flags or delete compatibility paths merely because
   unit/contract tests pass.
