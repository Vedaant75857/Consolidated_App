# 2026-08-01 — Unified Backend Host

- Implemented one thin Python host at `backend/unified_app.py` (port 8000) that
  mounts existing Module 1/2/3 applications under namespaces; no large merged
  `app.py` was introduced.
- Added `backend/unified_loader.py` to isolate legacy import names, plus focused
  host/collision and Module 3 upload-to-inventory regression tests.
- Updated transfer URLs, Vite proxy rewrites, root startup/rollback scripts, and
  startup documentation.
- Validation passed: host tests, M1 105 tests, M2 1 test, M3 79 tests, live host
  health smoke, and all frontend builds.
- Review found and the implementation fixed a Module 3 lazy `services` import
  regression. Remaining risk: process-global dotenv/environment configuration
  and the transitional legacy import-alias mechanism need future hardening.

# 2026-08-01 — Unified Frontend Suite Foundation

- Added `frontend/suite` with a single React root, lazy landing/module routes,
  suite navigation, loading/error/unknown-route handling, and module-specific
  theme-provider wrappers.
- Migrated frontend browser API calls to explicit namespaced module bases;
  added suite startup/build commands, legacy frontend rollback, suite setup,
  and updated local documentation.
- The thin unified backend now serves built suite static files and non-API SPA
  fallbacks; `backend/tests` passed (2 tests).
- Frontend suite build was attempted but blocked by absent suite dependencies
  (`tsc` unavailable). Review confirmed provider/route fixes but identified
  unscoped global module CSS as the remaining cutover blocker. React 18/19
  convergence, a reproducible lockfile, and browser/upload/transfer smoke tests
remain open.

# 2026-08-01 — Portable Zscaler CA configuration

- Added `backend/tls_config.py`, a dependency-free bootstrap for the unified
  Python backend. It honors `PROCIP_CA_BUNDLE` and standard CA variables and
  detects the Bain Windows PEM only when it exists; verification stays enabled.
- Added focused tests and documented the repo-vs-external-tool boundary in
  `README.md` and `ENVIRONMENT.md`.
- Validation passed: `backend/tests/test_tls_config.py` and
  `backend/tests/test_unified_app.py` (5 tests).

# 2026-08-01 — Shared ingestion foundation

- Added `backend/ingestion/` as a non-invasive foundation: typed contracts,
  source classification, structured issues, archive safety, reader capability
  registry and typed direct-transfer manifest.
- Hardened archive handling against ZIP bombs, traversal (including Windows
  paths), symlinks, duplicate/case-colliding entries, encrypted members and
  invalid/non-finite limits; reads use bounded chunks.
- Hardened non-seekable detection, deterministic JSON serialization, registry
  atomicity, fatal issue defaults and transfer manifest invariants/tamper checks.
- Added `backend/tests/test_shared_ingestion_foundation.py`.
- Validation passed: shared foundation 30 tests; unified host 2 tests.
- Backend review approved the foundation after blocker fixes. No module routes,
  existing loaders or frontend contracts were changed; real readers, DuckDB
  sink, adapters and module cutover remain future phases.

# 2026-08-01 - Shared ingestion readers, sinks, and gated adapters

- Implemented real delimited and OOXML Excel readers, immutable raw plus typed
  DuckDB sinks, recursive archive ingestion-depth enforcement, and cleanup on
  later failure or cancellation.
- Added disabled-by-default Module 1 shadow routing and legacy-default Module 2
  and Module 3 adapters. Parity evidence is bound to payload/session/version;
  verified predecessor state is required for ordered cutover.
- Bound transfer manifests to actual Parquet/Arrow payload bytes, contract
  version, and encoding; added tamper and downgrade regression coverage.
- Fixed reviewer findings covering partial publication, oversized decimals,
  table-name collisions, unsafe live-session shadowing, stale cross-file parity
  evidence, module import alias collisions, and manifest metadata tampering.
- Added shared boundary, reader/sink/service, adapter, transfer, and unified-host
  tests. Validation passed: backend tests 50; adapter tests 23; Module 1 116;
  Module 2 8; Module 3 84 (plus 6 subtests).
- Production module and typed-transfer cutover remains disabled: current transfer
  routes still use CSV and lack destination manifest verification/parity.
