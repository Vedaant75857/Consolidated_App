# Project State

Last updated: 2026-07-14

## Portable startup and sharing automation complete

- Root `start.bat` prepends the portable Node/Python runtimes, validates required
  executables, runs setup exactly once only when root dependencies are absent, and
  launches `dev` with npm lifecycle scripts suppressed to avoid duplicate setup.
- Root `bundle.bat` builds from an explicit eleven-input staging allowlist, excludes
  development metadata/caches/dependencies, retains runtime-owned portable npm,
  and replaces the final archive only after successful temporary compression.
- `setup.ps1` now prefers `bin/python-portable/python.exe` when creating `.venv`.
- `bin/python-portable` is full official CPython 3.13.0 x64 with pip, venv,
  ensurepip, SSL, headers, libraries, and tools; the obsolete embedded-runtime
  backup was removed after independent validation.
- `share/ProcIP_Suite.zip` contains exactly the approved inputs (63,345,054 bytes;
  4,432 entries) with no application/root dependency trees or forbidden metadata.
- No backend/frontend application logic or API contract changed.

Validation passed for portable Python clean dependency installation and native
imports, portable Node/npm and all five dependency trees, every startup guard and
exit-propagation case, alternate-CWD execution, bundle allowlist/exclusions, and
failure cleanup. Remaining narrow gaps: no repeated real seven-process launch and
no injected post-compression failure. Python requirement ranges can still resolve
to newer compatible versions in future installs.

## Current status

The ProcIP suite is consolidated and operational. It has three independent Flask
backends and four independent Vite frontends under the root `backend/` and
`frontend/` directories. The previous application parent directories have been
retired after relocation and parity validation.

The current branch is `chore/consolidate-backend-frontend`, created from checkpoint
`d0922dc`. Folder-consolidation phases 0 through 4 are complete. Deployment
readiness remains an optional separate phase.

## Application structure

```text
backend/
  module1/          Data Consolidation / Data Stitcher backend
  module2/          Data Normalizer backend
  module3/          Summarization / Spend Analyzer backend
frontend/
  landing/          Suite landing page
  module1/          Data Stitcher frontend
  module2/          Data Normalizer frontend
  module3/          Spend Analyzer frontend
```

The three Flask services remain separate processes. The four Vite applications
retain separate manifests, lockfiles, source trees, and dependency graphs.

## Development startup

`npm run dev` is the canonical full-suite command. Its `predev` lifecycle runs
`npm run setup`, which invokes `setup.ps1` before any application starts.

Setup currently performs:

- root npm dependency reconciliation;
- dependency reconciliation for all four frontend applications;
- root `.venv` creation and Python 3.13 validation;
- pip upgrade;
- installation of the combined backend runtime and development requirements;
- immediate failure when an installer returns a nonzero exit code.

Module 1 uses `npm install --legacy-peer-deps` because React 19 is outside
`@glideapps/glide-data-grid`'s declared peer range. Other npm trees use normal
`npm install` resolution.

## Local runtime map

| Process | Port | Notes |
| --- | ---: | --- |
| Landing frontend | 3010 | Links to the three module frontends |
| Module 1 backend | 3001 | Flask; `/api/health` |
| Module 1 frontend | 3002 | Proxies `/api` to port 3001 |
| Module 2 backend | 5000 | Flask; `/api/health` |
| Module 2 frontend | 3003 | Proxies `/api` to port 5000 |
| Module 3 backend | 3005 | Flask; `/api/health` |
| Module 3 frontend | 3004 | Proxies `/api` to port 3005 |

## Runtime and contract boundaries

- The root `.venv` is the canonical local Python environment.
- Backend dotenv resolution is module-local: Module 1 `.env.local`, Module 2
  `.env`, and Module 3 `.env.local`.
- Session databases are module-isolated. Module-specific session path variables
  take precedence over the legacy shared `SESSION_DB_DIR` setting.
- Frontends call same-origin `/api`; Vite proxies route local requests to the
  matching backend.
- Cross-module navigation and transfers preserve the browser-session API key with
  a temporary URL fragment handoff and remove the fragment after hydration.
- Module 3 playground operations use stable column keys and copy-on-write tables.
  The Module 3 Apply endpoint and Apply UI remain intentionally absent.

## Latest validation baseline

- Automatic dependency setup: passed end to end on 2026-07-14.
- Root Python: 3.13; combined requirements installed; `pip check` previously passed.
- Module 1 backend: 52 passing tests and 2 known pre-existing
  value-distribution failures.
- Module 2 backend: 1 passing test.
- Module 3 backend: 72 passing tests.
- All four frontend production builds passed after consolidation.
- Module 3 frontend contract tests: 7 passing.
- Full root startup previously opened all seven expected listeners; all frontend
  pages and backend health endpoints returned HTTP 200.
- Disposable upload, preview, session restoration, export, API-key handoff, and
  cross-module transfer smokes passed during consolidation validation.

## Known risks and follow-ups

- npm audit findings remain across the root and frontend dependency trees.
- Module 1's React/glide-data-grid peer-range mismatch remains and is handled by
  the setup compatibility flag.
- Existing frontend large-bundle warnings remain non-failing.
- Real browser click-through coverage and real cross-process socket transfer tests
  remain useful manual follow-ups.
- Production frontend origins and `/api` routes are deployment-specific.
- The Flask/DuckDB backends require persistent writable storage. Hosting them on
  stateless serverless functions requires a separate persistence, upload,
  concurrency, and long-running-job design.
- Dependency bootstrap can require internet access and makes `npm run dev` slower
  because declared dependencies are reconciled on every startup.

## Next optional phase

Deployment readiness may create four independently rooted frontend deployments and
three persistent backend services. A Vercel Functions backend migration must not
begin until external persistence and upload/runtime constraints are designed.
