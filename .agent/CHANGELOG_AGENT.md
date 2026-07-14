# Agent Changelog

## 2026-07-14 - Portable startup and clean bundle automation

- Mode: execution; visible agents: `backend_implementer`, `test_engineer`, and
  `backend_reviewer`.
- Replaced the embeddable runtime with full official CPython 3.13.0 x64 including
  pip, venv, ensurepip, SSL, headers, libraries, and tools; removed the verified
  old-runtime backup after validation.
- Updated `setup.ps1` to prefer portable Python for root `.venv` creation.
- Added `start.bat` with portable PATH precedence, executable guards, conditional
  setup-once behavior, npm lifecycle suppression, and exit-code propagation.
- Added `bundle.bat` with an explicit eleven-input staging allowlist, recursive
  application exclusions, atomic final ZIP replacement, and failure cleanup.
- Validation passed: clean requirements install, `pip check`, native imports,
  Node/npm plus five `npm ls --depth=0` checks, all startup guard/exit cases,
  alternate-CWD execution, ZIP allowlist/exclusions, and preflight failure safety.
- Generated `share/ProcIP_Suite.zip` (63,345,054 bytes; 4,432 entries). No
  backend/frontend application logic, API contract, or lockfile changed.
- Remaining risks: future ranged Python dependency resolution can drift; a real
  seven-process launch and synthetic post-compression failure were not repeated.

## 2026-07-14 - Portable runtime package-alignment review

- Mode: review; no application or automation files changed.
- Confirmed portable Node 24.18.0/npm 11.16.0 is accepted by all five lockfile v3
  dependency trees; `npm ls --depth=0` passed in the root and four frontends.
- Confirmed the existing CPython 3.13.0 x64 `.venv` passes `pip check` and native
  dependencies have CPython 3.13 Windows x64 wheels.
- Confirmed corrected `bin/python-portable` still contains the minimal embedded
  Python 3.13.13 without pip or `venv`; it must be replaced with full CPython
  3.13.x x64.
- Retained the known Module 1 React 19/glide-data-grid peer exception handled by
  `--legacy-peer-deps` in `setup.ps1`.
- Remaining risk: bounded Python requirements do not guarantee byte-for-byte
  reproducible package versions on future clean installs.

## 2026-07-14 - Current application documentation reset

- Mode: execution; no visible subagents were created because no project-local documentation specialist exists and the scope was limited to two documentation files.
- Replaced the migration-heavy `.agent/STATE.md` with a concise current operational snapshot covering application structure, startup, ports, contracts, validation baseline, risks, and the optional deployment phase.
- Rewrote the root `README.md` with prerequisites, automatic dependency setup behavior, local URLs, commands, repository layout, environment guidance, architecture boundaries, and current development caveats.
- Validation: Markdown structure, documented paths, npm commands, ports, API proxy targets, health endpoints, and diff whitespace were checked against current repository configuration.

## 2026-07-14 - Automatic dependency bootstrap before development startup

- Mode: execution; no visible subagents were created because this was a small root-orchestration change.
- Added root `setup` and `predev` npm lifecycle scripts so `npm run dev` completes dependency setup before launching the seven applications.
- Extended `setup.ps1` to reconcile root npm dependencies, all four frontend dependency trees, the root Python 3.13 environment, pip, and backend/dev requirements; native installer failures now stop startup.
- Preserved Module 1's existing `--legacy-peer-deps` installation workaround for its React 19 / glide-data-grid peer-range mismatch.
- Validation: PowerShell parsing, package-script inspection, and `npm run setup` passed end to end; all npm trees were current and all declared Python requirements were satisfied.
- Remaining risk: dependency setup requires registry access when packages are missing; existing npm audit findings and Module 1's peer-range mismatch remain dependency-maintenance follow-ups.

## 2026-07-14 - Folder consolidation Phase 4

- Mode: execution.
- Reconciled tracked inventories and Flask route maps against the checkpoint/baseline; only approved generated artifacts and obsolete wrappers/docs were removed.
- Retained backend baselines: Module 1 `52 passed, 2` known failures; Module 2 `1 passed`; Module 3 `72 passed`; `pip check` passed.
- All four frontend builds passed and Module 3 frontend contract tests passed `7/7`.
- Root `npm run dev` opened all seven expected ports; four frontend pages and three backend health endpoints returned 200; spawned processes were stopped and ports closed.
- Disposable functional smokes passed upload, preview, close/reopen session restoration, and download/export for Modules 1, 2, and 3.
- Executable contract checks passed API-key fragment hydration/storage/stripping, Home/landing navigation, and transfers M1→M2, M1→M3, and M2→M3.
- Moved the Module 1 non-secret environment template to `backend/module1/.env.example` and the byte-identical Module 3 sample CSV to `backend/module3/test-data/`; removed redundant legacy README/ignore files.
- After all parity gates passed and tracked/nonignored-file guards succeeded, removed `landing-page`, `DataConsolidationAppV7`, `ProcIP_Module2-main`, `SummarizationModule`, and the empty disposable `.sessions_test` directory.
- Final integrity checks found no active old-layout references, tracked generated/runtime artifacts, secrets, dependency drift, or open suite listeners; staged and unstaged diff checks passed.
- Remaining non-blocking risks: no real browser click/new-tab run, no real cross-process socket transfer, no separate alias-store write smoke, deployment origins remain environment-specific, and existing npm/bundle/peer warnings remain.

## 2026-07-14 - Folder consolidation Phases 2 and 3

- Mode: execution.
- Moved the four independent Vite applications to `frontend/landing`, `frontend/module1`, `frontend/module2`, and `frontend/module3`; retained app-local manifests, lockfiles, configs, sources, assets, and tests.
- Preserved all eight ignored frontend environment files without staging values; corrected browser navigation to frontend origins and aligned Home links with landing port `3010`.
- Removed the tracked Module 1 `dist/index.html` and Module 3 `tsconfig.tsbuildinfo`; added `*.tsbuildinfo` to the root ignore rules.
- Rewired root `package.json` and `setup.ps1`; added consolidated `README.md`, `ENVIRONMENT.md`, and a relocated Module 2 README.
- Retired obsolete Module 1/Module 3 wrapper manifests and the stale legacy Module 2 README after replacement validation.
- Contract review confirmed unchanged endpoint, method, request/response, DTO, and Vite proxy behavior; backend ports remain `3001`, `5000`, and `3005`.
- Deterministic installs completed from retained lockfiles; Module 1 required the existing `--legacy-peer-deps` workaround. All four lockfiles remained byte-identical.
- Validation: all four frontend builds passed after final fixes; Module 3 frontend contract tests passed `7/7`; generated-artifact/ignore/inventory checks and staged/unstaged `git diff --check` passed.
- Remaining risks: Phase 4 browser/integration smoke and legacy generated-directory cleanup; deployment-specific frontend origins; pre-existing npm audit, bundle-size, and React peer-range warnings.

## 2026-07-14 - Folder consolidation Phases 0 and 1

- Mode: execution.
- Created migration branch `chore/consolidate-backend-frontend` from `d0922dc`; existing worktree changes were limited to living-context files.
- Confirmed no suite listeners were active; inventoried 288 tracked files across seven apps, ignored environment filenames, assets, ports, route maps, and session locations.
- Recorded pre-move parity: Module 1 `52 passed, 2` known failures; Module 2 `1 passed`; Module 3 `72 passed`; all four frontend builds passed; Module 3 frontend tests `7 passed`; `pip check` passed.
- Moved 157 tracked backend files to `backend/module1`, `backend/module2`, and `backend/module3`; inventory parity is exact.
- Repaired file-relative dotenv paths, module-isolated session resolution, Module 2 FX fallback, Module 1 alias-bundle resolution, configurable/validated cross-backend base URLs, and Module 1 root-level pytest collection.
- Contract review found no endpoint, method, request, response, or frontend-client changes.
- Post-move validation: route counts `75/23/37`; all health checks 200; backend suites retained baseline; `pip check` and `git diff --check` passed.
- Review failures fixed: Module 2 `.env` filename preservation, dotenv-before-database initialization, shared session-root isolation, whitespace-only session settings, and rejection of URL query/fragment components.
- Remaining risks: no live-socket or real cross-backend HTTP transfer smoke; legacy parents retain ignored caches/logs; root scripts/docs await Phase 3; frontend relocation awaits Phase 2.
