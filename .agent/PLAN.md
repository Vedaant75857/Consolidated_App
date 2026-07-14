# Active Plan - Portable Startup And Clean Bundle Automation

Mode: execution complete

Last updated: 2026-07-14

## Goal

Add root `start.bat` and `bundle.bat` automation for portable Windows startup and
clean sharing without changing backend or frontend application logic.

## Verified Scope And Reconciliation Findings

- Root orchestration is defined by `package.json` and `setup.ps1`.
- `bin/node-portable/node.exe` and `npm.cmd` exist and report Node 24.18.0 and npm
  11.16.0.
- The folder rename is complete: required `bin/python-portable` now exists.
- The current embedded Python reports 3.13.13 but has no `pip` or `venv` module.
  Because `.venv` is excluded from the bundle, it cannot bootstrap a clean shared
  copy without a system Python.
- `package.json` has `predev: npm run setup`; plain `npm run dev` would therefore
  run setup unconditionally after the batch file's conditional check.
- All requested archive inputs exist except `start.bat`, which is the planned new
  file. `bundle.bat` is intentionally absent from the archive allowlist.
- Frontend directories currently contain nested development `node_modules` trees.
  `bin/node-portable/node_modules`, however, is runtime-owned and required by
  portable `npm.cmd`.
- The existing `.venv` uses CPython 3.13.0 x64 and passes `pip check`. Installed
  native dependencies have CPython 3.13 Windows x64 wheels, so a full official
  CPython 3.13.x x64 runtime is aligned.
- Node 24.18.0/npm 11.16.0 satisfies the engine constraints in all five lockfile
  version 3 dependency trees. Root and all four frontend `npm ls --depth=0`
  checks pass.
- Module 1's React 19/glide-data-grid peer mismatch remains known and is already
  handled by the scoped `--legacy-peer-deps` branch in `setup.ps1`.
- Python requirements use bounded ranges rather than exact pins. Compatibility is
  verified, but exact future clean-install versions are not locked.

## Preconditions

1. [x] Rename/correct `bin/pyton-portable` to the required
   `bin/python-portable` path.
2. [x] Replace the embedded distribution with full official CPython 3.13.x x64 so
   it can create a Python
   3.13 virtual environment and install requirements (`venv` and pip available).
3. [x] Retain runtime-owned `bin/node-portable/node_modules` while excluding all
   root/application dependency trees; the user confirmed this exception.

## Planned Changes

### 0. Portable Python replacement and setup alignment

1. Move the current embedded runtime to a temporary backup outside `bin` so it
   cannot enter the archive.
2. Install full official CPython 3.13.x x64 into `bin/python-portable` with pip,
   `venv`, the standard library, headers, and tools; omit launcher, PATH mutation,
   documentation, tests, Tcl/Tk, shortcuts, and file associations.
3. Update only root orchestration in `setup.ps1` so `.venv` creation prefers
   `bin/python-portable/python.exe` before system `py`, LocalAppData Python, or
   PATH fallbacks.
4. Preserve the existing `.venv` and old runtime backup until the new interpreter
   passes all validation. Delete the backup only after success; restore it if the
   replacement fails.

### 1. Root `start.bat`

1. Use `@echo off`, `setlocal`, and `pushd "%~dp0"` so startup is independent of
   the caller's current directory.
2. Prepend the requested paths for the process lifetime using:

   ```bat
   set PATH=%~dp0bin\node-portable;%~dp0bin\python-portable;%PATH%
   ```

3. Fail early with a useful message if portable `node.exe`, `npm.cmd`, or
   `python.exe` is absent.
4. Run `call npm run setup` exactly once when either root `node_modules\` or
   `.venv\` is missing. Abort immediately when setup returns nonzero.
5. Launch the named root development script with
   `call npm --ignore-scripts run dev`. This runs `dev` while suppressing the
   existing `predev` lifecycle, preserving the requested conditional-only setup.
6. Propagate the setup/dev exit code, restore the caller directory with `popd`,
   and end the local environment scope.

### 2. Root `bundle.bat`

1. Use `setlocal` and `pushd "%~dp0"`; create root `share\` when absent.
2. Validate every allowlisted input before attempting the archive and fail if any
   required input is missing.
3. Build a disposable staging tree containing only:
   `backend`, `frontend`, `bin`, `requirements-backend.txt`,
   `requirements-dev.txt`, `package.json`, `package-lock.json`, `setup.ps1`,
   `start.bat`, `ENVIRONMENT.md`, and `README.md`.
4. While staging `backend` and `frontend`, exclude development/cache directories
   named `.agent`, `.agents`, `.codex`, `logs`, `node_modules`, and `.venv` at any
   depth. Copy `bin` as the portable runtime, retaining its npm-owned files.
5. Remove/replace any existing `share/ProcIP_Suite.zip` so the output is clean and
   deterministic with respect to stale archive entries.
6. Configure PowerShell `Compress-Archive` with an explicit array of the eleven
   staged allowlist paths only; never archive the repository root or `share`.
7. Clean the disposable staging tree in a `finally` path and propagate failures.

## Validation Plan

1. Static checks:
   - verify all eleven allowlist entries exist after `start.bat` is added;
   - verify the exact portable executables exist at the requested paths;
   - inspect both batch files for quoting, `call`, error propagation, and
     launch-from-any-directory behavior.
2. Startup guard harness/check:
   - both `node_modules` and `.venv` present: setup runs zero times;
   - either one missing: setup runs once;
   - both missing: setup runs once;
   - setup failure: dev is not launched;
   - dev launch bypasses `predev` and preserves its exit code.
3. Runtime path check after the PATH prepend: `where node`, `where npm`, and
   `where python` must resolve first to the repository's portable runtimes.
4. Python runtime/package alignment:
   - assert Python 3.13 x64 and successful imports of `venv`, `ensurepip`, and
     `ssl`;
   - verify `python -m pip` and create a disposable virtual environment;
   - install `requirements-dev.txt` into the disposable environment, run
     `pip check`, and import-smoke DuckDB, pandas, NumPy, Pillow, pydantic-core,
     and python-calamine;
   - do not delete the old runtime backup until every check passes.
5. Node/package alignment:
   - verify Node 24.18.0/npm 11.16.0 and lockfile version 3 compatibility;
   - run `npm ls --depth=0` in the root and all four frontend roots;
   - retain Module 1's scoped `--legacy-peer-deps` installation behavior;
   - confirm setup does not introduce unintended lockfile changes.
6. Bundle check:
   - create the archive and enumerate entries without extracting into source;
   - assert all eleven allowlisted top-level inputs are present and there are no
     other top-level inputs;
   - assert `.agent`, `.agents`, `.codex`, `logs`, and `.venv` never occur;
   - assert application/root `node_modules` never occur;
   - allow `node_modules` only below `bin/node-portable` as runtime-owned npm;
   - assert `share` and the ZIP itself are not nested in the archive.
7. Run both scripts from a different working directory to verify `%~dp0`/`pushd`
   reconciliation.
8. Run focused clean-copy setup/build checks after runtime and archive validation.
   Full application regression suites are unnecessary unless focused validation
   reveals runtime-related failures; no application logic or API contracts change.

## Acceptance Criteria

1. `start.bat` uses the exact requested PATH prepend and only invokes setup when
   root `node_modules` or `.venv` is absent.
2. Startup never performs a second implicit setup through `predev`.
3. A clean machine can bootstrap with the bundled full CPython 3.13.x x64 runtime,
   and the resulting environment passes dependency and native-import checks.
4. `share/ProcIP_Suite.zip` contains exactly the requested source/runtime inputs,
   with no development metadata, logs, root/application dependency trees, virtual
   environment, staging tree, or prior archive.
5. Node/npm satisfies all locked dependency engines, Module 1 retains its peer
   workaround, and setup causes no unintended lockfile drift.
6. Backend and frontend application files remain unchanged.

All acceptance criteria passed on 2026-07-14. The generated archive is
`share/ProcIP_Suite.zip` (63,345,054 bytes; 4,432 entries).

## Remaining Risks / Blockers

- No blocker remains for the approved portable startup and bundle scope.
- Python requirement ranges permit newer compatible packages on future installs;
  exact byte-for-byte Python dependency reproduction is not guaranteed without a
  separate constraints/lock decision.
- Module 1 retains its known React 19/glide-data-grid peer-range mismatch, handled
  by the existing scoped compatibility flag.
- Dependency installation requires package-registry access on a clean machine.
- A real seven-process development launch was not repeated; the startup harness,
  dependency trees, portable runtimes, clean Python install, and archive were
  validated independently.
- A synthetic failure after temporary ZIP creation was not injected. Preflight
  failure preservation and explicit temporary/staging cleanup were verified.

## Visible Project-Local Agents Used During Execution

- `backend_implementer`: replaced the portable Python runtime, updated setup, added
  both batch files, and resolved review findings.
- `test_engineer`: independently validated Python/Node dependencies, startup guard
  behavior, alternate-CWD execution, archive contents, and failure cleanup.
- `backend_reviewer`: reviewed quoting, failure safety, cleanup, and application/API
  isolation; its findings were fixed and revalidated.

## Agents Considered But Skipped

- `product_planner`: requirements and archive allowlist are explicit.
- `frontend_architect` / `backend_architect`: no application architecture or API
  change is planned.
- `api_contract_reviewer` / `contract_sync_agent`: no frontend/backend contract
  changes are involved.
- `performance_reviewer`: the scripts do not affect application computation paths.
- `contract_sync_agent`: skipped because no frontend/backend contract changed.
- `frontend_implementer` / `frontend_reviewer`: skipped because no frontend code
  changed.
