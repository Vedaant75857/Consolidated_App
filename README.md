# ProcIP Suite

ProcIP is a local development suite with three procurement-data workflows:

- Module 1: Data Consolidation / Data Stitcher
- Module 2: Data Normalizer
- Module 3: Summarization / Spend Analyzer

The repository contains three module backends and four frontend feature areas.
The default local workflow presents one suite frontend and one unified Flask
backend process; the module folders remain separately owned.

## Quick start

Prerequisites:

- Windows PowerShell
- Node.js and npm
- Python 3.13

From the repository root, run:

```powershell
npm run dev
```

Before starting the applications, npm automatically runs `setup.ps1`. The setup
process:

1. Installs or reconciles root npm dependencies.
2. Installs or reconciles dependencies for the suite host and all legacy
   frontends.
3. Creates the root `.venv` with Python 3.13 when it is missing.
4. Upgrades pip and installs `requirements-dev.txt`, including backend runtime
   requirements and pytest.
5. Stops startup if any required installation step fails.

The first run can take several minutes and requires registry access when packages
are missing. Later runs still check every declared dependency before startup.
The setup process installs declared packages; it cannot install Node.js, Python,
or restore missing project source files.

Press `Ctrl+C` in the development terminal to stop the suite.

## Local applications

| Application | URL | API target |
| --- | --- | --- |
| Suite frontend (landing + modules) | `http://localhost:3000/` | Same-origin `/api/module1/*`, `/api/module2/*`, `/api/module3/*` |
| Unified backend | `http://127.0.0.1:8000` | `/api/module1/*`, `/api/module2/*`, `/api/module3/*` |
| Suite routes | `/stitcher/`, `/normalizer/`, `/summarizer/` | Explicit module API base per route |

Browser clients use explicit module API bases (`/api/module1`,
`/api/module2`, and `/api/module3`) because endpoint suffixes collide between
modules. Host health is available at
`http://127.0.0.1:8000/api/health`; module health is available under the
corresponding module namespace.

## Common commands

Run dependency setup without starting applications:

```powershell
npm run setup
```

The normal development command starts the suite HMR server and the unified
backend:

```powershell
npm run dev
```

Start individual legacy applications when needed:

```powershell
npm run dev:landing
npm run dev:backend
npm run dev:stitcher-fe
npm run dev:normalizer-fe
npm run dev:summarizer-fe
npm run dev:legacy-frontends
```

During the verification window, the old three-process backend startup remains
available as an explicit backend rollback command:

```powershell
npm run dev:legacy-backends
```

For a single local process serving the built suite, build the frontend and run
the unified host (the host serves `frontend/suite/dist` with SPA fallback):

```powershell
npm run build:suite
& .\.venv\Scripts\python.exe backend/unified_app.py
```

Build all frontends:

```powershell
npm run build:frontends
```

Run the backend test suites:

```powershell
& .\.venv\Scripts\python.exe -B -m pytest backend/module1/tests -q
& .\.venv\Scripts\python.exe -B -m pytest backend/module2/tests -q
& .\.venv\Scripts\python.exe -B -m pytest backend/module3/tests -q
```

Run Module 3 frontend contract tests:

```powershell
npm --prefix frontend/module3 test
```

## Repository layout

```text
backend/
  module1/          Data Stitcher Flask application
  module2/          Data Normalizer Flask application
  module3/          Spend Analyzer Flask application
frontend/
  suite/            Unified React/Vite host and route shell
  landing/          ProcIP landing application
  module1/          Data Stitcher Vite application
  module2/          Data Normalizer Vite application
  module3/          Spend Analyzer Vite application
.venv/              Shared local Python 3.13 environment (generated)
requirements-backend.txt
requirements-dev.txt
package.json        Root orchestration commands
setup.ps1           Dependency bootstrap
ENVIRONMENT.md      Environment and deployment configuration
```

Do not merge module routes, services, or feature ownership. The suite is a thin
route shell with lazy-loaded module Apps; the unified backend is a small
composition host that namespace-routes the existing applications.

## Environment configuration

Backend credentials and frontend navigation settings live in module-local ignored
environment files. See [ENVIRONMENT.md](ENVIRONMENT.md) for filenames, variables,
local behavior, and production routing requirements.

If Zscaler or another TLS-inspecting proxy is enabled, set `PROCIP_CA_BUNDLE` to
your organization's PEM bundle before `npm run dev`. See the TLS section in
[ENVIRONMENT.md](ENVIRONMENT.md). Do not set certificate verification to false.

Never commit API keys, environment files, session databases, generated builds,
logs, or `node_modules`.

## Current development notes

- Module 1 uses React 19 while `@glideapps/glide-data-grid` declares peer support
  only through React 18. `setup.ps1` applies the existing
  `--legacy-peer-deps` compatibility workaround only for Module 1.
- npm currently reports dependency audit findings. They are maintenance items and
  should not be fixed with a broad forced upgrade without regression testing.
- Runtime data is stored in module-isolated local session directories and DuckDB
  files. Production backends therefore require persistent storage and are not a
  move-only fit for stateless serverless functions.
- Production frontend origins and `/api` routing must be configured per deployment.
