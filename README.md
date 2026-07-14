# ProcIP Suite

ProcIP is a local development suite with three procurement-data workflows:

- Module 1: Data Consolidation / Data Stitcher
- Module 2: Data Normalizer
- Module 3: Summarization / Spend Analyzer

The repository contains three independent Flask backends, three module frontends,
and one landing frontend. They share root-level startup tooling but remain separate
applications with separate runtime processes and frontend dependency trees.

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
2. Installs or reconciles dependencies for all four frontends.
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
| Landing page | `http://localhost:3010` | None |
| Module 1 frontend | `http://localhost:3002` | `http://127.0.0.1:3001` |
| Module 2 frontend | `http://localhost:3003` | `http://127.0.0.1:5000` |
| Module 3 frontend | `http://localhost:3004` | `http://127.0.0.1:3005` |

Each module frontend proxies same-origin `/api` requests to its corresponding
Flask backend. Backend health checks are available at `/api/health` on ports
`3001`, `5000`, and `3005`.

## Common commands

Run dependency setup without starting applications:

```powershell
npm run setup
```

Start one application:

```powershell
npm run dev:landing
npm run dev:stitcher-be
npm run dev:stitcher-fe
npm run dev:normalizer-be
npm run dev:normalizer-fe
npm run dev:summarizer-be
npm run dev:summarizer-fe
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

Do not merge the Flask applications into one Python process or merge the four
frontend dependency trees. The applications contain overlapping route and Python
package names and intentionally run independently.

## Environment configuration

Backend credentials and frontend navigation settings live in module-local ignored
environment files. See [ENVIRONMENT.md](ENVIRONMENT.md) for filenames, variables,
local behavior, and production routing requirements.

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
