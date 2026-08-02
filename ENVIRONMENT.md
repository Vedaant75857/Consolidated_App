# Environment and deployment matrix

Keep backend credentials in each backend's module-local dotenv file. Keep Vite
navigation settings in each frontend application's own `.env` or
`.env.production` file. Environment files are ignored and must not be committed.

## Backend credentials

| File | Variable | Purpose |
| --- | --- | --- |
| `backend/module1/.env.local` | `PORTKEY_API_KEY` | Authenticates Module 1 AI calls through Portkey. |
| `backend/module2/.env` | `OPENAI_API_KEY` | Authenticates Module 2 OpenAI normalization calls. |
| `backend/module3/.env.local` | `PORTKEY_API_KEY` | Authenticates Module 3 AI calls through Portkey. |

Create these ignored files locally and provide real credentials through the
deployment platform's secret manager in production. Never commit credential
values.

## TLS certificates behind Zscaler inspection

The repository never includes a company-specific Zscaler certificate. The
unified Python backend automatically uses the standard Windows Bain path
`C:\\Bain\\Setup\\Zscaler\\zscaler.pem` when that file exists. Other users
should set `PROCIP_CA_BUNDLE` to the PEM file supplied by their own IT team:

```powershell
$env:PROCIP_CA_BUNDLE = 'C:\\path\\to\\organization-ca-bundle.pem'
npm run dev
```

The backend maps this to `REQUESTS_CA_BUNDLE` and `SSL_CERT_FILE` for Python
HTTP clients. It never disables certificate verification. For a persistent
setting, configure `PROCIP_CA_BUNDLE` in the shell or deployment environment
instead of committing it to `.env`.

This setting only affects Python processes started by this repository. `pip`
and npm may need their own CA settings (`PIP_CERT` and npm's `cafile`), while
Claude Code requires `NODE_EXTRA_CA_CERTS` and AWS CLI uses `ca_bundle`.
External tools cannot be configured by application code.

## Frontend navigation

| Application | File | Variables |
| --- | --- | --- |
| Landing | `frontend/landing/.env.production` | `VITE_STITCHER_FE`, `VITE_NORMALIZER_FE`, `VITE_ANALYZER_FE` |
| Module 1 | `frontend/module1/.env.production` | `VITE_HOME_URL`, `VITE_NORMALIZER_FE`, `VITE_ANALYZER_FE` |
| Module 2 | `frontend/module2/.env.production` | `VITE_HOME_URL`, `VITE_ANALYZER_FE` |
| Module 3 | `frontend/module3/.env.production` | `VITE_HOME_URL` |

Production navigation values must be deployed **frontend origins**. Do not use
backend origins such as ports 3001, 5000, or 3005 for these variables. For
example, replace the illustrative domains below with the actual deployed
frontend origins:

```dotenv
# frontend/landing/.env.production
VITE_STITCHER_FE=https://stitcher.example.com
VITE_NORMALIZER_FE=https://normalizer.example.com
VITE_ANALYZER_FE=https://analyzer.example.com

# frontend/module1/.env.production
VITE_HOME_URL=https://suite.example.com
VITE_NORMALIZER_FE=https://normalizer.example.com
VITE_ANALYZER_FE=https://analyzer.example.com

# frontend/module2/.env.production
VITE_HOME_URL=https://suite.example.com
VITE_ANALYZER_FE=https://analyzer.example.com

# frontend/module3/.env.production
VITE_HOME_URL=https://suite.example.com
```

The frontends continue to call same-origin `/api` routes. A production host must
route each module frontend's `/api/*` requests to its corresponding backend.
