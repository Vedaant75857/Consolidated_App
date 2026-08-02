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

## Zscaler / TLS CA bundle

If the machine is behind a Zscaler proxy, place the Zscaler root CA PEM at one of the fallback paths, or set an environment variable before starting any backend module.

| Variable | Purpose |
| --- | --- |
| `PROCIP_CA_BUNDLE` | Primary PEM bundle path used by `configure_tls()`. |
| `REQUESTS_CA_BUNDLE` | Fallback PEM bundle path used by `configure_tls()` when `PROCIP_CA_BUNDLE` is unset. |

`configure_tls()` also sets `SSL_CERT_FILE` and `REQUESTS_CA_BUNDLE` for the discovered bundle so `requests` and `httpx` (used by `portkey-ai`) both trust it. Fallback paths are checked in this order when no environment variable is set:

1. `C:\Bain\Setup\Zscaler\zscaler.pem`
2. `C:\Bain\Setup\Zscaler\zscaler 2026_05.pem`
3. `~/zscaler.pem`
3. `/usr/local/share/ca-certificates/zscaler.pem`
4. `/etc/ssl/certs/zscaler.pem`
