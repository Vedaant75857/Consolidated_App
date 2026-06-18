# Active Plan - Landing Page Session API Key

Mode: execution. Frontend application code was edited for checklist items 1-7.

Target: landing page API-key configuration that applies to Data Stitcher, Data Normalizer, and Spend Summarizer for the current browser session.

## User Request

Add a landing-page feature where the user configures their API key once, and that key is applied across all modules for the session.

## Affected Areas

- Landing launcher:
  - `landing-page/src/App.tsx`
  - optional `landing-page/src/apiKeySession.ts`
- Module app shells and key hydration:
  - `DataConsolidationAppV7/frontend/src/App.tsx`
  - `ProcIP_Module2-main/frontend/src/App.tsx`
  - `SummarizationModule/frontend/src/App.tsx`
- Existing key-entry surfaces:
  - `DataConsolidationAppV7/frontend/src/components/module-1/DataLoading.tsx`
  - `ProcIP_Module2-main/frontend/src/components/module-1/DataLoading.tsx`
  - `SummarizationModule/frontend/src/components/data_preview/DataLoading.tsx`
  - `SummarizationModule/frontend/src/components/upload/UploadStep.tsx` only if still reachable
  - `SummarizationModule/frontend/src/components/spend_quality_assessment/DataQualityStep.tsx`
- Cross-module transfer links that currently include `apiKey` in query strings:
  - `DataConsolidationAppV7/frontend/src/components/module-1/MergeOutputsPanel.tsx`
  - `ProcIP_Module2-main/frontend/src/components/module-2/NormDashboard.tsx`
- Backend AI-key consumers, mostly for compatibility and optional error cleanup:
  - `SummarizationModule/backend/routes/mapping_routes.py`
  - `SummarizationModule/backend/routes/views_routes.py`
  - `SummarizationModule/backend/routes/email_routes.py`
  - `SummarizationModule/backend/app.py`
  - `ProcIP_Module2-main/backend/app.py`
  - `DataConsolidationAppV7/backend/module-1/routes/*` AI routes

## Current Behavior Snapshot

- Landing page cards link directly to module frontend URLs and do not configure or propagate an API key.
- Each module stores its own key in browser `sessionStorage`:
  - `datastitcher_apiKey`
  - `normalizer_apiKey`
  - `summarizer_apiKey`
- Normalizer and Summarizer can read `apiKey` from query params during some imports; Stitcher does not appear to hydrate from an `apiKey` URL param.
- Cross-module transfers currently append `apiKey` in query strings, which exposes the key in address bars, history, logs, screenshots, and referrers.
- Backends already expect `apiKey` in JSON request bodies for AI routes; no server-side key storage is needed.
- Modules run on separate Vite ports in dev, so `sessionStorage` is not shared across origins.

## Planned Behavior

Use a browser-session-only API key managed from the landing page.

Rules:

- Landing page provides a visible API-key control near module launch.
- Key is stored only in `sessionStorage`, not `localStorage` and not backend session metadata.
- Canonical landing/session key name: `procip_api_key`.
- Module cards remain usable without a key; AI-required actions continue to show missing-key behavior.
- Launching a module with a configured key passes it via URL fragment, not query string:
  - `#apiKey=<encoded>`
- Each module reads `location.hash` once, stores the key in its own session storage, then strips the fragment using `history.replaceState`.
- Each module initializes from this order:
  1. URL fragment `#apiKey=...`
  2. temporary legacy query param `?apiKey=...`, then strip it
  3. canonical `sessionStorage["procip_api_key"]`
  4. module legacy key
- Module key edits write to `procip_api_key` and may mirror to the legacy module key for compatibility.
- Cross-module transfers keep `sessionId`, `source`, and `imported` params but stop putting the key in query strings; use fragment handoff only when needed.
- Backend request bodies can continue sending `{ apiKey }`.

## API Contract Plan

Primary compatibility contract stays unchanged:

```json
{ "apiKey": "<user session key>" }
```

Recommended additive backend cleanup:

- Add small key resolver helpers where practical:
  - `X-API-Key`
  - JSON body `apiKey`
  - existing env fallback where already supported
- Return a consistent missing-key error for AI-required routes:

```json
{ "error": "Missing API key", "code": "MISSING_API_KEY" }
```

Use HTTP `400` for missing or whitespace-only keys.

Do not accept new query-string API key contracts.

## Implementation Checklist

1. [x] Add a landing-page API key control with password input, show/hide toggle, save/update, clear, and text-visible configured/missing status.
2. [x] Add a small API-key session helper in each frontend app or a copied local helper:
   - `getSessionApiKey`
   - `setSessionApiKey`
   - `clearSessionApiKey`
   - `hydrateApiKeyFromUrl`
   - `buildApiKeyFragmentUrl`
3. [x] Update landing module card URLs to include `#apiKey=<encoded>` only when a non-empty key is saved.
4. [x] Update Stitcher, Normalizer, and Summarizer app shells to hydrate from hash/query/canonical/legacy storage in the planned order.
5. [x] Update module key setters so user edits write to `procip_api_key` and maintain legacy module keys for refresh compatibility.
6. [x] Remove `apiKey=` query-string handoff from Stitcher and Normalizer transfer links; preserve non-secret route params.
7. [x] Keep existing backend body `apiKey` calls working.
8. [ ] Optionally add backend key resolver helpers and standardized `MISSING_API_KEY` errors if implementation scope includes backend cleanup. Deferred.
9. [ ] Review logging so full API keys are never logged; reduce existing key prefix/length logging in `SummarizationModule/backend/shared/ai_client.py` if backend cleanup is included. Deferred.

## Execution Status - 2026-06-18

- Implemented the approved frontend scope across the landing page, Data Stitcher, Data Normalizer, and Spend Summarizer.
- Added copied local `apiKeySession.ts` helpers for each frontend app.
- Fixed frontend review finding by associating the landing-page `Session API Key` label with the password input.
- Contract sync found no backend changes required; existing `{ apiKey }` request bodies remain the active compatibility contract.
- Backend cleanup items 8 and 9 were intentionally deferred because the first pass did not require backend contract changes.

## Validation Plan

Frontend build/type checks:

```powershell
cd C:\Users\75857\Downloads\Consolidated_App\landing-page
npm run build
```

```powershell
cd C:\Users\75857\Downloads\Consolidated_App\DataConsolidationAppV7
npm run lint
npm run build
```

```powershell
cd C:\Users\75857\Downloads\Consolidated_App\ProcIP_Module2-main\frontend
npm run lint
npm run build
```

```powershell
cd C:\Users\75857\Downloads\Consolidated_App\SummarizationModule\frontend
npm run build
```

Manual checks:

- Save, replace, reveal/hide, and clear the key on the landing page.
- Open each module from the landing page and verify its API-key status is configured without duplicate entry.
- Refresh each module and verify the key persists for the browser session.
- Edit the key in one module, then open another module and verify the edited key is applied.
- Open each module directly with no landing key and verify existing manual key entry still works.
- Confirm no navigation URL contains `?apiKey=` after new launches or transfers.
- Confirm URL fragments containing `apiKey` are stripped immediately after hydration.
- Close the tab/session and verify the key is not retained.

Backend validation if backend resolver cleanup is included:

- Existing body `apiKey` clients still pass.
- Optional `X-API-Key` clients pass.
- Missing or whitespace key returns HTTP `400` with `MISSING_API_KEY`.
- No full key appears in logs or errors.

## Risks And Open Questions

- `sessionStorage` is origin-scoped; the URL-fragment handoff is required for current separate-port dev setup.
- URL fragments are safer than query params because they are not sent to the server, but they are still visible client-side until stripped.
- A purely same-origin production deployment could read `procip_api_key` directly, but keeping fragment hydration supports both shapes.
- Multiple storage keys can drift unless key reads and writes are centralized through a helper.
- Backend error normalization is useful but not strictly required for the first user-facing feature.

## Visible Agents Created

Execution:

- `frontend_implementer` (`Curie`): implemented the frontend session-key helper, landing UI, module hydration, and transfer-link changes.
- `contract_sync_agent` (`Lagrange`): reviewed frontend/backend and cross-module API-key contracts.
- `frontend_reviewer` (`Meitner`): reviewed UI, storage, URL stripping, and accessibility; found one low-severity label association issue.
- `test_engineer` (`Leibniz`): ran/assessed focused validation and identified remaining automated test gaps.

Planning:

- `product_planner` (`Maxwell`): user flow, acceptance criteria, edge cases, non-goals.
- `frontend_architect` (`Nietzsche`): landing UI, module hydration, storage strategy, cross-module transfer risks.
- `backend_architect` (`Nash`): AI-key route behavior, backend compatibility, logging/security risks.
- `api_contract_reviewer` (`Franklin`): cross-origin handoff contract, query-vs-fragment risk, endpoint compatibility.
- `test_engineer` (`Plato`): available validation commands, frontend/manual test gaps.

## Agents Considered But Skipped

- `frontend_implementer` and `backend_implementer`: skipped because this was plan mode and application code must not be edited.
- `frontend_reviewer`, `backend_reviewer`, and `contract_sync_agent`: skipped until execution creates changes to review/synchronize.
- `performance_reviewer`: skipped because this feature does not materially affect performance-sensitive paths.
- `debug_triage_agent`: skipped because no bug or failing test was provided.
