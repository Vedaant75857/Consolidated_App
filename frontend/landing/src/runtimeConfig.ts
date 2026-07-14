/**
 * Fetches runtime URLs from the launcher's /config.json endpoint.
 * Falls back gracefully to Vite env vars when running in dev mode
 * or if the endpoint is unavailable.
 */

let _config: Record<string, string> | null = null;
let _fetching: Promise<Record<string, string>> | null = null;

export async function loadRuntimeConfig(): Promise<Record<string, string>> {
  if (_config) return _config;
  if (_fetching) return _fetching;
  _fetching = (async () => {
    try {
      const resp = await fetch("/config.json");
      if (resp.ok) _config = await resp.json();
    } catch {
      /* Dev mode or endpoint unavailable — fall back to env vars */
    }
    _config ??= {};
    return _config;
  })();
  return _fetching;
}

export function getConfig(): Record<string, string> {
  return _config ?? {};
}
