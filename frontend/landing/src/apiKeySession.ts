export const PROCIP_API_KEY = "procip_api_key";

export function getSessionApiKey(): string {
  if (typeof window === "undefined") return "";
  return window.sessionStorage.getItem(PROCIP_API_KEY) || "";
}

export function setSessionApiKey(apiKey: string): string {
  if (typeof window === "undefined") return apiKey.trim();
  const trimmed = apiKey.trim();
  if (trimmed) {
    window.sessionStorage.setItem(PROCIP_API_KEY, trimmed);
  } else {
    window.sessionStorage.removeItem(PROCIP_API_KEY);
  }
  return trimmed;
}

export function clearSessionApiKey() {
  if (typeof window === "undefined") return;
  window.sessionStorage.removeItem(PROCIP_API_KEY);
}

export function hydrateApiKeyFromUrl(): string {
  if (typeof window === "undefined") return "";
  const url = new URL(window.location.href);
  const hashParams = new URLSearchParams(url.hash.replace(/^#/, ""));
  const fragmentKey = hashParams.get("apiKey") || "";
  const queryKey = url.searchParams.get("apiKey") || "";
  const hydrated = fragmentKey || queryKey || getSessionApiKey();

  if (hydrated.trim()) setSessionApiKey(hydrated);

  if (fragmentKey || queryKey) {
    hashParams.delete("apiKey");
    url.searchParams.delete("apiKey");
    url.hash = hashParams.toString() ? `#${hashParams.toString()}` : "";
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }

  return getSessionApiKey();
}

export function buildApiKeyFragmentUrl(url: string, apiKey = getSessionApiKey()): string {
  const trimmed = apiKey.trim();
  if (!trimmed) return url;
  const next = new URL(url, typeof window !== "undefined" ? window.location.href : "http://localhost");
  const hashParams = new URLSearchParams(next.hash.replace(/^#/, ""));
  hashParams.set("apiKey", trimmed);
  next.hash = hashParams.toString();
  return next.toString();
}
