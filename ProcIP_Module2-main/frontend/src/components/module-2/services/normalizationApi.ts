const BASE = "/api/normalize";
const DEFAULT_TIMEOUT_MS = 300_000; // 5 minutes for long-running normalization agents
const RETRY_ATTEMPTS = 2;
const RETRY_DELAY_MS = 1000;

async function request<T = any>(
  path: string,
  options?: RequestInit & { timeoutMs?: number; retries?: number }
): Promise<T> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, retries = RETRY_ATTEMPTS, ...fetchOptions } = options || {};
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);

      const res = await fetch(`${BASE}${path}`, {
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
        ...fetchOptions,
      });
      clearTimeout(timeout);

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Request failed: ${res.status}`);
      }
      return res.json();
    } catch (err: any) {
      lastError = err;
      if (err.name === "AbortError") {
        throw new Error(`Request to ${path} timed out after ${timeoutMs / 1000}s`);
      }
      if (attempt < retries) {
        await new Promise(r => setTimeout(r, RETRY_DELAY_MS * (attempt + 1)));
        continue;
      }
    }
  }
  throw lastError || new Error(`Request to ${path} failed after ${retries + 1} attempts`);
}

export async function checkHealth(): Promise<{ status: string }> {
  return request("/status");
}

export async function importFromStitching(csv: string, sheetKey?: string) {
  return request("/bridge/import", {
    method: "POST",
    body: JSON.stringify({ csv, sheet_key: sheetKey || "imported_merged" }),
  });
}

export async function exportNormalized(): Promise<Blob> {
  const res = await fetch(`${BASE}/bridge/export`);
  if (!res.ok) throw new Error("Failed to export normalized data");
  return res.blob();
}

export async function listSheets(): Promise<{ sheets: Array<{ sheet_key: string; rows: number; columns: string[] }> }> {
  return request("/bridge/sheets");
}

export async function uploadFiles(formData: FormData) {
  const res = await fetch(`${BASE}/upload`, { method: "POST", body: formData });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || "Upload failed");
  }
  return res.json();
}

export async function getHeaders(): Promise<{ headers: string[] }> {
  return request("/headers");
}

export async function mapHeaders(apiKey: string) {
  return request("/map-headers", {
    method: "POST",
    body: JSON.stringify({ api_key: apiKey }),
  });
}

export async function applyMappings(mappings: Record<string, Record<string, string>>) {
  return request("/apply-mappings", {
    method: "POST",
    body: JSON.stringify({ mappings }),
  });
}

export async function setApiKey(apiKey: string) {
  return request("/set-api-key", {
    method: "POST",
    body: JSON.stringify({ api_key: apiKey }),
  });
}

export async function getPreview(sheetKey: string) {
  return request(`/preview/${encodeURIComponent(sheetKey)}`);
}

export type NormOperation =
  | "fix-dates"
  | "fix-terms"
  | "add-record-id"
  | "fix-supplier-country"
  | "fix-regions"
  | "fix-supplier-names"
  | "fix-plant-names";

export async function runNormOperation(operation: NormOperation, options: Record<string, any> = {}) {
  return request(`/${operation}`, {
    method: "POST",
    body: JSON.stringify(options),
  });
}

export async function detectSpendColumns() {
  return request("/normalize-spend/detect", { method: "POST" });
}

export async function normalizeSpend(options: {
  currency_col: string;
  spend_cols: string[];
  date_col: string;
  target_currency?: string;
}) {
  return request("/normalize-spend", {
    method: "POST",
    body: JSON.stringify(options),
  });
}

export async function applyPendingOperation() {
  return request("/apply-pending-operation", { method: "POST" });
}

export async function discardPendingOperation() {
  return request("/discard-pending-operation", { method: "POST" });
}

export async function downloadResults(): Promise<Blob> {
  const res = await fetch(`${BASE}/download`);
  if (!res.ok) throw new Error("Download failed");
  return res.blob();
}

export async function getStatus() {
  return request("/status");
}

export async function getProgress(): Promise<{
  active: boolean;
  current: number;
  total: number;
  message: string;
  percent: number;
  eta_seconds?: number;
}> {
  return request("/bridge/progress", { retries: 0 });
}
