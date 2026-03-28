import type {
  ColumnInfo,
  FileInventoryItem,
  AIMapping,
  StandardField,
  CastReport,
  ViewDefinition,
  ViewResult,
} from "../types";

const BASE = "/api";

async function post<T>(path: string, body?: any): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: body instanceof FormData ? {} : { "Content-Type": "application/json" },
    body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || `Request failed: ${res.status}`);
  }
  return res.json();
}

export async function uploadFile(file: File) {
  const fd = new FormData();
  fd.append("file", file);
  return post<{
    sessionId: string;
    columns: ColumnInfo[];
    fileInventory: FileInventoryItem[];
    preview: Record<string, any[]>;
    warnings: string[];
  }>("/upload", fd);
}

export async function getSessionState(sessionId: string) {
  return get<{
    step: number;
    columns: ColumnInfo[] | null;
    fileInventory: FileInventoryItem[] | null;
    mapping: Record<string, string | null> | null;
    castReport: CastReport | null;
    viewResults: ViewResult[] | null;
  }>(`/session/${sessionId}/state`);
}

export async function mapColumns(sessionId: string, apiKey: string) {
  return post<{
    mappings: AIMapping[];
    standardFields: StandardField[];
  }>("/map-columns", { sessionId, apiKey });
}

export async function confirmMapping(
  sessionId: string,
  mapping: Record<string, string | null>
) {
  return post<{ castReport: CastReport }>("/confirm-mapping", {
    sessionId,
    mapping,
  });
}

export async function getAvailableViews(sessionId: string) {
  return post<{ views: ViewDefinition[] }>("/available-views", { sessionId });
}

export async function computeViews(
  sessionId: string,
  selectedViews: string[],
  config: { topN?: number; paretoThreshold?: number },
  apiKey: string
) {
  return post<{ views: ViewResult[] }>("/compute-views", {
    sessionId,
    selectedViews,
    config,
    apiKey,
  });
}

export async function exportCsv(sessionId: string, viewId: string) {
  const res = await fetch(`${BASE}/export/csv/${viewId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId }),
  });
  if (!res.ok) throw new Error("CSV export failed");
  return res.blob();
}

export async function exportPdf(
  sessionId: string,
  chartImages: Record<string, string>
) {
  const res = await fetch(`${BASE}/export/pdf`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, chartImages }),
  });
  if (!res.ok) throw new Error("PDF export failed");
  return res.blob();
}
