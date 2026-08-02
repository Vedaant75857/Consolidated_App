import type {
  PreviewColumnValuesResponse,
  PreviewFilter,
  PreviewOperationRequest,
  PreviewOperationResult,
  PreviewStateResponse,
  PreviewView,
} from "../../../types/excelPreview";

import { MODULE_API_BASE } from "../../../apiBase";

const BASE = MODULE_API_BASE;
const DEFAULT_TIMEOUT_MS = 120_000;

const BACKEND_UNREACHABLE_MSG =
  "Backend API not reachable. Make sure the stitcher backend is running on port 3001.";

/** True when the response body is an HTML error page instead of JSON. */
function isHtmlErrorBody(text: string): boolean {
  const trimmed = text.trim().toLowerCase();
  return trimmed.startsWith("<!doctype html") || trimmed.startsWith("<html");
}

/** Map raw error text to a user-friendly message when the API proxy hit the wrong server. */
function friendlyApiErrorMessage(text: string, status: number, statusText: string): string {
  if (isHtmlErrorBody(text)) {
    return BACKEND_UNREACHABLE_MSG;
  }
  const snippet = text.trim().slice(0, 200);
  if (snippet) return snippet;
  return `Request failed: ${status} ${statusText}`.trim();
}

/** Parse an error message from a failed fetch Response (JSON or plain text). */
export async function parseFetchError(res: Response, fallback?: string): Promise<string> {
  const text = await res.text();
  try {
    const data = JSON.parse(text);
    if (data && typeof data.error === "string" && data.error.trim()) {
      return data.error;
    }
  } catch {
    return friendlyApiErrorMessage(text, res.status, res.statusText);
  }
  return fallback || `Request failed: ${res.status} ${res.statusText}`.trim();
}

async function jsonPost<T = any>(
  path: string,
  body: any,
  options?: RequestInit & { timeoutMs?: number },
): Promise<T> {
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const callerSignal = options?.signal;
  const abortFromCaller = () => controller.abort(callerSignal?.reason);
  if (callerSignal?.aborted) abortFromCaller();
  else callerSignal?.addEventListener("abort", abortFromCaller, { once: true });

  try {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      ...options,
      signal: controller.signal,
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      let code: string | null = null;
      let message = `Request failed: ${res.status} ${res.statusText}`.trim();
      try {
        const data = JSON.parse(text);
        if (data && typeof data.error === "string" && data.error.trim()) {
          message = data.error;
        }
        code = data.code || null;
      } catch {
        message = friendlyApiErrorMessage(text, res.status, res.statusText);
      }
      const err = new Error(message);
      (err as any).code = code;
      (err as any).status = res.status;
      throw err;
    }
    return res.json();
  } catch (err: any) {
    if (err?.name === "AbortError") {
      if (callerSignal?.aborted) throw err;
      throw new Error(`Request to ${path} timed out after ${timeoutMs / 1000}s`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
    callerSignal?.removeEventListener("abort", abortFromCaller);
  }
}

async function jsonGet<T = any>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    throw new Error(await parseFetchError(res));
  }
  return res.json();
}

// --- Upload / File operations ---

export async function uploadFile(file: File): Promise<any> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await fetch(`${BASE}/upload`, { method: "POST", body: formData });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || "Failed to upload file");
  return res.json();
}

export async function deleteTable(sessionId: string, tableKey: string): Promise<any> {
  return jsonPost("/delete-table", { sessionId, tableKey });
}

export async function deleteRows(sessionId: string, tableKey: string, rowIds: (string | number)[]): Promise<any> {
  return jsonPost("/delete-rows", { sessionId, tableKey, rowIds });
}

export async function setHeaderRow(sessionId: string, tableKey: string, headerRowIndex: number, customColumnNames?: Record<number, string>): Promise<any> {
  return jsonPost("/set-header-row", { sessionId, tableKey, headerRowIndex, customColumnNames });
}

export async function getRawPreview(sessionId: string, tableKey: string): Promise<any> {
  return jsonPost("/get-raw-preview", { sessionId, tableKey });
}

// --- Cleaning ---

export async function getStandardFieldDtypes(): Promise<any> {
  return jsonGet("/standard-field-dtypes");
}

export async function cleanTable(sessionId: string, tableKey: string, config: any): Promise<any> {
  return jsonPost("/clean-table", { sessionId, tableKey, config });
}

export async function cleanGroup(sessionId: string, groupId: string, config: any): Promise<any> {
  return jsonPost("/clean-group", { sessionId, groupId, config });
}

// --- Column Removal ---

export async function removeColumns(sessionId: string, groupId: string, columns: string[]): Promise<any> {
  return jsonPost("/remove-columns", { sessionId, groupId, columns });
}

export async function restoreColumns(sessionId: string, groupId: string, columns: string[]): Promise<any> {
  return jsonPost("/restore-columns", { sessionId, groupId, columns });
}

// --- Append ---

export async function saveAppendGroups(sessionId: string, appendGroups: any[], unassigned: any[]): Promise<void> {
  fetch(`${BASE}/save-append-groups`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, appendGroups, unassigned }),
  }).catch(err => console.error("Failed to sync groups to server:", err));
}

// --- Header Normalisation ---

export async function headerNormGroupPreview(sessionId: string, groupIds: string[], limit = 50): Promise<any> {
  return jsonPost("/header-norm-group-preview", { sessionId, groupIds, limit });
}

export async function headerNormUploadExcel(formData: FormData): Promise<any> {
  const res = await fetch(`${BASE}/header-norm-upload-excel`, { method: "POST", body: formData });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || "Upload failed");
  return res.json();
}

// --- Execution engine ---

export async function executionRun(body: {
  sessionId: string;
  operation: string;
  apiKey: string;
  input: Record<string, any>;
  options: { mode: string; autoPrepare: boolean; persist: boolean };
}): Promise<any> {
  const res = await fetch(`${BASE}/execution/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await res.json();
  if (!res.ok || payload?.ok === false) {
    const missing = payload?.missing_requirements;
    if (Array.isArray(missing) && missing.length > 0) {
      throw new Error(`Missing requirements: ${missing.join(", ")}`);
    }
    throw new Error(payload?.error || "Operation failed.");
  }
  return payload;
}

export async function executionState(sessionId: string): Promise<any> {
  return jsonGet(`/execution/state?sessionId=${encodeURIComponent(sessionId)}`);
}

// --- Insights ---

export async function groupInsights(sessionId: string, apiKey: string, signal?: AbortSignal): Promise<any> {
  const res = await fetch(`${BASE}/group-insights`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, apiKey }),
    signal,
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || "Failed to fetch group insights");
  return res.json();
}

export async function preMergeAnalysis(sessionId: string, apiKey: string): Promise<any> {
  return jsonPost("/pre-merge-analysis", { sessionId, apiKey });
}

export async function groupPreview(sessionId: string, groupIds: string[]): Promise<any> {
  return jsonPost("/group-preview", { sessionId, groupIds });
}

// --- Merge ---

export async function mergeRecommendBase(sessionId: string, apiKey: string, groupIds: string[]): Promise<any> {
  return jsonPost("/merge/recommend-base", { sessionId, apiKey, groupIds });
}

export async function mergeCommonColumns(sessionId: string, baseGroupId: string, sourceGroupId: string): Promise<any> {
  return jsonPost("/merge/common-columns", { sessionId, baseGroupId, sourceGroupId });
}

export async function mergeSimulate(sessionId: string, body: any): Promise<any> {
  return jsonPost("/merge/simulate", { sessionId, ...body });
}

export async function mergeExecute(sessionId: string, body: any): Promise<Response> {
  return fetch(`${BASE}/merge/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, ...body }),
  });
}

export async function mergeFinalize(sessionId: string, body: any): Promise<any> {
  return jsonPost("/merge/finalize", { sessionId, ...body });
}

export async function mergeSkip(sessionId: string, groupIds: string[]): Promise<any> {
  return jsonPost("/merge/skip", { sessionId, groupIds });
}

export async function mergeRegisterMergedGroup(sessionId: string, body: any): Promise<any> {
  return jsonPost("/merge/register-merged-group", { sessionId, ...body });
}

export function mergeDownloadStepCsvUrl(sessionId: string, sourceGroupId: string): string {
  return `${BASE}/merge/download-step-csv?sessionId=${encodeURIComponent(sessionId)}&sourceGroupId=${encodeURIComponent(sourceGroupId)}`;
}

export function mergeDownloadStepXlsxUrl(sessionId: string, sourceGroupId: string): string {
  return `${BASE}/merge/download-step-xlsx?sessionId=${encodeURIComponent(sessionId)}&sourceGroupId=${encodeURIComponent(sourceGroupId)}`;
}

export function mergeDownloadCsvUrl(sessionId: string): string {
  return `${BASE}/merge/download-csv?sessionId=${encodeURIComponent(sessionId)}`;
}

export function mergeDownloadXlsxUrl(sessionId: string): string {
  return `${BASE}/merge/download-xlsx?sessionId=${encodeURIComponent(sessionId)}`;
}

export function mergeDownloadAllUrl(sessionId: string): string {
  return `${BASE}/merge/download-all?sessionId=${encodeURIComponent(sessionId)}`;
}

export async function mergeTablePreview(sessionId: string, body: any): Promise<any> {
  return jsonPost("/merge/table-preview", { sessionId, ...body });
}

export async function mergeHistory(sessionId: string): Promise<any> {
  return jsonGet(`/merge/history?sessionId=${encodeURIComponent(sessionId)}`);
}

// --- Data Quality Assessment (per-panel endpoints) ---

export async function postDqaDate(
  sessionId: string,
  apiKey: string,
  tableName: string,
  dateColumn?: string,
  tableKey?: string,
): Promise<any> {
  return jsonPost("/dqa/date", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
    dateColumn: dateColumn || undefined,
  });
}

export async function postDqaCurrency(
  sessionId: string,
  apiKey: string,
  tableName: string,
  tableKey?: string,
  identifiedColumns?: string[],
): Promise<any> {
  return jsonPost("/dqa/currency", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
    identifiedColumns: identifiedColumns?.length ? identifiedColumns : undefined,
  });
}

export async function postDqaPaymentTerms(
  sessionId: string,
  apiKey: string,
  tableName: string,
  tableKey?: string,
  identifiedColumns?: string[],
): Promise<any> {
  return jsonPost("/dqa/payment-terms", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
    identifiedColumns: identifiedColumns?.length ? identifiedColumns : undefined,
  });
}

export async function postDqaCountryRegion(
  sessionId: string,
  apiKey: string,
  tableName: string,
  tableKey?: string,
  identifiedCountryColumns?: string[],
  identifiedRegionColumns?: string[],
): Promise<any> {
  return jsonPost("/dqa/country-region", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
    identifiedCountryColumns: identifiedCountryColumns?.length
      ? identifiedCountryColumns
      : undefined,
    identifiedRegionColumns: identifiedRegionColumns?.length
      ? identifiedRegionColumns
      : undefined,
  });
}

export async function postDqaSupplier(
  sessionId: string,
  apiKey: string,
  tableName: string,
  tableKey?: string,
  vendorColumn?: string,
): Promise<any> {
  return jsonPost("/dqa/supplier", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
    vendorColumn: vendorColumn || undefined,
  });
}

export async function postDqaSuggestColumns(
  sessionId: string,
  apiKey: string,
  tableName: string,
  tableKey?: string,
): Promise<Record<string, string[]>> {
  return jsonPost("/dqa/suggest-columns", {
    sessionId, apiKey,
    tableName: tableName || undefined,
    tableKey,
  });
}

export async function postDqaFillRate(
  sessionId: string,
  tableName: string,
  tableKey?: string,
): Promise<any> {
  return jsonPost("/dqa/fill-rate", {
    sessionId,
    tableName: tableName || undefined,
    tableKey,
  });
}

// --- Excel Preview Operations (Unified) ---

export async function previewOperation(
  sessionId: string,
  tableKey: string,
  op: PreviewOperationRequest["op"],
  params: Record<string, unknown> = {},
  view?: PreviewView,
): Promise<PreviewOperationResult> {
  return jsonPost<PreviewOperationResult>("/preview/operation", { sessionId, tableKey, op, params, view });
}

export async function previewApply(
  sessionId: string,
  tableKey: string,
  target: { kind: string; id: string; mode?: string },
): Promise<any> {
  return jsonPost("/preview/apply", { sessionId, tableKey, target });
}

export async function previewState(
  sessionId: string,
  tableKey: string,
  options: PreviewView = {},
  signal?: AbortSignal,
): Promise<PreviewStateResponse> {
  return jsonPost<PreviewStateResponse>("/preview/state", { sessionId, tableKey, ...options }, { signal });
}

export async function previewColumnValues(
  sessionId: string,
  tableKey: string,
  column: string,
  options: {
    search?: string;
    filters?: PreviewFilter[];
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<PreviewColumnValuesResponse> {
  return jsonPost<PreviewColumnValuesResponse>(
    "/preview/column-values",
    { sessionId, tableKey, column, ...options },
    { signal },
  );
}

export async function previewUndo(sessionId: string, tableKey: string, view?: PreviewView): Promise<PreviewOperationResult> {
  return jsonPost<PreviewOperationResult>("/preview/undo", { sessionId, tableKey, view });
}

export async function previewRedo(sessionId: string, tableKey: string, view?: PreviewView): Promise<PreviewOperationResult> {
  return jsonPost<PreviewOperationResult>("/preview/redo", { sessionId, tableKey, view });
}

export async function previewRefreshInventory(sessionId: string): Promise<any> {
  return jsonPost("/preview/refresh-inventory", { sessionId });
}

// --- Chat ---

export async function chat(body: {
  sessionId: string;
  apiKey: string;
  messages: any[];
  message: string;
  stage?: number;
  selectedItem?: any;
  context?: string;
}): Promise<Response> {
  return fetch(`${BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
