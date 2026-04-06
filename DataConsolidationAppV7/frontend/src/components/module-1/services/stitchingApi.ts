const BASE = "/api";

async function jsonPost<T = any>(path: string, body: any, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    ...options,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.error || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function jsonGet<T = any>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.error || `Request failed: ${res.status}`);
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

export async function headerNormDownloadExcel(sessionId: string, tables: any[]): Promise<Blob> {
  const res = await fetch(`${BASE}/header-norm-download-excel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, tables }),
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || "Download failed");
  return res.blob();
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

// --- Data Quality Assessment ---

export async function postDataQualityAssessment(
  sessionId: string,
  apiKey: string,
  tableName: string,
): Promise<any> {
  return jsonPost("/data-quality-assessment", { sessionId, apiKey, tableName });
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
