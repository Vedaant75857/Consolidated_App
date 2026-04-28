import type {
  ColumnInfo,
  FileInventoryItem,
  UploadWarning,
  PreviewData,
  AIMapping,
  StandardField,
  CastReport,
  ViewDefinition,
  ViewResult,
  EmailContext,
  AnalysisFeasibilityResult,
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
    previews: Record<string, PreviewData>;
    warnings: UploadWarning[];
  }>("/upload", fd);
}

export async function getSessionState(sessionId: string) {
  return get<{
    step: number;
    columns: ColumnInfo[] | null;
    fileInventory: FileInventoryItem[] | null;
    previews: Record<string, PreviewData> | null;
    mapping: Record<string, string | null> | null;
    castReport: CastReport | null;
    viewResults: ViewResult[] | null;
    aiMappings: AIMapping[] | null;
    standardFields: StandardField[] | null;
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
  return post<{ castReport: CastReport; procurementViews?: AnalysisFeasibilityResult }>("/confirm-mapping", {
    sessionId,
    mapping,
  });
}

export async function getAvailableViews(sessionId: string) {
  return post<{ views: ViewDefinition[] }>("/available-views", { sessionId });
}

export async function getProcurementViews(sessionId: string) {
  return post<AnalysisFeasibilityResult>("/procurement-views", {
    sessionId,
  });
}

export async function computeViews(
  sessionId: string,
  selectedViews: string[],
  config: { topN?: number; paretoThreshold?: number }
) {
  return post<{ views: ViewResult[] }>("/compute-views", {
    sessionId,
    selectedViews,
    config,
  });
}

export async function recomputeView(
  sessionId: string,
  viewId: string,
  config: { topN?: number; paretoThreshold?: number }
) {
  return post<{ view: ViewResult }>("/recompute-view", {
    sessionId,
    viewId,
    config,
  });
}

export async function generateSummary(
  sessionId: string,
  viewId: string,
  apiKey: string
) {
  return post<{ viewId: string; summary: string }>("/generate-summary", {
    sessionId,
    viewId,
    apiKey,
  });
}

export async function deleteTable(sessionId: string, tableKey: string) {
  return post<{
    inventory: FileInventoryItem[];
    previews: Record<string, PreviewData>;
  }>("/delete-table", { sessionId, tableKey });
}

export async function setHeaderRow(
  sessionId: string,
  tableKey: string,
  headerRowIndex: number,
  customColumnNames?: Record<number, string>
) {
  return post<{
    inventory: FileInventoryItem[];
    previews: Record<string, PreviewData>;
    columns: ColumnInfo[];
  }>("/set-header-row", { sessionId, tableKey, headerRowIndex, customColumnNames });
}

export async function getRawPreview(sessionId: string, tableKey: string) {
  return post<{ rawPreview: any[][] }>("/get-raw-preview", {
    sessionId,
    tableKey,
  });
}

export async function deleteRows(
  sessionId: string,
  tableKey: string,
  rowIds: (string | number)[]
) {
  return post<{
    deletedCount: number;
    preview: PreviewData;
    inventoryRow: FileInventoryItem;
  }>("/delete-rows", { sessionId, tableKey, rowIds });
}

export async function generateEmail(
  sessionId: string,
  apiKey: string,
  context: EmailContext
) {
  return post<{
    email: string | null;
    subject?: string;
    error?: string;
    fallback?: string;
  }>("/generate-email", { sessionId, apiKey, context });
}

export function cleanupSession(sessionId: string) {
  const payload = JSON.stringify({ sessionId });
  const sent = navigator.sendBeacon(
    `${BASE}/cleanup-session`,
    new Blob([payload], { type: "application/json" })
  );
  if (!sent) {
    fetch(`${BASE}/cleanup-session`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload,
      keepalive: true,
    }).catch(() => {});
  }
}

/* ── Spend Quality Assessment (Executive Summary) ────────────────────── */

export interface DatePivotResult {
  years: number[];
  months: string[];
  cells: Record<string, Record<string, number>>;
  feasible: boolean;
  message?: string;
}

export interface ParetoThresholdMetrics {
  totalSpend: number;
  transactionCount: number;
  uniqueTransactions: number;
  supplierCount: number;
}

export interface ParetoAnalysisResult {
  thresholds: number[];
  metrics: Record<string, ParetoThresholdMetrics>;
  feasible: boolean;
  message?: string;
  totalDatasetSpend: number;
}

export interface DescriptionTop10Item {
  description: string;
  spend: number;
}

export interface DescriptionBackendStats {
  avgLength: number;
  multiWordCount: number;
  multiWordSpend: number;
  nullProxyCount: number;
  nullProxySpend: number;
  totalPopulated: number;
  totalSpend: number;
}

export interface DescriptionQualityItem {
  fieldKey: string;
  displayName: string;
  mapped: boolean;
  spendCovered: number | null;
  top10: DescriptionTop10Item[];
  backendStats: DescriptionBackendStats | null;
  aiInsight: string;
}

export interface SpendBifurcationCurrency {
  code: string;
  positiveSpend: number;
  negativeSpend: number;
}

export interface SpendBifurcationReporting {
  positiveSpend: number;
  negativeSpend: number;
}

export interface SpendBifurcationResult {
  reporting: SpendBifurcationReporting | null;
  local: SpendBifurcationCurrency[] | SpendBifurcationReporting | null;
}

export interface ExecutiveSummaryResult {
  totalRows: number;
  datePivot: DatePivotResult;
  spendBifurcation: SpendBifurcationResult;
  paretoAnalysis: ParetoAnalysisResult;
  descriptionQuality: DescriptionQualityItem[];
}

export async function getExecutiveSummary(
  sessionId: string,
  apiKey: string,
  force = false
) {
  return post<ExecutiveSummaryResult>("/executive-summary", {
    sessionId,
    apiKey,
    force,
  });
}

/* ── CSV Export ──────────────────────────────────────────────────────── */

export async function exportCsv(sessionId: string, viewId: string) {
  const res = await fetch(`${BASE}/export/csv/${viewId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId }),
  });
  if (!res.ok) throw new Error("CSV export failed");
  return res.blob();
}
