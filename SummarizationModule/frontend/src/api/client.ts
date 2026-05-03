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
const DEFAULT_TIMEOUT_MS = 120_000;

async function post<T>(
  path: string,
  body?: any,
  timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: body instanceof FormData ? {} : { "Content-Type": "application/json" },
      body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(err.error || `Request failed: ${res.status}`);
    }
    return res.json();
  } catch (err: any) {
    if (err?.name === "AbortError") {
      throw new Error(`Request to ${path} timed out after ${timeoutMs / 1000}s`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
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
  aiInsight: string[];
}

export interface SpendBifurcationCurrency {
  code: string;
  positiveSpend: number;
  negativeSpend: number;
}

export interface SpendBifurcationReporting {
  positiveSpend: number;
  negativeSpend: number;
  negPctOfPos?: number;
  negRowCount?: number;
  netSpend?: number;
}

export interface SpendBifurcationResult {
  reporting: SpendBifurcationReporting | null;
  local: SpendBifurcationCurrency[] | SpendBifurcationReporting | null;
}

export interface DatePeriodResult {
  startDate: string;
  endDate: string;
  periodLabel: string;
  monthsCovered: number;
  feasible: boolean;
  message?: string;
}

export interface SpendBreakdownResult {
  ltmSpend: number;
  currentFySpend: number;
  priorFySpend: number;
  currentFyLabel: string;
  priorFyLabel: string;
  yoyAbs: number;
  yoyPct: number;
  feasible: boolean;
  message?: string;
}

export interface SupplierBreakdownResult {
  totalSuppliers: number;
  suppliersTo80Pct: number;
  top10: { supplier: string; spend: number; sharePct: number }[];
  duplicateNameFlags: number;
  feasible: boolean;
  message?: string;
}

export interface CategorizationBuckets {
  high: number;
  medium: number;
  low: number;
}

export interface CategorizationEffortResult {
  metrics: {
    rowCount: number;
    avgWordCount: number;
    avgCharLength: number;
    fillRate: number;
    uniqueCount: number;
    distinctPairs: number;
    sampledCount: number;
  };
  buckets: CategorizationBuckets | null;
  bucketsPct: CategorizationBuckets | null;
  qualityVerdict: "high" | "medium" | "low" | null;
  recommendedMethod: "MapAI" | "Creactives" | null;
  mapAICost: number;
  reasoning: string | null;
  qualityWarning: boolean;
  feasible: boolean;
  message?: string;
}

export interface FlagsResult {
  spendConsistency: {
    flaggedMonths: { month: string; spend: number; deviationPct: number }[];
    avgMonthlySpend: number;
  } | null;
  descriptionQuality: {
    fillRate: number;
    avgWordCount: number;
    message: string;
  } | null;
  vendorQuality: {
    fillRate: number;
    populatedCount: number;
    totalRows: number;
  } | null;
  nullColumns: {
    flaggedColumns: { name: string; fillRate: number; spendCoverage: number }[];
  } | null;
}

export interface ColumnFillRateItem {
  columnName: string;
  fillRate: number;
  spendCoverage: number | null;
}

export interface ColumnFillRateResult {
  columns: ColumnFillRateItem[];
  feasible: boolean;
}

export interface ExecutiveSummaryResult {
  totalRows: number;
  datePeriod: DatePeriodResult;
  spendBreakdown: SpendBreakdownResult;
  supplierBreakdown: SupplierBreakdownResult;
  categorizationEffort: CategorizationEffortResult;
  flags: FlagsResult;
  columnFillRate: ColumnFillRateResult;
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

/* ── Not Procurable Spend ────────────────────────────────────────────── */

export interface SearchableColumn {
  fieldKey: string;
  displayName: string;
}

export interface KeywordSearchResult {
  keyword: string;
  matchingRows: number;
  totalSpend: number;
}

export async function getNotProcurableColumns(sessionId: string) {
  return post<{ columns: SearchableColumn[] }>("/not-procurable/columns", {
    sessionId,
  });
}

export async function searchNotProcurableKeyword(
  sessionId: string,
  columns: string[],
  keyword: string
) {
  return post<KeywordSearchResult>("/not-procurable/search", {
    sessionId,
    columns,
    keyword,
  });
}

/* ── Intercompany Spend ──────────────────────────────────────────────── */

export async function getIntercompanyColumns(sessionId: string) {
  return post<{ columns: SearchableColumn[] }>("/intercompany/columns", {
    sessionId,
  });
}

export async function searchIntercompanyKeyword(
  sessionId: string,
  columns: string[],
  keyword: string
) {
  return post<KeywordSearchResult>("/intercompany/search", {
    sessionId,
    columns,
    keyword,
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
