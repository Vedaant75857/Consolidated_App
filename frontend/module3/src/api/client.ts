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
import type {
  PreviewColumnValuesRequest,
  PreviewColumnValuesResult,
  PreviewHistoryRequest,
  PreviewOperationRequest,
  PreviewOperationResult,
  PreviewRefreshInventoryRequest,
  PreviewRefreshInventoryResult,
  PreviewStateRequest,
  PreviewTableState,
} from "../types/excelPreview";

const BASE = "/api";
const DEFAULT_TIMEOUT_MS = 120_000;

export class ApiClientError extends Error {
  code?: string;
  details?: Record<string, unknown>;
  status: number;

  constructor(message: string, status: number, code?: string, details?: Record<string, unknown>) {
    super(message);
    this.name = "ApiClientError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

async function post<T>(
  path: string,
  body?: any,
  timeoutMs: number = DEFAULT_TIMEOUT_MS,
  signal?: AbortSignal,
): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const abortFromSignal = () => controller.abort();
  if (signal?.aborted) {
    controller.abort();
  } else {
    signal?.addEventListener("abort", abortFromSignal, { once: true });
  }
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  try {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: body instanceof FormData ? {} : { "Content-Type": "application/json" },
      body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new ApiClientError(
        err.error || `Request failed: ${res.status}`,
        res.status,
        typeof err.code === "string" ? err.code : undefined,
        err.details && typeof err.details === "object" ? err.details : undefined,
      );
    }
    return res.json();
  } catch (err: any) {
    if (err?.name === "AbortError") {
      if (!timedOut) throw new Error(`Request to ${path} was cancelled`);
      throw new Error(`Request to ${path} timed out after ${timeoutMs / 1000}s`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abortFromSignal);
  }
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiClientError(
      err.error || `Request failed: ${res.status}`,
      res.status,
      typeof err.code === "string" ? err.code : undefined,
      err.details && typeof err.details === "object" ? err.details : undefined,
    );
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

export async function previewState(request: PreviewStateRequest, options?: { signal?: AbortSignal }) {
  return post<PreviewTableState>("/preview/state", request, DEFAULT_TIMEOUT_MS, options?.signal);
}

export async function previewColumnValues(
  request: PreviewColumnValuesRequest,
  options?: { signal?: AbortSignal },
) {
  return post<PreviewColumnValuesResult>(
    "/preview/column-values",
    request,
    DEFAULT_TIMEOUT_MS,
    options?.signal,
  );
}

export async function previewOperation(request: PreviewOperationRequest) {
  return post<PreviewOperationResult>("/preview/operation", request);
}

export async function previewUndo(request: PreviewHistoryRequest) {
  return post<PreviewOperationResult>("/preview/undo", request);
}

export async function previewRedo(request: PreviewHistoryRequest) {
  return post<PreviewOperationResult>("/preview/redo", request);
}

export async function previewRefreshInventory(request: PreviewRefreshInventoryRequest) {
  return post<PreviewRefreshInventoryResult>("/preview/refresh-inventory", request);
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

export interface SpendBifurcationResult {
  positiveSpend: number | null;
  positivePctOfNet: number | null;
  negativeSpend: number | null;
  negativePctOfNet: number | null;
  netSpend: number | null;
  feasible: boolean;
  message?: string;
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
  ltmPeriodLabel?: string;
  currentFySpend: number;
  priorFySpend: number;
  currentFyLabel: string;
  priorFyLabel: string;
  yoyAbs: number;
  yoyPct: number;
  latestFullYearSpend?: number | null;
  latestFullYearLabel?: string | null;
  feasible: boolean;
  message?: string;
}

export interface ExecutiveSummaryDateSource {
  fieldKey: string;
  displayName: string;
  sourceColumn: string | null;
  fallback: boolean;
  message?: string;
}

export interface ExecutiveSummaryWarning {
  code: "DATE_FALLBACK_USED" | string;
  severity: "warning" | string;
  message: string;
  fieldKey?: string;
  sourceColumn?: string | null;
}

export interface SupplierBreakdownResult {
  totalSuppliers: number;
  suppliersTo80Pct: number | null;
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

export interface MapAICostRange {
  low: number;
  high: number;
  text: string;
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
    topVendorPairsCount?: number | null;
  };
  buckets: CategorizationBuckets | null;
  bucketsPct: CategorizationBuckets | null;
  qualityVerdict: "high" | "medium" | "low" | null;
  recommendedMethod: "MapAI" | "Creactives" | null;
  mapAICost: number;
  mapAICostRange?: MapAICostRange;
  reasoning: string | null;
  qualityWarning: boolean;
  feasible: boolean;
  message?: string;
}

export interface ColumnFillRateItem {
  columnName: string;
  sourceColumn: string;
  order: number;
  fillRate: number;
  spendCoverage: number | null;
}

export interface ColumnFillRateResult {
  columns: ColumnFillRateItem[];
  feasible: boolean;
}

export interface ExecutiveSummaryRow {
  key:
    | "timePeriod"
    | "ltmSpend"
    | "supplierConcentration"
    | "descriptionQuality"
    | "categorizationMethod";
  label: string;
  text: string;
}

export interface ExecutiveSummaryRowsResult {
  rows: ExecutiveSummaryRow[];
}

export interface ExecutiveSummaryResult {
  totalRows: number;
  executiveSummary: ExecutiveSummaryRowsResult;
  datePeriod: DatePeriodResult;
  spendBreakdown: SpendBreakdownResult;
  supplierBreakdown: SupplierBreakdownResult;
  categorizationEffort: CategorizationEffortResult;
  columnFillRate: ColumnFillRateResult;
  datePivot: DatePivotResult;
  spendBifurcation: SpendBifurcationResult;
  paretoAnalysis: ParetoAnalysisResult;
  descriptionQuality: DescriptionQualityItem[];
  dateSource?: ExecutiveSummaryDateSource | null;
  warnings?: ExecutiveSummaryWarning[];
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

export interface NotProcurableDetectResult {
  feasible: boolean;
  message: string | null;
  columnsUsed: string[];
  keywords: KeywordSearchResult[];
  uniqueMatchingRows: number;
  uniqueTotalSpend: number;
}

export async function detectNotProcurableSpend(
  sessionId: string,
  columns?: string[]
) {
  return post<NotProcurableDetectResult>(
    "/not-procurable/detect",
    {
      sessionId,
      ...(columns && columns.length > 0 ? { columns } : {}),
    },
    600_000
  );
}

/* ── Intercompany Spend ──────────────────────────────────────────────── */

export interface IntercompanyVendorMatch {
  vendor: string;
  matchingRows: number;
  totalSpend: number;
}

export interface IntercompanyDetectResult {
  feasible: boolean;
  message: string | null;
  clientName: string;
  summary: KeywordSearchResult | null;
  vendors: IntercompanyVendorMatch[];
}

export async function getIntercompanyColumns(sessionId: string) {
  return post<{ columns: SearchableColumn[]; vendorColumn: string }>(
    "/intercompany/columns",
    { sessionId }
  );
}

export async function detectIntercompanySpend(
  sessionId: string,
  clientName: string
) {
  return post<IntercompanyDetectResult>(
    "/intercompany/detect",
    { sessionId, clientName },
    600_000
  );
}

export async function searchIntercompanyKeyword(
  sessionId: string,
  clientName: string
) {
  return post<KeywordSearchResult>("/intercompany/search", {
    sessionId,
    clientName,
  });
}

/* ── CAPEX / OPEX Spend ──────────────────────────────────────────────── */

export interface CapexOpexCategory {
  label: string;
  matchingRows: number;
  totalSpend: number;
}

export interface CapexOpexClassifyResult {
  feasible: boolean;
  message: string | null;
  sourceColumn: string | null;
  sourceColumnDisplayName: string | null;
  categories: CapexOpexCategory[];
  assumptions: string[];
  unclassifiedRows: number | null;
  unclassifiedSpend: number | null;
}

export async function getCapexOpexColumns(sessionId: string) {
  return post<{ columns: SearchableColumn[] }>("/capex-opex/columns", {
    sessionId,
  });
}

export async function classifyCapexOpexSpend(
  sessionId: string,
  column?: string
) {
  return post<CapexOpexClassifyResult>(
    "/capex-opex/classify",
    {
      sessionId,
      ...(column ? { column } : {}),
    },
    600_000
  );
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

export interface BinaryExportResult {
  blob: Blob;
  filename?: string;
}

function getDownloadFilename(contentDisposition: string | null): string | undefined {
  if (!contentDisposition) return undefined;
  const encoded = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i)?.[1];
  if (encoded) {
    try {
      return decodeURIComponent(encoded);
    } catch {
      return undefined;
    }
  }
  return contentDisposition.match(/filename="?([^";]+)"?/i)?.[1];
}

export async function exportCompleteAnalysis(sessionId: string): Promise<BinaryExportResult> {
  const res = await fetch(`${BASE}/export/xlsx/complete-analysis`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiClientError(
      err.error || "Excel export failed",
      res.status,
      typeof err.code === "string" ? err.code : undefined,
    );
  }
  return {
    blob: await res.blob(),
    filename: getDownloadFilename(res.headers.get("Content-Disposition")),
  };
}
