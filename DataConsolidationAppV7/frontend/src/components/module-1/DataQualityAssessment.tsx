import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "motion/react";
import {
  AlertTriangle,
  ArrowRight,
  BarChart3,
  CalendarDays,
  ChevronDown,
  Coins,
  FileText,
  Globe,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
  TableProperties,
  Users,
} from "lucide-react";
import { SurfaceCard, PrimaryButton, itemVariants } from "../common/ui";
import {
  postDqaDate,
  postDqaCurrency,
  postDqaPaymentTerms,
  postDqaCountryRegion,
  postDqaSupplier,
  postDqaFillRate,
  postDqaSpendBifurcation,
  postDqaSuggestColumns,
} from "./services/stitchingApi";
import type { MergeOutput } from "../../types";

/* ── Types ─────────────────────────────────────────────────────────────── */

interface FormatEntry {
  fileName: string;
  dominantFormat: string;
  formatPcts: Record<string, number>;
  examples: Record<string, string>;
  consistent: boolean;
}

interface ReportingPivot {
  type: "reporting";
  years: number[];
  months: string[];
  cells: Record<string, Record<string, number>>;
  spendColumn: string;
  feasible: boolean;
}

interface CurrencyCrosstabPivot {
  type: "currency_crosstab";
  rowLabels: string[];
  currencies: string[];
  cells: Record<string, Record<string, number>>;
  spendColumn: string;
  currencyColumn: string;
  feasible: boolean;
}

type PivotData = ReportingPivot | CurrencyCrosstabPivot;

interface TotalSpendSummary {
  type: "reporting" | "local_by_currency";
  total?: number;
  currencies?: { code: string; total: number }[];
  column: string;
}

interface DateResult {
  availableDateColumns: string[];
  selectedColumn: string | null;
  formatTable: FormatEntry[];
  pivotData: PivotData | null;
  totalSpendSummary: TotalSpendSummary | null;
  consistent: boolean;
  aiInsight: string[] | null;
}

interface CurrencyTableRow {
  currencyCode: string;
  rowCount: number;
  rowPct: number;
  localSpend: number | null;
  reportingSpend: number | null;
}

interface CurrencyResult {
  exists: boolean;
  availableCurrencyColumns: string[];
  currencyColumn: string | null;
  distinctCount: number;
  codes: string[];
  currencyTable: CurrencyTableRow[];
  hasLocalSpend: boolean;
  hasReportingSpend: boolean;
  aiInsight: string[] | null;
}

interface PaymentTermRow {
  term: string;
  spend: number | null;
  rowCount: number;
  pctOfTotal: number | null;
  pctOfRows: number | null;
  currencySpend: Record<string, number>;
}

interface PaymentTermsResult {
  exists: boolean;
  availablePaymentTermsColumns: string[];
  paymentTerms: PaymentTermRow[];
  totalSpend: number;
  uniqueCount: number;
  spendColumn: string | null;
  aiInsight: string[] | null;
  isReporting: boolean;
  currencyColumns: string[];
}

interface CountryRegionResult {
  countryColumn: string | null;
  regionColumn: string | null;
  countryValues: string[] | null;
  regionValues: string[] | null;
  countryAiInsight: string[] | null;
  regionAiInsight: string[] | null;
  availableCountryColumns: string[];
}

interface SupplierResult {
  exists: boolean;
  availableSupplierColumns: string[];
  supplierCount: number;
  topNCount: number;
  totalSpendCovered: number;
  spendColumn: string | null;
  aiInsight: string[] | null;
  hasReportingSpend: boolean;
  paretoVendorCount: number | null;
  paretoVendorPct: number | null;
  top20: Array<{ vendor: string; spend: number }>;
}

interface FillRateColumn {
  columnName: string;
  pctRowsCovered: number;
  spendCoverage: number | { code: string; pct: number }[] | null;
}

interface FillRateResult {
  columns: FillRateColumn[];
  spendType: "reporting" | "local" | "none";
  spendColumn: string | null;
}

interface SpendBifurcationCurrency {
  code: string;
  positiveSpend: number;
  negativeSpend: number;
}

interface SpendBifurcationResult {
  type: "reporting" | "local" | "local_single" | "none";
  positiveSpend?: number;
  negativeSpend?: number;
  currencies?: SpendBifurcationCurrency[];
  column: string | null;
}

interface DataQualityAssessmentProps {
  sessionId: string;
  apiKey: string;
  mergeOutputs: MergeOutput[];
  singleTableName?: string;
  addLog: (
    step: string,
    type: "info" | "success" | "error",
    message: string,
  ) => void;
  setAiLoading: (v: boolean) => void;
  setLoadingMessage: (v: string) => void;
  setStep: (s: number) => void;
  cancelRef?: React.MutableRefObject<(() => void) | null>;
}

const ERROR_CODE_TABLE_MISSING = "TABLE_MISSING";

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fmtSpend(val: number | null | undefined): string {
  if (val == null) return "N/A";
  const abs = Math.abs(val);
  if (abs >= 1_000_000_000) return (val / 1_000_000_000).toFixed(1) + "B";
  if (abs >= 1_000_000) return (val / 1_000_000).toFixed(1) + "M";
  if (abs >= 1_000) return (val / 1_000).toFixed(1) + "K";
  return Math.round(val).toLocaleString();
}

/** Renders **bold** markdown within a text string. */
function renderBoldMarkdown(text: string): ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith("**") && part.endsWith("**") ? (
      <strong key={i}>{part.slice(2, -2)}</strong>
    ) : (
      part
    ),
  );
}

function tableNameForVersion(version: number): string {
  return `final_merged_v${version}`;
}

/* ── Per-panel state hook ──────────────────────────────────────────────── */

interface PanelState<T> {
  loading: boolean;
  error: string | null;
  data: T | null;
}

/* ── Shared sub-components ─────────────────────────────────────────────── */

const EXPAND_TRANSITION = { duration: 0.25, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] };

/** Renders AI insight as 3 bullet points from a string array, with a colored left border accent. */
function StructuredInsight({
  insight,
  accentColor,
}: {
  insight: string[] | null;
  accentColor: "blue" | "amber" | "violet" | "emerald" | "rose";
}) {
  if (!insight || insight.length === 0) return null;

  const borderMap: Record<string, string> = {
    blue: "border-blue-400 dark:border-blue-500",
    amber: "border-amber-400 dark:border-amber-500",
    violet: "border-violet-400 dark:border-violet-500",
    emerald: "border-emerald-400 dark:border-emerald-500",
    rose: "border-rose-400 dark:border-rose-500",
  };

  const bgMap: Record<string, string> = {
    blue: "bg-blue-50/70 dark:bg-blue-950/30",
    amber: "bg-amber-50/70 dark:bg-amber-950/30",
    violet: "bg-violet-50/70 dark:bg-violet-950/30",
    emerald: "bg-emerald-50/70 dark:bg-emerald-950/30",
    rose: "bg-rose-50/70 dark:bg-rose-950/30",
  };

  return (
    <div
      className={`mx-6 my-4 rounded-2xl border-l-4 ${borderMap[accentColor]} ${bgMap[accentColor]} px-6 py-4`}
    >
      <ul className="space-y-2">
        {insight.map((line, i) => (
          <li key={i} className="flex items-start gap-2.5 text-sm leading-relaxed text-neutral-700 dark:text-neutral-300">
            <span className="mt-[7px] h-1.5 w-1.5 shrink-0 rounded-full bg-current opacity-50" />
            <span>{renderBoldMarkdown(line)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

/** Collapsible "View Details" / "Hide Details" toggle with smooth animation. */
function DeepDiveSection({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);

  return (
    <div>
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2 px-6 py-3 text-xs font-semibold text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-300 transition-colors w-full text-left border-t border-neutral-100 dark:border-neutral-800"
      >
        <ChevronDown
          className={`w-3.5 h-3.5 transition-transform ${open ? "" : "-rotate-90"}`}
        />
        {open ? "Hide Details" : "View Details"}
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="bg-neutral-50/50 dark:bg-neutral-800/20 border-t border-neutral-100 dark:border-neutral-800">
              {children}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function PanelHeader({
  icon,
  iconBg,
  title,
  subtitle,
  expanded,
  onToggle,
}: {
  icon: ReactNode;
  iconBg: string;
  title: string;
  subtitle: string;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      onClick={onToggle}
      className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-3xl"
    >
      <div className="flex items-center gap-3">
        <span
          className={`w-8 h-8 rounded-lg ${iconBg} flex items-center justify-center`}
        >
          {icon}
        </span>
        <div>
          <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
            {title}
          </h3>
          <p className="text-xs text-neutral-400">{subtitle}</p>
        </div>
      </div>
      <ChevronDown
        className={`w-4 h-4 text-neutral-400 transition-transform ${
          expanded ? "" : "-rotate-90"
        }`}
      />
    </button>
  );
}

function PanelSpinner({ message }: { message: string }) {
  return (
    <div className="flex items-center gap-3 px-6 py-8 justify-center">
      <Loader2 className="w-5 h-5 text-red-500 animate-spin" />
      <span className="text-sm text-neutral-500 dark:text-neutral-400">
        {message}
      </span>
    </div>
  );
}

function PanelError({
  message,
  onRetry,
}: {
  message: string;
  onRetry?: () => void;
}) {
  return (
    <div className="px-6 py-6 text-center">
      <p className="text-red-600 dark:text-red-400 text-sm font-medium mb-2">
        {message}
      </p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="inline-flex items-center gap-1.5 text-xs font-semibold text-red-600 hover:text-red-700 dark:text-red-400"
        >
          <RefreshCw className="w-3 h-3" /> Retry
        </button>
      )}
    </div>
  );
}

/** Centered empty-state message with a muted icon. */
function EmptyState({
  icon: Icon,
  message,
}: {
  icon: React.ComponentType<{ className?: string }>;
  message: string;
}) {
  return (
    <div className="flex flex-col items-center justify-center py-10 gap-3">
      <Icon className="w-8 h-8 text-neutral-300 dark:text-neutral-600" />
      <p className="text-sm text-neutral-500 dark:text-neutral-400">{message}</p>
    </div>
  );
}

function SubSectionHeader({
  title,
  expanded,
  onToggle,
}: {
  title: string;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      onClick={onToggle}
      className="flex items-center justify-between w-full px-6 py-2 text-left border-t border-neutral-100 dark:border-neutral-800 hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors"
    >
      <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
        {title}
      </p>
      <ChevronDown
        className={`w-3 h-3 text-neutral-400 transition-transform ${
          expanded ? "" : "-rotate-90"
        }`}
      />
    </button>
  );
}

/* ── Component ─────────────────────────────────────────────────────────── */

export default function DataQualityAssessment({
  sessionId,
  apiKey,
  mergeOutputs,
  singleTableName,
  addLog,
  setAiLoading,
  setLoadingMessage,
  setStep,
  cancelRef,
}: DataQualityAssessmentProps) {
  const isSingleTable = !!singleTableName;
  const latestVersion =
    mergeOutputs.length > 0
      ? Math.max(...mergeOutputs.map((o) => o.version))
      : 1;

  const [selectedVersion, setSelectedVersion] = useState(latestVersion);
  const [sessionExpired, setSessionExpired] = useState(false);
  const [tableMissing, setTableMissing] = useState(false);
  const completedPanelsRef = useRef(0);

  const markPanelComplete = useCallback(() => {
    completedPanelsRef.current += 1;
    if (runningRef.current) {
      setLoadingMessage(
        `Data Quality Assessment… ${completedPanelsRef.current}/7 panels complete`,
      );
    }
  }, [setLoadingMessage]);

  const [dateState, setDateState] = useState<PanelState<DateResult>>({
    loading: false,
    error: null,
    data: null,
  });
  const [currencyState, setCurrencyState] = useState<
    PanelState<CurrencyResult>
  >({ loading: false, error: null, data: null });
  const [paymentState, setPaymentState] = useState<
    PanelState<PaymentTermsResult>
  >({ loading: false, error: null, data: null });
  const [countryState, setCountryState] = useState<
    PanelState<CountryRegionResult>
  >({ loading: false, error: null, data: null });
  const [supplierState, setSupplierState] = useState<
    PanelState<SupplierResult>
  >({ loading: false, error: null, data: null });
  const [fillRateState, setFillRateState] = useState<
    PanelState<FillRateResult>
  >({ loading: false, error: null, data: null });
  const [bifurcationState, setBifurcationState] = useState<
    PanelState<SpendBifurcationResult>
  >({ loading: false, error: null, data: null });

  const [expandedPanels, setExpandedPanels] = useState<Set<string>>(
    new Set(["fillrate", "date", "bifurcation", "currency", "payment", "country", "supplier"]),
  );

  const [selectedDateColumn, setSelectedDateColumn] = useState<
    string | undefined
  >(undefined);

  const [selectedCountryColumn, setSelectedCountryColumn] = useState<
    string | undefined
  >(undefined);

  const [selectedCurrencyColumn, setSelectedCurrencyColumn] = useState<
    string | undefined
  >(undefined);

  const [selectedPaymentTermsColumn, setSelectedPaymentTermsColumn] = useState<
    string | undefined
  >(undefined);

  const [selectedSupplierColumn, setSelectedSupplierColumn] = useState<
    string | undefined
  >(undefined);

  const [aiColumnSuggestions, setAiColumnSuggestions] = useState<
    Record<string, string[]>
  >({});

  const hasRunRef = useRef(false);

  const getTableKey = useCallback(
    () => (isSingleTable ? singleTableName : undefined),
    [isSingleTable, singleTableName],
  );

  const [tableMissingRetry, setTableMissingRetry] = useState(0);
  const MAX_TABLE_MISSING_RETRIES = 2;

  const allPanelsSettled =
    !dateState.loading &&
    !currencyState.loading &&
    !paymentState.loading &&
    !countryState.loading &&
    !supplierState.loading &&
    !fillRateState.loading &&
    !bifurcationState.loading;

  const [runningAssessment, setRunningAssessment] = useState(false);
  const runningRef = useRef(false);

  useEffect(() => {
    if (runningAssessment && allPanelsSettled && completedPanelsRef.current > 0) {
      runningRef.current = false;
      setAiLoading(false);
      setLoadingMessage("");
      setRunningAssessment(false);
      addLog("Data Quality", "success", "Assessment complete.");
    }
  }, [runningAssessment, allPanelsSettled, setAiLoading, setLoadingMessage, addLog]);

  const MAX_PANEL_RETRIES = 3;
  const RETRY_BASE_MS = 2000;

  async function withRetry<T>(
    fn: () => Promise<T>,
    attempt = 0,
  ): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      if (attempt < MAX_PANEL_RETRIES - 1) {
        await new Promise((r) => setTimeout(r, RETRY_BASE_MS * 2 ** attempt));
        return withRetry(fn, attempt + 1);
      }
      throw err;
    }
  }

  /* ── Panel runners ────────────────────────────────────────────────────── */

  const runDatePanel = useCallback(
    async (version: number, dateCol?: string) => {
      setDateState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaDate(sessionId, apiKey, tn, dateCol, getTableKey()),
        );
        setDateState({ loading: false, error: null, data });
        if (
          !dateCol &&
          data.selectedColumn &&
          data.availableDateColumns?.length > 0
        ) {
          setSelectedDateColumn(data.selectedColumn);
        }
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setDateState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setDateState({
            loading: false,
            error: err?.message || "Date analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, markPanelComplete],
  );

  const runCurrencyPanel = useCallback(
    async (version: number, currencyCol?: string) => {
      setCurrencyState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaCurrency(sessionId, apiKey, tn, getTableKey(), currencyCol),
        );
        setCurrencyState({ loading: false, error: null, data });
        if (!currencyCol && data.currencyColumn && data.availableCurrencyColumns?.length > 0) {
          setSelectedCurrencyColumn(data.currencyColumn);
        }
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setCurrencyState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setCurrencyState({
            loading: false,
            error: err?.message || "Currency analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, markPanelComplete],
  );

  const runPaymentPanel = useCallback(
    async (version: number, paymentCol?: string) => {
      setPaymentState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaPaymentTerms(sessionId, apiKey, tn, getTableKey(), paymentCol),
        );
        setPaymentState({ loading: false, error: null, data });
        if (!paymentCol && data.paymentTerms?.length > 0 && data.availablePaymentTermsColumns?.length > 0) {
          setSelectedPaymentTermsColumn(data.availablePaymentTermsColumns[0]);
        }
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setPaymentState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setPaymentState({
            loading: false,
            error: err?.message || "Payment terms analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, markPanelComplete],
  );

  const runCountryPanel = useCallback(
    async (version: number, countryCol?: string) => {
      setCountryState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaCountryRegion(sessionId, apiKey, tn, getTableKey(), countryCol),
        );
        setCountryState({ loading: false, error: null, data });
        if (!countryCol && data.countryColumn) {
          setSelectedCountryColumn(data.countryColumn);
        }
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setCountryState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setCountryState({
            loading: false,
            error: err?.message || "Country/Region analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, markPanelComplete],
  );

  const runSupplierPanel = useCallback(
    async (version: number, vendorCol?: string) => {
      setSupplierState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaSupplier(sessionId, apiKey, tn, getTableKey(), vendorCol),
        );
        setSupplierState({ loading: false, error: null, data });
        if (!vendorCol && data.exists && data.availableSupplierColumns?.length > 0) {
          setSelectedSupplierColumn(data.availableSupplierColumns[0]);
        }
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setSupplierState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setSupplierState({
            loading: false,
            error: err?.message || "Supplier analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, markPanelComplete],
  );

  const runFillRatePanel = useCallback(
    async (version: number) => {
      setFillRateState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaFillRate(sessionId, tn, getTableKey()),
        );
        setFillRateState({ loading: false, error: null, data });
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setFillRateState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setFillRateState({
            loading: false,
            error: err?.message || "Fill rate analysis failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, isSingleTable, getTableKey, markPanelComplete],
  );

  const runBifurcationPanel = useCallback(
    async (version: number) => {
      setBifurcationState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaSpendBifurcation(sessionId, tn, getTableKey()),
        );
        setBifurcationState({ loading: false, error: null, data });
      } catch (err: any) {
        const code = err?.code || "";
        if (code === ERROR_CODE_TABLE_MISSING) {
          setBifurcationState({ loading: false, error: null, data: null });
          setTableMissingRetry((c) => c + 1);
        } else {
          setBifurcationState({
            loading: false,
            error: err?.message || "Spend bifurcation failed",
            data: null,
          });
        }
      } finally {
        markPanelComplete();
      }
    },
    [sessionId, isSingleTable, getTableKey, markPanelComplete],
  );

  const runAllPanels = useCallback(
    async (version: number) => {
      completedPanelsRef.current = 0;
      setTableMissing(false);
      setTableMissingRetry(0);
      addLog("Data Quality", "info", "Running data quality assessment…");
      setAiLoading(true);
      setRunningAssessment(true);
      runningRef.current = true;
      setLoadingMessage("Data Quality Assessment… asking AI for column suggestions…");

      const tn = isSingleTable ? "" : tableNameForVersion(version);
      let suggestions: Record<string, string[]> = {};
      try {
        suggestions = await postDqaSuggestColumns(sessionId, apiKey, tn, getTableKey());
        setAiColumnSuggestions(suggestions);
      } catch {
        /* AI suggestions are best-effort — proceed without them */
      }

      setLoadingMessage("Data Quality Assessment… 0/7 panels complete");

      const pickFirst = (role: string) => suggestions[role]?.[0] || undefined;

      runFillRatePanel(version);
      runDatePanel(version, pickFirst("date"));
      runBifurcationPanel(version);
      runCurrencyPanel(version, pickFirst("currency_code"));
      runPaymentPanel(version, pickFirst("payment_terms"));
      runCountryPanel(version, pickFirst("country"));
      runSupplierPanel(version, pickFirst("vendor_name"));
    },
    [
      runFillRatePanel,
      runDatePanel,
      runBifurcationPanel,
      runCurrencyPanel,
      runPaymentPanel,
      runCountryPanel,
      runSupplierPanel,
      addLog,
      setAiLoading,
      setLoadingMessage,
      sessionId,
      apiKey,
      isSingleTable,
      getTableKey,
    ],
  );

  /* ── Single-panel re-runs (no global overlay / counter) ──────────────── */

  const rerunDateAbortRef = useRef<AbortController | null>(null);

  const rerunDatePanelOnly = useCallback(
    async (version: number, dateCol: string) => {
      if (rerunDateAbortRef.current) rerunDateAbortRef.current.abort();
      const ac = new AbortController();
      rerunDateAbortRef.current = ac;

      setDateState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaDate(sessionId, apiKey, tn, dateCol, getTableKey()),
        );
        if (ac.signal.aborted) return;
        setDateState({ loading: false, error: null, data });
      } catch (err: any) {
        if (ac.signal.aborted) return;
        setDateState({
          loading: false,
          error: err?.message || "Date analysis failed",
          data: null,
        });
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const rerunCountryAbortRef = useRef<AbortController | null>(null);

  const rerunCountryPanelOnly = useCallback(
    async (version: number, countryCol: string) => {
      if (rerunCountryAbortRef.current) rerunCountryAbortRef.current.abort();
      const ac = new AbortController();
      rerunCountryAbortRef.current = ac;

      setCountryState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaCountryRegion(sessionId, apiKey, tn, getTableKey(), countryCol),
        );
        if (ac.signal.aborted) return;
        setCountryState({ loading: false, error: null, data });
      } catch (err: any) {
        if (ac.signal.aborted) return;
        setCountryState({
          loading: false,
          error: err?.message || "Country/Region analysis failed",
          data: null,
        });
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const rerunCurrencyAbortRef = useRef<AbortController | null>(null);

  const rerunCurrencyPanelOnly = useCallback(
    async (version: number, currencyCol: string) => {
      if (rerunCurrencyAbortRef.current) rerunCurrencyAbortRef.current.abort();
      const ac = new AbortController();
      rerunCurrencyAbortRef.current = ac;

      setCurrencyState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaCurrency(sessionId, apiKey, tn, getTableKey(), currencyCol),
        );
        if (ac.signal.aborted) return;
        setCurrencyState({ loading: false, error: null, data });
      } catch (err: any) {
        if (ac.signal.aborted) return;
        setCurrencyState({
          loading: false,
          error: err?.message || "Currency analysis failed",
          data: null,
        });
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const rerunPaymentAbortRef = useRef<AbortController | null>(null);

  const rerunPaymentPanelOnly = useCallback(
    async (version: number, paymentCol: string) => {
      if (rerunPaymentAbortRef.current) rerunPaymentAbortRef.current.abort();
      const ac = new AbortController();
      rerunPaymentAbortRef.current = ac;

      setPaymentState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaPaymentTerms(sessionId, apiKey, tn, getTableKey(), paymentCol),
        );
        if (ac.signal.aborted) return;
        setPaymentState({ loading: false, error: null, data });
      } catch (err: any) {
        if (ac.signal.aborted) return;
        setPaymentState({
          loading: false,
          error: err?.message || "Payment terms analysis failed",
          data: null,
        });
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const rerunSupplierAbortRef = useRef<AbortController | null>(null);

  const rerunSupplierPanelOnly = useCallback(
    async (version: number, vendorCol: string) => {
      if (rerunSupplierAbortRef.current) rerunSupplierAbortRef.current.abort();
      const ac = new AbortController();
      rerunSupplierAbortRef.current = ac;

      setSupplierState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await withRetry(() =>
          postDqaSupplier(sessionId, apiKey, tn, getTableKey(), vendorCol),
        );
        if (ac.signal.aborted) return;
        setSupplierState({ loading: false, error: null, data });
      } catch (err: any) {
        if (ac.signal.aborted) return;
        setSupplierState({
          loading: false,
          error: err?.message || "Supplier analysis failed",
          data: null,
        });
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const cancelAssessment = useCallback(() => {
    runningRef.current = false;
    setRunningAssessment(false);
    setAiLoading(false);
    setLoadingMessage("");
    if (rerunDateAbortRef.current) rerunDateAbortRef.current.abort();
    if (rerunCountryAbortRef.current) rerunCountryAbortRef.current.abort();
    if (rerunCurrencyAbortRef.current) rerunCurrencyAbortRef.current.abort();
    if (rerunPaymentAbortRef.current) rerunPaymentAbortRef.current.abort();
    if (rerunSupplierAbortRef.current) rerunSupplierAbortRef.current.abort();
    setDateState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setCurrencyState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setPaymentState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setCountryState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setSupplierState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setFillRateState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    setBifurcationState((s) => (s.loading ? { loading: false, error: "Cancelled", data: null } : s));
    addLog("Data Quality", "info", "Assessment cancelled by user.");
  }, [setAiLoading, setLoadingMessage, addLog]);

  useEffect(() => {
    if (cancelRef) cancelRef.current = cancelAssessment;
    return () => { if (cancelRef) cancelRef.current = null; };
  }, [cancelRef, cancelAssessment]);

  useEffect(() => {
    if (!hasRunRef.current && sessionId && apiKey) {
      hasRunRef.current = true;
      runAllPanels(selectedVersion);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (tableMissingRetry === 0) return;
    if (tableMissingRetry > MAX_TABLE_MISSING_RETRIES) {
      setTableMissing(true);
      return;
    }
    const timer = setTimeout(() => {
      runAllPanels(selectedVersion);
    }, 1500);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableMissingRetry]);

  const togglePanel = (id: string) =>
    setExpandedPanels((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  /* ── Render ───────────────────────────────────────────────────────────── */

  if (tableMissing) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard title="Merge Data Not Found" icon={AlertTriangle}>
          <div className="flex flex-col items-center text-center py-6 gap-4">
            <AlertTriangle className="w-10 h-10 text-amber-500" />
            <div>
              <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-1">
                Merged table is missing
              </p>
              <p className="text-xs text-neutral-500 dark:text-neutral-400 max-w-md">
                The merged data table could not be found. This can happen if earlier steps were changed. Please go back to the Merge step and re-run the merge.
              </p>
            </div>
            <PrimaryButton onClick={() => setStep(6)}>
              <ArrowRight className="w-4 h-4 rotate-180" />
              Back to Merge
            </PrimaryButton>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  if (sessionExpired) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard title="Session Expired" icon={AlertTriangle}>
          <div className="flex flex-col items-center text-center py-6 gap-4">
            <AlertTriangle className="w-10 h-10 text-amber-500" />
            <div>
              <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-1">
                Your session data was reset
              </p>
              <p className="text-xs text-neutral-500 dark:text-neutral-400 max-w-md">
                The session is no longer available. Please go back to the Upload step and re-upload your files.
              </p>
            </div>
            <PrimaryButton onClick={() => setStep(1)}>
              <ArrowRight className="w-4 h-4 rotate-180" />
              Back to Upload
            </PrimaryButton>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {/* Header card */}
      <SurfaceCard noPadding>
        <div className="rounded-3xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-start justify-between gap-6">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-6 h-6" />
                <h2 className="text-xl font-semibold tracking-tight">
                  Data Quality Assessment
                </h2>
              </div>
              <p className="text-red-50/90 text-sm max-w-xl">
                {isSingleTable
                  ? "Analysis of your dataset quality across key procurement parameters."
                  : "Analysis of your merged dataset quality across key procurement parameters."}
              </p>
            </div>
          </div>
        </div>
      </SurfaceCard>

      {/* Merge output selector + re-run */}
      <SurfaceCard>
        <div className="flex items-center justify-between gap-4 flex-wrap">
          {!isSingleTable ? (
            <div className="flex items-center gap-3">
              <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">
                Merge Output:
              </label>
              <div className="relative">
                <select
                  value={selectedVersion}
                  onChange={(e) => setSelectedVersion(Number(e.target.value))}
                  className="appearance-none pl-3 pr-8 py-1.5 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm font-medium text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-red-500/30 focus:border-red-500 outline-none"
                >
                  {mergeOutputs.map((o) => (
                    <option key={o.version} value={o.version}>
                      {o.label} ({o.rows.toLocaleString()} rows)
                    </option>
                  ))}
                </select>
                <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-4 h-4 text-neutral-400 pointer-events-none" />
              </div>
            </div>
          ) : (
            <p className="text-sm font-medium text-neutral-700 dark:text-neutral-300">
              Assessing:{" "}
              <span className="font-bold">{singleTableName}</span>
            </p>
          )}
          <button
            onClick={() => {
              setSelectedDateColumn(undefined);
              setSelectedCountryColumn(undefined);
              setSelectedCurrencyColumn(undefined);
              setSelectedPaymentTermsColumn(undefined);
              setSelectedSupplierColumn(undefined);
              setAiColumnSuggestions({});
              runAllPanels(selectedVersion);
            }}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 text-sm font-semibold text-neutral-700 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700 transition-colors"
          >
            <RefreshCw className="w-4 h-4" /> Re-run All
          </button>
        </div>
      </SurfaceCard>

      {/* ── Panel 0: Fill Rate Summary ──────────────────────────────────── */}
      <FillRateSummaryPanel
        state={fillRateState}
        expanded={expandedPanels.has("fillrate")}
        onToggle={() => togglePanel("fillrate")}
      />

      {/* ── Panel 1: Date Analysis ────────────────────────────────────── */}
      <DatePanel
        state={dateState}
        expanded={expandedPanels.has("date")}
        onToggle={() => togglePanel("date")}
        selectedDateColumn={selectedDateColumn}
        onDateColumnChange={(col) => {
          setSelectedDateColumn(col);
          rerunDatePanelOnly(selectedVersion, col);
        }}
      />

      {/* ── Panel 1b: Spend Bifurcation ────────────────────────────────── */}
      <SpendBifurcationPanel
        state={bifurcationState}
        expanded={expandedPanels.has("bifurcation")}
        onToggle={() => togglePanel("bifurcation")}
      />

      {/* ── Panel 2: Currency Analysis ────────────────────────────────── */}
      <CurrencyPanel
        state={currencyState}
        expanded={expandedPanels.has("currency")}
        onToggle={() => togglePanel("currency")}
        selectedCurrencyColumn={selectedCurrencyColumn}
        onCurrencyColumnChange={(col) => {
          setSelectedCurrencyColumn(col);
          rerunCurrencyPanelOnly(selectedVersion, col);
        }}
        aiSuggestions={aiColumnSuggestions["currency_code"]}
      />

      {/* ── Panel 3: Payment Terms ────────────────────────────────────── */}
      <PaymentTermsPanel
        state={paymentState}
        expanded={expandedPanels.has("payment")}
        onToggle={() => togglePanel("payment")}
        selectedPaymentTermsColumn={selectedPaymentTermsColumn}
        onPaymentTermsColumnChange={(col) => {
          setSelectedPaymentTermsColumn(col);
          rerunPaymentPanelOnly(selectedVersion, col);
        }}
        aiSuggestions={aiColumnSuggestions["payment_terms"]}
      />

      {/* ── Panel 4: Country / Region ─────────────────────────────────── */}
      <CountryRegionPanel
        state={countryState}
        expanded={expandedPanels.has("country")}
        onToggle={() => togglePanel("country")}
        selectedCountryColumn={selectedCountryColumn}
        onCountryColumnChange={(col) => {
          setSelectedCountryColumn(col);
          rerunCountryPanelOnly(selectedVersion, col);
        }}
      />

      {/* ── Panel 5: Supplier ─────────────────────────────────────────── */}
      <SupplierPanel
        state={supplierState}
        expanded={expandedPanels.has("supplier")}
        onToggle={() => togglePanel("supplier")}
        selectedSupplierColumn={selectedSupplierColumn}
        onSupplierColumnChange={(col) => {
          setSelectedSupplierColumn(col);
          rerunSupplierPanelOnly(selectedVersion, col);
        }}
        aiSuggestions={aiColumnSuggestions["vendor_name"]}
      />
    </motion.div>
  );
}

/** Merge backend-returned available columns with AI suggestions, deduplicating. */
function mergeColumnLists(
  backendCols?: string[],
  aiSuggestions?: string[],
): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const col of [...(aiSuggestions ?? []), ...(backendCols ?? [])]) {
    if (!seen.has(col)) {
      seen.add(col);
      result.push(col);
    }
  }
  return result;
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 1 – Date Analysis
   ══════════════════════════════════════════════════════════════════════════ */

function DatePanel({
  state,
  expanded,
  onToggle,
  selectedDateColumn,
  onDateColumnChange,
}: {
  state: PanelState<DateResult>;
  expanded: boolean;
  onToggle: () => void;
  selectedDateColumn: string | undefined;
  onDateColumnChange: (col: string) => void;
}) {
  const d = state.data;
  const subtitle = d
    ? `${d.selectedColumn ?? "—"} · ${d.consistent ? "Consistent formats" : "Inconsistent formats detected"}`
    : "Analysing date formats and spend patterns";

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <CalendarDays className="w-4 h-4 text-blue-600 dark:text-blue-400" />
        }
        iconBg="bg-blue-100 dark:bg-blue-950/40"
        title="Date Analysis"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Detecting date formats and building spend pivot…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  {/* Date column selector — always visible when a column was picked */}
                  {d.selectedColumn && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Date column:
                      </label>
                      {d.availableDateColumns.length > 1 ? (
                        <select
                          value={selectedDateColumn ?? d.selectedColumn ?? ""}
                          onChange={(e) => onDateColumnChange(e.target.value)}
                          className="text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-2 py-1 text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-blue-500/30 outline-none"
                        >
                          {d.availableDateColumns.map((c) => (
                            <option key={c} value={c}>
                              {c}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <span className="text-sm font-medium text-neutral-800 dark:text-neutral-200">
                          {d.selectedColumn}
                        </span>
                      )}
                    </div>
                  )}

                  {/* AI Insight */}
                  <StructuredInsight insight={d.aiInsight} accentColor="blue" />

                  {/* Deep dive: format table + total spend + pivot */}
                  {(d.formatTable.length > 0 || d.totalSpendSummary || d.pivotData?.feasible) && (
                    <DeepDiveSection>
                      {d.formatTable.length > 0 && (
                        <DateFormatTable entries={d.formatTable} />
                      )}

                      {d.totalSpendSummary && (
                        <DateTotalSpendBlock summary={d.totalSpendSummary} />
                      )}

                      {d.pivotData?.feasible && (
                        <div>
                          {d.pivotData.type === "reporting" ? (
                            <ReportingPivotTable pivot={d.pivotData} />
                          ) : (
                            <CurrencyCrosstabTable pivot={d.pivotData} />
                          )}
                        </div>
                      )}
                    </DeepDiveSection>
                  )}
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

function DateFormatTable({ entries }: { entries: FormatEntry[] }) {
  return (
    <div className="overflow-x-auto">
      <div className="px-6 py-2.5 border-b border-neutral-100 dark:border-neutral-800">
        <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
          Date Format Breakdown
        </p>
      </div>
      <div className="rounded-b-2xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
              <th className="px-6 py-3">File</th>
              <th className="px-4 py-3">Dominant Format</th>
              <th className="px-4 py-3">Format Breakdown</th>
              <th className="px-4 py-3">Example</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {entries.map((entry, idx) => (
              <tr
                key={entry.fileName}
                className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
              >
                <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 max-w-[200px] truncate">
                  {entry.fileName}
                </td>
                <td className="px-4 py-2.5">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-bold ${
                      entry.consistent
                        ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-400"
                        : "bg-amber-100 text-amber-700 dark:bg-amber-950/40 dark:text-amber-400"
                    }`}
                  >
                    {entry.dominantFormat}
                  </span>
                </td>
                <td className="px-4 py-2.5 text-xs text-neutral-600 dark:text-neutral-400">
                  {Object.entries(entry.formatPcts).map(([fmt, pct]) => (
                    <span key={fmt} className="mr-2">
                      {fmt}: {pct}%
                    </span>
                  ))}
                </td>
                <td className="px-4 py-2.5 text-xs text-neutral-500 dark:text-neutral-400 font-mono">
                  {Object.values(entry.examples)[0] ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function DateTotalSpendBlock({ summary }: { summary: TotalSpendSummary }) {
  return (
    <div className="px-6 py-4 border-t border-neutral-100 dark:border-neutral-800">
      <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400 mb-2">
        Total Spend
      </p>
      <p className="text-sm font-semibold tabular-nums text-neutral-800 dark:text-neutral-200">
        {summary.type === "reporting"
          ? fmtSpend(summary.total ?? 0)
          : summary.currencies
              ?.map((c) => `${fmtSpend(c.total)} ${c.code}`)
              .join(", ") ?? "N/A"}
      </p>
    </div>
  );
}

function ReportingPivotTable({ pivot }: { pivot: ReportingPivot }) {
  return (
    <div className="overflow-x-auto">
      <div className="px-6 py-2.5 bg-neutral-50 dark:bg-neutral-800/50 border-b border-neutral-100 dark:border-neutral-800">
        <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
          Spend by Year &amp; Month ({pivot.spendColumn})
        </p>
      </div>
      <div className="rounded-b-2xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
              <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">
                Month
              </th>
              {pivot.years.map((yr) => (
                <th key={yr} className="px-4 py-3 text-right min-w-[120px]">
                  {yr}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {pivot.months.map((monthName, idx) => {
              const monthNum = String(idx + 1);
              return (
                <tr
                  key={monthNum}
                  className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                >
                  <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                    {monthName}
                  </td>
                  {pivot.years.map((yr) => {
                    const val = pivot.cells[String(yr)]?.[monthNum] ?? 0;
                    return (
                      <td
                        key={yr}
                        className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                      >
                        {val === 0 ? (
                          <span className="text-neutral-300 dark:text-neutral-600">
                            0
                          </span>
                        ) : (
                          fmtSpend(val)
                        )}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CurrencyCrosstabTable({ pivot }: { pivot: CurrencyCrosstabPivot }) {
  return (
    <div className="overflow-x-auto">
      <div className="px-6 py-2.5 bg-neutral-50 dark:bg-neutral-800/50 border-b border-neutral-100 dark:border-neutral-800">
        <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
          Spend by Month &amp; Currency ({pivot.spendColumn})
        </p>
      </div>
      <div className="rounded-b-2xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
              <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">
                Period
              </th>
              {pivot.currencies.map((ccy) => (
                <th key={ccy} className="px-4 py-3 text-right min-w-[110px]">
                  {ccy}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {pivot.rowLabels.map((label, idx) => (
              <tr
                key={label}
                className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
              >
                <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10 whitespace-nowrap">
                  {label}
                </td>
                {pivot.currencies.map((ccy) => {
                  const val = pivot.cells[label]?.[ccy] ?? 0;
                  return (
                    <td
                      key={ccy}
                      className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                    >
                      {val === 0 ? (
                        <span className="text-neutral-300 dark:text-neutral-600">
                          0
                        </span>
                      ) : (
                        fmtSpend(val)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 2 – Currency Analysis
   ══════════════════════════════════════════════════════════════════════════ */

function CurrencyPanel({
  state,
  expanded,
  onToggle,
  selectedCurrencyColumn,
  onCurrencyColumnChange,
  aiSuggestions,
}: {
  state: PanelState<CurrencyResult>;
  expanded: boolean;
  onToggle: () => void;
  selectedCurrencyColumn: string | undefined;
  onCurrencyColumnChange: (col: string) => void;
  aiSuggestions?: string[];
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.distinctCount} currencies detected`
      : "No currency column found"
    : "Analysing currency distribution";

  const availableCols = mergeColumnLists(d?.availableCurrencyColumns, aiSuggestions);

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <Coins className="w-4 h-4 text-amber-600 dark:text-amber-400" />
        }
        iconBg="bg-amber-100 dark:bg-amber-950/40"
        title="Currency Analysis"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing currency quality…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && !d.exists && (
                <EmptyState icon={Coins} message="No currency column found" />
              )}
              {d && d.exists && (
                <>
                  {availableCols.length > 1 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Currency column:
                      </label>
                      <select
                        value={selectedCurrencyColumn ?? d.currencyColumn ?? ""}
                        onChange={(e) => onCurrencyColumnChange(e.target.value)}
                        className="text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-2 py-1 text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-amber-500/30 outline-none"
                      >
                        {availableCols.map((c) => (
                          <option key={c} value={c}>{c}</option>
                        ))}
                      </select>
                    </div>
                  )}

                  <StructuredInsight insight={d.aiInsight} accentColor="amber" />

                  {d.currencyTable.length > 0 && (
                    <DeepDiveSection>
                      <div className="overflow-x-auto rounded-b-2xl">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                              <th className="px-6 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">Currency Code</th>
                              <th className="px-4 py-3 text-center">% of Rows</th>
                              <th className="px-4 py-3 text-right">Local Spend</th>
                              <th className="px-4 py-3 text-right">Reporting Spend</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {d.currencyTable.map((row, idx) => (
                              <tr
                                key={row.currencyCode}
                                className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                              >
                                <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                                  {row.currencyCode}
                                </td>
                                <td className="px-4 py-2.5 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {row.rowPct.toFixed(1)}%
                                </td>
                                <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {d.hasLocalSpend ? fmtSpend(row.localSpend) : "N/A"}
                                </td>
                                <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {d.hasReportingSpend ? fmtSpend(row.reportingSpend) : "N/A"}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </DeepDiveSection>
                  )}
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 3 – Payment Terms
   ══════════════════════════════════════════════════════════════════════════ */

function PaymentTermsPanel({
  state,
  expanded,
  onToggle,
  selectedPaymentTermsColumn,
  onPaymentTermsColumnChange,
  aiSuggestions,
}: {
  state: PanelState<PaymentTermsResult>;
  expanded: boolean;
  onToggle: () => void;
  selectedPaymentTermsColumn: string | undefined;
  onPaymentTermsColumnChange: (col: string) => void;
  aiSuggestions?: string[];
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.uniqueCount} unique payment terms`
      : "No Payment Terms column found"
    : "Analysing payment terms";

  const currencyCols = d?.currencyColumns ?? [];
  const availableCols = mergeColumnLists(d?.availablePaymentTermsColumns, aiSuggestions);

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <FileText className="w-4 h-4 text-violet-600 dark:text-violet-400" />
        }
        iconBg="bg-violet-100 dark:bg-violet-950/40"
        title="Payment Terms Analysis"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing payment terms…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && !d.exists && (
                <EmptyState icon={FileText} message="Payment Terms column not found" />
              )}
              {d && d.exists && (
                <>
                  {availableCols.length > 1 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Payment terms column:
                      </label>
                      <select
                        value={selectedPaymentTermsColumn ?? d.availablePaymentTermsColumns?.[0] ?? ""}
                        onChange={(e) => onPaymentTermsColumnChange(e.target.value)}
                        className="text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-2 py-1 text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-violet-500/30 outline-none"
                      >
                        {availableCols.map((c) => (
                          <option key={c} value={c}>{c}</option>
                        ))}
                      </select>
                    </div>
                  )}

                  <StructuredInsight insight={d.aiInsight} accentColor="violet" />

                  {d.paymentTerms.length > 0 && (
                    <DeepDiveSection>
                      <div className="overflow-x-auto rounded-b-2xl">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                              <th className="px-6 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">Term</th>
                              <th className="px-4 py-3 text-right">Spend (Reporting)</th>
                              <th className="px-4 py-3 text-center">% of Rows</th>
                              {currencyCols.map((ccy) => (
                                <th key={ccy} className="px-4 py-3 text-right">
                                  {ccy}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {d.paymentTerms.map((row, idx) => (
                              <tr
                                key={row.term}
                                className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                              >
                                <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                                  {row.term}
                                </td>
                                <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {fmtSpend(row.spend)}
                                </td>
                                <td className="px-4 py-2.5 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {row.pctOfRows != null
                                    ? `${row.pctOfRows.toFixed(1)}%`
                                    : "—"}
                                </td>
                                {currencyCols.map((ccy) => (
                                  <td
                                    key={ccy}
                                    className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                                  >
                                    {row.currencySpend?.[ccy] != null
                                      ? fmtSpend(row.currencySpend[ccy])
                                      : "—"}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </DeepDiveSection>
                  )}
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 4 – Country / Region
   ══════════════════════════════════════════════════════════════════════════ */

function CountryRegionPanel({
  state,
  expanded,
  onToggle,
  selectedCountryColumn,
  onCountryColumnChange,
}: {
  state: PanelState<CountryRegionResult>;
  expanded: boolean;
  onToggle: () => void;
  selectedCountryColumn: string | undefined;
  onCountryColumnChange: (col: string) => void;
}) {
  const d = state.data;
  const parts: string[] = [];
  if (d?.countryColumn) parts.push(`Country: ${d.countryValues?.length ?? 0}`);
  if (d?.regionColumn) parts.push(`Region: ${d.regionValues?.length ?? 0}`);
  const subtitle = d
    ? parts.length > 0
      ? parts.join(" · ") + " unique values"
      : "No country or region columns found"
    : "Analysing country & region data";

  const availableCols = d?.availableCountryColumns ?? [];

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <Globe className="w-4 h-4 text-emerald-600 dark:text-emerald-400" />
        }
        iconBg="bg-emerald-100 dark:bg-emerald-950/40"
        title="Country & Region Analysis"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing country & region values…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  {/* Country column dropdown */}
                  {availableCols.length > 1 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Country column:
                      </label>
                      <select
                        value={selectedCountryColumn ?? d.countryColumn ?? ""}
                        onChange={(e) => onCountryColumnChange(e.target.value)}
                        className="text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-2 py-1 text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-emerald-500/30 outline-none"
                      >
                        {availableCols.map((c) => (
                          <option key={c} value={c}>
                            {c}
                          </option>
                        ))}
                      </select>
                    </div>
                  )}

                  {/* AI insights — country and region stacked */}
                  <StructuredInsight insight={d.countryAiInsight} accentColor="emerald" />
                  <StructuredInsight insight={d.regionAiInsight} accentColor="emerald" />

                  {/* Deep dive: country and region value grids */}
                  {(
                    (d.countryValues && d.countryValues.length > 0) ||
                    (d.regionValues && d.regionValues.length > 0)
                  ) && (
                    <DeepDiveSection>
                      {d.countryValues && d.countryValues.length > 0 && (
                        <div className="px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
                          <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400 mb-3">
                            Unique Country Values ({d.countryValues.length})
                            {d.countryColumn && (
                              <span className="ml-1 font-normal normal-case text-neutral-400">
                                — {d.countryColumn}
                              </span>
                            )}
                          </p>
                          <div className="rounded-xl overflow-hidden border border-neutral-200 dark:border-neutral-700">
                            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                              {d.countryValues.slice(0, 100).map((v, idx) => (
                                <div
                                  key={v}
                                  className={`px-4 py-2 text-sm text-neutral-700 dark:text-neutral-300 border-b border-r border-neutral-100 dark:border-neutral-800 ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                                >
                                  {v}
                                </div>
                              ))}
                            </div>
                            {d.countryValues.length > 100 && (
                              <div className="px-4 py-2 text-xs text-neutral-400 text-center bg-neutral-50/50 dark:bg-neutral-800/30">
                                +{d.countryValues.length - 100} more
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {d.regionValues && d.regionValues.length > 0 && (
                        <div className="px-6 py-4">
                          <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400 mb-3">
                            Unique Region Values ({d.regionValues.length})
                            {d.regionColumn && (
                              <span className="ml-1 font-normal normal-case text-neutral-400">
                                — {d.regionColumn}
                              </span>
                            )}
                          </p>
                          <div className="rounded-xl overflow-hidden border border-neutral-200 dark:border-neutral-700">
                            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                              {d.regionValues.slice(0, 100).map((v, idx) => (
                                <div
                                  key={v}
                                  className={`px-4 py-2 text-sm text-neutral-700 dark:text-neutral-300 border-b border-r border-neutral-100 dark:border-neutral-800 ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                                >
                                  {v}
                                </div>
                              ))}
                            </div>
                            {d.regionValues.length > 100 && (
                              <div className="px-4 py-2 text-xs text-neutral-400 text-center bg-neutral-50/50 dark:bg-neutral-800/30">
                                +{d.regionValues.length - 100} more
                              </div>
                            )}
                          </div>
                        </div>
                      )}
                    </DeepDiveSection>
                  )}

                  {!d.countryColumn && !d.regionColumn && (
                    <EmptyState icon={Globe} message="No country or region columns found in the dataset." />
                  )}
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 5 – Supplier
   ══════════════════════════════════════════════════════════════════════════ */

function SupplierPanel({
  state,
  expanded,
  onToggle,
  selectedSupplierColumn,
  onSupplierColumnChange,
  aiSuggestions,
}: {
  state: PanelState<SupplierResult>;
  expanded: boolean;
  onToggle: () => void;
  selectedSupplierColumn: string | undefined;
  onSupplierColumnChange: (col: string) => void;
  aiSuggestions?: string[];
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.supplierCount.toLocaleString()} unique suppliers`
      : "No Vendor Name column found"
    : "Analysing supplier names";

  const availableCols = mergeColumnLists(d?.availableSupplierColumns, aiSuggestions);

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <Users className="w-4 h-4 text-rose-600 dark:text-rose-400" />
        }
        iconBg="bg-rose-100 dark:bg-rose-950/40"
        title="Supplier Analysis"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing supplier names for normalisation opportunities…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && !d.exists && (
                <EmptyState icon={Users} message="No Vendor Name column found" />
              )}
              {d && d.exists && (
                <>
                  {availableCols.length > 1 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Supplier column:
                      </label>
                      <select
                        value={selectedSupplierColumn ?? d.availableSupplierColumns?.[0] ?? ""}
                        onChange={(e) => onSupplierColumnChange(e.target.value)}
                        className="text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-2 py-1 text-neutral-800 dark:text-neutral-200 focus:ring-2 focus:ring-rose-500/30 outline-none"
                      >
                        {availableCols.map((c) => (
                          <option key={c} value={c}>{c}</option>
                        ))}
                      </select>
                    </div>
                  )}

                  <StructuredInsight insight={d.aiInsight} accentColor="rose" />

                  {d.hasReportingSpend && (
                    <DeepDiveSection>
                      {d.paretoVendorCount != null && d.paretoVendorPct != null && (
                        <div className="px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
                          <p className="text-sm text-neutral-700 dark:text-neutral-300">
                            <strong className="font-semibold">{d.paretoVendorCount.toLocaleString()}</strong> suppliers account for{" "}
                            <strong className="font-semibold">{d.paretoVendorPct.toFixed(1)}%</strong> of total spend
                          </p>
                        </div>
                      )}

                      {d.top20 && d.top20.length > 0 && (
                        <div className="overflow-x-auto rounded-b-2xl">
                          <div className="px-6 py-2.5 border-b border-neutral-100 dark:border-neutral-800">
                            <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
                              Top {d.top20.length} Suppliers by Spend
                            </p>
                          </div>
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                                <th className="px-6 py-3 w-8">#</th>
                                <th className="px-4 py-3">Vendor</th>
                                <th className="px-4 py-3 text-right">Spend</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                              {d.top20.map((row, idx) => (
                                <tr
                                  key={row.vendor}
                                  className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                                >
                                  <td className="px-6 py-2.5 tabular-nums text-neutral-400 text-xs">
                                    {idx + 1}
                                  </td>
                                  <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 max-w-[300px] truncate">
                                    {row.vendor}
                                  </td>
                                  <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                    {fmtSpend(row.spend)}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </DeepDiveSection>
                  )}
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 0 – Fill Rate Summary
   ══════════════════════════════════════════════════════════════════════════ */

function FillRateSummaryPanel({
  state,
  expanded,
  onToggle,
}: {
  state: PanelState<FillRateResult>;
  expanded: boolean;
  onToggle: () => void;
}) {
  const d = state.data;
  const subtitle = d
    ? `${d.columns.length} columns analysed`
    : "Computing fill rates…";

  const spendHeader =
    d?.spendType === "reporting"
      ? "% of Total Spend"
      : d?.spendType === "local"
        ? "% of Local Spend by Currency"
        : null;

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <TableProperties className="w-4 h-4 text-teal-600 dark:text-teal-400" />
        }
        iconBg="bg-teal-100 dark:bg-teal-950/40"
        title="Fill Rate Summary"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Computing fill rates…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && d.columns.length > 0 && (
                <div className="overflow-x-auto rounded-b-2xl">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-6 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">Column Name</th>
                        <th className="px-4 py-3 text-center">% Rows Covered</th>
                        {spendHeader && (
                          <th className="px-4 py-3 text-center">{spendHeader}</th>
                        )}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {d.columns.map((col, idx) => (
                        <tr
                          key={col.columnName}
                          className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                        >
                          <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 max-w-[260px] truncate sticky left-0 bg-white dark:bg-neutral-900 z-10">
                            {col.columnName}
                          </td>
                          <td className="px-4 py-2.5 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                            {col.pctRowsCovered.toFixed(1)}%
                          </td>
                          {spendHeader && (
                            <td className="px-4 py-2.5 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                              <FillRateSpendCell
                                coverage={col.spendCoverage}
                                spendType={d.spendType}
                              />
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

function FillRateSpendCell({
  coverage,
  spendType,
}: {
  coverage: FillRateColumn["spendCoverage"];
  spendType: string;
}) {
  if (coverage == null) return <span className="text-neutral-400">N/A</span>;
  if (spendType === "reporting" && typeof coverage === "number") {
    return <>{coverage.toFixed(1)}%</>;
  }
  if (Array.isArray(coverage)) {
    return (
      <span className="flex flex-wrap justify-center gap-1">
        {coverage.map((c) => (
          <span
            key={c.code}
            className="text-xs px-1.5 py-0.5 rounded bg-neutral-100 dark:bg-neutral-800"
          >
            {c.code}({c.pct}%)
          </span>
        ))}
      </span>
    );
  }
  return <span className="text-neutral-400">N/A</span>;
}

/* ══════════════════════════════════════════════════════════════════════════
   Spend Bifurcation Panel
   ══════════════════════════════════════════════════════════════════════════ */

function SpendBifurcationPanel({
  state,
  expanded,
  onToggle,
}: {
  state: PanelState<SpendBifurcationResult>;
  expanded: boolean;
  onToggle: () => void;
}) {
  const d = state.data;
  const subtitle = d
    ? d.type === "none"
      ? "No spend column found"
      : `Positive vs negative spend (${d.column})`
    : "Computing spend bifurcation…";

  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={
          <SplitSquareHorizontal className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
        }
        iconBg="bg-indigo-100 dark:bg-indigo-950/40"
        title="Spend Bifurcation"
        subtitle={subtitle}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={EXPAND_TRANSITION}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Computing spend bifurcation…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && d.type === "none" && (
                <EmptyState icon={SplitSquareHorizontal} message="No spend column found in the dataset." />
              )}
              {d && (d.type === "reporting" || d.type === "local_single") && (
                <div className="overflow-x-auto rounded-b-2xl">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-6 py-3 text-right">Total Positive Spend</th>
                        <th className="px-4 py-3 text-right">Total Negative Spend</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td className="px-6 py-3 text-right tabular-nums font-semibold text-emerald-700 dark:text-emerald-400">
                          {fmtSpend(d.positiveSpend ?? 0)}
                        </td>
                        <td className="px-4 py-3 text-right tabular-nums font-semibold text-red-600 dark:text-red-400">
                          {fmtSpend(d.negativeSpend ?? 0)}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              )}
              {d && d.type === "local" && d.currencies && (
                <div className="overflow-x-auto rounded-b-2xl">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-6 py-3">Currency</th>
                        <th className="px-4 py-3 text-right">Total +ve Spend</th>
                        <th className="px-4 py-3 text-right">Total -ve Spend</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {d.currencies.map((c, idx) => (
                        <tr
                          key={c.code}
                          className={`hover:bg-neutral-100/60 dark:hover:bg-neutral-700/20 transition-colors ${idx % 2 === 0 ? "bg-neutral-50/30 dark:bg-neutral-800/10" : ""}`}
                        >
                          <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200">
                            {c.code}
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-emerald-700 dark:text-emerald-400">
                            {fmtSpend(c.positiveSpend)}
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-red-600 dark:text-red-400">
                            {fmtSpend(c.negativeSpend)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}
