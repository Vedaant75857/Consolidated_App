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
  Users,
} from "lucide-react";
import { SurfaceCard, PrimaryButton, itemVariants } from "../common/ui";
import {
  postDqaDate,
  postDqaCurrency,
  postDqaPaymentTerms,
  postDqaCountryRegion,
  postDqaSupplier,
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

interface DateResult {
  availableDateColumns: string[];
  selectedColumn: string | null;
  formatTable: FormatEntry[];
  pivotData: PivotData | null;
  consistent: boolean;
  aiInsight: string;
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
  currencyColumn: string | null;
  distinctCount: number;
  codes: string[];
  currencyTable: CurrencyTableRow[];
  hasLocalSpend: boolean;
  hasReportingSpend: boolean;
  aiInsight: string;
}

interface PaymentTermRow {
  term: string;
  spend: number | null;
  rowCount: number;
  pctOfTotal: number | null;
}

interface PaymentTermsResult {
  exists: boolean;
  paymentTerms: PaymentTermRow[];
  totalSpend: number;
  uniqueCount: number;
  spendColumn: string | null;
  aiInsight: string;
}

interface CountryRegionResult {
  countryColumn: string | null;
  regionColumn: string | null;
  countryValues: string[] | null;
  regionValues: string[] | null;
  countryAiInsight: string | null;
  regionAiInsight: string | null;
}

interface SupplierResult {
  exists: boolean;
  supplierCount: number;
  topNCount: number;
  totalSpendCovered: number;
  spendColumn: string | null;
  aiInsight: string;
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
}

const SESSION_EXPIRED_PATTERN = /not found in session/i;

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fmtSpend(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return Math.round(val).toLocaleString();
}

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

function renderInsightMarkdown(text: string): ReactNode {
  const lines = text.split("\n").filter((l) => l.trim());
  const hasBullets = lines.some((l) => l.trim().startsWith("- "));
  if (hasBullets) {
    return (
      <ul className="list-disc list-inside space-y-1">
        {lines.map((line, i) => {
          const content = line.replace(/^-\s*/, "");
          return <li key={i}>{renderBoldMarkdown(content)}</li>;
        })}
      </ul>
    );
  }
  return renderBoldMarkdown(text);
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
}: DataQualityAssessmentProps) {
  const isSingleTable = !!singleTableName;
  const latestVersion =
    mergeOutputs.length > 0
      ? Math.max(...mergeOutputs.map((o) => o.version))
      : 1;

  const [selectedVersion, setSelectedVersion] = useState(latestVersion);
  const [sessionExpired, setSessionExpired] = useState(false);

  // Per-panel state
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

  // Expanded panels
  const [expandedPanels, setExpandedPanels] = useState<Set<string>>(
    new Set(["date", "currency", "payment", "country", "supplier"]),
  );

  // Date column selector
  const [selectedDateColumn, setSelectedDateColumn] = useState<
    string | undefined
  >(undefined);

  const hasRunRef = useRef(false);

  const getTableName = useCallback(
    (version: number) =>
      isSingleTable ? singleTableName! : tableNameForVersion(version),
    [isSingleTable, singleTableName],
  );

  const getTableKey = useCallback(
    () => (isSingleTable ? singleTableName : undefined),
    [isSingleTable, singleTableName],
  );

  // ── Panel runners ────────────────────────────────────────────────────

  const checkSessionError = useCallback((err: any) => {
    const msg = err?.message || "";
    if (SESSION_EXPIRED_PATTERN.test(msg)) {
      setSessionExpired(true);
      return true;
    }
    return false;
  }, []);

  const runDatePanel = useCallback(
    async (version: number, dateCol?: string) => {
      if (sessionExpired) return;
      setDateState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await postDqaDate(
          sessionId,
          apiKey,
          tn,
          dateCol,
          getTableKey(),
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
        if (!checkSessionError(err)) {
          setDateState({
            loading: false,
            error: err?.message || "Date analysis failed",
            data: null,
          });
        }
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, sessionExpired, checkSessionError],
  );

  const runCurrencyPanel = useCallback(
    async (version: number) => {
      if (sessionExpired) return;
      setCurrencyState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await postDqaCurrency(
          sessionId,
          apiKey,
          tn,
          getTableKey(),
        );
        setCurrencyState({ loading: false, error: null, data });
      } catch (err: any) {
        if (!checkSessionError(err)) {
          setCurrencyState({
            loading: false,
            error: err?.message || "Currency analysis failed",
            data: null,
          });
        }
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, sessionExpired, checkSessionError],
  );

  const runPaymentPanel = useCallback(
    async (version: number) => {
      if (sessionExpired) return;
      setPaymentState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await postDqaPaymentTerms(
          sessionId,
          apiKey,
          tn,
          getTableKey(),
        );
        setPaymentState({ loading: false, error: null, data });
      } catch (err: any) {
        if (!checkSessionError(err)) {
          setPaymentState({
            loading: false,
            error: err?.message || "Payment terms analysis failed",
            data: null,
          });
        }
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, sessionExpired, checkSessionError],
  );

  const runCountryPanel = useCallback(
    async (version: number) => {
      if (sessionExpired) return;
      setCountryState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await postDqaCountryRegion(
          sessionId,
          apiKey,
          tn,
          getTableKey(),
        );
        setCountryState({ loading: false, error: null, data });
      } catch (err: any) {
        if (!checkSessionError(err)) {
          setCountryState({
            loading: false,
            error: err?.message || "Country/Region analysis failed",
            data: null,
          });
        }
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey, sessionExpired, checkSessionError],
  );

  const runSupplierPanel = useCallback(
    async (version: number) => {
      if (sessionExpired) return;
      setSupplierState({ loading: true, error: null, data: null });
      try {
        const tn = isSingleTable ? "" : tableNameForVersion(version);
        const data = await postDqaSupplier(
          sessionId,
          apiKey,
          tn,
          getTableKey(),
        );
        setSupplierState({ loading: false, error: null, data });
      } catch (err: any) {
        if (!checkSessionError(err)) {
          setSupplierState({
            loading: false,
            error: err?.message || "Supplier analysis failed",
            data: null,
          });
        }
      }
    },
    [sessionId, apiKey, isSingleTable, getTableKey],
  );

  const runAllPanels = useCallback(
    (version: number) => {
      addLog("Data Quality", "info", "Running data quality assessment…");
      setAiLoading(true);
      setLoadingMessage("Running Data Quality Assessment…");

      const promises = [
        runDatePanel(version),
        runCurrencyPanel(version),
        runPaymentPanel(version),
        runCountryPanel(version),
        runSupplierPanel(version),
      ];

      Promise.allSettled(promises).then(() => {
        setAiLoading(false);
        setLoadingMessage("");
        addLog("Data Quality", "success", "Assessment complete.");
      });
    },
    [
      runDatePanel,
      runCurrencyPanel,
      runPaymentPanel,
      runCountryPanel,
      runSupplierPanel,
      addLog,
      setAiLoading,
      setLoadingMessage,
    ],
  );

  useEffect(() => {
    if (!hasRunRef.current && sessionId && apiKey) {
      hasRunRef.current = true;
      runAllPanels(selectedVersion);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const togglePanel = (id: string) =>
    setExpandedPanels((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  // ── Render ───────────────────────────────────────────────────────────

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
                The server was restarted and the data tables are no longer available. Please go back to the Upload step and re-upload your files.
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
              runAllPanels(selectedVersion);
            }}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 text-sm font-semibold text-neutral-700 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700 transition-colors"
          >
            <RefreshCw className="w-4 h-4" /> Re-run All
          </button>
        </div>
      </SurfaceCard>

      {/* ── Panel 1: Date Analysis ──────────────────────────────────────── */}
      <DatePanel
        state={dateState}
        expanded={expandedPanels.has("date")}
        onToggle={() => togglePanel("date")}
        selectedDateColumn={selectedDateColumn}
        onDateColumnChange={(col) => {
          setSelectedDateColumn(col);
          runDatePanel(selectedVersion, col);
        }}
      />

      {/* ── Panel 2: Currency Analysis ──────────────────────────────────── */}
      <CurrencyPanel
        state={currencyState}
        expanded={expandedPanels.has("currency")}
        onToggle={() => togglePanel("currency")}
      />

      {/* ── Panel 3: Payment Terms ──────────────────────────────────────── */}
      <PaymentTermsPanel
        state={paymentState}
        expanded={expandedPanels.has("payment")}
        onToggle={() => togglePanel("payment")}
      />

      {/* ── Panel 4: Country / Region ───────────────────────────────────── */}
      <CountryRegionPanel
        state={countryState}
        expanded={expandedPanels.has("country")}
        onToggle={() => togglePanel("country")}
      />

      {/* ── Panel 5: Supplier ───────────────────────────────────────────── */}
      <SupplierPanel
        state={supplierState}
        expanded={expandedPanels.has("supplier")}
        onToggle={() => togglePanel("supplier")}
      />
    </motion.div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Shared sub-components
   ══════════════════════════════════════════════════════════════════════════ */

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

function AiInsightBanner({ insight }: { insight: string }) {
  if (!insight) return null;
  return (
    <div className="px-6 py-4 bg-blue-50/70 dark:bg-blue-950/30 border-b border-neutral-100 dark:border-neutral-800">
      <p className="text-[10px] uppercase tracking-wider font-semibold text-blue-600 dark:text-blue-400 mb-2">
        AI Insights
      </p>
      <div className="text-sm leading-relaxed text-neutral-700 dark:text-neutral-300">
        {renderInsightMarkdown(insight)}
      </div>
    </div>
  );
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
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Detecting date formats and building spend pivot…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  {/* Date column selector */}
                  {d.availableDateColumns.length > 1 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 flex items-center gap-3">
                      <label className="text-xs font-medium text-neutral-600 dark:text-neutral-400">
                        Date column:
                      </label>
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
                    </div>
                  )}

                  {/* AI Insight */}
                  <AiInsightBanner insight={d.aiInsight} />

                  {/* Format table */}
                  {d.formatTable.length > 0 && (
                    <div className="overflow-x-auto">
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
                          {d.formatTable.map((entry) => (
                            <tr
                              key={entry.fileName}
                              className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
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
                                {Object.entries(entry.formatPcts).map(
                                  ([fmt, pct]) => (
                                    <span key={fmt} className="mr-2">
                                      {fmt}: {pct}%
                                    </span>
                                  ),
                                )}
                              </td>
                              <td className="px-4 py-2.5 text-xs text-neutral-500 dark:text-neutral-400 font-mono">
                                {Object.values(entry.examples)[0] ?? "—"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Pivot table */}
                  {d.pivotData?.feasible && (
                    <div className="border-t border-neutral-100 dark:border-neutral-800">
                      {d.pivotData.type === "reporting" ? (
                        <ReportingPivotTable pivot={d.pivotData} />
                      ) : (
                        <CurrencyCrosstabTable pivot={d.pivotData} />
                      )}
                    </div>
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

function ReportingPivotTable({ pivot }: { pivot: ReportingPivot }) {
  return (
    <div className="overflow-x-auto">
      <div className="px-6 py-2.5 bg-neutral-50 dark:bg-neutral-800/50 border-b border-neutral-100 dark:border-neutral-800">
        <p className="text-[10px] uppercase tracking-wider font-semibold text-neutral-500 dark:text-neutral-400">
          Spend by Year &amp; Month ({pivot.spendColumn})
        </p>
      </div>
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
                className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
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
          {pivot.rowLabels.map((label) => (
            <tr
              key={label}
              className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
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
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 2 – Currency Analysis
   ══════════════════════════════════════════════════════════════════════════ */

function CurrencyPanel({
  state,
  expanded,
  onToggle,
}: {
  state: PanelState<CurrencyResult>;
  expanded: boolean;
  onToggle: () => void;
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.distinctCount} currencies detected`
      : "No currency column found"
    : "Analysing currency distribution";

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
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing currency quality…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  <AiInsightBanner insight={d.aiInsight} />

                  {d.currencyTable.length > 0 && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                            <th className="px-6 py-3">Currency Code</th>
                            <th className="px-4 py-3 text-center">
                              % of Rows
                            </th>
                            <th className="px-4 py-3 text-right">
                              Local Spend
                            </th>
                            <th className="px-4 py-3 text-right">
                              Reporting Spend
                            </th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                          {d.currencyTable.map((row) => (
                            <tr
                              key={row.currencyCode}
                              className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                            >
                              <td className="px-6 py-2 font-medium text-neutral-800 dark:text-neutral-200">
                                {row.currencyCode}
                              </td>
                              <td className="px-4 py-2 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                {row.rowPct.toFixed(1)}%
                              </td>
                              <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                {d.hasLocalSpend
                                  ? fmtSpend(row.localSpend)
                                  : "N/A"}
                              </td>
                              <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                {d.hasReportingSpend
                                  ? fmtSpend(row.reportingSpend)
                                  : "N/A"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
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
}: {
  state: PanelState<PaymentTermsResult>;
  expanded: boolean;
  onToggle: () => void;
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.uniqueCount} unique payment terms`
      : "No Payment Terms column found"
    : "Analysing payment terms";

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
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing payment terms…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  <AiInsightBanner insight={d.aiInsight} />

                  {d.paymentTerms.length > 0 && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                            <th className="px-6 py-3">Payment Term</th>
                            <th className="px-4 py-3 text-right">Spend</th>
                            <th className="px-4 py-3 text-center">
                              % of Total
                            </th>
                            <th className="px-4 py-3 text-right">Rows</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                          {d.paymentTerms.map((row) => (
                            <tr
                              key={row.term}
                              className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                            >
                              <td className="px-6 py-2 font-medium text-neutral-800 dark:text-neutral-200">
                                {row.term}
                              </td>
                              <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                {fmtSpend(row.spend)}
                              </td>
                              <td className="px-4 py-2 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                {row.pctOfTotal != null
                                  ? `${row.pctOfTotal.toFixed(1)}%`
                                  : "—"}
                              </td>
                              <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                {row.rowCount.toLocaleString()}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
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
}: {
  state: PanelState<CountryRegionResult>;
  expanded: boolean;
  onToggle: () => void;
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
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing country & region values…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  {/* Country sub-section */}
                  {d.countryAiInsight && (
                    <div className="px-6 py-4 bg-blue-50/70 dark:bg-blue-950/30 border-b border-neutral-100 dark:border-neutral-800">
                      <p className="text-[10px] uppercase tracking-wider font-semibold text-blue-600 dark:text-blue-400 mb-1">
                        Country Insights
                        {d.countryColumn && (
                          <span className="ml-1 font-normal normal-case text-neutral-500">
                            ({d.countryColumn})
                          </span>
                        )}
                      </p>
                      <div className="text-sm leading-relaxed text-neutral-700 dark:text-neutral-300">
                        {renderInsightMarkdown(d.countryAiInsight)}
                      </div>
                    </div>
                  )}

                  {d.countryValues && d.countryValues.length > 0 && (
                    <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800">
                      <p className="text-xs font-semibold text-neutral-500 dark:text-neutral-400 mb-2">
                        Unique Country Values ({d.countryValues.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {d.countryValues.slice(0, 100).map((v) => (
                          <span
                            key={v}
                            className="text-xs px-2 py-0.5 rounded-full bg-neutral-100 dark:bg-neutral-800 text-neutral-700 dark:text-neutral-300"
                          >
                            {v}
                          </span>
                        ))}
                        {d.countryValues.length > 100 && (
                          <span className="text-xs text-neutral-400">
                            +{d.countryValues.length - 100} more
                          </span>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Region sub-section */}
                  {d.regionAiInsight && (
                    <div className="px-6 py-4 bg-blue-50/70 dark:bg-blue-950/30 border-b border-neutral-100 dark:border-neutral-800">
                      <p className="text-[10px] uppercase tracking-wider font-semibold text-blue-600 dark:text-blue-400 mb-1">
                        Region Insights
                        {d.regionColumn && (
                          <span className="ml-1 font-normal normal-case text-neutral-500">
                            ({d.regionColumn})
                          </span>
                        )}
                      </p>
                      <div className="text-sm leading-relaxed text-neutral-700 dark:text-neutral-300">
                        {renderInsightMarkdown(d.regionAiInsight)}
                      </div>
                    </div>
                  )}

                  {d.regionValues && d.regionValues.length > 0 && (
                    <div className="px-6 py-3">
                      <p className="text-xs font-semibold text-neutral-500 dark:text-neutral-400 mb-2">
                        Unique Region Values ({d.regionValues.length})
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {d.regionValues.slice(0, 100).map((v) => (
                          <span
                            key={v}
                            className="text-xs px-2 py-0.5 rounded-full bg-neutral-100 dark:bg-neutral-800 text-neutral-700 dark:text-neutral-300"
                          >
                            {v}
                          </span>
                        ))}
                        {d.regionValues.length > 100 && (
                          <span className="text-xs text-neutral-400">
                            +{d.regionValues.length - 100} more
                          </span>
                        )}
                      </div>
                    </div>
                  )}

                  {!d.countryColumn && !d.regionColumn && (
                    <div className="px-6 py-6 text-center text-sm text-neutral-500 dark:text-neutral-400">
                      No country or region columns found in the dataset.
                    </div>
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
}: {
  state: PanelState<SupplierResult>;
  expanded: boolean;
  onToggle: () => void;
}) {
  const d = state.data;
  const subtitle = d
    ? d.exists
      ? `${d.supplierCount.toLocaleString()} unique suppliers`
      : "No Vendor Name column found"
    : "Analysing supplier names";

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
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {state.loading && (
                <PanelSpinner message="Analysing supplier names for normalisation opportunities…" />
              )}
              {state.error && !d && <PanelError message={state.error} />}
              {d && (
                <>
                  <AiInsightBanner insight={d.aiInsight} />

                  {d.exists && (
                    <div className="px-6 py-4">
                      <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                        <div className="rounded-xl bg-neutral-50 dark:bg-neutral-800/50 p-4">
                          <p className="text-[10px] uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-1">
                            Total Unique Suppliers
                          </p>
                          <p className="text-lg font-bold tabular-nums text-neutral-800 dark:text-neutral-200">
                            {d.supplierCount.toLocaleString()}
                          </p>
                        </div>
                        <div className="rounded-xl bg-neutral-50 dark:bg-neutral-800/50 p-4">
                          <p className="text-[10px] uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-1">
                            Suppliers Analysed
                          </p>
                          <p className="text-lg font-bold tabular-nums text-neutral-800 dark:text-neutral-200">
                            {d.topNCount.toLocaleString()}
                          </p>
                        </div>
                        {d.totalSpendCovered > 0 && (
                          <div className="rounded-xl bg-neutral-50 dark:bg-neutral-800/50 p-4">
                            <p className="text-[10px] uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-1">
                              Spend Covered
                            </p>
                            <p className="text-lg font-bold tabular-nums text-neutral-800 dark:text-neutral-200">
                              {fmtSpend(d.totalSpendCovered)}
                            </p>
                          </div>
                        )}
                      </div>
                    </div>
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
