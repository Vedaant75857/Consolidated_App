import React, { useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  BarChart3,
  CalendarDays,
  ChevronDown,
  Columns3,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
  TrendingUp,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  getExecutiveSummary,
  type ExecutiveSummaryResult,
  type DatePivotResult,
  type ParetoAnalysisResult,
  type ParetoThresholdMetrics,
  type SpendBifurcationResult,
  type ColumnFillRateResult,
  type FlagsResult,
  type CategorizationEffortResult,
} from "../../api/client";

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fmtSpend(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return Math.round(val).toLocaleString();
}

function fmtCurrency(val: number): string {
  const abs = Math.abs(val);
  if (abs >= 1_000_000_000) return `USD ${(val / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `USD ${(val / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `USD ${(val / 1_000).toFixed(0)}K`;
  return `USD ${Math.round(val).toLocaleString()}`;
}

/* ── Props ─────────────────────────────────────────────────────────────── */

interface ExecutiveSummaryProps {
  sessionId: string;
  apiKey: string;
  onLoaded?: () => void;
}

/* ── Component ─────────────────────────────────────────────────────────── */

export default function ExecutiveSummary({
  sessionId,
  apiKey,
  onLoaded,
}: ExecutiveSummaryProps) {
  const [result, setResult] = useState<ExecutiveSummaryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [fillRateOpen, setFillRateOpen] = useState(true);
  const [datePivotOpen, setDatePivotOpen] = useState(true);
  const [bifurcationOpen, setBifurcationOpen] = useState(true);
  const [paretoOpen, setParetoOpen] = useState(true);

  const MAX_RETRIES = 3;
  const RETRY_BASE_MS = 2000;
  const [retryAttempt, setRetryAttempt] = useState(0);

  const runAssessment = useCallback(
    async (force = false) => {
      setLoading(true);
      setError(null);

      for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        setRetryAttempt(attempt);
        try {
          const data = await getExecutiveSummary(sessionId, apiKey, force);
          setResult(data);
          setRetryAttempt(0);
          onLoaded?.();
          setLoading(false);
          return;
        } catch (err: any) {
          if (attempt < MAX_RETRIES - 1) {
            await new Promise((r) => setTimeout(r, RETRY_BASE_MS * 2 ** attempt));
          } else {
            setError(err?.message || "Spend Quality Assessment failed");
          }
        }
      }

      setRetryAttempt(0);
      setLoading(false);
    },
    [sessionId, apiKey, onLoaded]
  );

  useEffect(() => {
    if (!result && !loading && !error && sessionId && apiKey) {
      runAssessment(false);
    } else if (!apiKey && !error) {
      setError("API key is required to run the Spend Quality Assessment. Please enter your API key above.");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* ── Loading ─────────────────────────────────────────────────────────── */

  if (loading) {
    return (
      <motion.div
        variants={itemVariants}
        className="flex flex-col items-center justify-center py-24 gap-4"
      >
        <div className="relative">
          <div className="absolute inset-0 rounded-full bg-red-500/20 blur-xl animate-pulse" />
          <div className="relative w-16 h-16 rounded-full bg-gradient-to-br from-red-500 to-rose-600 flex items-center justify-center shadow-lg">
            <Loader2 className="w-8 h-8 text-white animate-spin" />
          </div>
        </div>
        <div className="text-center">
          <p className="text-lg font-semibold text-neutral-800 dark:text-neutral-200">
            Generating Spend Quality Assessment
          </p>
          <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1 max-w-md">
            {retryAttempt > 0
              ? `Attempt ${retryAttempt + 1}/${MAX_RETRIES}… Retrying…`
              : "Computing metrics and generating AI insights. This may take a moment."}
          </p>
        </div>
      </motion.div>
    );
  }

  /* ── Error ───────────────────────────────────────────────────────────── */

  if (error && !result) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard>
          <div className="text-center py-8">
            <p className="text-red-600 dark:text-red-400 font-semibold mb-2">
              Spend Quality Assessment Failed
            </p>
            <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-4">
              {error}
            </p>
            <button
              onClick={() => runAssessment(true)}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors"
            >
              <RefreshCw className="w-4 h-4" /> Retry
            </button>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  /* ── Results ─────────────────────────────────────────────────────────── */

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {/* Panel 1: Executive Summary & Flags */}
      {result && (
        <ExecSummaryFlagsPanel
          result={result}
          onRefresh={() => runAssessment(true)}
          loading={loading}
        />
      )}

      {result && (
        <>
          {/* Panel 2: Column Fill Rate */}
          <ColumnFillRatePanel
            data={result.columnFillRate}
            expanded={fillRateOpen}
            onToggle={() => setFillRateOpen((p) => !p)}
          />

          {/* Panel 3: Spend Bifurcation */}
          <SpendBifurcationPanel
            data={result.spendBifurcation}
            expanded={bifurcationOpen}
            onToggle={() => setBifurcationOpen((p) => !p)}
          />

          {/* Panel 4: Date Spend Pivot */}
          <DatePivotPanel
            data={result.datePivot}
            expanded={datePivotOpen}
            onToggle={() => setDatePivotOpen((p) => !p)}
          />

          {/* Panel 5: Pareto Analysis */}
          <ParetoPanel
            data={result.paretoAnalysis}
            expanded={paretoOpen}
            onToggle={() => setParetoOpen((p) => !p)}
          />
        </>
      )}
    </motion.div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 1 – Executive Summary & Flags (one-liners only)
   ══════════════════════════════════════════════════════════════════════════ */

function ExecSummaryFlagsPanel({
  result,
  onRefresh,
  loading,
}: {
  result: ExecutiveSummaryResult;
  onRefresh: () => void;
  loading: boolean;
}) {
  const { datePeriod, spendBreakdown, supplierBreakdown, categorizationEffort, flags } = result;

  const flagLines = buildFlagLines(flags);

  return (
    <SurfaceCard noPadding>
      <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
        <div className="flex items-start justify-between gap-6">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-4">
              <BarChart3 className="w-6 h-6" />
              <h2 className="text-xl font-semibold tracking-tight">
                Executive Summary
              </h2>
            </div>

            {/* Always-shown one-liners */}
            <ul className="space-y-2.5 text-sm text-red-50/95">
              {/* Date Period */}
              <OneLiner
                text={datePeriod?.feasible
                  ? `Time period of the data provided is **${datePeriod.periodLabel}** (${datePeriod.monthsCovered} months)`
                  : "Date period could not be determined (invoice_date not mapped)"}
              />

              {/* Spend Breakdown */}
              <OneLiner
                text={spendBreakdown?.feasible
                  ? `Total LTM spend is **${fmtCurrency(spendBreakdown.ltmSpend)}**; ${spendBreakdown.currentFyLabel} spend is **${fmtCurrency(spendBreakdown.currentFySpend)}**`
                  : "Spend breakdown not available (missing date or spend columns)"}
              />

              {/* Supplier Breakdown */}
              <OneLiner
                text={supplierBreakdown?.feasible
                  ? `Total suppliers are **${supplierBreakdown.totalSuppliers.toLocaleString()}**; top 80% spend suppliers are **${supplierBreakdown.suppliersTo80Pct.toLocaleString()}**`
                  : "Supplier breakdown not available (supplier column not mapped)"}
              />

              {/* Categorization Effort */}
              <CategorizationOneLiner data={categorizationEffort} />
            </ul>

            {/* Conditional flag one-liners */}
            {flagLines.length > 0 && (
              <div className="mt-4 pt-3 border-t border-white/20">
                <ul className="space-y-2">
                  {flagLines.map((fl, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-red-50/90">
                      <span className={`mt-[5px] h-2 w-2 shrink-0 rounded-full ${fl.severity === "red" ? "bg-red-300" : "bg-amber-300"}`} />
                      <span>{renderBold(fl.text)}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          <div className="flex items-center gap-3 shrink-0">
            <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur text-center">
              <p className="text-[10px] uppercase tracking-wider text-red-200">
                Total Rows
              </p>
              <p className="text-lg font-bold tabular-nums mt-0.5">
                {result.totalRows.toLocaleString()}
              </p>
            </div>
            <button
              onClick={onRefresh}
              disabled={loading}
              className="rounded-xl bg-white/15 hover:bg-white/25 px-3 py-3 backdrop-blur transition-colors disabled:opacity-50"
              title="Re-run Assessment"
            >
              <RefreshCw className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>
    </SurfaceCard>
  );
}

/** Renders a bullet line with **bold** markdown fragments. */
function OneLiner({ text }: { text: string }) {
  return (
    <li className="flex items-start gap-2">
      <span className="mt-[7px] h-1.5 w-1.5 shrink-0 rounded-full bg-white/60" />
      <span>{renderBold(text)}</span>
    </li>
  );
}

function CategorizationOneLiner({ data }: { data: CategorizationEffortResult }) {
  if (!data?.feasible) {
    return <OneLiner text="Categorization assessment not available (description column not mapped)" />;
  }

  if (!data.qualityVerdict) {
    return <OneLiner text="Categorization assessment in progress…" />;
  }

  const verdict = data.qualityVerdict;
  const method = data.recommendedMethod ?? "MapAI";
  const cost = data.mapAICost;

  let text: string;
  if (method === "MapAI" && cost > 0) {
    text = `Descriptions are of **${verdict}** quality. **${method}** is recommended with an estimated cost of approximately **${fmtCurrency(cost)}**`;
  } else if (method === "Creactives") {
    text = `Descriptions are of **${verdict}** quality. **Creactives** is recommended due to dataset size`;
  } else {
    text = `Descriptions are of **${verdict}** quality. **${method}** is recommended`;
  }

  return <OneLiner text={text} />;
}

/** Parse **bold** markdown within a string. */
function renderBold(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith("**") && part.endsWith("**") ? (
      <strong key={i} className="font-bold text-white">{part.slice(2, -2)}</strong>
    ) : (
      <React.Fragment key={i}>{part}</React.Fragment>
    )
  );
}

interface FlagLine {
  text: string;
  severity: "amber" | "red";
}

function buildFlagLines(flags: FlagsResult | undefined): FlagLine[] {
  if (!flags) return [];
  const lines: FlagLine[] = [];

  if (flags.spendConsistency) {
    const months = flags.spendConsistency.flaggedMonths
      .map((m) => `**${m.month}**`)
      .join(" and ");
    lines.push({
      text: `Large deviations in spend can be observed in ${months} — please check if the data is complete for these months`,
      severity: "amber",
    });
  }

  if (flags.descriptionQuality) {
    lines.push({
      text: "Descriptions are not detailed enough for spend analysis; advisable to use other sources",
      severity: "amber",
    });
  }

  if (flags.vendorQuality) {
    lines.push({
      text: `Vendor name column is sparsely populated (**${flags.vendorQuality.fillRate.toFixed(0)}%** filled); consider improving vendor data`,
      severity: "amber",
    });
  }

  if (flags.nullColumns && flags.nullColumns.flaggedColumns.length > 0) {
    const colNames = flags.nullColumns.flaggedColumns
      .map((c) => `**${c.name}**`)
      .join(" and ");
    lines.push({
      text: `${colNames} columns have low spend coverage, which could lead to incomplete analysis`,
      severity: "red",
    });
  }

  return lines;
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 2 – Column Fill Rate with Spend Coverage
   ══════════════════════════════════════════════════════════════════════════ */

function ColumnFillRatePanel({
  data,
  expanded,
  onToggle,
}: {
  data: ColumnFillRateResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <button
        onClick={onToggle}
        className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
      >
        <div className="flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-violet-100 dark:bg-violet-950/40 flex items-center justify-center">
            <Columns3 className="w-4 h-4 text-violet-600 dark:text-violet-400" />
          </span>
          <div>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
              Column Fill Rate with Spend Coverage
            </h3>
            <p className="text-xs text-neutral-400">
              Fill rate and spend coverage for every mapped column
            </p>
          </div>
        </div>
        <ChevronDown
          className={`w-4 h-4 text-neutral-400 transition-transform ${
            expanded ? "" : "-rotate-90"
          }`}
        />
      </button>

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
              {!data?.feasible || data.columns.length === 0 ? (
                <div className="px-6 py-8 text-center text-sm text-neutral-500 dark:text-neutral-400">
                  Column fill rate data not available.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-6 py-3">Column</th>
                        <th className="px-4 py-3 text-right">Fill Rate (%)</th>
                        <th className="px-4 py-3 text-right">Spend Coverage (%)</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {data.columns.map((col) => (
                        <tr
                          key={col.columnName}
                          className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                        >
                          <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200">
                            {col.columnName}
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                            <FillRateBadge value={col.fillRate} />
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                            {col.spendCoverage != null
                              ? <FillRateBadge value={col.spendCoverage} />
                              : <span className="text-neutral-400">N/A</span>}
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

function FillRateBadge({ value }: { value: number }) {
  const color =
    value >= 90
      ? "text-emerald-700 dark:text-emerald-400"
      : value >= 70
        ? "text-amber-700 dark:text-amber-400"
        : "text-red-600 dark:text-red-400";
  return <span className={color}>{value.toFixed(1)}%</span>;
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 3 – Spend Bifurcation
   ══════════════════════════════════════════════════════════════════════════ */

function SpendBifurcationPanel({
  data,
  expanded,
  onToggle,
}: {
  data: SpendBifurcationResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  const [view, setView] = React.useState<"reporting" | "local">("reporting");

  const hasReporting = data.reporting != null;
  const hasLocal = data.local != null;
  const noData = !hasReporting && !hasLocal;

  const effectiveView =
    view === "reporting" && hasReporting
      ? "reporting"
      : hasLocal
        ? "local"
        : "reporting";

  return (
    <SurfaceCard noPadding>
      <button
        onClick={onToggle}
        className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
      >
        <div className="flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-indigo-100 dark:bg-indigo-950/40 flex items-center justify-center">
            <SplitSquareHorizontal className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
          </span>
          <div>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
              Negative vs Positive Spend
            </h3>
            <p className="text-xs text-neutral-400">
              Positive vs negative spend breakdown
              {hasReporting && data.reporting?.netSpend != null && (
                <span> &middot; Net: {fmtSpend(data.reporting.netSpend)}</span>
              )}
            </p>
          </div>
        </div>
        <ChevronDown
          className={`w-4 h-4 text-neutral-400 transition-transform ${
            expanded ? "" : "-rotate-90"
          }`}
        />
      </button>

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
              {noData ? (
                <div className="px-6 py-6 text-center text-sm text-neutral-500 dark:text-neutral-400">
                  No spend columns mapped. Map total_spend or local_spend to
                  see bifurcation.
                </div>
              ) : (
                <>
                  {hasReporting && hasLocal && (
                    <div className="px-6 pt-4 pb-2 flex gap-1">
                      <button
                        onClick={() => setView("reporting")}
                        className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                          effectiveView === "reporting"
                            ? "bg-indigo-100 dark:bg-indigo-950/40 text-indigo-700 dark:text-indigo-300"
                            : "text-neutral-500 hover:bg-neutral-100 dark:hover:bg-neutral-800"
                        }`}
                      >
                        Reporting Currency
                      </button>
                      <button
                        onClick={() => setView("local")}
                        className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                          effectiveView === "local"
                            ? "bg-indigo-100 dark:bg-indigo-950/40 text-indigo-700 dark:text-indigo-300"
                            : "text-neutral-500 hover:bg-neutral-100 dark:hover:bg-neutral-800"
                        }`}
                      >
                        Local Currency
                      </button>
                    </div>
                  )}

                  {effectiveView === "reporting" && data.reporting && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                            <th className="px-6 py-3 text-right">Total Positive Spend</th>
                            <th className="px-4 py-3 text-right">Total Negative Spend</th>
                            <th className="px-4 py-3 text-right">Negative % of Positive</th>
                            <th className="px-4 py-3 text-right">Negative Rows</th>
                            <th className="px-4 py-3 text-right">Net Spend</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td className="px-6 py-3 text-right tabular-nums font-semibold text-emerald-700 dark:text-emerald-400">
                              {fmtSpend(data.reporting.positiveSpend)}
                            </td>
                            <td className="px-4 py-3 text-right tabular-nums font-semibold text-red-600 dark:text-red-400">
                              {fmtSpend(data.reporting.negativeSpend)}
                            </td>
                            <td className="px-4 py-3 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                              {data.reporting.negPctOfPos != null
                                ? `${data.reporting.negPctOfPos.toFixed(1)}%`
                                : "N/A"}
                            </td>
                            <td className="px-4 py-3 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                              {data.reporting.negRowCount != null
                                ? data.reporting.negRowCount.toLocaleString()
                                : "N/A"}
                            </td>
                            <td className="px-4 py-3 text-right tabular-nums font-semibold text-neutral-800 dark:text-neutral-200">
                              {fmtSpend(data.reporting.netSpend ?? null)}
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  )}

                  {effectiveView === "local" && data.local && (
                    <div className="overflow-x-auto">
                      {Array.isArray(data.local) ? (
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                              <th className="px-6 py-3">Currency</th>
                              <th className="px-4 py-3 text-right">Total +ve Spend</th>
                              <th className="px-4 py-3 text-right">Total -ve Spend</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {data.local.map((c) => (
                              <tr
                                key={c.code}
                                className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                              >
                                <td className="px-6 py-2 font-medium text-neutral-800 dark:text-neutral-200">
                                  {c.code}
                                </td>
                                <td className="px-4 py-2 text-right tabular-nums text-emerald-700 dark:text-emerald-400">
                                  {fmtSpend(c.positiveSpend)}
                                </td>
                                <td className="px-4 py-2 text-right tabular-nums text-red-600 dark:text-red-400">
                                  {fmtSpend(c.negativeSpend)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : (
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
                                {fmtSpend(data.local.positiveSpend)}
                              </td>
                              <td className="px-4 py-3 text-right tabular-nums font-semibold text-red-600 dark:text-red-400">
                                {fmtSpend(data.local.negativeSpend)}
                              </td>
                            </tr>
                          </tbody>
                        </table>
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

/* ══════════════════════════════════════════════════════════════════════════
   Panel 4 – Date Spend Pivot
   ══════════════════════════════════════════════════════════════════════════ */

function DatePivotPanel({
  data,
  expanded,
  onToggle,
}: {
  data: DatePivotResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <button
        onClick={onToggle}
        className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
      >
        <div className="flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-blue-100 dark:bg-blue-950/40 flex items-center justify-center">
            <CalendarDays className="w-4 h-4 text-blue-600 dark:text-blue-400" />
          </span>
          <div>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
              Year / Monthly Spend Split
            </h3>
            <p className="text-xs text-neutral-400">
              Spend by year and month (reporting currency)
            </p>
          </div>
        </div>
        <ChevronDown
          className={`w-4 h-4 text-neutral-400 transition-transform ${
            expanded ? "" : "-rotate-90"
          }`}
        />
      </button>

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
              {!data.feasible ? (
                <div className="px-6 py-8 text-center text-sm text-neutral-500 dark:text-neutral-400">
                  {data.message || "Date pivot not available."}
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">
                          Month
                        </th>
                        {data.years.map((yr) => (
                          <th key={yr} className="px-4 py-3 text-right min-w-[120px]">
                            {yr}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {data.months.map((monthName, idx) => {
                        const monthNum = String(idx + 1);
                        return (
                          <tr
                            key={monthNum}
                            className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                          >
                            <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                              {monthName}
                            </td>
                            {data.years.map((yr) => {
                              const val = data.cells[String(yr)]?.[monthNum] ?? 0;
                              return (
                                <td
                                  key={yr}
                                  className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                                >
                                  {val === 0 ? (
                                    <span className="text-neutral-300 dark:text-neutral-600">0</span>
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
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 5 – Pareto Analysis (80/85/90/95)
   ══════════════════════════════════════════════════════════════════════════ */

const PARETO_ROW_LABELS: { key: keyof ParetoThresholdMetrics; label: string }[] = [
  { key: "totalSpend", label: "Total Spend" },
  { key: "transactionCount", label: "Number of Transactions" },
  { key: "uniqueTransactions", label: "Unique Transactions" },
  { key: "supplierCount", label: "Number of Suppliers" },
];

function ParetoPanel({
  data,
  expanded,
  onToggle,
}: {
  data: ParetoAnalysisResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <button
        onClick={onToggle}
        className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
      >
        <div className="flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-emerald-100 dark:bg-emerald-950/40 flex items-center justify-center">
            <TrendingUp className="w-4 h-4 text-emerald-600 dark:text-emerald-400" />
          </span>
          <div>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
              Pareto Cuts (80 / 85 / 90 / 95)
            </h3>
            <p className="text-xs text-neutral-400">
              Spend &amp; supplier analysis at key thresholds
              {data.feasible && data.totalDatasetSpend > 0 && (
                <span> &middot; Total: {fmtSpend(data.totalDatasetSpend)}</span>
              )}
            </p>
          </div>
        </div>
        <ChevronDown
          className={`w-4 h-4 text-neutral-400 transition-transform ${
            expanded ? "" : "-rotate-90"
          }`}
        />
      </button>

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
              {!data.feasible ? (
                <div className="px-6 py-8 text-center text-sm text-neutral-500 dark:text-neutral-400">
                  {data.message || "Pareto analysis not available."}
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10 min-w-[180px]">
                          Metric
                        </th>
                        {data.thresholds.map((t) => (
                          <th key={t} className="px-4 py-3 text-right min-w-[120px]">
                            Top {t}%
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {PARETO_ROW_LABELS.map(({ key, label }) => (
                        <tr
                          key={key}
                          className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                        >
                          <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                            {label}
                          </td>
                          {data.thresholds.map((t) => {
                            const m = data.metrics[String(t)];
                            const val = m ? m[key] : 0;
                            return (
                              <td
                                key={t}
                                className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                              >
                                {fmtSpend(val)}
                              </td>
                            );
                          })}
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
