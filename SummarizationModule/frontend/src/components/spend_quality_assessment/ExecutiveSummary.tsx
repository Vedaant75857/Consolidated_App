import React, { useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  BarChart3,
  CalendarDays,
  ChevronDown,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
  TrendingUp,
  FileText,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  getExecutiveSummary,
  type ExecutiveSummaryResult,
  type DatePivotResult,
  type ParetoAnalysisResult,
  type ParetoThresholdMetrics,
  type DescriptionQualityItem,
  type SpendBifurcationResult,
} from "../../api/client";

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fmtSpend(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return Math.round(val).toLocaleString();
}

function renderBoldMarkdown(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith("**") && part.endsWith("**") ? (
      <strong key={i}>{part.slice(2, -2)}</strong>
    ) : (
      part
    )
  );
}

function renderInsightMarkdown(text: string): React.ReactNode {
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

  const [datePivotOpen, setDatePivotOpen] = useState(true);
  const [bifurcationOpen, setBifurcationOpen] = useState(true);
  const [paretoOpen, setParetoOpen] = useState(true);
  const [descQualityOpen, setDescQualityOpen] = useState(true);
  const [expandedDescs, setExpandedDescs] = useState<Set<string>>(new Set());

  const runAssessment = useCallback(
    async (force = false) => {
      setLoading(true);
      setError(null);
      try {
        const data = await getExecutiveSummary(sessionId, apiKey, force);
        setResult(data);
        onLoaded?.();
      } catch (err: any) {
        setError(err?.message || "Spend Quality Assessment failed");
      } finally {
        setLoading(false);
      }
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

  const toggleDescExpanded = (fk: string) =>
    setExpandedDescs((prev) => {
      const next = new Set(prev);
      if (next.has(fk)) next.delete(fk);
      else next.add(fk);
      return next;
    });

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
            Computing metrics and generating AI insights. This may take a
            moment.
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
      {/* Header card */}
      <SurfaceCard noPadding>
        <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-start justify-between gap-6">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-6 h-6" />
                <h2 className="text-xl font-semibold tracking-tight">
                  Spend Quality Assessment
                </h2>
              </div>
              <p className="text-red-50/90 text-sm max-w-xl">
                Data quality overview covering date coverage, spend
                concentration, and description quality across your procurement
                dataset.
              </p>
            </div>
            <div className="flex items-center gap-3 shrink-0">
              {result && (
                <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur text-center">
                  <p className="text-[10px] uppercase tracking-wider text-red-200">
                    Total Rows
                  </p>
                  <p className="text-lg font-bold tabular-nums mt-0.5">
                    {result.totalRows.toLocaleString()}
                  </p>
                </div>
              )}
              <button
                onClick={() => runAssessment(true)}
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

      {result && (
        <>
          {/* ── Panel 1: Date Spend Pivot ──────────────────────────────── */}
          <DatePivotPanel
            data={result.datePivot}
            expanded={datePivotOpen}
            onToggle={() => setDatePivotOpen((p) => !p)}
          />

          {/* ── Panel 1b: Spend Bifurcation ──────────────────────────── */}
          <SpendBifurcationPanel
            data={result.spendBifurcation}
            expanded={bifurcationOpen}
            onToggle={() => setBifurcationOpen((p) => !p)}
          />

          {/* ── Panel 2: Pareto Analysis ───────────────────────────────── */}
          <ParetoPanel
            data={result.paretoAnalysis}
            expanded={paretoOpen}
            onToggle={() => setParetoOpen((p) => !p)}
          />

          {/* ── Panel 3: Description Quality ───────────────────────────── */}
          <DescriptionQualityPanel
            items={result.descriptionQuality}
            expanded={descQualityOpen}
            onToggle={() => setDescQualityOpen((p) => !p)}
            expandedDescs={expandedDescs}
            onToggleDesc={toggleDescExpanded}
          />
        </>
      )}
    </motion.div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 1 – Date Spend Pivot
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
              Date Spend Pivot
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
                          <th
                            key={yr}
                            className="px-4 py-3 text-right min-w-[120px]"
                          >
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
                              const val =
                                data.cells[String(yr)]?.[monthNum] ?? 0;
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
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel 2 – Pareto Analysis
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
              Spend &amp; Supplier Analysis
            </h3>
            <p className="text-xs text-neutral-400">
              Pareto thresholds at 80%, 85%, 90%, 95%, 99%
              {data.feasible && data.totalDatasetSpend > 0 && (
                <span>
                  {" "}
                  &middot; Total: {fmtSpend(data.totalDatasetSpend)}
                </span>
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
                          <th
                            key={t}
                            className="px-4 py-3 text-right min-w-[120px]"
                          >
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

/* ══════════════════════════════════════════════════════════════════════════
   Panel 3 – Description Quality Analysis
   ══════════════════════════════════════════════════════════════════════════ */

function DescriptionQualityPanel({
  items,
  expanded,
  onToggle,
  expandedDescs,
  onToggleDesc,
}: {
  items: DescriptionQualityItem[];
  expanded: boolean;
  onToggle: () => void;
  expandedDescs: Set<string>;
  onToggleDesc: (fk: string) => void;
}) {
  const mappedCount = items.filter((i) => i.mapped).length;

  return (
    <SurfaceCard noPadding>
      <button
        onClick={onToggle}
        className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
      >
        <div className="flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-amber-100 dark:bg-amber-950/40 flex items-center justify-center">
            <FileText className="w-4 h-4 text-amber-600 dark:text-amber-400" />
          </span>
          <div>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
              Description Quality Analysis
            </h3>
            <p className="text-xs text-neutral-400">
              {mappedCount} of {items.length} description columns mapped
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
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                      <th className="px-4 py-3 w-48">Description Type</th>
                      <th className="px-4 py-3 w-36 text-right">
                        Spend Covered
                      </th>
                      <th className="px-4 py-3 min-w-[240px]">
                        Top 10 Descriptions
                      </th>
                      <th className="px-4 py-3 min-w-[320px]">AI Insight</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {items.map((item) => {
                      const isExpanded = expandedDescs.has(item.fieldKey);
                      return (
                        <tr
                          key={item.fieldKey}
                          className="align-top hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                        >
                          {/* Description type */}
                          <td className="px-4 py-3">
                            <span
                              className={`font-medium ${
                                item.mapped
                                  ? "text-neutral-800 dark:text-neutral-200"
                                  : "text-neutral-400 dark:text-neutral-500 italic"
                              }`}
                            >
                              {item.displayName}
                            </span>
                            {!item.mapped && (
                              <span className="block text-[10px] text-neutral-400 mt-0.5">
                                Not mapped
                              </span>
                            )}
                          </td>

                          {/* Spend covered */}
                          <td className="px-4 py-3 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                            {item.mapped && item.spendCovered != null
                              ? fmtSpend(item.spendCovered)
                              : "N/A"}
                          </td>

                          {/* Top 10 descriptions */}
                          <td className="px-4 py-3">
                            {!item.mapped || item.top10.length === 0 ? (
                              <span className="text-neutral-400 dark:text-neutral-500 italic text-xs">
                                N/A
                              </span>
                            ) : (
                              <div>
                                <button
                                  onClick={() =>
                                    onToggleDesc(item.fieldKey)
                                  }
                                  className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:underline"
                                >
                                  {isExpanded
                                    ? "Hide top 10"
                                    : "Show top 10"}
                                </button>
                                {isExpanded && (
                                  <div className="mt-2 space-y-1">
                                    {item.top10.map((d, i) => (
                                      <div
                                        key={i}
                                        className="flex justify-between gap-2 text-xs"
                                      >
                                        <span className="text-neutral-700 dark:text-neutral-300 truncate max-w-[180px]" title={d.description}>
                                          {i + 1}. {d.description}
                                        </span>
                                        <span className="text-neutral-500 dark:text-neutral-400 tabular-nums shrink-0">
                                          {fmtSpend(d.spend)}
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            )}
                          </td>

                          {/* AI Insight */}
                          <td className="px-4 py-3">
                            <span
                              className={`text-sm leading-relaxed ${
                                item.mapped
                                  ? "text-neutral-700 dark:text-neutral-300"
                                  : "text-neutral-400 dark:text-neutral-500 italic"
                              }`}
                            >
                              {item.aiInsight
                                ? renderInsightMarkdown(item.aiInsight)
                                : "N/A"}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

/* ══════════════════════════════════════════════════════════════════════════
   Panel – Spend Bifurcation
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
              Spend Bifurcation
            </h3>
            <p className="text-xs text-neutral-400">
              Positive vs negative spend breakdown
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
                  {/* Toggle between reporting and local */}
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

                  {/* Reporting view */}
                  {effectiveView === "reporting" && data.reporting && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                            <th className="px-6 py-3 text-right">
                              Total Positive Spend
                            </th>
                            <th className="px-4 py-3 text-right">
                              Total Negative Spend
                            </th>
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
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Local view */}
                  {effectiveView === "local" && data.local && (
                    <div className="overflow-x-auto">
                      {Array.isArray(data.local) ? (
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                              <th className="px-6 py-3">Currency</th>
                              <th className="px-4 py-3 text-right">
                                Total +ve Spend
                              </th>
                              <th className="px-4 py-3 text-right">
                                Total -ve Spend
                              </th>
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
                              <th className="px-6 py-3 text-right">
                                Total Positive Spend
                              </th>
                              <th className="px-4 py-3 text-right">
                                Total Negative Spend
                              </th>
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
