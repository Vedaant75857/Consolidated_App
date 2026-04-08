import React, { Fragment, useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { BarChart3, ChevronDown, Loader2, RefreshCw, TableProperties } from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  getExecutiveSummary,
  type ExecutiveSummaryResult,
  type ExecutiveSummaryGroup,
  type FillRateSummaryItem,
} from "../../api/client";

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fillRateColor(rate: number): { bg: string; text: string } {
  if (rate > 80)
    return {
      bg: "bg-emerald-100 dark:bg-emerald-950/40",
      text: "text-emerald-700 dark:text-emerald-400",
    };
  if (rate > 60)
    return {
      bg: "bg-amber-100 dark:bg-amber-950/40",
      text: "text-amber-700 dark:text-amber-400",
    };
  return {
    bg: "bg-red-100 dark:bg-red-950/40",
    text: "text-red-700 dark:text-red-400",
  };
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
  const lines = text.split('\n').filter(l => l.trim());
  const hasBullets = lines.some(l => l.trim().startsWith('- '));
  if (hasBullets) {
    return (
      <ul className="list-disc list-inside space-y-1">
        {lines.map((line, i) => {
          const content = line.replace(/^-\s*/, '');
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
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [fillRateExpanded, setFillRateExpanded] = useState(true);

  const runAssessment = useCallback(
    async (force = false) => {
      setLoading(true);
      setError(null);
      try {
        const data = await getExecutiveSummary(sessionId, apiKey, force);
        setResult(data);
        setExpandedGroups(
          new Set(
            (data.parameters ?? []).map(
              (p: ExecutiveSummaryGroup) => p.group
            )
          )
        );
        onLoaded?.();
      } catch (err: any) {
        setError(err?.message || "Executive summary generation failed");
      } finally {
        setLoading(false);
      }
    },
    [sessionId, apiKey, onLoaded]
  );

  useEffect(() => {
    if (!result && !loading && !error && sessionId && apiKey) {
      runAssessment(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const toggleGroup = (group: string) =>
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(group)) next.delete(group);
      else next.add(group);
      return next;
    });

  /* ── Loading state ───────────────────────────────────────────────────── */

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
            Generating Executive Summary
          </p>
          <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1 max-w-md">
            Computing metrics and generating AI insights. This may take a
            moment.
          </p>
        </div>
      </motion.div>
    );
  }

  /* ── Error state ─────────────────────────────────────────────────────── */

  if (error && !result) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard>
          <div className="text-center py-8">
            <p className="text-red-600 dark:text-red-400 font-semibold mb-2">
              Executive Summary Failed
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

  const fillRateSummary: FillRateSummaryItem[] =
    result?.fillRateSummary ?? [];

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
                  Executive Summary
                </h2>
              </div>
              <p className="text-red-50/90 text-sm max-w-xl">
                High-level data quality overview across key procurement
                parameters.
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
                title="Re-run Executive Summary"
              >
                <RefreshCw className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>
      </SurfaceCard>

      {/* ── Fill Rate Summary ──────────────────────────────────────────── */}
      {fillRateSummary.length > 0 && (
        <SurfaceCard noPadding>
          <button
            onClick={() => setFillRateExpanded(!fillRateExpanded)}
            className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
          >
            <div className="flex items-center gap-3">
              <span className="w-8 h-8 rounded-lg bg-indigo-100 dark:bg-indigo-950/40 flex items-center justify-center">
                <TableProperties className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
              </span>
              <div>
                <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                  Fill Rate Summary
                </h3>
                <p className="text-xs text-neutral-400">
                  {fillRateSummary.length} mapped column
                  {fillRateSummary.length !== 1 ? "s" : ""}
                </p>
              </div>
            </div>
            <ChevronDown
              className={`w-4 h-4 text-neutral-400 transition-transform ${
                fillRateExpanded ? "" : "-rotate-90"
              }`}
            />
          </button>

          <AnimatePresence initial={false}>
            {fillRateExpanded && (
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
                          <th className="px-6 py-3">Column</th>
                          <th className="px-4 py-3 w-28 text-center">
                            Fill Rate
                          </th>
                          <th className="px-4 py-3 w-36 text-center">
                            Unique Values
                          </th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                        {fillRateSummary.map((item) => {
                          const colors = fillRateColor(item.fillRate);
                          return (
                            <tr
                              key={item.fieldKey}
                              className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                            >
                              <td className="px-6 py-3">
                                <span className="font-medium text-neutral-800 dark:text-neutral-200">
                                  {item.displayName}
                                </span>
                              </td>
                              <td className="px-4 py-3 text-center">
                                <span
                                  className={`inline-block px-3 py-1 rounded-lg text-xs font-bold tabular-nums ${colors.bg} ${colors.text}`}
                                >
                                  {item.fillRate.toFixed(1)}%
                                </span>
                              </td>
                              <td className="px-4 py-3 text-center">
                                <span className="text-sm font-semibold tabular-nums text-neutral-700 dark:text-neutral-300">
                                  {item.uniqueValues.toLocaleString()}
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
      )}

      {/* ── Parameter group insights ───────────────────────────────────── */}
      {result &&
        result.parameters.map((group) => {
          const mappedCols = group.columns.filter((c) => c.mapped);
          const unmappedCols = group.columns.filter((c) => !c.mapped);
          const isExpanded = expandedGroups.has(group.group);

          return (
            <SurfaceCard key={group.group} noPadding>
              <button
                onClick={() => toggleGroup(group.group)}
                className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
              >
                <div className="flex items-center gap-3">
                  <span className="w-8 h-8 rounded-lg bg-red-100 dark:bg-red-950/40 flex items-center justify-center">
                    <BarChart3 className="w-4 h-4 text-red-600 dark:text-red-400" />
                  </span>
                  <div>
                    <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                      {group.group}
                    </h3>
                    <p className="text-xs text-neutral-400">
                      {mappedCols.length} column
                      {mappedCols.length !== 1 ? "s" : ""} found
                      {unmappedCols.length > 0 &&
                        `, ${unmappedCols.length} not mapped`}
                    </p>
                  </div>
                </div>
                <ChevronDown
                  className={`w-4 h-4 text-neutral-400 transition-transform ${
                    isExpanded ? "" : "-rotate-90"
                  }`}
                />
              </button>

              <AnimatePresence initial={false}>
                {isExpanded && (
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
                              <th className="px-6 py-3 w-56">Column</th>
                              <th className="px-4 py-3 w-28 text-center">
                                Fill Rate
                              </th>
                              <th className="px-6 py-3">Insight</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {group.columns.map((col) => {
                              const colors = col.mapped
                                ? fillRateColor(col.fillRate)
                                : {
                                    bg: "bg-neutral-100 dark:bg-neutral-800",
                                    text: "text-neutral-400 dark:text-neutral-500",
                                  };
                              const currencyQuality: any[] | undefined = col.stats?.currencyQuality;
                              const spendByCurrency: any[] | undefined = col.stats?.spendByCurrency;

                              return (
                                <Fragment key={col.parameterKey}>
                                  <tr className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors align-top">
                                    <td className="px-6 py-3">
                                      <span
                                        className={`font-medium ${
                                          col.mapped
                                            ? "text-neutral-800 dark:text-neutral-200"
                                            : "text-neutral-400 dark:text-neutral-500 italic"
                                        }`}
                                      >
                                        {col.columnName}
                                      </span>
                                    </td>
                                    <td className="px-4 py-3 text-center">
                                      <span
                                        className={`inline-block px-3 py-1 rounded-lg text-xs font-bold tabular-nums ${colors.bg} ${colors.text}`}
                                      >
                                        {col.mapped
                                          ? `${col.fillRate.toFixed(1)}%`
                                          : "N/A"}
                                      </span>
                                    </td>
                                    <td className="px-6 py-3">
                                      <span
                                        className={`text-sm leading-relaxed ${
                                          col.mapped
                                            ? "text-neutral-700 dark:text-neutral-300"
                                            : "text-neutral-400 dark:text-neutral-500 italic"
                                        }`}
                                      >
                                        {col.mapped
                                          ? renderInsightMarkdown(col.insight || "")
                                          : renderBoldMarkdown(col.insight || "Column not present in data.")}
                                      </span>
                                    </td>
                                  </tr>
                                  {group.group === "Spend" && col.mapped && spendByCurrency && spendByCurrency.length > 0 && (
                                    <tr>
                                      <td colSpan={3} className="px-6 py-4">
                                        <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 overflow-hidden">
                                          <div className="px-4 py-2.5 bg-neutral-50 dark:bg-neutral-800/60 border-b border-neutral-200 dark:border-neutral-700">
                                            <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                                              Spend by Currency
                                            </h4>
                                          </div>
                                          <table className="w-full text-sm">
                                            <thead>
                                              <tr className="bg-neutral-50/50 dark:bg-neutral-800/30 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                                                <th className="px-4 py-2.5">Currency Code</th>
                                                <th className="px-4 py-2.5 text-center">% of Rows Covered</th>
                                                <th className="px-4 py-2.5 text-right">Total Amount in Local Currency</th>
                                              </tr>
                                            </thead>
                                            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                                              {spendByCurrency.map((cq: any) => (
                                                <tr key={cq.currencyCode} className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors">
                                                  <td className="px-4 py-2 font-medium text-neutral-800 dark:text-neutral-200">
                                                    {cq.currencyCode}
                                                  </td>
                                                  <td className="px-4 py-2 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                                    {cq.rowPct.toFixed(1)}%
                                                  </td>
                                                  <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                                    {cq.localSpend != null
                                                      ? Math.round(cq.localSpend).toLocaleString()
                                                      : "N/A"}
                                                  </td>
                                                </tr>
                                              ))}
                                            </tbody>
                                          </table>
                                        </div>
                                      </td>
                                    </tr>
                                  )}
                                  {group.group === "Currency" && col.mapped && currencyQuality && currencyQuality.length > 0 && (
                                    <tr>
                                      <td colSpan={3} className="px-6 py-4">
                                        <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 overflow-hidden">
                                          <div className="px-4 py-2.5 bg-neutral-50 dark:bg-neutral-800/60 border-b border-neutral-200 dark:border-neutral-700">
                                            <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                                              Currency Quality Analysis
                                            </h4>
                                          </div>
                                          <table className="w-full text-sm">
                                            <thead>
                                              <tr className="bg-neutral-50/50 dark:bg-neutral-800/30 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                                                <th className="px-4 py-2.5">Currency Code</th>
                                                <th className="px-4 py-2.5 text-center">% of Rows Covered</th>
                                                <th className="px-4 py-2.5 text-right">Total Amount in Local Currency</th>
                                                <th className="px-4 py-2.5 text-right">Total Amount in Reporting Currency</th>
                                              </tr>
                                            </thead>
                                            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                                              {currencyQuality.map((cq: any) => (
                                                <tr key={cq.currencyCode} className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors">
                                                  <td className="px-4 py-2 font-medium text-neutral-800 dark:text-neutral-200">
                                                    {cq.currencyCode}
                                                  </td>
                                                  <td className="px-4 py-2 text-center tabular-nums text-neutral-700 dark:text-neutral-300">
                                                    {cq.rowPct.toFixed(1)}%
                                                  </td>
                                                  <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                                    {col.stats.hasLocalSpend
                                                      ? Math.round(cq.localSpend ?? 0).toLocaleString()
                                                      : "N/A"}
                                                  </td>
                                                  <td className="px-4 py-2 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                                                    {col.stats.hasReportingSpend
                                                      ? Math.round(cq.reportingSpend ?? 0).toLocaleString()
                                                      : "N/A"}
                                                  </td>
                                                </tr>
                                              ))}
                                            </tbody>
                                          </table>
                                        </div>
                                      </td>
                                    </tr>
                                  )}
                                </Fragment>
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
        })}
    </motion.div>
  );
}