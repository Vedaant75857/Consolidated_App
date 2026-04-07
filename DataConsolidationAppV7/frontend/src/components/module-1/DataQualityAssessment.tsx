import { Fragment, type ReactNode, useCallback, useEffect, useState } from "react";
import { motion } from "motion/react";
import { BarChart3, ChevronDown, Loader2, RefreshCw, TableProperties } from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import { postDataQualityAssessment } from "./services/stitchingApi";
import type { MergeOutput } from "../../types";

/* ── Types ─────────────────────────────────────────────────────────────── */

interface ColumnEntry {
  columnName: string;
  parameterKey: string;
  fillRate: number;
  mapped: boolean;
  stats: Record<string, any>;
  insight: string;
}

interface ParameterGroup {
  group: string;
  columns: ColumnEntry[];
}

interface FillRateSummaryItem {
  columnName: string;
  filledRows: number;
  totalRows: number;
  fillRate: number;
  uniqueValues: number;
}

interface DqaResult {
  totalRows: number;
  parameters: ParameterGroup[];
  fillRateSummary: FillRateSummaryItem[];
}

interface DataQualityAssessmentProps {
  sessionId: string;
  apiKey: string;
  mergeOutputs: MergeOutput[];
  addLog: (step: string, type: "info" | "success" | "error", message: string) => void;
  setAiLoading: (v: boolean) => void;
  setLoadingMessage: (v: string) => void;
}

/* ── Helpers ───────────────────────────────────────────────────────────── */

const HIDDEN_COLUMNS = new Set([
  "Cost Center Description",
  "GL Account Description",
  "Contract Description",
]);

function renderBoldMarkdown(text: string): ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith("**") && part.endsWith("**")
      ? <strong key={i}>{part.slice(2, -2)}</strong>
      : part
  );
}

function fillRateColor(rate: number): { bg: string; text: string } {
  if (rate > 80) return { bg: "bg-emerald-100 dark:bg-emerald-950/40", text: "text-emerald-700 dark:text-emerald-400" };
  if (rate > 60) return { bg: "bg-amber-100 dark:bg-amber-950/40", text: "text-amber-700 dark:text-amber-400" };
  return { bg: "bg-red-100 dark:bg-red-950/40", text: "text-red-700 dark:text-red-400" };
}

function formatCurrency(value: number, label: string | null): string {
  const formatted = value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  return label ? `${formatted} (${label})` : formatted;
}

function tableNameForVersion(version: number): string {
  return `final_merged_v${version}`;
}

/* ── Component ─────────────────────────────────────────────────────────── */

export default function DataQualityAssessment({
  sessionId,
  apiKey,
  mergeOutputs,
  addLog,
  setAiLoading,
  setLoadingMessage,
}: DataQualityAssessmentProps) {
  const latestVersion = mergeOutputs.length > 0
    ? Math.max(...mergeOutputs.map((o) => o.version))
    : 1;

  const [selectedVersion, setSelectedVersion] = useState(latestVersion);
  const [result, setResult] = useState<DqaResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [fillRateExpanded, setFillRateExpanded] = useState(true);

  const runAssessment = useCallback(
    async (version: number) => {
      setLoading(true);
      setAiLoading(true);
      setLoadingMessage("Running Data Quality Assessment...");
      setError(null);
      setResult(null);
      addLog("Data Quality", "info", "Running data quality assessment...");
      try {
        const tableName = tableNameForVersion(version);
        const data = await postDataQualityAssessment(sessionId, apiKey, tableName);
        setResult(data);
        setExpandedGroups(new Set((data.parameters ?? []).map((p: ParameterGroup) => p.group)));
        addLog("Data Quality", "success", `Assessment complete — ${data.totalRows?.toLocaleString() ?? 0} rows analysed`);
      } catch (err: any) {
        const msg = err?.message || "Assessment failed";
        setError(msg);
        addLog("Data Quality", "error", msg);
      } finally {
        setLoading(false);
        setAiLoading(false);
        setLoadingMessage("");
      }
    },
    [sessionId, apiKey, addLog, setAiLoading, setLoadingMessage],
  );

  // Auto-run on first mount when no results yet
  useEffect(() => {
    if (!result && !loading && !error && sessionId && apiKey) {
      runAssessment(selectedVersion);
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

  /* ── Loading overlay ─────────────────────────────────────────────────── */

  if (loading) {
    return (
      <motion.div variants={itemVariants} className="flex flex-col items-center justify-center py-24 gap-4">
        <div className="relative">
          <div className="absolute inset-0 rounded-full bg-red-500/20 blur-xl animate-pulse" />
          <div className="relative w-16 h-16 rounded-full bg-gradient-to-br from-red-500 to-rose-600 flex items-center justify-center shadow-lg">
            <Loader2 className="w-8 h-8 text-white animate-spin" />
          </div>
        </div>
        <div className="text-center">
          <p className="text-lg font-semibold text-neutral-800 dark:text-neutral-200">
            Running Data Quality Assessment
          </p>
          <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1 max-w-md">
            Computing metrics across all parameter groups and generating AI insights. This may take a moment.
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
            <p className="text-red-600 dark:text-red-400 font-semibold mb-2">Assessment Failed</p>
            <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-4">{error}</p>
            <button
              onClick={() => runAssessment(selectedVersion)}
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
        <div className="rounded-3xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-start justify-between gap-6">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-6 h-6" />
                <h2 className="text-xl font-semibold tracking-tight">Data Quality Assessment</h2>
              </div>
              <p className="text-red-50/90 text-sm max-w-xl">
                Executive summary of your merged dataset quality across key procurement parameters.
              </p>
            </div>
            {result && (
              <div className="flex gap-3 text-center shrink-0">
                <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur">
                  <p className="text-[10px] uppercase tracking-wider text-red-200">Total Rows</p>
                  <p className="text-lg font-bold tabular-nums mt-0.5">{result.totalRows.toLocaleString()}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </SurfaceCard>

      {/* Merge output selector + re-run */}
      <SurfaceCard>
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-3">
            <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Merge Output:</label>
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
          <button
            onClick={() => runAssessment(selectedVersion)}
            disabled={loading}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 text-sm font-semibold text-neutral-700 dark:text-neutral-300 hover:bg-neutral-200 dark:hover:bg-neutral-700 transition-colors disabled:opacity-50"
          >
            <RefreshCw className="w-4 h-4" /> Re-run
          </button>
        </div>
      </SurfaceCard>

      {/* Fill Rate Summary */}
      {result && result.fillRateSummary && result.fillRateSummary.length > 0 && (
        <SurfaceCard noPadding>
          <button
            onClick={() => setFillRateExpanded(!fillRateExpanded)}
            className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-3xl"
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
                  {result.fillRateSummary.length} column{result.fillRateSummary.length !== 1 ? "s" : ""}
                </p>
              </div>
            </div>
            <ChevronDown
              className={`w-4 h-4 text-neutral-400 transition-transform ${fillRateExpanded ? "" : "-rotate-90"}`}
            />
          </button>

          {fillRateExpanded && (
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                      <th className="px-6 py-3">Column</th>
                      <th className="px-4 py-3 w-28 text-center">Fill Rate</th>
                      <th className="px-4 py-3 w-36 text-center">Unique Values</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {result.fillRateSummary.map((item) => {
                      const colors = fillRateColor(item.fillRate);
                      return (
                        <tr key={item.columnName} className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors">
                          <td className="px-6 py-3">
                            <span className="font-medium text-neutral-800 dark:text-neutral-200">
                              {item.columnName}
                            </span>
                            <span className="ml-2 text-xs text-emerald-600 dark:text-emerald-400">
                              ({item.filledRows.toLocaleString()} / {item.totalRows.toLocaleString()})
                            </span>
                          </td>
                          <td className="px-4 py-3 text-center">
                            <span className={`inline-block px-3 py-1 rounded-lg text-xs font-bold tabular-nums ${colors.bg} ${colors.text}`}>
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
          )}
        </SurfaceCard>
      )}

      {/* Results table */}
      {result && result.parameters.map((group) => {
        const visibleColumns = group.columns.filter((c) => !HIDDEN_COLUMNS.has(c.columnName));
        const mappedCols = visibleColumns.filter((c) => c.mapped);
        const unmappedCols = visibleColumns.filter((c) => !c.mapped);
        const isExpanded = expandedGroups.has(group.group);

        return (
          <SurfaceCard key={group.group} noPadding>
            <button
              onClick={() => toggleGroup(group.group)}
              className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-3xl"
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
                    {mappedCols.length} column{mappedCols.length !== 1 ? "s" : ""} found
                    {unmappedCols.length > 0 && `, ${unmappedCols.length} not mapped`}
                  </p>
                </div>
              </div>
              <ChevronDown
                className={`w-4 h-4 text-neutral-400 transition-transform ${isExpanded ? "" : "-rotate-90"}`}
              />
            </button>

            {isExpanded && (
              <div className="border-t border-neutral-100 dark:border-neutral-800">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                        <th className="px-6 py-3 w-56">Column</th>
                        <th className="px-4 py-3 w-28 text-center">Fill Rate</th>
                        <th className="px-6 py-3">Insight</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                      {visibleColumns.map((col) => {
                        const colors = col.mapped
                          ? fillRateColor(col.fillRate)
                          : { bg: "bg-neutral-100 dark:bg-neutral-800", text: "text-neutral-400 dark:text-neutral-500" };
                        const currencyQuality: any[] | undefined = col.stats?.currencyQuality;

                        return (
                          <Fragment key={col.parameterKey}>
                            <tr className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors">
                              <td className="px-6 py-3">
                                <span className={`font-medium ${col.mapped ? "text-neutral-800 dark:text-neutral-200" : "text-neutral-400 dark:text-neutral-500 italic"}`}>
                                  {col.columnName}
                                </span>
                              </td>
                              <td className="px-4 py-3 text-center">
                                <span className={`inline-block px-3 py-1 rounded-lg text-xs font-bold tabular-nums ${colors.bg} ${colors.text}`}>
                                  {col.mapped ? `${col.fillRate.toFixed(1)}%` : "N/A"}
                                </span>
                              </td>
                              <td className="px-6 py-3">
                                {group.group === "Description" && col.mapped && (
                                  <ul className="text-sm text-neutral-700 dark:text-neutral-300 mb-2 space-y-1 list-disc list-inside">
                                    <li>
                                      No. of alphanumeric values:{" "}
                                      <strong>{(col.stats.alphanumericCount ?? 0).toLocaleString()}</strong>
                                    </li>
                                    <li>
                                      Total non-procurable spend:{" "}
                                      <strong>
                                        {col.stats.nonProcurableSpend != null
                                          ? formatCurrency(col.stats.nonProcurableSpend, col.stats.currencyLabel)
                                          : "N/A"}
                                      </strong>
                                    </li>
                                  </ul>
                                )}
                                <span className={`text-sm leading-relaxed ${col.mapped ? "text-neutral-700 dark:text-neutral-300" : "text-neutral-400 dark:text-neutral-500 italic"}`}>
                                  {renderBoldMarkdown(col.insight || (col.mapped ? "" : "Column not present in data."))}
                                </span>
                              </td>
                            </tr>
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
            )}
          </SurfaceCard>
        );
      })}
    </motion.div>
  );
}
