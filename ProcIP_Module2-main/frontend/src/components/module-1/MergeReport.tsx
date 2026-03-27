import React, { useState } from "react";
import { motion } from "motion/react";
import { CheckCircle2, ChevronDown, ChevronRight, Clock, Download, FileText, Loader2, Package, RefreshCw, RotateCcw, Table2 } from "lucide-react";
import { SurfaceCard, SecondaryButton, FillBar, itemVariants } from "../common/ui";

interface MergeReportProps {
  mergeResult: any;
  mergeApprovedSources: any[];
  mergeHistory: any[];
  groupNameMap: Record<string, string>;
  onDownloadXlsx: (version?: number) => void;
  onDownloadCsv: () => void;
  onDownloadAllZip: () => void;
  onDownloadReport: () => void;
  onRedoMerge: () => void;
  onMergeAgain: () => void;
  mergeAgainLoading?: boolean;
}

export default function MergeReport({
  mergeResult,
  mergeApprovedSources,
  mergeHistory,
  groupNameMap,
  onDownloadXlsx,
  onDownloadCsv,
  onDownloadAllZip,
  onDownloadReport,
  onRedoMerge,
  onMergeAgain,
  mergeAgainLoading,
}: MergeReportProps) {
  const [historyOpen, setHistoryOpen] = useState(true);

  if (!mergeResult) {
    return (
      <motion.div variants={itemVariants} className="flex flex-col items-center justify-center py-20 gap-4 text-neutral-500 dark:text-neutral-400">
        <Loader2 className="w-8 h-8 animate-spin" />
        <p className="text-sm font-medium">Preparing merge report...</p>
      </motion.div>
    );
  }

  const { rows, cols, columns, preview, column_stats, skipped } = mergeResult;
  const hasMultipleVersions = mergeHistory && mergeHistory.length > 1;

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {/* Hero Banner */}
      <SurfaceCard noPadding>
        <div className="rounded-3xl bg-gradient-to-r from-emerald-600 to-teal-600 p-7 text-white">
          <div className="flex items-start justify-between gap-6">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <CheckCircle2 className="w-6 h-6" />
                <h2 className="text-xl font-semibold tracking-tight">
                  {skipped ? "Merge Skipped — Single Table Output" : "Merge Complete"}
                </h2>
              </div>
              <p className="text-emerald-50/90 text-sm max-w-xl">
                {skipped
                  ? "Your single table has been saved as the final merged output."
                  : `Successfully merged ${mergeApprovedSources.length} source table(s) into a unified dataset.`}
              </p>
              {mergeResult.version && (
                <p className="text-emerald-100/70 text-xs mt-1">Version {mergeResult.version} &middot; {mergeResult.file_label}</p>
              )}
            </div>
            <div className="flex gap-3 text-center shrink-0">
              <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur">
                <p className="text-[10px] uppercase tracking-wider text-emerald-200">Rows</p>
                <p className="text-lg font-bold tabular-nums mt-0.5">{(rows ?? 0).toLocaleString()}</p>
              </div>
              <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur">
                <p className="text-[10px] uppercase tracking-wider text-emerald-200">Columns</p>
                <p className="text-lg font-bold tabular-nums mt-0.5">{cols ?? 0}</p>
              </div>
              {!skipped && (
                <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur">
                  <p className="text-[10px] uppercase tracking-wider text-emerald-200">Sources</p>
                  <p className="text-lg font-bold tabular-nums mt-0.5">{mergeApprovedSources.length}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </SurfaceCard>

      {/* Merge History Panel */}
      {mergeHistory && mergeHistory.length > 0 && (
        <SurfaceCard noPadding>
          <button
            onClick={() => setHistoryOpen((p) => !p)}
            className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-3xl"
          >
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 text-neutral-400" />
              <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                Merge History ({mergeHistory.length} version{mergeHistory.length !== 1 ? "s" : ""})
              </h3>
            </div>
            {historyOpen ? <ChevronDown className="w-4 h-4 text-neutral-400" /> : <ChevronRight className="w-4 h-4 text-neutral-400" />}
          </button>
          {historyOpen && (
            <div className="px-6 pb-5 space-y-2">
              {mergeHistory.map((entry: any) => (
                <div
                  key={entry.version}
                  className={`flex items-center justify-between rounded-xl border p-3 transition-colors ${
                    entry.version === mergeResult.version
                      ? "border-emerald-300 dark:border-emerald-700 bg-emerald-50/50 dark:bg-emerald-950/20"
                      : "border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800"
                  }`}
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${
                      entry.version === mergeResult.version
                        ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300"
                        : "bg-neutral-100 text-neutral-600 dark:bg-neutral-700 dark:text-neutral-300"
                    }`}>
                      v{entry.version}
                    </span>
                    <div className="min-w-0">
                      <p className="text-xs font-medium text-neutral-700 dark:text-neutral-300 truncate">
                        {entry.file_label || entry.table_name}
                      </p>
                      <p className="text-[10px] text-neutral-400">
                        {entry.rows?.toLocaleString()} rows &middot; {entry.cols} cols &middot; {new Date(entry.timestamp).toLocaleTimeString()}
                      </p>
                    </div>
                  </div>
                  <button
                    onClick={() => onDownloadXlsx(entry.version)}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-neutral-100 dark:bg-neutral-700 hover:bg-emerald-100 dark:hover:bg-emerald-900/30 text-neutral-600 dark:text-neutral-300 hover:text-emerald-700 dark:hover:text-emerald-300 transition-colors shrink-0"
                  >
                    <Download className="w-3.5 h-3.5" /> .xlsx
                  </button>
                </div>
              ))}
            </div>
          )}
        </SurfaceCard>
      )}

      {/* Per-Source Summary */}
      {!skipped && mergeApprovedSources.length > 0 && (
        <SurfaceCard title="Per-Source Merge Details" icon={Table2}>
          <div className="space-y-3">
            {mergeApprovedSources.map((src, i) => {
              const report = src.validation_report;
              return (
                <div key={i} className="rounded-xl border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 p-4">
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                      Source {i + 1}: {groupNameMap[src.source_group_id] || src.source_group_id}
                    </h4>
                    <span className="text-[10px] font-bold bg-emerald-100 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-400 px-2 py-0.5 rounded-full">Approved</span>
                  </div>
                  <div className="flex flex-wrap gap-4 text-xs text-neutral-500 dark:text-neutral-400">
                    <span>Keys: {(src.key_pairs || []).map((kp: any) => `${kp.base_col}↔${kp.source_col}`).join(", ")}</span>
                    <span>Pulled: {(src.pull_columns || []).length} columns</span>
                    {report && (
                      <>
                        <span>Rows: {report.result_rows}</span>
                        <span>Explosion: {report.explosion_factor}×</span>
                      </>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </SurfaceCard>
      )}

      {/* Column Stats */}
      {column_stats && column_stats.length > 0 && (
        <SurfaceCard title="Column Quality" icon={FileText}>
          <div className="overflow-auto max-h-[400px] rounded-xl border border-neutral-200 dark:border-neutral-700">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-neutral-50 dark:bg-neutral-800">
                <tr>
                  <th className="px-3 py-2 text-left font-bold text-neutral-500">Column</th>
                  <th className="px-3 py-2 text-left font-bold text-neutral-500">Fill Rate</th>
                  <th className="px-3 py-2 text-right font-bold text-neutral-500">Nulls</th>
                  <th className="px-3 py-2 text-right font-bold text-neutral-500">Distinct</th>
                </tr>
              </thead>
              <tbody>
                {column_stats.map((cs: any) => (
                  <tr key={cs.column_name} className="border-t border-neutral-100 dark:border-neutral-800">
                    <td className="px-3 py-1.5 font-medium text-neutral-700 dark:text-neutral-300">{cs.column_name}</td>
                    <td className="px-3 py-1.5"><FillBar rate={cs.fill_rate} /></td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-neutral-500">{cs.null_count}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-neutral-500">{cs.distinct_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SurfaceCard>
      )}

      {/* Preview */}
      {preview && preview.length > 0 && (
        <SurfaceCard title="Data Preview" subtitle={`First ${preview.length} rows of final_merged`}>
          <div className="overflow-auto max-h-[350px] rounded-xl border border-neutral-200 dark:border-neutral-700">
            <table className="w-full text-[11px]">
              <thead className="sticky top-0 bg-neutral-50 dark:bg-neutral-800">
                <tr>
                  {Object.keys(preview[0]).map((col) => (
                    <th key={col} className="px-2 py-1.5 font-bold text-neutral-500 whitespace-nowrap border-b">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.slice(0, 50).map((row: any, ri: number) => (
                  <tr key={ri} className="hover:bg-neutral-50 dark:hover:bg-neutral-800/50">
                    {Object.values(row).map((val: any, ci: number) => (
                      <td key={ci} className="px-2 py-1 border-b border-neutral-100 dark:border-neutral-800 whitespace-nowrap max-w-[180px] truncate">{String(val ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SurfaceCard>
      )}

      {/* Actions */}
      <SurfaceCard title="What's Next?" subtitle="Choose how to proceed with your merged data">
        <div className={`grid grid-cols-1 ${hasMultipleVersions ? "sm:grid-cols-4" : "sm:grid-cols-3"} gap-3 mb-4`}>
          <button
            onClick={onRedoMerge}
            className="flex flex-col items-center gap-2 rounded-2xl border-2 border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-5 hover:border-red-300 dark:hover:border-red-700 hover:bg-red-50/30 dark:hover:bg-red-950/20 transition-all group"
          >
            <span className="w-10 h-10 rounded-xl bg-neutral-100 dark:bg-neutral-700 flex items-center justify-center group-hover:bg-red-100 dark:group-hover:bg-red-900/30 transition-colors">
              <RotateCcw className="w-5 h-5 text-neutral-500 group-hover:text-red-600 dark:group-hover:text-red-400 transition-colors" />
            </span>
            <span className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">Redo Merge</span>
            <span className="text-[11px] text-neutral-400 text-center leading-tight">Same files, pick different keys</span>
          </button>

          <button
            onClick={onMergeAgain}
            disabled={mergeAgainLoading}
            className="flex flex-col items-center gap-2 rounded-2xl border-2 border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-5 hover:border-blue-300 dark:hover:border-blue-700 hover:bg-blue-50/30 dark:hover:bg-blue-950/20 transition-all group disabled:opacity-60"
          >
            <span className="w-10 h-10 rounded-xl bg-neutral-100 dark:bg-neutral-700 flex items-center justify-center group-hover:bg-blue-100 dark:group-hover:bg-blue-900/30 transition-colors">
              {mergeAgainLoading
                ? <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />
                : <RefreshCw className="w-5 h-5 text-neutral-500 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors" />}
            </span>
            <span className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">Perform Another Merge</span>
            <span className="text-[11px] text-neutral-400 text-center leading-tight">Use this output in a new merge</span>
          </button>

          <button
            onClick={() => onDownloadXlsx()}
            className="flex flex-col items-center gap-2 rounded-2xl border-2 border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-5 hover:border-emerald-300 dark:hover:border-emerald-700 hover:bg-emerald-50/30 dark:hover:bg-emerald-950/20 transition-all group"
          >
            <span className="w-10 h-10 rounded-xl bg-neutral-100 dark:bg-neutral-700 flex items-center justify-center group-hover:bg-emerald-100 dark:group-hover:bg-emerald-900/30 transition-colors">
              <Download className="w-5 h-5 text-neutral-500 group-hover:text-emerald-600 dark:group-hover:text-emerald-400 transition-colors" />
            </span>
            <span className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">Download This Output</span>
            <span className="text-[11px] text-neutral-400 text-center leading-tight">Export as Excel (.xlsx)</span>
          </button>

          {hasMultipleVersions && (
            <button
              onClick={onDownloadAllZip}
              className="flex flex-col items-center gap-2 rounded-2xl border-2 border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-5 hover:border-violet-300 dark:hover:border-violet-700 hover:bg-violet-50/30 dark:hover:bg-violet-950/20 transition-all group"
            >
              <span className="w-10 h-10 rounded-xl bg-neutral-100 dark:bg-neutral-700 flex items-center justify-center group-hover:bg-violet-100 dark:group-hover:bg-violet-900/30 transition-colors">
                <Package className="w-5 h-5 text-neutral-500 group-hover:text-violet-600 dark:group-hover:text-violet-400 transition-colors" />
              </span>
              <span className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">Finalize All & Download</span>
              <span className="text-[11px] text-neutral-400 text-center leading-tight">ZIP of all {mergeHistory.length} outputs</span>
            </button>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-3 pt-2 border-t border-neutral-100 dark:border-neutral-800">
          <SecondaryButton onClick={onDownloadCsv}>
            <Download className="w-4 h-4" />
            Download CSV
          </SecondaryButton>
          <SecondaryButton onClick={onDownloadReport}>
            <FileText className="w-4 h-4" />
            Download Audit Report
          </SecondaryButton>
        </div>
      </SurfaceCard>
    </motion.div>
  );
}
