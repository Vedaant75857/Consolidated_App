import React, { useState } from "react";
import { CheckCircle2, AlertTriangle, ChevronRight, ChevronDown, Layers, ArrowRight, Table2 } from "lucide-react";
import { motion } from "motion/react";
import { PrimaryButton, ResultCapsules, Tooltip, FillBar } from "../common/ui";

interface AppendReportProps {
  appendReport: any[];
  onProceed: () => void;
}

const DEFINITIONS: Record<string, string> = {
  row_integrity:
    "Validates that the total rows in the appended group equals the sum of rows from each source table, confirming no data was lost during stacking.",
  fill_rate:
    "Percentage of non-null values in a column. Low fill rate may indicate missing data from source tables that lacked this column.",
  distinct_count:
    "Number of unique non-null values. High counts suggest identifiers or free-text; low counts suggest categories or flags.",
  source_contribution:
    "How many rows each original file contributed to the unified group.",
  null_count:
    "Number of rows where this column is empty, null, or blank. High null counts in mapped columns may indicate mapping issues.",
};

function GroupCard({ group }: { key?: React.Key; group: any }) {
  const [expanded, setExpanded] = useState(false);
  const integrityOk = group.row_integrity;

  return (
    <div className="border border-neutral-200 dark:border-neutral-700 rounded-2xl overflow-hidden">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-6 py-4 bg-neutral-50 dark:bg-neutral-800 hover:bg-neutral-100 dark:hover:bg-neutral-700 transition-colors text-left"
      >
        <div className="flex items-center gap-3">
          <Layers className="w-4 h-4 text-neutral-500 dark:text-neutral-400" />
          <span className="text-sm font-bold text-neutral-900 dark:text-white">{group.group_id}</span>
          <span className="text-[10px] font-bold bg-neutral-200 dark:bg-neutral-700 text-neutral-600 dark:text-neutral-300 px-2 py-0.5 rounded-full">
            {group.total_rows.toLocaleString()} rows
          </span>
          <span className="text-[10px] font-bold bg-neutral-200 dark:bg-neutral-700 text-neutral-600 dark:text-neutral-300 px-2 py-0.5 rounded-full">
            {group.total_cols} cols
          </span>
          {group.is_standalone && (
            <span className="text-[10px] font-bold bg-blue-50 dark:bg-blue-950/30 text-blue-600 dark:text-blue-400 px-2 py-0.5 rounded-full">STANDALONE</span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {integrityOk ? (
            <span className="flex items-center gap-1 text-[10px] font-bold text-emerald-600 dark:text-emerald-400">
              <CheckCircle2 className="w-3.5 h-3.5" /> Integrity OK
            </span>
          ) : (
            <span className="flex items-center gap-1 text-[10px] font-bold text-red-600 dark:text-red-400">
              <AlertTriangle className="w-3.5 h-3.5" /> Row Mismatch
            </span>
          )}
          {expanded ? <ChevronDown className="w-4 h-4 text-neutral-400 dark:text-neutral-500" /> : <ChevronRight className="w-4 h-4 text-neutral-400 dark:text-neutral-500" />}
        </div>
      </button>

      {expanded && (
        <div className="p-6 space-y-6">
          {/* Row Integrity */}
          <div className={`flex items-start gap-3 p-4 rounded-xl border ${integrityOk ? "bg-emerald-50 dark:bg-emerald-950/30 border-emerald-100" : "bg-red-50 dark:bg-red-950/30 border-red-100"}`}>
            {integrityOk ? (
              <CheckCircle2 className="w-5 h-5 text-emerald-600 dark:text-emerald-400 mt-0.5 shrink-0" />
            ) : (
              <AlertTriangle className="w-5 h-5 text-red-600 dark:text-red-400 mt-0.5 shrink-0" />
            )}
            <div>
              <p className={`text-sm font-bold ${integrityOk ? "text-emerald-800 dark:text-emerald-400" : "text-red-800 dark:text-red-400"}`}>
                Row Integrity Check {integrityOk ? "Passed" : "Failed"}
              </p>
              <p className="text-xs text-neutral-600 dark:text-neutral-300 mt-1">
                Expected {(group.expected_total_rows ?? 0).toLocaleString()} rows (sum of source tables), got {(group.total_rows ?? 0).toLocaleString()} rows.
              </p>
              <p className="text-[10px] text-neutral-400 dark:text-neutral-500 mt-1 italic">{DEFINITIONS.row_integrity}</p>
            </div>
          </div>

          {/* Source Table Contributions */}
          {group.tables_detail && group.tables_detail.length > 0 && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Table2 className="w-4 h-4 text-neutral-500 dark:text-neutral-400" />
                <h4 className="text-sm font-bold text-neutral-800 dark:text-neutral-200">Source Table Contributions</h4>
                <Tooltip text={DEFINITIONS.source_contribution} />
              </div>
              <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl overflow-hidden">
                <table className="min-w-full text-xs">
                  <thead className="bg-neutral-50 dark:bg-neutral-800">
                    <tr>
                      <th className="text-left px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">Source Table</th>
                      <th className="text-right px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">Rows</th>
                      <th className="text-right px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">% of Total</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {group.tables_detail.map((td: any) => (
                      <tr key={td.table_key} className="hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors">
                        <td className="px-4 py-2.5 font-medium text-neutral-700 dark:text-neutral-300 truncate max-w-[300px]" title={td.table_key}>
                          {td.table_key}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums font-bold text-neutral-900 dark:text-white">
                          {td.rows_contributed.toLocaleString()}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-neutral-500 dark:text-neutral-400">
                          {group.total_rows > 0 ? ((td.rows_contributed / group.total_rows) * 100).toFixed(1) : 0}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Column Statistics */}
          {group.column_stats && group.column_stats.length > 0 && (
            <ColumnStatsTable stats={group.column_stats} totalRows={group.total_rows} />
          )}
        </div>
      )}
    </div>
  );
}

function ColumnStatsTable({ stats, totalRows }: { stats: any[]; totalRows: number }) {
  const [expanded, setExpanded] = useState(false);
  const display = expanded ? stats : stats.slice(0, 10);

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <h4 className="text-sm font-bold text-neutral-800 dark:text-neutral-200">Column Statistics</h4>
        <span className="text-[10px] font-bold bg-neutral-200 dark:bg-neutral-700 text-neutral-600 dark:text-neutral-300 px-2 py-0.5 rounded-full">
          {stats.length} columns
        </span>
      </div>
      <div className="border border-neutral-200 rounded-xl overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-neutral-50">
              <tr>
                <th className="text-left px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">Column</th>
                <th className="text-left px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">
                  <span className="flex items-center gap-1">Fill Rate <Tooltip text={DEFINITIONS.fill_rate} /></span>
                </th>
                <th className="text-right px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">
                  <span className="flex items-center justify-end gap-1">Nulls <Tooltip text={DEFINITIONS.null_count} /></span>
                </th>
                <th className="text-right px-4 py-2.5 font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider">
                  <span className="flex items-center justify-end gap-1">Distinct <Tooltip text={DEFINITIONS.distinct_count} /></span>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
              {display.map((cs: any) => (
                <tr key={cs.column_name} className="hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors">
                  <td className="px-4 py-2.5 font-medium text-neutral-700 dark:text-neutral-300 truncate max-w-[250px]" title={cs.column_name}>
                    {cs.column_name}
                  </td>
                  <td className="px-4 py-2.5">
                    <FillBar rate={cs.fill_rate} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums text-neutral-600 dark:text-neutral-300">
                    {cs.null_count.toLocaleString()}
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums font-bold text-neutral-900 dark:text-white">
                    {cs.distinct_count.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {stats.length > 10 && (
          <button
            type="button"
            onClick={() => setExpanded(!expanded)}
            className="w-full px-4 py-2.5 text-xs font-bold text-red-600 dark:text-red-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors border-t border-neutral-100 dark:border-neutral-800"
          >
            {expanded ? "Show fewer columns" : `Show all ${stats.length} columns`}
          </button>
        )}
      </div>
    </div>
  );
}

export default function AppendReport({ appendReport, onProceed }: AppendReportProps) {
  if (!appendReport || appendReport.length === 0) return null;

  const totalRows = appendReport.reduce((s, g) => s + g.total_rows, 0);
  const totalGroups = appendReport.length;
  const allIntegrity = appendReport.every((g) => g.row_integrity);

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-3xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-hidden mt-6"
    >
      {/* Header */}
      <div className="p-8 text-center bg-gradient-to-br from-emerald-600 to-teal-600 text-white rounded-t-3xl">
        <div className="w-16 h-16 rounded-2xl bg-white/20 backdrop-blur-md text-white flex items-center justify-center mx-auto mb-6 shadow-xl">
          <CheckCircle2 className="w-10 h-10" />
        </div>
        <h2 className="text-3xl font-bold tracking-tight mb-2">Append Complete</h2>
        <p className="text-emerald-100 max-w-md mx-auto">
          Your source tables have been stacked into unified groups. Review the quality report below.
        </p>
        <div className="mt-4">
          <ResultCapsules items={[
            `${totalGroups} group${totalGroups !== 1 ? "s" : ""} created`,
            `${totalRows.toLocaleString()} total rows`,
            allIntegrity ? "All integrity passed" : "Integrity warnings",
          ]} />
        </div>
      </div>

      {/* Summary Row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-6">
        <div className="bg-neutral-50 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 rounded-2xl p-5 text-center">
          <p className="text-[10px] font-black text-neutral-400 dark:text-neutral-500 uppercase tracking-widest mb-2">Groups Created</p>
          <p className="text-4xl font-black text-red-600 dark:text-red-400 tabular-nums">{totalGroups}</p>
        </div>
        <div className="bg-neutral-50 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 rounded-2xl p-5 text-center">
          <p className="text-[10px] font-black text-neutral-400 dark:text-neutral-500 uppercase tracking-widest mb-2">Total Rows</p>
          <p className="text-4xl font-black text-red-600 dark:text-red-400 tabular-nums">{totalRows.toLocaleString()}</p>
        </div>
        <div className={`border rounded-2xl p-5 text-center ${allIntegrity ? "bg-emerald-50 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800" : "bg-red-50 dark:bg-red-950/30 border-red-200 dark:border-red-800"}`}>
          <p className="text-[10px] font-black text-neutral-400 dark:text-neutral-500 uppercase tracking-widest mb-2">Data Integrity</p>
          <p className={`text-4xl font-black ${allIntegrity ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"}`}>
            {allIntegrity ? "PASS" : "WARN"}
          </p>
        </div>
      </div>

      {/* Per-Group Cards */}
      <div className="px-6 pb-6 space-y-4">
        {appendReport.map((group: any) => (
          <GroupCard key={group.group_id} group={group} />
        ))}
      </div>

      {/* Proceed Button */}
      <div className="p-6 border-t border-neutral-100 dark:border-neutral-800 flex justify-end">
        <PrimaryButton onClick={onProceed}>
          Proceed to Header Normalisation
          <ArrowRight className="w-4 h-4" />
        </PrimaryButton>
      </div>
    </motion.section>
  );
}
