import React, { useState, useEffect, useCallback } from "react";
import { Loader2, Sparkles, ArrowRight, Check, ChevronDown, ChevronRight, Copy, BarChart3, Hash, Trash2, RotateCcw } from "lucide-react";
import { motion } from "motion/react";
import { PrimaryButton, SecondaryButton } from "../common/ui";

export interface CleaningConfig {
  removeNullRows: boolean;
  removeNullColumns: boolean;
  caseMode: "upper" | "lower" | "none";
  trimWhitespace: boolean;
  deduplicateColumns: string[];
}

export const DEFAULT_CONFIG: CleaningConfig = {
  removeNullRows: true,
  removeNullColumns: true,
  caseMode: "upper",
  trimWhitespace: true,
  deduplicateColumns: [],
};

interface DedupStats {
  group_id: string;
  total_rows: number;
  unique_rows: number;
  duplicate_rows: number;
  decrease_pct: number;
  dedup_columns?: string[];
}

interface DedupResult {
  group_id: string;
  rows_before: number;
  rows_after: number;
  duplicates_removed: number;
  decrease_pct: number;
}

interface ColumnFormatAnalysis {
  column: string;
  total_values: number;
  has_leading_zeros: boolean;
  pct_leading_zeros: number;
  all_numeric: boolean;
  pct_numeric: number;
  min_len: number;
  max_len: number;
  mode_len: number;
  recommendation: "strip" | "pad" | "none";
  reason: string;
}

interface StdAction {
  column: string;
  operation: "strip" | "pad" | "none";
  pad_length: number;
}

interface ConcatColumnInfo {
  column_name: string;
  source_columns: string[];
}

interface DataCleaningProps {
  step: number;
  groupSchema: any[];
  groupNameMap?: Record<string, string>;
  sessionId: string;
  cleaningConfigs: Record<string, any>;
  dedupResults: Record<string, DedupResult>;
  standardizeConfigs: Record<string, any>;
  concatConfigs: Record<string, ConcatColumnInfo[]>;
  loading: boolean;
  onCleanGroup: (groupId: string, config: CleaningConfig) => Promise<void>;
  onDedupPreview: (groupId: string, columns: string[]) => Promise<DedupStats | null>;
  onDedupApply: (groupId: string, columns: string[]) => Promise<DedupResult | null>;
  onAnalyzeColumns: (groupId: string, columns: string[]) => Promise<ColumnFormatAnalysis[]>;
  onApplyStandardize: (groupId: string, actions: StdAction[]) => Promise<void>;
  onConcatApply: (groupId: string, columns: string[]) => Promise<any>;
  onDeleteConcatColumn: (groupId: string, columnName: string) => Promise<any>;
  removedColumns: Record<string, string[]>;
  onRemoveColumns: (groupId: string, columns: string[]) => Promise<any>;
  onRestoreColumns: (groupId: string, columns: string[]) => Promise<any>;
  onProceed: () => void;
  onSkip: () => void;
}

export default function DataCleaning({
  step,
  groupSchema,
  groupNameMap = {},
  sessionId,
  cleaningConfigs,
  dedupResults,
  standardizeConfigs,
  concatConfigs,
  loading,
  onCleanGroup,
  onDedupPreview,
  onDedupApply,
  onAnalyzeColumns,
  onApplyStandardize,
  onConcatApply,
  onDeleteConcatColumn,
  removedColumns,
  onRemoveColumns,
  onRestoreColumns,
  onProceed,
  onSkip,
}: DataCleaningProps) {
  const [subStep, setSubStep] = useState<"5a" | "5b">("5a");
  const [innerTab, setInnerTab] = useState<"dedup" | "colstd" | "concat" | "colrem">("dedup");
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null);
  const [localConfig, setLocalConfig] = useState<CleaningConfig>(DEFAULT_CONFIG);
  const [groupPreviews, setGroupPreviews] = useState<Record<string, { columns: string[]; rows: any[] }>>({});

  // Dedup state
  const [dedupColumns, setDedupColumns] = useState<string[]>([]);
  const [dedupPreviewStats, setDedupPreviewStats] = useState<DedupStats | null>(null);
  const [dedupPreviewLoading, setDedupPreviewLoading] = useState(false);
  const [dedupApplyLoading, setDedupApplyLoading] = useState(false);

  // 5c: Column Standardization state
  const [stdSelectedCols, setStdSelectedCols] = useState<string[]>([]);
  const [stdAnalysisResults, setStdAnalysisResults] = useState<ColumnFormatAnalysis[]>([]);
  const [stdActions, setStdActions] = useState<Record<string, StdAction>>({});
  const [stdAnalyzeLoading, setStdAnalyzeLoading] = useState(false);
  const [stdApplyLoading, setStdApplyLoading] = useState(false);

  // 5d: Concatenation state
  const [concatSelectedCols, setConcatSelectedCols] = useState<string[]>([]);
  const [concatApplyLoading, setConcatApplyLoading] = useState(false);
  const [concatDeleteLoading, setConcatDeleteLoading] = useState<string | null>(null);

  // Column Removal state
  const [colRemSelected, setColRemSelected] = useState<string[]>([]);
  const [colRemApplyLoading, setColRemApplyLoading] = useState(false);
  const [colRemRestoreLoading, setColRemRestoreLoading] = useState<string | null>(null);

  useEffect(() => {
    if (groupSchema.length > 0 && !selectedGroup) {
      setSelectedGroup(groupSchema[0].group_id);
    }
  }, [groupSchema, selectedGroup]);

  const fetchGroupPreview = useCallback(async (groupId: string) => {
    if (!sessionId || groupPreviews[groupId]) return;
    try {
      const res = await fetch("/api/header-norm-group-preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, groupIds: [groupId] }),
      });
      if (res.ok) {
        const data = await res.json();
        const match = (data.previews || []).find((p: any) => p.group_id === groupId);
        if (match) {
          setGroupPreviews((prev) => ({ ...prev, [groupId]: { columns: match.columns || [], rows: match.rows || [] } }));
        }
      }
    } catch { /* ignore */ }
  }, [sessionId, groupPreviews]);

  useEffect(() => {
    if (selectedGroup) fetchGroupPreview(selectedGroup);
  }, [selectedGroup, fetchGroupPreview]);

  useEffect(() => {
    if (selectedGroup && cleaningConfigs[selectedGroup]) {
      setLocalConfig({ ...DEFAULT_CONFIG, ...cleaningConfigs[selectedGroup] });
    } else {
      setLocalConfig({ ...DEFAULT_CONFIG, deduplicateColumns: [] });
    }
  }, [selectedGroup, cleaningConfigs]);

  useEffect(() => {
    setDedupColumns([]);
    setDedupPreviewStats(null);
    setStdSelectedCols([]);
    setStdAnalysisResults([]);
    setStdActions({});
    setConcatSelectedCols([]);
    setColRemSelected([]);
  }, [selectedGroup]);

  const currentSchema = selectedGroup ? groupSchema.find((g: any) => g.group_id === selectedGroup) : null;
  const currentPreview = selectedGroup ? groupPreviews[selectedGroup] : null;
  const columns: string[] = currentPreview?.columns || currentSchema?.columns || [];
  const isCleaned = selectedGroup ? !!cleaningConfigs[selectedGroup] : false;
  const isDeduped = selectedGroup ? !!dedupResults[selectedGroup] : false;
  const isStandardized = selectedGroup ? !!standardizeConfigs[selectedGroup] : false;
  const hasConcats = selectedGroup ? (concatConfigs[selectedGroup]?.length || 0) > 0 : false;
  const hasRemovedCols = selectedGroup ? (removedColumns[selectedGroup]?.length || 0) > 0 : false;

  const handleApplyCleaning = async () => {
    if (!selectedGroup) return;
    await onCleanGroup(selectedGroup, localConfig);
    setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup]; return n; });
    fetchGroupPreview(selectedGroup);
  };

  const toggleDedupColumn = (col: string) => {
    setDedupColumns((prev) => prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]);
    setDedupPreviewStats(null);
  };

  const handleDedupPreview = async () => {
    if (!selectedGroup || dedupColumns.length === 0) return;
    setDedupPreviewLoading(true);
    try {
      const stats = await onDedupPreview(selectedGroup, dedupColumns);
      setDedupPreviewStats(stats);
    } finally {
      setDedupPreviewLoading(false);
    }
  };

  const handleDedupApply = async () => {
    if (!selectedGroup || dedupColumns.length === 0) return;
    setDedupApplyLoading(true);
    try {
      await onDedupApply(selectedGroup, dedupColumns);
      setDedupPreviewStats(null);
      setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup]; return n; });
      fetchGroupPreview(selectedGroup);
    } finally {
      setDedupApplyLoading(false);
    }
  };

  const toggleStdColumn = (col: string) => {
    setStdSelectedCols((prev) => prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]);
  };

  const handleStdAnalyze = async () => {
    if (!selectedGroup || stdSelectedCols.length === 0) return;
    setStdAnalyzeLoading(true);
    try {
      const results = await onAnalyzeColumns(selectedGroup, stdSelectedCols);
      setStdAnalysisResults(results);
      const actions: Record<string, StdAction> = {};
      for (const r of results) {
        actions[r.column] = { column: r.column, operation: r.recommendation, pad_length: r.max_len };
      }
      setStdActions(actions);
    } finally {
      setStdAnalyzeLoading(false);
    }
  };

  const handleStdApply = async () => {
    if (!selectedGroup || stdAnalysisResults.length === 0) return;
    setStdApplyLoading(true);
    try {
      const actionsList = Object.values(stdActions).filter((a) => a.operation !== "none");
      await onApplyStandardize(selectedGroup, actionsList);
      setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup!]; return n; });
      fetchGroupPreview(selectedGroup);
    } finally {
      setStdApplyLoading(false);
    }
  };

  // 5d: Concatenation handlers
  const toggleConcatColumn = (col: string) => {
    setConcatSelectedCols((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  const handleConcatApply = async () => {
    if (!selectedGroup || concatSelectedCols.length < 2) return;
    setConcatApplyLoading(true);
    try {
      const result = await onConcatApply(selectedGroup, concatSelectedCols);
      if (result) {
        setConcatSelectedCols([]);
        setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup!]; return n; });
        fetchGroupPreview(selectedGroup);
      }
    } finally {
      setConcatApplyLoading(false);
    }
  };

  const handleDeleteConcat = async (columnName: string) => {
    if (!selectedGroup) return;
    setConcatDeleteLoading(columnName);
    try {
      const result = await onDeleteConcatColumn(selectedGroup, columnName);
      if (result) {
        setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup!]; return n; });
        fetchGroupPreview(selectedGroup);
      }
    } finally {
      setConcatDeleteLoading(null);
    }
  };

  // Column Removal handlers
  const toggleColRemColumn = (col: string) => {
    setColRemSelected((prev) => prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]);
  };

  const handleColRemApply = async () => {
    if (!selectedGroup || colRemSelected.length === 0) return;
    setColRemApplyLoading(true);
    try {
      const result = await onRemoveColumns(selectedGroup, colRemSelected);
      if (result) {
        setColRemSelected([]);
        setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup!]; return n; });
        fetchGroupPreview(selectedGroup);
      }
    } finally {
      setColRemApplyLoading(false);
    }
  };

  const handleColRemRestore = async (columnName: string) => {
    if (!selectedGroup) return;
    setColRemRestoreLoading(columnName);
    try {
      const result = await onRestoreColumns(selectedGroup, [columnName]);
      if (result) {
        setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup!]; return n; });
        fetchGroupPreview(selectedGroup);
      }
    } finally {
      setColRemRestoreLoading(null);
    }
  };

  if (step !== 5) return null;

  const gn = (id: string) => groupNameMap[id] || id;

  const groupSidebar = (statusKey: "clean" | "dedup" | "standardize" | "concat" | "colrem") => (
    <div className="w-64 border-r border-neutral-100 dark:border-neutral-800 bg-neutral-50/30 dark:bg-neutral-800 overflow-y-auto shrink-0">
      <div className="p-3">
        <p className="text-[10px] font-bold uppercase tracking-wider text-neutral-400 dark:text-neutral-500 px-2 mb-2">
          Groups ({groupSchema.length})
        </p>
        {groupSchema.map((gs: any) => {
          const isSelected = selectedGroup === gs.group_id;
          const isDone = statusKey === "clean" ? !!cleaningConfigs[gs.group_id]
            : statusKey === "dedup" ? !!dedupResults[gs.group_id]
            : statusKey === "standardize" ? !!standardizeConfigs[gs.group_id]
            : statusKey === "concat" ? (concatConfigs[gs.group_id]?.length || 0) > 0
            : (removedColumns[gs.group_id]?.length || 0) > 0;
          return (
            <button
              key={gs.group_id}
              type="button"
              onClick={() => setSelectedGroup(gs.group_id)}
              className={`w-full text-left px-3 py-2.5 rounded-lg text-xs font-medium transition-colors mb-1 flex items-center gap-2 ${
                isSelected
                  ? "bg-red-50 dark:bg-red-950/30 text-red-700 dark:text-red-400 border border-red-200 dark:border-red-800"
                  : "text-neutral-600 dark:text-neutral-300 hover:bg-neutral-100 dark:hover:bg-neutral-700 border border-transparent"
              }`}
            >
              <span className="truncate flex-1">{gn(gs.group_id)}</span>
              {isDone && (
                <span className="w-4 h-4 rounded-full bg-emerald-100 dark:bg-emerald-950/40 text-emerald-600 dark:text-emerald-400 flex items-center justify-center shrink-0">
                  <Check className="w-2.5 h-2.5" />
                </span>
              )}
              <span className="text-[10px] text-neutral-400 dark:text-neutral-500 shrink-0">
                {gs.rows}r
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-3xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-hidden"
    >
      {/* Header */}
      <div className="p-6 border-b border-neutral-100 dark:border-neutral-800">
        <h2 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
          <Sparkles className="w-5 h-5 text-red-600" />
          Data Cleaning
        </h2>
        <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1">
          Clean your appended group tables and remove duplicate rows.
        </p>

        {/* Sub-step tabs */}
        <div className="flex gap-1 mt-4 bg-neutral-100 dark:bg-neutral-800 rounded-xl p-1 w-fit">
          <button
            type="button"
            onClick={() => setSubStep("5a")}
            className={`px-4 py-2 rounded-lg text-xs font-bold transition-all ${
              subStep === "5a"
                ? "bg-white dark:bg-neutral-700 text-neutral-900 dark:text-white shadow-sm"
                : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-200"
            }`}
          >
            5a — Cleaning
            {Object.keys(cleaningConfigs).length > 0 && (
              <span className="ml-1.5 text-emerald-600 dark:text-emerald-400">({Object.keys(cleaningConfigs).length})</span>
            )}
          </button>
          <button
            type="button"
            onClick={() => setSubStep("5b")}
            className={`px-4 py-2 rounded-lg text-xs font-bold transition-all ${
              subStep === "5b"
                ? "bg-white dark:bg-neutral-700 text-neutral-900 dark:text-white shadow-sm"
                : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-200"
            }`}
          >
            5b — Data Cleaning (Additional)
            {(() => {
              const total = Object.keys(dedupResults).length + Object.keys(standardizeConfigs).length + Object.values(concatConfigs).flat().length + Object.values(removedColumns).flat().length;
              return total > 0 ? <span className="ml-1.5 text-emerald-600 dark:text-emerald-400">({total})</span> : null;
            })()}
          </button>
        </div>
      </div>

      {/* ===== Sub-step 5a: Cleaning ===== */}
      {subStep === "5a" && (
        <div className="flex min-h-[500px]">
          {groupSidebar("clean")}

          <div className="flex-1 overflow-y-auto">
            {selectedGroup && columns.length > 0 ? (
              <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm">{gn(selectedGroup)}</h3>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                      {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                      {isCleaned && <span className="text-emerald-600 dark:text-emerald-400 font-medium ml-2">Cleaned</span>}
                    </p>
                  </div>
                  <PrimaryButton onClick={handleApplyCleaning} disabled={loading} className="text-xs px-4 py-2">
                    {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                    Apply Cleaning
                  </PrimaryButton>
                </div>

                {/* Cleaning options */}
                <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl overflow-hidden">
                  <div className="px-4 py-3 bg-emerald-50/60 dark:bg-emerald-950/20 border-b border-neutral-200 dark:border-neutral-700">
                    <p className="text-xs font-bold text-emerald-700 dark:text-emerald-400">Cleaning Options</p>
                    <p className="text-[10px] text-emerald-600/70 dark:text-emerald-500/70">Configure which cleaning steps to apply</p>
                  </div>
                  <div className="p-4">
                    <div className="grid grid-cols-2 gap-4">
                      <label className="flex items-center gap-3 px-4 py-3 border border-neutral-200 dark:border-neutral-700 rounded-xl hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer transition-colors">
                        <input
                          type="checkbox"
                          checked={localConfig.removeNullRows}
                          onChange={(e) => setLocalConfig((p) => ({ ...p, removeNullRows: e.target.checked }))}
                          className="w-4 h-4 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                        />
                        <div>
                          <p className="text-xs font-bold text-neutral-900 dark:text-white">Remove Null Rows</p>
                          <p className="text-[10px] text-neutral-400 dark:text-neutral-500">Drop rows where all values are empty</p>
                        </div>
                      </label>

                      <label className="flex items-center gap-3 px-4 py-3 border border-neutral-200 dark:border-neutral-700 rounded-xl hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer transition-colors">
                        <input
                          type="checkbox"
                          checked={localConfig.removeNullColumns}
                          onChange={(e) => setLocalConfig((p) => ({ ...p, removeNullColumns: e.target.checked }))}
                          className="w-4 h-4 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                        />
                        <div>
                          <p className="text-xs font-bold text-neutral-900 dark:text-white">Remove Null Columns</p>
                          <p className="text-[10px] text-neutral-400 dark:text-neutral-500">Drop columns where all values are empty</p>
                        </div>
                      </label>

                      <label className="flex items-center gap-3 px-4 py-3 border border-neutral-200 dark:border-neutral-700 rounded-xl hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer transition-colors">
                        <input
                          type="checkbox"
                          checked={localConfig.trimWhitespace}
                          onChange={(e) => setLocalConfig((p) => ({ ...p, trimWhitespace: e.target.checked }))}
                          className="w-4 h-4 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                        />
                        <div>
                          <p className="text-xs font-bold text-neutral-900 dark:text-white">Trim Whitespace</p>
                          <p className="text-[10px] text-neutral-400 dark:text-neutral-500">Remove leading/trailing spaces</p>
                        </div>
                      </label>

                      <label className="flex items-center gap-3 px-4 py-3 border border-neutral-200 dark:border-neutral-700 rounded-xl hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer transition-colors">
                        <input
                          type="checkbox"
                          checked={localConfig.caseMode !== "none"}
                          onChange={(e) =>
                            setLocalConfig((p) => ({
                              ...p,
                              caseMode: e.target.checked ? "upper" : "none",
                            }))
                          }
                          className="w-4 h-4 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                        />
                        <div className="flex-1">
                          <p className="text-xs font-bold text-neutral-900 dark:text-white">Standardize Case</p>
                          <p className="text-[10px] text-neutral-400 dark:text-neutral-500">Apply to all text values</p>
                        </div>
                        {localConfig.caseMode !== "none" && (
                          <select
                            value={localConfig.caseMode}
                            onClick={(e) => e.stopPropagation()}
                            onChange={(e) => setLocalConfig((p) => ({ ...p, caseMode: e.target.value as "upper" | "lower" }))}
                            className="text-xs border border-neutral-200 dark:border-neutral-700 rounded-lg px-2 py-1.5 bg-white dark:bg-neutral-900 focus:outline-none focus:ring-2 focus:ring-red-500 transition-shadow"
                          >
                            <option value="upper">UPPER CASE</option>
                            <option value="lower">lower case</option>
                          </select>
                        )}
                      </label>
                    </div>
                  </div>
                </div>

                {/* Data preview */}
                {currentPreview && columns.length > 0 && (
                  <div>
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-3">
                      Data Preview ({columns.length} columns)
                    </p>
                    <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-64">
                      <table className="min-w-full text-xs">
                        <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0">
                          <tr>
                            {columns.map((col: string) => (
                              <th key={col} className="px-3 py-2 text-left font-bold text-neutral-500 whitespace-nowrap border-b border-neutral-200">
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-neutral-100">
                          {currentPreview.rows.map((row: any, ri: number) => (
                            <tr key={ri} className="hover:bg-red-50/30 dark:hover:bg-red-950/10">
                              {columns.map((col: string) => (
                                <td key={col} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate">
                                  {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
                {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to configure cleaning options."}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ===== Sub-step 5b: Data Cleaning (Additional) ===== */}
      {subStep === "5b" && (
        <div>
          {/* Inner tab bar */}
          <div className="px-6 pt-4 pb-2">
            <div className="flex gap-1 bg-neutral-50 dark:bg-neutral-800/50 rounded-lg p-1 w-fit">
              {([
                { key: "dedup" as const, label: "Deduplication", count: Object.keys(dedupResults).length },
                { key: "colstd" as const, label: "Column Standardization", count: Object.keys(standardizeConfigs).length },
                { key: "concat" as const, label: "Concatenation", count: Object.values(concatConfigs).flat().length },
                { key: "colrem" as const, label: "Column Removal", count: Object.values(removedColumns).flat().length },
              ]).map((tab) => (
                <button
                  key={tab.key}
                  type="button"
                  onClick={() => setInnerTab(tab.key)}
                  className={`px-3 py-1.5 rounded-md text-[11px] font-semibold transition-all ${
                    innerTab === tab.key
                      ? "bg-white dark:bg-neutral-700 text-neutral-900 dark:text-white shadow-sm"
                      : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-200"
                  }`}
                >
                  {tab.label}
                  {tab.count > 0 && (
                    <span className="ml-1.5 text-emerald-600 dark:text-emerald-400">({tab.count})</span>
                  )}
                </button>
              ))}
            </div>
          </div>

          <div className="flex min-h-[500px]">
            {groupSidebar(innerTab === "dedup" ? "dedup" : innerTab === "colstd" ? "standardize" : innerTab === "concat" ? "concat" : "colrem")}

            <div className="flex-1 overflow-y-auto">

          {/* ── Deduplication tab ── */}
          {innerTab === "dedup" && (
            <>
            {selectedGroup && columns.length > 0 ? (
              <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm flex items-center gap-2">
                      <Copy className="w-4 h-4 text-red-500" />
                      {gn(selectedGroup)}
                    </h3>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                      {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                      {isDeduped && <span className="text-emerald-600 dark:text-emerald-400 font-medium ml-2">Deduplicated</span>}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <SecondaryButton
                      onClick={handleDedupPreview}
                      disabled={dedupColumns.length === 0 || dedupPreviewLoading}
                      className="text-xs px-3 py-2"
                    >
                      {dedupPreviewLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <BarChart3 className="w-3 h-3" />}
                      Preview Stats
                    </SecondaryButton>
                    <PrimaryButton
                      onClick={handleDedupApply}
                      disabled={dedupColumns.length === 0 || dedupApplyLoading || !dedupPreviewStats}
                      className="text-xs px-4 py-2"
                    >
                      {dedupApplyLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                      Apply Deduplication
                    </PrimaryButton>
                  </div>
                </div>

                {/* Instructions */}
                <div className="px-4 py-3 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-xl text-xs text-blue-700 dark:text-blue-300">
                  Select columns that define uniqueness using the checkboxes above column headers. Click <strong>Preview Stats</strong> to see how many duplicates will be removed, then <strong>Apply</strong> to confirm.
                </div>

                {/* Dedup key summary */}
                {dedupColumns.length > 0 && (
                  <div className="px-4 py-2.5 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-xl text-[11px] text-amber-700 dark:text-amber-400 flex items-center justify-between">
                    <span>
                      Uniqueness key: <span className="font-bold">{dedupColumns.join(" + ")}</span>
                      {" "}&mdash; rows with the same combination will be deduplicated (first row kept).
                    </span>
                    <button
                      type="button"
                      onClick={() => { setDedupColumns([]); setDedupPreviewStats(null); }}
                      className="text-[10px] font-medium text-red-500 hover:text-red-700 transition-colors ml-3 shrink-0"
                    >
                      Clear all
                    </button>
                  </div>
                )}

                {/* Dedup preview stats */}
                {dedupPreviewStats && (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                    {[
                      { label: "Total Rows", value: dedupPreviewStats.total_rows.toLocaleString(), color: "text-neutral-700 dark:text-neutral-200" },
                      { label: "Unique Rows", value: dedupPreviewStats.unique_rows.toLocaleString(), color: "text-emerald-600 dark:text-emerald-400" },
                      { label: "Duplicates Found", value: dedupPreviewStats.duplicate_rows.toLocaleString(), color: dedupPreviewStats.duplicate_rows > 0 ? "text-red-600 dark:text-red-400" : "text-neutral-500" },
                      { label: "Row Decrease", value: `${dedupPreviewStats.decrease_pct}%`, color: dedupPreviewStats.decrease_pct > 0 ? "text-amber-600 dark:text-amber-400" : "text-neutral-500" },
                    ].map((s) => (
                      <div key={s.label} className="rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-4 text-center">
                        <p className="text-[10px] font-semibold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-1">{s.label}</p>
                        <p className={`text-xl font-bold tabular-nums ${s.color}`}>{s.value}</p>
                      </div>
                    ))}
                  </div>
                )}

                {/* Applied dedup result for this group */}
                {isDeduped && dedupResults[selectedGroup] && !dedupPreviewStats && (
                  <div className="rounded-xl border border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/20 p-4">
                    <p className="text-xs font-bold text-emerald-700 dark:text-emerald-400 mb-2">Deduplication Applied</p>
                    <div className="grid grid-cols-3 gap-4 text-center text-xs">
                      <div>
                        <p className="text-[10px] text-neutral-500 uppercase">Before</p>
                        <p className="font-bold text-neutral-700 dark:text-neutral-200">{dedupResults[selectedGroup].rows_before.toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-[10px] text-neutral-500 uppercase">After</p>
                        <p className="font-bold text-emerald-600">{dedupResults[selectedGroup].rows_after.toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-[10px] text-neutral-500 uppercase">Removed</p>
                        <p className="font-bold text-red-600">{dedupResults[selectedGroup].duplicates_removed.toLocaleString()} ({dedupResults[selectedGroup].decrease_pct}%)</p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Data preview with checkbox headers */}
                {currentPreview && columns.length > 0 && (
                  <div>
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-3">
                      Select Deduplication Key Columns
                    </p>
                    <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-[400px]">
                      <table className="min-w-full text-xs">
                        <thead className="sticky top-0 z-10">
                          {/* Checkbox row */}
                          <tr className="bg-neutral-100/80 dark:bg-neutral-800/80">
                            {columns.map((col: string) => {
                              const isSelected = dedupColumns.includes(col);
                              return (
                                <td key={col} className="px-3 py-2 text-center border-b border-neutral-200 dark:border-neutral-700">
                                  <button
                                    type="button"
                                    onClick={() => toggleDedupColumn(col)}
                                    className="mx-auto block"
                                    title={isSelected ? `Remove "${col}" from dedup key` : `Add "${col}" to dedup key`}
                                  >
                                    <span className={`inline-flex items-center justify-center w-5 h-5 rounded border-2 transition-colors cursor-pointer ${
                                      isSelected
                                        ? "bg-red-500 border-red-500 text-white"
                                        : "border-neutral-300 dark:border-neutral-600 hover:border-red-400 hover:bg-red-50 dark:hover:bg-red-950/20"
                                    }`}>
                                      {isSelected && <Check className="w-3 h-3" />}
                                    </span>
                                  </button>
                                </td>
                              );
                            })}
                          </tr>
                          {/* Column headers */}
                          <tr className="bg-neutral-50 dark:bg-neutral-800">
                            {columns.map((col: string) => {
                              const isSelected = dedupColumns.includes(col);
                              return (
                                <th
                                  key={col}
                                  className={`px-3 py-2 text-left font-bold whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700 transition-colors ${
                                    isSelected ? "text-red-700 dark:text-red-400 bg-red-50/50 dark:bg-red-950/20" : "text-neutral-500 dark:text-neutral-400"
                                  }`}
                                >
                                  {col}
                                </th>
                              );
                            })}
                          </tr>
                        </thead>
                        <tbody>
                          {currentPreview.rows.map((row: any, ri: number) => (
                            <tr key={ri} className="hover:bg-red-50/30 dark:hover:bg-red-950/10">
                              {columns.map((col: string) => (
                                <td key={col} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate border-b border-neutral-100 dark:border-neutral-800">
                                  {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                                </td>
                              ))}
                            </tr>
                          ))}
                          {currentPreview.rows.length === 0 && (
                            <tr>
                              <td colSpan={columns.length} className="px-4 py-8 text-center text-neutral-400 text-sm">
                                No preview data available
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
                {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to configure deduplication."}
              </div>
            )}
            </>
          )}

          {/* ── Column Standardization tab ── */}
          {innerTab === "colstd" && (
            <>
            {selectedGroup && columns.length > 0 ? (
              <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm flex items-center gap-2">
                      <Hash className="w-4 h-4 text-red-500" />
                      {gn(selectedGroup)}
                    </h3>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                      {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                      {isStandardized && <span className="text-emerald-600 dark:text-emerald-400 font-medium ml-2">Standardized</span>}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <SecondaryButton
                      onClick={handleStdAnalyze}
                      disabled={stdSelectedCols.length === 0 || stdAnalyzeLoading}
                      className="text-xs px-3 py-2"
                    >
                      {stdAnalyzeLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <BarChart3 className="w-3 h-3" />}
                      Analyze
                    </SecondaryButton>
                    <PrimaryButton
                      onClick={handleStdApply}
                      disabled={stdAnalysisResults.length === 0 || stdApplyLoading || Object.values(stdActions).every((a) => a.operation === "none")}
                      className="text-xs px-4 py-2"
                    >
                      {stdApplyLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                      Apply Standardization
                    </PrimaryButton>
                  </div>
                </div>

                {/* Instructions */}
                <div className="px-4 py-3 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-xl text-xs text-blue-700 dark:text-blue-300">
                  Select columns that may have leading zeros or inconsistent numeric formatting.
                  Click <strong>Analyze</strong> to auto-detect patterns, then choose <strong>Strip</strong> (remove leading zeros)
                  or <strong>Pad</strong> (add leading zeros to a fixed length) per column.
                </div>

                {/* Analysis results */}
                {stdAnalysisResults.length > 0 && (
                  <div className="space-y-3">
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider">
                      Analysis Results
                    </p>
                    {stdAnalysisResults.map((r) => {
                      const action = stdActions[r.column] || { column: r.column, operation: "none" as const, pad_length: r.max_len };
                      return (
                        <div key={r.column} className="border border-neutral-200 dark:border-neutral-700 rounded-xl p-4 space-y-3">
                          <div className="flex items-start justify-between gap-4">
                            <div className="flex-1 min-w-0">
                              <p className="text-sm font-bold text-neutral-900 dark:text-white">{r.column}</p>
                              <p className="text-[11px] text-neutral-500 dark:text-neutral-400 mt-1">{r.reason}</p>
                            </div>
                            <div className="flex items-center gap-3 shrink-0">
                              <select
                                value={action.operation}
                                onChange={(e) => setStdActions((prev) => ({
                                  ...prev,
                                  [r.column]: { ...action, operation: e.target.value as StdAction["operation"] },
                                }))}
                                className="text-xs border border-neutral-200 dark:border-neutral-700 rounded-lg px-3 py-1.5 bg-white dark:bg-neutral-900 font-semibold focus:outline-none focus:ring-2 focus:ring-red-500"
                              >
                                <option value="none">No Change</option>
                                <option value="strip">Strip Leading Zeros</option>
                                <option value="pad">Pad to Fixed Length</option>
                              </select>
                              {action.operation === "pad" && (
                                <input
                                  type="number"
                                  min={1}
                                  max={50}
                                  value={action.pad_length}
                                  onChange={(e) => setStdActions((prev) => ({
                                    ...prev,
                                    [r.column]: { ...action, pad_length: Math.max(1, parseInt(e.target.value) || 1) },
                                  }))}
                                  className="w-16 text-xs border border-neutral-200 dark:border-neutral-700 rounded-lg px-2 py-1.5 bg-white dark:bg-neutral-900 text-center font-mono focus:outline-none focus:ring-2 focus:ring-red-500"
                                  title="Target character length"
                                />
                              )}
                            </div>
                          </div>
                          <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-[10px]">
                            <div className="rounded-lg bg-neutral-50 dark:bg-neutral-800 px-2 py-1.5 text-center">
                              <p className="text-neutral-400 uppercase font-bold">Values</p>
                              <p className="font-bold text-neutral-700 dark:text-neutral-200">{r.total_values.toLocaleString()}</p>
                            </div>
                            <div className="rounded-lg bg-neutral-50 dark:bg-neutral-800 px-2 py-1.5 text-center">
                              <p className="text-neutral-400 uppercase font-bold">Leading 0s</p>
                              <p className={`font-bold ${r.has_leading_zeros ? "text-amber-600" : "text-neutral-500"}`}>{r.pct_leading_zeros}%</p>
                            </div>
                            <div className="rounded-lg bg-neutral-50 dark:bg-neutral-800 px-2 py-1.5 text-center">
                              <p className="text-neutral-400 uppercase font-bold">Numeric</p>
                              <p className={`font-bold ${r.all_numeric ? "text-emerald-600" : "text-neutral-500"}`}>{r.pct_numeric}%</p>
                            </div>
                            <div className="rounded-lg bg-neutral-50 dark:bg-neutral-800 px-2 py-1.5 text-center">
                              <p className="text-neutral-400 uppercase font-bold">Min Len</p>
                              <p className="font-bold text-neutral-700 dark:text-neutral-200">{r.min_len}</p>
                            </div>
                            <div className="rounded-lg bg-neutral-50 dark:bg-neutral-800 px-2 py-1.5 text-center">
                              <p className="text-neutral-400 uppercase font-bold">Max Len</p>
                              <p className="font-bold text-neutral-700 dark:text-neutral-200">{r.max_len}</p>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}

                {/* Column selection with checkboxes */}
                {currentPreview && columns.length > 0 && (
                  <div>
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-3">
                      Select Columns to Analyze
                    </p>
                    <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-[400px]">
                      <table className="min-w-full text-xs">
                        <thead className="sticky top-0 z-10">
                          <tr className="bg-neutral-100/80 dark:bg-neutral-800/80">
                            {columns.map((col: string) => {
                              const isSelected = stdSelectedCols.includes(col);
                              return (
                                <td key={col} className="px-3 py-2 text-center border-b border-neutral-200 dark:border-neutral-700">
                                  <button
                                    type="button"
                                    onClick={() => toggleStdColumn(col)}
                                    className="mx-auto block"
                                    title={isSelected ? `Remove "${col}"` : `Add "${col}" to analysis`}
                                  >
                                    <span className={`inline-flex items-center justify-center w-5 h-5 rounded border-2 transition-colors cursor-pointer ${
                                      isSelected
                                        ? "bg-red-500 border-red-500 text-white"
                                        : "border-neutral-300 dark:border-neutral-600 hover:border-red-400 hover:bg-red-50 dark:hover:bg-red-950/20"
                                    }`}>
                                      {isSelected && <Check className="w-3 h-3" />}
                                    </span>
                                  </button>
                                </td>
                              );
                            })}
                          </tr>
                          <tr className="bg-neutral-50 dark:bg-neutral-800">
                            {columns.map((col: string) => {
                              const isSelected = stdSelectedCols.includes(col);
                              return (
                                <th
                                  key={col}
                                  className={`px-3 py-2 text-left font-bold whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700 transition-colors ${
                                    isSelected ? "text-red-700 dark:text-red-400 bg-red-50/50 dark:bg-red-950/20" : "text-neutral-500 dark:text-neutral-400"
                                  }`}
                                >
                                  {col}
                                </th>
                              );
                            })}
                          </tr>
                        </thead>
                        <tbody>
                          {currentPreview.rows.map((row: any, ri: number) => (
                            <tr key={ri} className="hover:bg-red-50/30 dark:hover:bg-red-950/10">
                              {columns.map((col: string) => (
                                <td key={col} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate border-b border-neutral-100 dark:border-neutral-800">
                                  {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                                </td>
                              ))}
                            </tr>
                          ))}
                          {currentPreview.rows.length === 0 && (
                            <tr>
                              <td colSpan={columns.length} className="px-4 py-8 text-center text-neutral-400 text-sm">
                                No preview data available
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
                {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to configure column standardization."}
              </div>
            )}
            </>
          )}

          {/* ── Concatenation tab ── */}
          {innerTab === "concat" && (
            <>
            {selectedGroup && columns.length > 0 ? (
              <div className="p-6 space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm flex items-center gap-2">
                      <Copy className="w-4 h-4 text-red-500" />
                      {gn(selectedGroup)}
                    </h3>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                      {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                      {hasConcats && (
                        <span className="text-emerald-600 dark:text-emerald-400 font-medium ml-2">
                          {concatConfigs[selectedGroup].length} concat column(s)
                        </span>
                      )}
                    </p>
                  </div>
                  <PrimaryButton
                    onClick={handleConcatApply}
                    disabled={concatSelectedCols.length < 2 || concatApplyLoading}
                  >
                    {concatApplyLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                    Apply Concatenation
                  </PrimaryButton>
                </div>

                {/* Instructions */}
                <div className="px-4 py-3 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-xl text-xs text-blue-700 dark:text-blue-300">
                  Select <strong>2 or more columns</strong> to concatenate. Values are joined directly (no separator).
                  Null/empty values are skipped. The new column will be named <strong>Concat_col1_col2_...</strong>
                </div>

                {/* Formula preview */}
                {concatSelectedCols.length >= 2 && (
                  <div className="px-4 py-3 bg-neutral-50 dark:bg-neutral-800 rounded-xl text-xs font-mono text-neutral-700 dark:text-neutral-300">
                    New column: <strong className="text-red-600 dark:text-red-400">Concat_{concatSelectedCols.join("_")}</strong>
                  </div>
                )}
                {concatSelectedCols.length === 1 && (
                  <div className="px-4 py-3 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-xl text-xs text-amber-700 dark:text-amber-300">
                    Select at least one more column to concatenate.
                  </div>
                )}

                {/* Created concat columns */}
                {concatConfigs[selectedGroup]?.length > 0 && (
                  <div className="space-y-2">
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider">
                      Created Concat Columns
                    </p>
                    {concatConfigs[selectedGroup].map((cc) => (
                      <div key={cc.column_name} className="flex items-center justify-between border border-neutral-200 dark:border-neutral-700 rounded-xl px-4 py-3">
                        <div>
                          <p className="text-sm font-bold text-neutral-900 dark:text-white">{cc.column_name}</p>
                          <p className="text-[11px] text-neutral-500 dark:text-neutral-400">
                            From: {cc.source_columns.join(" + ")}
                          </p>
                        </div>
                        <SecondaryButton
                          onClick={() => handleDeleteConcat(cc.column_name)}
                          disabled={concatDeleteLoading === cc.column_name}
                        >
                          {concatDeleteLoading === cc.column_name
                            ? <Loader2 className="w-3 h-3 animate-spin" />
                            : "Delete"}
                        </SecondaryButton>
                      </div>
                    ))}
                  </div>
                )}

                {/* Column selection table */}
                {currentPreview && columns.length > 0 && (
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider">
                        Select Columns to Concatenate
                      </p>
                      {concatSelectedCols.length > 0 && (
                        <button
                          type="button"
                          onClick={() => setConcatSelectedCols([])}
                          className="text-[11px] text-red-500 hover:text-red-700 dark:hover:text-red-400 font-medium"
                        >
                          Clear all
                        </button>
                      )}
                    </div>
                    <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-[400px]">
                      <table className="min-w-full text-xs">
                        <thead className="sticky top-0 z-10">
                          <tr className="bg-neutral-100/80 dark:bg-neutral-800/80 backdrop-blur">
                            {columns.map((col) => {
                              const selectionIndex = concatSelectedCols.indexOf(col);
                              const isSelected = selectionIndex !== -1;
                              return (
                                <td key={col} className="px-3 py-2 text-center border-b border-neutral-200 dark:border-neutral-700">
                                  <button type="button" onClick={() => toggleConcatColumn(col)} className="mx-auto block">
                                    <span className={`inline-flex items-center justify-center w-5 h-5 rounded border-2 transition-colors cursor-pointer text-[10px] font-bold ${
                                      isSelected
                                        ? "bg-red-500 border-red-500 text-white"
                                        : "border-neutral-300 dark:border-neutral-600 hover:border-red-400"
                                    }`}>
                                      {isSelected && (selectionIndex + 1)}
                                    </span>
                                  </button>
                                </td>
                              );
                            })}
                          </tr>
                          <tr className="bg-neutral-50 dark:bg-neutral-800">
                            {columns.map((col) => (
                              <th key={col} className={`px-3 py-2 text-left font-bold whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700 ${
                                concatSelectedCols.includes(col) ? "text-red-700 dark:text-red-400" : "text-neutral-500 dark:text-neutral-400"
                              }`}>
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {currentPreview.rows.length > 0 ? (
                            currentPreview.rows.map((row: any, ri: number) => (
                              <tr key={ri} className="hover:bg-red-50/30 dark:hover:bg-red-950/10">
                                {columns.map((col) => (
                                  <td key={col} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate border-b border-neutral-100 dark:border-neutral-800">
                                    {row[col] != null ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                                  </td>
                                ))}
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td colSpan={columns.length} className="px-3 py-6 text-center text-neutral-400">
                                No preview data available
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
                {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to configure concatenation."}
              </div>
            )}
            </>
          )}

          {/* ── Column Removal tab ── */}
          {innerTab === "colrem" && (
            <>
            {selectedGroup && columns.length > 0 ? (
              <div className="p-6 space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm flex items-center gap-2">
                      <Trash2 className="w-4 h-4 text-red-500" />
                      {gn(selectedGroup)}
                    </h3>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                      {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                      {hasRemovedCols && (
                        <span className="text-orange-600 dark:text-orange-400 font-medium ml-2">
                          {removedColumns[selectedGroup].length} column(s) removed
                        </span>
                      )}
                    </p>
                  </div>
                  <PrimaryButton
                    onClick={handleColRemApply}
                    disabled={colRemSelected.length === 0 || colRemApplyLoading}
                    className="text-xs px-4 py-2"
                  >
                    {colRemApplyLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                    Remove Columns ({colRemSelected.length})
                  </PrimaryButton>
                </div>

                {/* Instructions */}
                <div className="px-4 py-3 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-xl text-xs text-blue-700 dark:text-blue-300">
                  Select columns to remove using the checkboxes above column headers. Removed columns can be restored later from the <strong>Removed Columns</strong> section below.
                </div>

                {/* Selection summary */}
                {colRemSelected.length > 0 && (
                  <div className="px-4 py-2.5 bg-orange-50 dark:bg-orange-950/30 border border-orange-200 dark:border-orange-800 rounded-xl text-[11px] text-orange-700 dark:text-orange-400 flex items-center justify-between">
                    <span>
                      Selected for removal: <span className="font-bold">{colRemSelected.join(", ")}</span>
                    </span>
                    <button
                      type="button"
                      onClick={() => setColRemSelected([])}
                      className="text-[10px] font-medium text-red-500 hover:text-red-700 transition-colors ml-3 shrink-0"
                    >
                      Clear all
                    </button>
                  </div>
                )}

                {/* Removed columns (restore section) */}
                {removedColumns[selectedGroup]?.length > 0 && (
                  <div className="space-y-2">
                    <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider">
                      Removed Columns
                    </p>
                    {removedColumns[selectedGroup].map((colName) => (
                      <div key={colName} className="flex items-center justify-between border border-orange-200 dark:border-orange-700 rounded-xl px-4 py-3 bg-orange-50/50 dark:bg-orange-950/10">
                        <div>
                          <p className="text-sm font-bold text-neutral-900 dark:text-white">{colName}</p>
                          <p className="text-[11px] text-neutral-500 dark:text-neutral-400">Removed — click Restore to bring it back</p>
                        </div>
                        <SecondaryButton
                          onClick={() => handleColRemRestore(colName)}
                          disabled={colRemRestoreLoading === colName}
                          className="text-xs"
                        >
                          {colRemRestoreLoading === colName
                            ? <Loader2 className="w-3 h-3 animate-spin" />
                            : <RotateCcw className="w-3 h-3" />}
                          Restore
                        </SecondaryButton>
                      </div>
                    ))}
                  </div>
                )}

                {/* Column selection table */}
                {currentPreview && columns.length > 0 && (
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider">
                        Select Columns to Remove
                      </p>
                      {colRemSelected.length > 0 && (
                        <button
                          type="button"
                          onClick={() => setColRemSelected([])}
                          className="text-[11px] text-red-500 hover:text-red-700 dark:hover:text-red-400 font-medium"
                        >
                          Clear all
                        </button>
                      )}
                    </div>
                    <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-[400px]">
                      <table className="min-w-full text-xs">
                        <thead className="sticky top-0 z-10">
                          <tr className="bg-neutral-100/80 dark:bg-neutral-800/80 backdrop-blur">
                            {columns.map((col) => {
                              const isSelected = colRemSelected.includes(col);
                              return (
                                <td key={col} className="px-3 py-2 text-center border-b border-neutral-200 dark:border-neutral-700">
                                  <button type="button" onClick={() => toggleColRemColumn(col)} className="mx-auto block">
                                    <span className={`inline-flex items-center justify-center w-5 h-5 rounded border-2 transition-colors cursor-pointer ${
                                      isSelected
                                        ? "bg-red-500 border-red-500 text-white"
                                        : "border-neutral-300 dark:border-neutral-600 hover:border-red-400"
                                    }`}>
                                      {isSelected && <Check className="w-3 h-3" />}
                                    </span>
                                  </button>
                                </td>
                              );
                            })}
                          </tr>
                          <tr className="bg-neutral-50 dark:bg-neutral-800">
                            {columns.map((col) => (
                              <th key={col} className={`px-3 py-2 text-left font-bold whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700 ${
                                colRemSelected.includes(col) ? "text-red-700 dark:text-red-400 bg-red-50/50 dark:bg-red-950/20" : "text-neutral-500 dark:text-neutral-400"
                              }`}>
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {currentPreview.rows.length > 0 ? (
                            currentPreview.rows.map((row: any, ri: number) => (
                              <tr key={ri} className="hover:bg-red-50/30 dark:hover:bg-red-950/10">
                                {columns.map((col) => (
                                  <td key={col} className={`px-3 py-1.5 whitespace-nowrap max-w-[200px] truncate border-b border-neutral-100 dark:border-neutral-800 ${
                                    colRemSelected.includes(col) ? "text-red-400 dark:text-red-600 line-through" : "text-neutral-700 dark:text-neutral-300"
                                  }`}>
                                    {row[col] != null ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                                  </td>
                                ))}
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td colSpan={columns.length} className="px-3 py-6 text-center text-neutral-400">
                                No preview data available
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
                {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to remove columns."}
              </div>
            )}
            </>
          )}

            </div>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="p-6 border-t border-neutral-100 dark:border-neutral-800 flex justify-between items-center">
        <div className="text-xs text-neutral-500 flex items-center gap-3">
          {Object.keys(cleaningConfigs).length > 0 && (
            <span className="rounded-full border border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/30 px-3 py-1 text-xs font-medium text-emerald-700 dark:text-emerald-400">
              {Object.keys(cleaningConfigs).length} group{Object.keys(cleaningConfigs).length !== 1 ? "s" : ""} cleaned
            </span>
          )}
          {Object.keys(dedupResults).length > 0 && (
            <span className="rounded-full border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-950/30 px-3 py-1 text-xs font-medium text-blue-700 dark:text-blue-400">
              {Object.keys(dedupResults).length} group{Object.keys(dedupResults).length !== 1 ? "s" : ""} deduplicated
            </span>
          )}
          {Object.values(concatConfigs).flat().length > 0 && (
            <span className="rounded-full border border-purple-200 dark:border-purple-800 bg-purple-50 dark:bg-purple-950/30 px-3 py-1 text-xs font-medium text-purple-700 dark:text-purple-400">
              {Object.values(concatConfigs).flat().length} concat column{Object.values(concatConfigs).flat().length !== 1 ? "s" : ""} created
            </span>
          )}
          {Object.values(removedColumns).flat().length > 0 && (
            <span className="rounded-full border border-orange-200 dark:border-orange-800 bg-orange-50 dark:bg-orange-950/30 px-3 py-1 text-xs font-medium text-orange-700 dark:text-orange-400">
              {Object.values(removedColumns).flat().length} column{Object.values(removedColumns).flat().length !== 1 ? "s" : ""} removed
            </span>
          )}
          {Object.keys(cleaningConfigs).length === 0 && Object.keys(dedupResults).length === 0 && Object.values(concatConfigs).flat().length === 0 && Object.values(removedColumns).flat().length === 0 && (
            <span>No cleaning or dedup applied yet. You can skip this step.</span>
          )}
        </div>
        <div className="flex gap-3">
          <SecondaryButton onClick={onSkip}>
            Skip
          </SecondaryButton>
          <PrimaryButton onClick={onProceed} disabled={loading}>
            Proceed to Merge
            <ArrowRight className="w-4 h-4" />
          </PrimaryButton>
        </div>
      </div>
    </motion.section>
  );
}
