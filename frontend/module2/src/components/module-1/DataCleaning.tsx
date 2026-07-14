import React, { useState, useEffect, useCallback } from "react";
import { Loader2, Sparkles, ArrowRight, Check, ChevronDown, ChevronRight, Copy } from "lucide-react";
import { motion } from "motion/react";
import { PrimaryButton, SecondaryButton } from "../common/ui";

interface CleaningConfig {
  removeNullRows: boolean;
  removeNullColumns: boolean;
  dropColumns: string[];
  caseMode: "upper" | "lower" | "none";
  trimWhitespace: boolean;
  columnTypes: Record<string, "string" | "number" | "date">;
  deduplicateColumns: string[];
}

const DEFAULT_CONFIG: CleaningConfig = {
  removeNullRows: true,
  removeNullColumns: true,
  dropColumns: [],
  caseMode: "upper",
  trimWhitespace: true,
  columnTypes: {},
  deduplicateColumns: [],
};

interface DataCleaningProps {
  step: number;
  groupSchema: any[];
  groupNameMap?: Record<string, string>;
  sessionId: string;
  cleaningConfigs: Record<string, any>;
  loading: boolean;
  onCleanGroup: (groupId: string, config: CleaningConfig) => Promise<void>;
  onProceed: () => void;
  onSkip: () => void;
}

export default function DataCleaning({
  step,
  groupSchema,
  groupNameMap = {},
  sessionId,
  cleaningConfigs,
  loading,
  onCleanGroup,
  onProceed,
  onSkip,
}: DataCleaningProps) {
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null);
  const [localConfig, setLocalConfig] = useState<CleaningConfig>(DEFAULT_CONFIG);
  const [expandedPreview, setExpandedPreview] = useState(true);
  const [optionalExpanded, setOptionalExpanded] = useState(true);
  const [dedupeDropdownOpen, setDedupeDropdownOpen] = useState(false);
  const [groupPreviews, setGroupPreviews] = useState<Record<string, { columns: string[]; rows: any[] }>>({});
  const [dtypeMap, setDtypeMap] = useState<Record<string, string>>({});

  useEffect(() => {
    fetch("/api/standard-field-dtypes")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setDtypeMap)
      .catch((err) => console.warn("[DataCleaning] Failed to load dtype defaults:", err));
  }, []);

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
    } catch { /* ignore preview fetch errors */ }
  }, [sessionId, groupPreviews]);

  useEffect(() => {
    if (selectedGroup) {
      fetchGroupPreview(selectedGroup);
    }
  }, [selectedGroup, fetchGroupPreview]);

  useEffect(() => {
    if (selectedGroup && cleaningConfigs[selectedGroup]) {
      setLocalConfig({ ...DEFAULT_CONFIG, ...cleaningConfigs[selectedGroup] });
    } else {
      const schema = groupSchema.find((g) => g.group_id === selectedGroup);
      const cols: string[] = schema?.columns || [];
      const defaultTypes: Record<string, "string" | "number" | "date"> = {};
      for (const col of cols) {
        defaultTypes[col] = (dtypeMap[col] as "string" | "number" | "date") || "string";
      }
      setLocalConfig({ ...DEFAULT_CONFIG, columnTypes: defaultTypes, deduplicateColumns: [] });
    }
  }, [selectedGroup, cleaningConfigs, dtypeMap, groupSchema]);

  const currentSchema = selectedGroup ? groupSchema.find((g) => g.group_id === selectedGroup) : null;
  const currentPreview = selectedGroup ? groupPreviews[selectedGroup] : null;
  const columns = currentPreview?.columns || currentSchema?.columns || [];
  const visibleColumns = columns.filter((c: string) => !localConfig.dropColumns.includes(c));
  const isCleaned = selectedGroup ? !!cleaningConfigs[selectedGroup] : false;

  const toggleDropColumn = (col: string) => {
    setLocalConfig((prev) => ({
      ...prev,
      dropColumns: prev.dropColumns.includes(col)
        ? prev.dropColumns.filter((c) => c !== col)
        : [...prev.dropColumns, col],
    }));
  };

  const setColumnType = (col: string, type: "string" | "number" | "date") => {
    setLocalConfig((prev) => ({
      ...prev,
      columnTypes: { ...prev.columnTypes, [col]: type },
    }));
  };

  const toggleDeduplicateColumn = (col: string) => {
    setLocalConfig((prev) => ({
      ...prev,
      deduplicateColumns: prev.deduplicateColumns.includes(col)
        ? prev.deduplicateColumns.filter((c) => c !== col)
        : [...prev.deduplicateColumns, col],
    }));
  };

  const handleApply = async () => {
    if (!selectedGroup) return;
    await onCleanGroup(selectedGroup, localConfig);
    setGroupPreviews((prev) => { const n = { ...prev }; delete n[selectedGroup]; return n; });
    fetchGroupPreview(selectedGroup);
  };

  if (step !== 5) return null;

  const gn = (id: string) => groupNameMap[id] || id;

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-3xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-hidden"
    >
      <div className="p-6 border-b border-neutral-100 dark:border-neutral-800">
        <h2 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
          <Sparkles className="w-5 h-5 text-red-600" />
          Data Cleaning
        </h2>
        <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1">
          Clean your appended group tables. Select a group, configure cleaning options, and apply.
        </p>
      </div>

      <div className="flex min-h-[500px]">
        {/* Group selector sidebar */}
        <div className="w-64 border-r border-neutral-100 dark:border-neutral-800 bg-neutral-50/30 dark:bg-neutral-800 overflow-y-auto shrink-0">
          <div className="p-3">
            <p className="text-[10px] font-bold uppercase tracking-wider text-neutral-400 dark:text-neutral-500 px-2 mb-2">
              Groups ({groupSchema.length})
            </p>
            {groupSchema.map((gs) => {
              const isSelected = selectedGroup === gs.group_id;
              const isGroupCleaned = !!cleaningConfigs[gs.group_id];
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
                  {isGroupCleaned && (
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

        {/* Main config area */}
        <div className="flex-1 overflow-y-auto">
          {selectedGroup && columns.length > 0 ? (
            <div className="p-6 space-y-6">
              {/* Group info */}
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="font-semibold tracking-tight text-neutral-900 dark:text-white text-sm">{gn(selectedGroup)}</h3>
                  <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">
                    {currentSchema?.rows?.toLocaleString()} rows, {columns.length} columns
                    {isCleaned && <span className="text-emerald-600 dark:text-emerald-400 font-medium ml-2">Cleaned</span>}
                  </p>
                </div>
                <PrimaryButton onClick={handleApply} disabled={loading} className="text-xs px-4 py-2">
                  {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
                  Apply Cleaning
                </PrimaryButton>
              </div>

              {/* Best Practices */}
              <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl overflow-hidden">
                <div className="px-4 py-3 bg-emerald-50/60 dark:bg-emerald-950/20 border-b border-neutral-200 dark:border-neutral-700">
                  <p className="text-xs font-bold text-emerald-700 dark:text-emerald-400">Best Practices</p>
                  <p className="text-[10px] text-emerald-600/70 dark:text-emerald-500/70">These steps are applied by default</p>
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

              {/* Optional */}
              <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl overflow-hidden">
                <button
                  type="button"
                  onClick={() => setOptionalExpanded((v) => !v)}
                  className="w-full px-4 py-3 bg-neutral-50/60 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700 flex items-center justify-between cursor-pointer hover:bg-neutral-100/60 dark:hover:bg-neutral-700/60 transition-colors"
                >
                  <div className="text-left">
                    <p className="text-xs font-bold text-neutral-600 dark:text-neutral-300">Optional</p>
                    <p className="text-[10px] text-neutral-400 dark:text-neutral-500">Additional cleaning options</p>
                  </div>
                  {optionalExpanded ? (
                    <ChevronDown className="w-4 h-4 text-neutral-400 dark:text-neutral-500 shrink-0" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-neutral-400 dark:text-neutral-500 shrink-0" />
                  )}
                </button>

                {optionalExpanded && (
                  <div className="p-4 space-y-6">
                    {/* Remove Duplicates */}
                    <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl">
                      <div className="px-4 py-3 bg-neutral-50/50 dark:bg-neutral-800 border-b border-neutral-100 dark:border-neutral-800 rounded-t-xl flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Copy className="w-4 h-4 text-neutral-500 dark:text-neutral-400" />
                          <div>
                            <p className="text-xs font-bold text-neutral-900 dark:text-white">Remove Duplicates</p>
                            <p className="text-[10px] text-neutral-400 dark:text-neutral-500">
                              Select columns that define uniqueness — only the first occurrence of each combination is kept
                            </p>
                          </div>
                        </div>
                        {localConfig.deduplicateColumns.length > 0 && (
                          <button
                            type="button"
                            onClick={() => setLocalConfig((p) => ({ ...p, deduplicateColumns: [] }))}
                            className="text-[10px] font-medium text-red-500 hover:text-red-700 transition-colors"
                          >
                            Clear all
                          </button>
                        )}
                      </div>
                      <div className="p-4">
                        {localConfig.deduplicateColumns.length > 0 && (
                          <div className="mb-3 px-3 py-2 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-lg text-[11px] text-amber-700 dark:text-amber-400">
                            Uniqueness key: <span className="font-bold">{localConfig.deduplicateColumns.join(" + ")}</span>
                            {" "}&mdash; rows with the same combination of these values will be deduplicated (first row kept).
                          </div>
                        )}
                        {/* Dropdown selector */}
                        <div className="relative">
                          <button
                            type="button"
                            onClick={() => setDedupeDropdownOpen((o) => !o)}
                            className="w-full flex items-center justify-between gap-2 px-3 py-2.5 border border-neutral-200 dark:border-neutral-700 rounded-lg bg-white dark:bg-neutral-900 text-xs transition-all hover:border-neutral-300 dark:hover:border-neutral-600 focus:outline-none focus:ring-2 focus:ring-neutral-400 dark:focus:ring-neutral-500"
                          >
                            <span className={localConfig.deduplicateColumns.length > 0 ? "text-neutral-900 dark:text-white font-medium" : "text-neutral-400 dark:text-neutral-500"}>
                              {localConfig.deduplicateColumns.length > 0
                                ? `${localConfig.deduplicateColumns.length} column${localConfig.deduplicateColumns.length !== 1 ? "s" : ""} selected`
                                : "Select columns for deduplication…"}
                            </span>
                            <ChevronDown className={`w-3.5 h-3.5 text-neutral-400 transition-transform ${dedupeDropdownOpen ? "rotate-180" : ""}`} />
                          </button>

                          {dedupeDropdownOpen && (
                            <>
                              <div className="fixed inset-0 z-10" onClick={() => setDedupeDropdownOpen(false)} />
                              <div className="absolute z-20 mt-1 w-full max-h-80 overflow-y-auto border border-neutral-200 dark:border-neutral-700 rounded-lg bg-white dark:bg-neutral-900 shadow-lg py-1">
                                {visibleColumns.map((col: string) => {
                                  const isSelected = localConfig.deduplicateColumns.includes(col);
                                  return (
                                    <button
                                      key={col}
                                      type="button"
                                      onClick={() => toggleDeduplicateColumn(col)}
                                      className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-left hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                                    >
                                      <span className={`w-3.5 h-3.5 rounded-sm border flex items-center justify-center shrink-0 transition-colors ${
                                        isSelected ? "bg-red-600 border-red-600" : "border-neutral-300 dark:border-neutral-600"
                                      }`}>
                                        {isSelected && <Check className="w-2.5 h-2.5 text-white" />}
                                      </span>
                                      <span className={isSelected ? "text-neutral-900 dark:text-white font-medium" : "text-neutral-600 dark:text-neutral-300"}>
                                        {col}
                                      </span>
                                    </button>
                                  );
                                })}
                              </div>
                            </>
                          )}
                        </div>

                        {localConfig.deduplicateColumns.length === 0 && (
                          <p className="text-[10px] text-neutral-400 dark:text-neutral-500 mt-2 italic">
                            No columns selected — deduplication is disabled.
                          </p>
                        )}
                      </div>
                    </div>

                    {/* Column controls */}
                    <div>
                      <p className="text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-3">
                        Column Settings
                        {localConfig.dropColumns.length > 0 && (
                          <span className="ml-2 text-red-500 normal-case">
                            ({localConfig.dropColumns.length} column{localConfig.dropColumns.length !== 1 ? "s" : ""} marked for removal)
                          </span>
                        )}
                      </p>
                      <div className="border border-neutral-200 dark:border-neutral-700 rounded-xl overflow-hidden">
                        <table className="min-w-full text-xs">
                          <thead className="bg-neutral-50 dark:bg-neutral-800">
                            <tr>
                              <th className="px-4 py-2.5 text-left font-bold text-neutral-500 dark:text-neutral-400 w-8">Keep</th>
                              <th className="px-4 py-2.5 text-left font-bold text-neutral-500 dark:text-neutral-400">Column Name</th>
                              <th className="px-4 py-2.5 text-left font-bold text-neutral-500 dark:text-neutral-400">Sample Values</th>
                              <th className="px-4 py-2.5 text-left font-bold text-neutral-500 dark:text-neutral-400 w-32">Data Type</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {columns.map((col: string) => {
                              const isDropped = localConfig.dropColumns.includes(col);
                              const samples = (currentPreview?.rows || [])
                                .map((r: any) => r[col])
                                .filter((v: any) => v !== null && v !== undefined && v !== "")
                                .slice(0, 3);
                              return (
                                <tr
                                  key={col}
                                  className={`transition-colors ${isDropped ? "bg-red-50/50 dark:bg-red-950/30 opacity-60" : "hover:bg-neutral-50/50 dark:hover:bg-neutral-800"}`}
                                >
                                  <td className="px-4 py-2">
                                    <input
                                      type="checkbox"
                                      checked={!isDropped}
                                      onChange={() => toggleDropColumn(col)}
                                      className="w-3.5 h-3.5 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                                    />
                                  </td>
                                  <td className={`px-4 py-2 font-bold ${isDropped ? "line-through text-neutral-400 dark:text-neutral-500" : "text-neutral-900 dark:text-white"}`}>
                                    {col}
                                  </td>
                                  <td className="px-4 py-2 text-neutral-500 dark:text-neutral-400 max-w-[200px] truncate">
                                    {samples.length > 0 ? samples.map(String).join(", ") : <span className="italic text-neutral-300 dark:text-neutral-600">empty</span>}
                                  </td>
                                  <td className="px-4 py-2">
                                    <select
                                      value={localConfig.columnTypes[col] || "string"}
                                      onChange={(e) => setColumnType(col, e.target.value as any)}
                                      disabled={isDropped}
                                      className="w-full text-xs border border-neutral-200 dark:border-neutral-700 rounded-lg px-2 py-1 bg-white dark:bg-neutral-900 focus:outline-none focus:ring-2 focus:ring-red-500 disabled:opacity-40 transition-shadow"
                                    >
                                      <option value="string">String</option>
                                      <option value="number">Number</option>
                                      <option value="date">Date</option>
                                    </select>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Data preview */}
              <div>
                <button
                  type="button"
                  onClick={() => setExpandedPreview(!expandedPreview)}
                  className="flex items-center gap-2 text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-3 hover:text-neutral-600 dark:hover:text-neutral-300 transition-colors"
                >
                  {expandedPreview ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                  Data Preview ({visibleColumns.length} columns)
                </button>
                {expandedPreview && currentPreview && visibleColumns.length > 0 && (
                  <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-xl max-h-64">
                    <table className="min-w-full text-xs">
                      <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0">
                        <tr>
                          {visibleColumns.map((col: string) => (
                            <th
                              key={col}
                              className="px-3 py-2 text-left font-bold text-neutral-500 whitespace-nowrap border-b border-neutral-200"
                            >
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-neutral-100">
                        {currentPreview.rows.map((row: any, ri: number) => (
                          <tr key={ri} className="hover:bg-red-50/30">
                            {visibleColumns.map((col: string) => (
                              <td
                                key={col}
                                className="px-3 py-1.5 whitespace-nowrap text-neutral-700 max-w-[200px] truncate"
                              >
                                {row[col] != null ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-neutral-400 text-sm">
              {groupSchema.length === 0 ? "No groups available. Complete the append step first." : "Select a group from the list to configure cleaning options."}
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="p-6 border-t border-neutral-100 flex justify-between items-center">
        <div className="text-xs text-neutral-500">
          {Object.keys(cleaningConfigs).length > 0 ? (
            <span className="rounded-full border border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/30 px-3 py-1 text-xs font-medium text-emerald-700 dark:text-emerald-400">
              {Object.keys(cleaningConfigs).length} group{Object.keys(cleaningConfigs).length !== 1 ? "s" : ""} cleaned
            </span>
          ) : (
            <span>No cleaning applied yet. You can skip this step.</span>
          )}
        </div>
        <div className="flex gap-3">
          <SecondaryButton onClick={onSkip}>
            Skip Cleaning
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
