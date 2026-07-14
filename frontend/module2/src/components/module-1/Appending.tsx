import React, { useState, useRef, useEffect } from "react";
import { Loader2, ChevronRight, ChevronDown, LayoutGrid, ListChecks, ArrowRight, Info, Plus, MoreHorizontal, Undo2, Ban, X, Eye, EyeOff, MessageSquare, Database } from "lucide-react";
import { motion } from "motion/react";
import AppendReport from "./AppendReport";
import { PrimaryButton, EmptyState } from "../common/ui";

interface AppendingProps {
  step: number;
  appendGroups: any[];
  unassigned: any[];
  excludedTables: string[];
  appendGroupMappings: any[];
  setAppendGroupMappings: (mappings: any[]) => void;
  previews: Record<string, { columns: string[]; rows: any[] }>;
  loading: boolean;
  handleGenerateAppendPlan: () => void;
  handleGenerateAppendMapping: () => void;
  handleExecuteAppend: () => void;
  moveTableToGroup: (tableKey: string, targetGroupId: string | null) => void;
  createNewGroup: (tableKeys: string[]) => void;
  excludeTable: (tableKey: string) => void;
  restoreTable: (tableKey: string) => void;
  appendReport: any[] | null;
  handleProceedToHeaderNorm: () => void;
  onSelectChatItem?: (item: { type: string; id: string; label: string }) => void;
}

/* ─── Table Action Menu ─── */
function TableActionMenu({
  tableKey,
  currentGroupId,
  appendGroups,
  moveTableToGroup,
  excludeTable,
}: {
  tableKey: string;
  currentGroupId: string | null;
  appendGroups: any[];
  moveTableToGroup: (tableKey: string, targetGroupId: string | null) => void;
  excludeTable: (tableKey: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const otherGroups = appendGroups.filter(g => g.group_id !== currentGroupId);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        className="p-1 rounded-md hover:bg-neutral-200 dark:hover:bg-neutral-700 text-neutral-400 dark:text-neutral-500 hover:text-neutral-700 dark:hover:text-neutral-300 transition-colors"
      >
        <MoreHorizontal className="w-3.5 h-3.5" />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 w-52 bg-white dark:bg-neutral-900 rounded-xl border border-neutral-200 dark:border-neutral-700 shadow-xl py-1 text-xs">
          {currentGroupId !== null && (
            <button
              onClick={() => { moveTableToGroup(tableKey, null); setOpen(false); }}
              className="w-full text-left px-3 py-2 hover:bg-neutral-50 dark:hover:bg-neutral-800 text-neutral-700 dark:text-neutral-300 flex items-center gap-2"
            >
              <Undo2 className="w-3 h-3 text-neutral-400" /> Move to Unassigned
            </button>
          )}
          {otherGroups.map(g => (
            <button
              key={g.group_id}
              onClick={() => { moveTableToGroup(tableKey, g.group_id); setOpen(false); }}
              className="w-full text-left px-3 py-2 hover:bg-neutral-50 dark:hover:bg-neutral-800 text-neutral-700 dark:text-neutral-300 flex items-center gap-2"
            >
              <ArrowRight className="w-3 h-3 text-red-400" /> Move to {g.group_name || g.group_id}
            </button>
          ))}
          <div className="border-t border-neutral-100 dark:border-neutral-800 my-1" />
          <button
            onClick={() => { if (window.confirm(`Exclude "${tableKey}" from processing?`)) { excludeTable(tableKey); setOpen(false); } }}
            className="w-full text-left px-3 py-2 hover:bg-red-50 dark:hover:bg-red-950/30 text-red-600 dark:text-red-400 flex items-center gap-2"
          >
            <Ban className="w-3 h-3" /> Exclude from processing
          </button>
        </div>
      )}
    </div>
  );
}

/* ─── Main Component ─── */
export default function Appending({
  step,
  appendGroups,
  unassigned,
  excludedTables,
  appendGroupMappings,
  setAppendGroupMappings,
  previews,
  loading,
  handleGenerateAppendPlan,
  handleGenerateAppendMapping,
  handleExecuteAppend,
  moveTableToGroup,
  createNewGroup,
  excludeTable,
  restoreTable,
  appendReport,
  handleProceedToHeaderNorm,
  onSelectChatItem,
}: AppendingProps) {
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [showExcluded, setShowExcluded] = useState(false);
  const [previewTable, setPreviewTable] = useState<string | null>(null);

  const toggleTableSelection = (tableKey: string) => {
    setSelectedTables(prev =>
      prev.includes(tableKey) ? prev.filter(t => t !== tableKey) : [...prev, tableKey]
    );
  };

  const handleCreateGroup = () => {
    if (selectedTables.length > 0) {
      createNewGroup(selectedTables);
      setSelectedTables([]);
    }
  };

  return (
    <div className="space-y-6">
      {step === 3 && (
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="rounded-3xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-visible"
        >
          {/* Header */}
          <div className="p-6 border-b border-neutral-100 dark:border-neutral-800">
            <div className="flex justify-between items-start">
              <div>
                <h2 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
                  <LayoutGrid className="w-5 h-5 text-red-600" />
                  Append Strategy
                </h2>
                <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1">AI has grouped your files. Adjust groups, exclude tables, or create new groups below.</p>
              </div>
              <div className="flex gap-2 shrink-0">
                {selectedTables.length > 0 && (
                  <button
                    onClick={handleCreateGroup}
                    className="inline-flex items-center px-4 py-2 text-xs font-bold rounded-lg shadow-sm text-white bg-red-600 hover:bg-red-700 transition-all gap-2"
                  >
                    <Plus className="w-3.5 h-3.5" />
                    Create Group ({selectedTables.length})
                  </button>
                )}
              </div>
            </div>
            {selectedTables.length > 0 && (
              <div className="mt-3 flex items-center gap-2 text-xs text-red-600 dark:text-red-400 font-medium bg-red-50 dark:bg-red-950/30 px-3 py-2 rounded-lg">
                <Info className="w-3.5 h-3.5" />
                {selectedTables.length} table{selectedTables.length !== 1 ? "s" : ""} selected &mdash; click &quot;Create Group&quot; to form a new group, or click tables again to deselect.
              </div>
            )}
          </div>

          <div className="p-6 space-y-6">
            {appendGroups.length === 0 && unassigned.length === 0 && excludedTables.length === 0 ? (
              <EmptyState
                icon={LayoutGrid}
                title="No Append Plan Yet"
                description="Let AI analyze your tables and suggest how to group them for appending."
                action={
                  <PrimaryButton onClick={handleGenerateAppendPlan} disabled={loading}>
                    {loading ? <Loader2 className="animate-spin w-4 h-4" /> : null}
                    Generate Append Plan
                    <ArrowRight className="w-4 h-4" />
                  </PrimaryButton>
                }
              />
            ) : (
              <>
                {/* Vertical group cards */}
                <div className="space-y-4">
                  {appendGroups.map((g) => (
                    <div
                      key={g.group_id}
                      className="border border-neutral-200 dark:border-neutral-700 rounded-2xl bg-white dark:bg-neutral-900"
                    >
                      {/* Card header */}
                      <div className="flex items-center justify-between px-5 py-3 border-b border-neutral-100 dark:border-neutral-800 bg-neutral-50/50 dark:bg-neutral-800/50 rounded-t-2xl">
                        <div className="flex items-center gap-2 min-w-0">
                          <Database className="w-4 h-4 text-red-500 shrink-0" />
                          <h3 className="font-bold text-sm text-neutral-900 dark:text-white truncate">
                            {g.group_name || g.group_id}
                          </h3>
                          <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-neutral-200 dark:bg-neutral-700 text-neutral-500 dark:text-neutral-400 shrink-0">
                            {g.tables?.length} {g.tables?.length === 1 ? "file" : "files"}
                          </span>
                        </div>
                        <div className="flex items-center gap-1 shrink-0">
                          {onSelectChatItem && (
                            <button
                              type="button"
                              onClick={() => onSelectChatItem({ type: "group", id: g.group_id, label: g.group_name || g.group_id })}
                              className="p-1.5 rounded-md hover:bg-red-50 dark:hover:bg-red-950/30 text-neutral-400 hover:text-red-600 transition-colors"
                              title="Ask AI about this group"
                            >
                              <MessageSquare className="w-3.5 h-3.5" />
                            </button>
                          )}
                        </div>
                      </div>

                      {/* Card body: files left, summary right */}
                      <div className="flex min-h-[120px]">
                        {/* Left: files */}
                        <div className="flex-1 min-w-0 p-4 space-y-1.5">
                          {g.tables?.map((t: string) => {
                            const isShowingPreview = previewTable === t;
                            const preview = previews[t];
                            return (
                              <div key={t} className="border border-neutral-100 dark:border-neutral-800 rounded-lg">
                                <div className="flex items-center gap-1.5 px-3 py-2">
                                  <span
                                    onClick={() => toggleTableSelection(t)}
                                    className={`flex-1 text-xs font-medium cursor-pointer truncate ${
                                      selectedTables.includes(t) ? "text-red-600 font-bold" : "text-neutral-700 dark:text-neutral-300"
                                    }`}
                                  >
                                    {t}
                                  </span>
                                  <button
                                    type="button"
                                    onClick={() => setPreviewTable(isShowingPreview ? null : t)}
                                    className="p-0.5 rounded hover:bg-neutral-200 dark:hover:bg-neutral-700 text-neutral-400 transition-colors"
                                    title="Preview table"
                                  >
                                    {isShowingPreview ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => { if (window.confirm(`Exclude "${t}" from processing?`)) excludeTable(t); }}
                                    className="p-0.5 rounded hover:bg-red-50 text-neutral-400 hover:text-red-600 transition-colors"
                                    title="Exclude"
                                  >
                                    <X className="w-3 h-3" />
                                  </button>
                                  <TableActionMenu
                                    tableKey={t}
                                    currentGroupId={g.group_id}
                                    appendGroups={appendGroups}
                                    moveTableToGroup={moveTableToGroup}
                                    excludeTable={excludeTable}
                                  />
                                </div>
                                {isShowingPreview && preview && preview.columns.length > 0 && (
                                  <div className="overflow-x-auto border-t border-neutral-100 dark:border-neutral-800 max-h-48">
                                    <table className="min-w-full text-[10px]">
                                      <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0">
                                        <tr>
                                          {preview.columns.map(col => (
                                            <th key={col} className="px-2 py-1.5 text-left font-bold text-neutral-500 whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700">{col}</th>
                                          ))}
                                        </tr>
                                      </thead>
                                      <tbody className="divide-y divide-neutral-50 dark:divide-neutral-800">
                                        {preview.rows.slice(0, 5).map((row, ri) => (
                                          <tr key={ri}>
                                            {preview.columns.map(col => (
                                              <td key={col} className="px-2 py-1 whitespace-nowrap text-neutral-600 dark:text-neutral-300 max-w-[150px] truncate">
                                                {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600">-</span>}
                                              </td>
                                            ))}
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                )}
                              </div>
                            );
                          })}
                        </div>

                        {/* Right: AI summary */}
                        <div className="w-[340px] shrink-0 border-l border-neutral-100 dark:border-neutral-800 p-4 flex items-start">
                          {g.reason ? (
                            <p className="text-xs text-neutral-600 dark:text-neutral-300 leading-relaxed italic border-l-2 border-red-300 dark:border-red-700 pl-3">
                              {g.reason}
                            </p>
                          ) : (
                            <p className="text-xs text-neutral-400 dark:text-neutral-500 italic">No summary available.</p>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>

                {/* Unassigned tables */}
                {unassigned.length > 0 && (
                  <div className="border border-dashed border-neutral-200 dark:border-neutral-700 rounded-xl p-4 bg-neutral-50/30 dark:bg-neutral-800/30">
                    <h4 className="text-[10px] font-bold text-neutral-400 uppercase tracking-widest mb-2 flex items-center gap-1">
                      <Info className="w-3 h-3" /> Unassigned ({unassigned.length})
                    </h4>
                    <div className="flex flex-wrap gap-2">
                      {unassigned.map((u: any) => (
                        <span
                          key={u.table_key}
                          onClick={() => toggleTableSelection(u.table_key)}
                          className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border cursor-pointer transition-all ${
                            selectedTables.includes(u.table_key) ? "bg-red-600 border-red-600 text-white" : "bg-white dark:bg-neutral-900 border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 hover:border-red-400"
                          }`}
                        >
                          {u.table_key}
                          <TableActionMenu tableKey={u.table_key} currentGroupId={null} appendGroups={appendGroups} moveTableToGroup={moveTableToGroup} excludeTable={excludeTable} />
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* Excluded tables */}
                {excludedTables.length > 0 && (
                  <div className="border border-dashed border-red-200 rounded-xl bg-red-50/20 dark:bg-red-950/30 overflow-hidden">
                    <button type="button" onClick={() => setShowExcluded(!showExcluded)} className="w-full flex items-center justify-between px-4 py-3 text-left">
                      <span className="text-[10px] font-bold text-red-400 uppercase tracking-widest flex items-center gap-1">
                        <Ban className="w-3 h-3" /> Excluded ({excludedTables.length})
                      </span>
                      {showExcluded ? <ChevronDown className="w-3 h-3 text-red-400" /> : <ChevronRight className="w-3 h-3 text-red-400" />}
                    </button>
                    {showExcluded && (
                      <div className="px-4 pb-3 flex flex-wrap gap-2">
                        {excludedTables.map((t) => (
                          <span key={t} className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium bg-white dark:bg-neutral-900 border border-red-200 text-neutral-500 dark:text-neutral-400">
                            <span className="line-through">{t}</span>
                            <button onClick={() => restoreTable(t)} className="text-red-500 hover:text-red-700 transition-colors" title="Restore"><Undo2 className="w-3 h-3" /></button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* Create new group card */}
                <button
                  type="button"
                  onClick={() => { if (selectedTables.length > 0) handleCreateGroup(); }}
                  className={`w-full border-2 border-dashed rounded-2xl p-4 flex items-center justify-center gap-3 transition-all ${
                    selectedTables.length > 0
                      ? "border-red-300 bg-red-50/30 dark:bg-red-950/30 text-red-600 dark:text-red-400 hover:bg-red-50 cursor-pointer"
                      : "border-neutral-200 dark:border-neutral-700 bg-neutral-50/20 dark:bg-neutral-800/30 text-neutral-400 cursor-default"
                  }`}
                >
                  <Plus className="w-5 h-5" />
                  <span className="text-sm font-bold">
                    {selectedTables.length > 0 ? `Create Group (${selectedTables.length} selected)` : "Select tables to create a group"}
                  </span>
                </button>
              </>
            )}

            {/* Header alignment mappings */}
            {appendGroupMappings.length > 0 && (
              <div className="space-y-10 border-t border-neutral-100 pt-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <ListChecks className="w-5 h-5 text-red-600" />
                    <h3 className="text-lg font-semibold text-neutral-900 dark:text-white">Header Alignment</h3>
                  </div>
                  <p className="text-sm text-neutral-500 dark:text-neutral-400">AI-aligned column mappings (editable)</p>
                </div>
                {appendGroupMappings.map((gm, gmIdx) => {
                  const sourceColumnsMap: Record<string, string[]> = {};
                  for (const pt of gm.per_table || []) {
                    const preview = previews[pt.table_key];
                    sourceColumnsMap[pt.table_key] = preview?.columns || Object.keys(pt.column_mapping || {}).map(k => pt.column_mapping[k]).filter(Boolean);
                  }
                  const updateMapping = (tableKey: string, canonicalCol: string, newSourceCol: string | null) => {
                    const updated = appendGroupMappings.map((g, idx) => {
                      if (idx !== gmIdx) return g;
                      return {
                        ...g,
                        per_table: g.per_table.map((pt: any) => {
                          if (pt.table_key !== tableKey) return pt;
                          const newMapping = { ...pt.column_mapping };
                          if (newSourceCol) { newMapping[canonicalCol] = newSourceCol; } else { delete newMapping[canonicalCol]; }
                          return { ...pt, column_mapping: newMapping };
                        })
                      };
                    });
                    setAppendGroupMappings(updated);
                  };
                  const groupName = appendGroups.find(g => g.group_id === gm.group_id)?.group_name || gm.group_id;
                  return (
                    <div key={gmIdx} className="space-y-4">
                      <div className="flex justify-between items-center">
                        <h3 className="font-bold text-neutral-900 dark:text-white">{groupName}</h3>
                        <p className="text-[10px] font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-widest">Schema Mapping (editable)</p>
                      </div>
                      <div className="overflow-x-auto border border-neutral-100 dark:border-neutral-800 rounded-2xl shadow-sm">
                        <table className="min-w-full divide-y divide-neutral-200 dark:divide-neutral-700 text-sm">
                          <thead className="bg-neutral-50 dark:bg-neutral-800">
                            <tr>
                              <th className="sticky left-0 z-10 bg-neutral-50 dark:bg-neutral-800 px-6 py-3 text-left text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider min-w-[220px]">Table</th>
                              {gm.canonical_schema?.map((col: string) => (
                                <th key={col} className="px-4 py-3 text-left text-xs font-bold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider border-l border-neutral-100 dark:border-neutral-800 whitespace-nowrap">
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800 bg-white dark:bg-neutral-900">
                            {gm.per_table?.map((pt: any) => {
                              const sourceCols = sourceColumnsMap[pt.table_key] || [];
                              return (
                                <tr key={pt.table_key} className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/50 transition-colors">
                                  <td className="sticky left-0 z-10 bg-white dark:bg-neutral-900 px-6 py-3 font-bold text-neutral-900 dark:text-white text-xs whitespace-nowrap min-w-[220px] border-r border-neutral-100 dark:border-neutral-800">
                                    {pt.table_key}
                                  </td>
                                  {gm.canonical_schema?.map((col: string) => {
                                    const currentValue = pt.column_mapping?.[col] || "";
                                    return (
                                      <td key={col} className="px-3 py-2 border-l border-neutral-100 dark:border-neutral-800">
                                        <select
                                          value={currentValue}
                                          onChange={(e) => updateMapping(pt.table_key, col, e.target.value || null)}
                                          className="w-full min-w-[130px] text-xs border border-neutral-200 dark:border-neutral-700 rounded-lg px-2 py-1.5 bg-white dark:bg-neutral-900 focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-transparent transition-shadow appearance-none"
                                        >
                                          <option value="">-- unmapped --</option>
                                          {sourceCols.map((sc) => (
                                            <option key={sc} value={sc}>{sc}</option>
                                          ))}
                                        </select>
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
                })}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="relative z-10 p-6 bg-neutral-50 dark:bg-neutral-800 border-t border-neutral-100 dark:border-neutral-800 flex flex-col sm:flex-row justify-between items-stretch sm:items-center gap-4">
            <div className="flex flex-col gap-1">
              {appendGroupMappings.length === 0 ? (
                <>
                  <p className="text-xs text-neutral-400 dark:text-neutral-500 italic">
                    Click table names to select, use X or &quot;...&quot; to exclude, then proceed.
                  </p>
                  {appendGroups.length === 0 && (unassigned.length > 0 || excludedTables.length > 0) && (
                    <p className="text-xs text-amber-600 dark:text-amber-400 font-medium">
                      No groups yet — create groups from the table tags above, or proceed to stack unassigned tables only.
                    </p>
                  )}
                </>
              ) : (
                <button
                  type="button"
                  onClick={handleGenerateAppendMapping}
                  disabled={loading}
                  className="text-sm font-medium text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors text-left disabled:opacity-50"
                >
                  Re-analyze Mappings
                </button>
              )}
            </div>
            {appendGroupMappings.length === 0 ? (
              <PrimaryButton onClick={handleGenerateAppendMapping} disabled={loading}>
                {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Analyze Header Alignment"}
                <ArrowRight className="w-4 h-4" />
              </PrimaryButton>
            ) : (
              <PrimaryButton onClick={handleExecuteAppend} disabled={loading}>
                {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Execute Append & Stack"}
                <ArrowRight className="w-4 h-4" />
              </PrimaryButton>
            )}
          </div>
        </motion.section>
      )}

      {step === 3 && appendReport && appendReport.length > 0 && (
        <AppendReport appendReport={appendReport} onProceed={handleProceedToHeaderNorm} />
      )}
    </div>
  );
}
