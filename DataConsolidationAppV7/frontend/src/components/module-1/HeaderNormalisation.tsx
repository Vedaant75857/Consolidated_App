import React, { useCallback, useEffect, useMemo, useState, useRef } from "react";
import ReactDOM from "react-dom";
import { ArrowRight, Columns3, Loader2, SkipForward, Maximize2, Minimize2, Download, Upload, CheckSquare, Square, ChevronDown, ChevronRight } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { PrimaryButton, SecondaryButton, SurfaceCard } from "../common/ui";

interface HeaderNormalisationProps {
  sessionId: string;
  apiKey: string;
  loading: boolean;
  decisions: any[] | null;
  standardFields: any[];
  groupSchema: any[];
  groupPreviewData: Record<string, { columns: string[]; rows: any[]; total_rows: number }>;
  groupNameMap?: Record<string, string>;
  onRun: () => void;
  onApply: (decisions: Record<string, any[]>) => void;
  onSkip: () => void;
  onFetchGroupPreview: (groupIds: string[]) => void;
}

type Action = "AUTO" | "REVIEW" | "DROP" | "KEEP";

interface ColDecision {
  source_col: string;
  suggested_std_field: string | null;
  confidence: number;
  reason: string;
  action: Action;
  top_alternatives: string[];
  mapped_to?: string | null;
}

function normalizeAction(action: unknown): Action {
  const a = String(action || "REVIEW").toUpperCase();
  if (a === "AUTO" || a === "REVIEW" || a === "DROP" || a === "KEEP") return a;
  return "REVIEW";
}

const ACTION_COLORS: Record<Action, string> = {
  AUTO: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 border-emerald-200 dark:border-emerald-800",
  REVIEW: "bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 border-amber-200 dark:border-amber-800",
  KEEP: "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 border-blue-200 dark:border-blue-800",
  DROP: "bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300 border-red-200 dark:border-red-800",
};

/* ─── Fullscreen Modal (portal-based, edge-to-edge like Merge) ─── */
function FullscreenModal({ title, children, onClose }: { title?: string; children: React.ReactNode; onClose: () => void }) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  return ReactDOM.createPortal(
    <div className="fixed inset-0 z-[9999] bg-white dark:bg-neutral-900 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-neutral-200 dark:border-neutral-700 shrink-0">
        <h3 className="text-lg font-bold text-neutral-900 dark:text-white">
          {title || "Fullscreen View"}
        </h3>
        <button
          onClick={onClose}
          className="flex items-center gap-2 px-4 py-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 hover:bg-neutral-200 dark:hover:bg-neutral-700 text-sm font-medium transition-colors"
        >
          <Minimize2 className="w-4 h-4" /> Close
        </button>
      </div>
      <div className="flex-1 overflow-auto p-6">{children}</div>
    </div>,
    document.body
  );
}

/* ─── Excel-like Table with Normalization Rows ─── */
function NormTable({
  columns,
  rows,
  decisions,
  stdFieldNames,
  onUpdateDecision,
  totalRows,
}: {
  columns: string[];
  rows: any[];
  decisions: ColDecision[];
  stdFieldNames: string[];
  onUpdateDecision: (sourceCol: string, patch: Partial<ColDecision>) => void;
  totalRows: number;
}) {
  const decisionMap = useMemo(() => {
    const m: Record<string, ColDecision> = {};
    for (const d of decisions) m[d.source_col] = d;
    return m;
  }, [decisions]);

  return (
    <div className="border border-neutral-300 dark:border-neutral-700 rounded-lg overflow-hidden">
      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="min-w-full border-collapse text-[11px] font-mono">
          {/* Row 1: Action dropdowns */}
          <thead className="sticky top-0 z-20">
            <tr className="bg-emerald-50 dark:bg-emerald-950/30">
              <th className="sticky left-0 z-30 bg-emerald-50 dark:bg-emerald-950/30 px-2 py-1.5 text-[9px] font-bold text-emerald-600 dark:text-emerald-400 uppercase tracking-wider border-b border-r border-neutral-300 dark:border-neutral-700 whitespace-nowrap min-w-[40px]">
                Action
              </th>
              {columns.map((col) => {
                const d = decisionMap[col];
                return (
                  <th key={`action-${col}`} className="px-1 py-1 border-b border-r border-neutral-300 dark:border-neutral-700 min-w-[140px]">
                    <select
                      value={d?.action || "KEEP"}
                      onChange={(e) => {
                        const action = normalizeAction(e.target.value);
                        onUpdateDecision(col, {
                          action,
                          mapped_to: d?.mapped_to || d?.suggested_std_field || null,
                        });
                      }}
                      className={`w-full px-1.5 py-1 rounded text-[10px] font-bold border cursor-pointer ${ACTION_COLORS[d?.action || "KEEP"]}`}
                    >
                      <option value="AUTO">AUTO</option>
                      <option value="REVIEW">REVIEW</option>
                      <option value="KEEP">KEEP</option>
                      <option value="DROP">DROP</option>
                    </select>
                  </th>
                );
              })}
            </tr>
            {/* Row 2: Standard header mapping dropdowns */}
            <tr className="bg-orange-50 dark:bg-orange-950/20">
              <th className="sticky left-0 z-30 bg-orange-50 dark:bg-orange-950/20 px-2 py-1.5 text-[9px] font-bold text-orange-600 dark:text-orange-400 uppercase tracking-wider border-b border-r border-neutral-300 dark:border-neutral-700 whitespace-nowrap">
                Mapped To
              </th>
              {columns.map((col) => {
                const d = decisionMap[col];
                return (
                  <th key={`target-${col}`} className="px-1 py-1 border-b border-r border-neutral-300 dark:border-neutral-700">
                    <select
                      value={d?.mapped_to || ""}
                      onChange={(e) => onUpdateDecision(col, { mapped_to: e.target.value || null })}
                      className="w-full px-1.5 py-1 rounded text-[10px] font-semibold border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900"
                    >
                      <option value="">-- Unmapped --</option>
                      {d?.top_alternatives && d.top_alternatives.length > 0 && (
                        <optgroup label="Suggested">
                          {d.top_alternatives.map((f) => (
                            <option key={f} value={f}>{f}</option>
                          ))}
                        </optgroup>
                      )}
                      <optgroup label="All Standard Fields">
                        {stdFieldNames.filter((f) => !(d?.top_alternatives || []).includes(f)).map((f) => (
                          <option key={f} value={f}>{f}</option>
                        ))}
                      </optgroup>
                    </select>
                  </th>
                );
              })}
            </tr>
            {/* Row 3: Original column headers */}
            <tr className="bg-neutral-700 dark:bg-neutral-800">
              <th className="sticky left-0 z-30 bg-neutral-700 dark:bg-neutral-800 px-2 py-2 text-[10px] font-bold text-white uppercase tracking-wider border-b border-r border-neutral-600 dark:border-neutral-700 whitespace-nowrap">
                #
              </th>
              {columns.map((col) => (
                <th key={`header-${col}`} className="px-2 py-2 text-left text-[10px] font-bold text-white tracking-wide border-b border-r border-neutral-600 dark:border-neutral-700 whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          {/* Data rows */}
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri} className={ri % 2 === 0 ? "bg-white dark:bg-neutral-950" : "bg-neutral-50 dark:bg-neutral-900"}>
                <td className="sticky left-0 z-10 px-2 py-1.5 text-[10px] text-neutral-400 dark:text-neutral-500 font-bold text-right border-r border-neutral-200 dark:border-neutral-800 bg-inherit whitespace-nowrap">
                  {ri + 1}
                </td>
                {columns.map((col) => {
                  const val = row[col];
                  return (
                    <td
                      key={col}
                      className="px-2 py-1.5 text-neutral-700 dark:text-neutral-300 border-r border-neutral-100 dark:border-neutral-800 whitespace-nowrap max-w-[200px] truncate"
                      title={val != null ? String(val) : ""}
                    >
                      {val != null ? String(val) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                    </td>
                  );
                })}
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={columns.length + 1} className="px-4 py-8 text-center text-sm text-neutral-400">
                  No preview data available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="px-3 py-1.5 bg-neutral-100 dark:bg-neutral-800 border-t border-neutral-200 dark:border-neutral-700 text-[10px] text-neutral-500 dark:text-neutral-400">
        Showing {rows.length} of {totalRows.toLocaleString()} rows &middot; {columns.length} columns
      </div>
    </div>
  );
}

/* ─── Group Panel ─── */
function GroupPanel({
  groupId,
  groupName,
  columns,
  rows,
  totalRows,
  decisions,
  stdFieldNames,
  onUpdateDecision,
  sessionId,
  loading,
}: {
  groupId: string;
  groupName: string;
  columns: string[];
  rows: any[];
  totalRows: number;
  decisions: ColDecision[];
  stdFieldNames: string[];
  onUpdateDecision: (groupKey: string, sourceCol: string, patch: Partial<ColDecision>) => void;
  sessionId: string;
  loading: boolean;
}) {
  const [expanded, setExpanded] = useState(true);
  const [fullscreen, setFullscreen] = useState(false);
  const [downloading, setDownloading] = useState(false);

  const handleUpdate = useCallback(
    (sourceCol: string, patch: Partial<ColDecision>) => onUpdateDecision(groupId, sourceCol, patch),
    [groupId, onUpdateDecision],
  );

  const handleDownloadExcel = async () => {
    setDownloading(true);
    try {
      const res = await fetch("/api/header-norm-download-excel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId,
          groupId,
          decisions: decisions.map((d) => ({
            source_col: d.source_col,
            action: d.action,
            mapped_to: d.mapped_to || d.suggested_std_field || "",
            suggested_std_field: d.suggested_std_field,
          })),
        }),
      });
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${groupId}_header_norm.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Excel download error:", err);
    } finally {
      setDownloading(false);
    }
  };

  const autoCount = decisions.filter((d) => d.action === "AUTO").length;
  const reviewCount = decisions.filter((d) => d.action === "REVIEW").length;
  const keepCount = decisions.filter((d) => d.action === "KEEP").length;
  const dropCount = decisions.filter((d) => d.action === "DROP").length;

  const tableContent = (
    <NormTable
      columns={columns}
      rows={rows}
      decisions={decisions}
      stdFieldNames={stdFieldNames}
      onUpdateDecision={handleUpdate}
      totalRows={totalRows}
    />
  );

  return (
    <>
      <div className="border border-neutral-200 dark:border-neutral-700 rounded-2xl bg-white dark:bg-neutral-900 shadow-sm overflow-hidden">
        {/* Panel header */}
        <button
          type="button"
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between px-5 py-4 bg-neutral-50 dark:bg-neutral-800 hover:bg-neutral-100 dark:hover:bg-neutral-700/50 transition-colors text-left"
        >
          <div className="flex items-center gap-3">
            {expanded ? <ChevronDown className="w-4 h-4 text-neutral-400" /> : <ChevronRight className="w-4 h-4 text-neutral-400" />}
            <Columns3 className="w-4 h-4 text-red-500" />
            <span className="font-bold text-sm text-neutral-900 dark:text-white">{groupName || groupId}</span>
            <span className="text-[10px] font-bold bg-neutral-200 dark:bg-neutral-700 text-neutral-600 dark:text-neutral-300 px-2 py-0.5 rounded-full">
              {totalRows.toLocaleString()} rows
            </span>
            <span className="text-[10px] font-bold bg-neutral-200 dark:bg-neutral-700 text-neutral-600 dark:text-neutral-300 px-2 py-0.5 rounded-full">
              {columns.length} cols
            </span>
          </div>
          <div className="flex items-center gap-2">
            {autoCount > 0 && <span className="text-[9px] font-bold bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 px-1.5 py-0.5 rounded">{autoCount} AUTO</span>}
            {reviewCount > 0 && <span className="text-[9px] font-bold bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 px-1.5 py-0.5 rounded">{reviewCount} REVIEW</span>}
            {keepCount > 0 && <span className="text-[9px] font-bold bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 px-1.5 py-0.5 rounded">{keepCount} KEEP</span>}
            {dropCount > 0 && <span className="text-[9px] font-bold bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300 px-1.5 py-0.5 rounded">{dropCount} DROP</span>}
          </div>
        </button>

        {/* Panel body */}
        <AnimatePresence>
          {expanded && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="overflow-hidden"
            >
              <div className="p-4 space-y-3">
                {/* Toolbar */}
                <div className="flex items-center gap-2 flex-wrap">
                  <button
                    type="button"
                    onClick={() => setFullscreen(true)}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-300 hover:border-red-300 dark:hover:border-red-800 hover:text-red-600 transition-all"
                  >
                    <Maximize2 className="w-3 h-3" />
                    Expand Fullscreen
                  </button>
                  <button
                    type="button"
                    onClick={handleDownloadExcel}
                    disabled={downloading || loading}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-300 hover:border-emerald-300 dark:hover:border-emerald-800 hover:text-emerald-600 transition-all disabled:opacity-50"
                  >
                    {downloading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                    Download Excel
                  </button>
                </div>

                {tableContent}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Fullscreen overlay */}
      {fullscreen && (
        <FullscreenModal
          title={`${groupName || groupId} — ${totalRows.toLocaleString()} rows · ${columns.length} columns`}
          onClose={() => setFullscreen(false)}
        >
          {tableContent}
        </FullscreenModal>
      )}
    </>
  );
}

/* ─── Main Component ─── */
export default function HeaderNormalisation({
  sessionId,
  apiKey,
  loading,
  decisions,
  standardFields,
  groupSchema,
  groupPreviewData,
  groupNameMap = {},
  onRun,
  onApply,
  onSkip,
  onFetchGroupPreview,
}: HeaderNormalisationProps) {
  const [edited, setEdited] = useState<Record<string, ColDecision[]>>({});
  const [manualExcelMode, setManualExcelMode] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const next: Record<string, ColDecision[]> = {};
    for (const tbl of decisions || []) {
      next[tbl.tableKey] = (tbl.decisions || []).map((d: any) => ({
        source_col: String(d.source_col || ""),
        suggested_std_field: d.suggested_std_field || null,
        confidence: Number(d.confidence || 0),
        reason: String(d.reason || ""),
        action: normalizeAction(d.action),
        top_alternatives: Array.isArray(d.top_alternatives) ? d.top_alternatives.map((x: any) => String(x)) : [],
        mapped_to: d.suggested_std_field || null,
      }));
    }
    setEdited(next);
  }, [decisions]);

  useEffect(() => {
    if (decisions && decisions.length > 0 && groupSchema.length > 0) {
      const groupIds = groupSchema.map((g: any) => g.group_id);
      const missing = groupIds.filter((gid: string) => !groupPreviewData[gid]);
      if (missing.length > 0) {
        onFetchGroupPreview(missing);
      }
    }
  }, [decisions, groupSchema, groupPreviewData, onFetchGroupPreview]);

  const tableKeys = useMemo(() => Object.keys(edited), [edited]);

  const stdFieldNames = useMemo(
    () => (standardFields || []).map((f: any) => String(f?.name || "")).filter(Boolean),
    [standardFields],
  );

  const updateRow = useCallback((groupKey: string, sourceCol: string, patch: Partial<ColDecision>) => {
    setEdited((prev) => {
      const arr = [...(prev[groupKey] || [])];
      const idx = arr.findIndex((x) => x.source_col === sourceCol);
      if (idx < 0) return prev;
      arr[idx] = { ...arr[idx], ...patch };
      return { ...prev, [groupKey]: arr };
    });
  }, []);

  const applyChanges = () => {
    const payload: Record<string, any[]> = {};
    for (const [tableKey, arr] of Object.entries(edited)) {
      payload[tableKey] = arr.map((d) => ({
        ...d,
        action: normalizeAction(d.action),
        suggested_std_field: d.mapped_to || d.suggested_std_field || null,
      }));
    }
    onApply(payload);
  };

  const handleUploadExcel = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const formData = new FormData();
      formData.append("sessionId", sessionId);
      formData.append("file", file);
      const res = await fetch("/api/header-norm-upload-excel", { method: "POST", body: formData });
      if (!res.ok) throw new Error("Upload failed");
      const data = await res.json();
      const uploadedDecisions = data.decisions || {};
      setEdited((prev) => {
        const next = { ...prev };
        for (const [groupKey, colDecs] of Object.entries(uploadedDecisions) as [string, any[]][]) {
          if (next[groupKey]) {
            const existing = [...next[groupKey]];
            for (const ud of colDecs) {
              const idx = existing.findIndex((x) => x.source_col === ud.source_col);
              if (idx >= 0) {
                existing[idx] = {
                  ...existing[idx],
                  action: normalizeAction(ud.action),
                  mapped_to: ud.mapped_to || null,
                  suggested_std_field: ud.suggested_std_field || existing[idx].suggested_std_field,
                  confidence: ud.confidence ?? existing[idx].confidence,
                  reason: ud.reason || existing[idx].reason,
                };
              }
            }
            next[groupKey] = existing;
          } else {
            next[groupKey] = colDecs.map((d: any) => ({
              source_col: String(d.source_col || ""),
              suggested_std_field: d.suggested_std_field || null,
              confidence: Number(d.confidence || 0),
              reason: String(d.reason || ""),
              action: normalizeAction(d.action),
              top_alternatives: [],
              mapped_to: d.mapped_to || null,
            }));
          }
        }
        return next;
      });
    } catch (err) {
      console.error("Excel upload error:", err);
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  if (!apiKey?.trim()) {
    return (
      <SurfaceCard className="p-6">
        <p className="text-sm text-neutral-600 dark:text-neutral-300">
          Enter an API key to run AI header normalization.
        </p>
      </SurfaceCard>
    );
  }

  return (
    <motion.section
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-3xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-hidden"
    >
      <div className="p-6 border-b border-neutral-100 dark:border-neutral-800">
        <h2 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
          <Columns3 className="w-5 h-5 text-red-600" />
          Header Normalisation
        </h2>
        <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1">
          Map source columns to the standard schema. Each group shows a data preview with inline mapping controls.
        </p>
      </div>

      <div className="p-6 space-y-5">
        {tableKeys.length === 0 ? (
          <div className="space-y-3">
            <p className="text-sm text-neutral-600 dark:text-neutral-300">
              Generate AI mapping suggestions for all groups/tables.
            </p>
            <PrimaryButton onClick={onRun} disabled={loading}>
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Run Header Normalisation
              <ArrowRight className="w-4 h-4" />
            </PrimaryButton>
          </div>
        ) : (
          <>
            {/* Manual Excel workflow toggle */}
            <div className="flex items-center gap-4 flex-wrap p-3 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50">
              <button
                type="button"
                onClick={() => setManualExcelMode(!manualExcelMode)}
                className="inline-flex items-center gap-2 text-xs font-semibold text-neutral-600 dark:text-neutral-300 hover:text-red-600 dark:hover:text-red-400 transition-colors"
              >
                {manualExcelMode
                  ? <CheckSquare className="w-4 h-4 text-red-500" />
                  : <Square className="w-4 h-4 text-neutral-400" />
                }
                Manual Excel Workflow
              </button>
              {manualExcelMode && (
                <div className="flex items-center gap-3 text-xs text-neutral-500 dark:text-neutral-400">
                  <span>Download group Excel files, edit mappings in rows 1-2, then re-upload:</span>
                  <label className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 text-neutral-600 dark:text-neutral-300 hover:border-red-300 dark:hover:border-red-800 hover:text-red-600 cursor-pointer transition-all font-semibold">
                    {uploading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Upload className="w-3 h-3" />}
                    Upload Excel
                    <input
                      ref={fileInputRef}
                      type="file"
                      accept=".xlsx,.xls"
                      onChange={handleUploadExcel}
                      className="hidden"
                    />
                  </label>
                </div>
              )}
            </div>

            {/* Per-group panels */}
            <div className="space-y-4">
              {tableKeys.map((groupKey) => {
                const preview = groupPreviewData[groupKey];
                const schema = groupSchema.find((g: any) => g.group_id === groupKey);
                const groupDecisions = edited[groupKey] || [];
                const cols = preview?.columns || groupDecisions.map((d) => d.source_col);
                const rows = preview?.rows || [];
                const total = preview?.total_rows ?? schema?.rows ?? 0;

                return (
                  <GroupPanel
                    key={groupKey}
                    groupId={groupKey}
                    groupName={groupNameMap[groupKey] || schema?.group_id || groupKey}
                    columns={cols}
                    rows={rows}
                    totalRows={total}
                    decisions={groupDecisions}
                    stdFieldNames={stdFieldNames}
                    onUpdateDecision={updateRow}
                    sessionId={sessionId}
                    loading={loading}
                  />
                );
              })}
            </div>
          </>
        )}

        {/* Footer buttons */}
        <div className="flex flex-wrap gap-2 pt-2">
          <PrimaryButton onClick={applyChanges} disabled={loading || tableKeys.length === 0}>
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
            Apply & Proceed to Data Cleaning
            <ArrowRight className="w-4 h-4" />
          </PrimaryButton>
          <SecondaryButton onClick={onRun} disabled={loading}>
            Re-Run AI
          </SecondaryButton>
          <SecondaryButton onClick={onSkip} disabled={loading}>
            <SkipForward className="w-4 h-4" />
            Skip to Data Cleaning
          </SecondaryButton>
        </div>
      </div>
    </motion.section>
  );
}
