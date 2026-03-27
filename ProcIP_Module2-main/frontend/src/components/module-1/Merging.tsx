import React, { useState, useCallback, useEffect, useRef } from "react";
import ReactDOM from "react-dom";
import { motion } from "motion/react";
import {
  ArrowRight,
  Check,
  Key,
  Link2,
  Loader2,
  Maximize2,
  Minimize2,
  RotateCcw,
  SkipForward,
  Sparkles,
  X,
  AlertTriangle,
  Download,
} from "lucide-react";
import { SurfaceCard, PrimaryButton, SecondaryButton, FillBar, itemVariants } from "../common/ui";
import type { LogEntry } from "./StatusLog";
import MergeReport from "./MergeReport";

export interface MergingProps {
  sessionId: string;
  apiKey: string;
  step: number;
  setStep: (s: number) => void;
  groupSchema: any[];
  groupNameMap: Record<string, string>;
  loading: boolean;
  setLoading: (v: boolean) => void;
  setAiLoading: (v: boolean) => void;
  setLoadingMessage: (v: string) => void;
  setError: (v: string | null) => void;
  addLog: (stepName: string, type: LogEntry["type"], message: string) => void;
  mergeBaseGroupId: string;
  setMergeBaseGroupId: (v: string) => void;
  mergeBaseRecommendation: any;
  setMergeBaseRecommendation: (v: any) => void;
  mergeSourceGroupIds: string[];
  setMergeSourceGroupIds: (v: string[]) => void;
  mergeCurrentSourceIdx: number;
  setMergeCurrentSourceIdx: (v: number) => void;
  mergeCommonColumns: any[];
  setMergeCommonColumns: (v: any[]) => void;
  mergeSelectedKeys: Array<{ base_col: string; source_col: string }>;
  setMergeSelectedKeys: React.Dispatch<React.SetStateAction<Array<{ base_col: string; source_col: string }>>>;
  mergePullColumns: string[];
  setMergePullColumns: React.Dispatch<React.SetStateAction<string[]>>;
  mergeSimulation: any;
  setMergeSimulation: (v: any) => void;
  mergeValidationReport: any;
  setMergeValidationReport: (v: any) => void;
  mergeResult: any;
  setMergeResult: (v: any) => void;
  mergeApprovedSources: any[];
  setMergeApprovedSources: (v: any[]) => void;
  mergeHistory: any[];
  setMergeHistory: (v: any[]) => void;
  onRegisterMergedGroup: (groupId: string, groupName: string, groupRow: any) => void;
}

const COLOR_MAP: Record<string, string> = {
  green: "bg-emerald-100 border-emerald-400 text-emerald-900 dark:bg-emerald-950/40 dark:border-emerald-600 dark:text-emerald-200",
  orange: "bg-amber-100 border-amber-400 text-amber-900 dark:bg-amber-950/40 dark:border-amber-600 dark:text-amber-200",
  red: "bg-red-100 border-red-400 text-red-900 dark:bg-red-950/40 dark:border-red-600 dark:text-red-200",
  grey: "bg-neutral-100 border-neutral-300 text-neutral-700 dark:bg-neutral-800/60 dark:border-neutral-600 dark:text-neutral-300",
};

const BADGE_COLOR_MAP: Record<string, string> = {
  green: "bg-emerald-500 text-white",
  orange: "bg-amber-500 text-white",
  red: "bg-red-500 text-white",
  grey: "bg-neutral-400 text-white",
};

function getStatColor(metric: string, value: number): string {
  if (metric === "match_rate") return value >= 80 ? "text-emerald-600" : value >= 50 ? "text-amber-600" : "text-red-600";
  if (metric === "row_explosion_factor") return value <= 1.1 ? "text-emerald-600" : value <= 2 ? "text-amber-600" : "text-red-600";
  if (metric === "estimated_null_rate") return value <= 20 ? "text-emerald-600" : value <= 50 ? "text-amber-600" : "text-red-600";
  return "text-neutral-700 dark:text-neutral-300";
}

export default function Merging(props: MergingProps) {
  const {
    sessionId, apiKey, step, setStep,
    groupSchema, groupNameMap, loading,
    setLoading, setAiLoading, setLoadingMessage, setError, addLog,
    mergeBaseGroupId, setMergeBaseGroupId,
    mergeBaseRecommendation, setMergeBaseRecommendation,
    mergeSourceGroupIds, setMergeSourceGroupIds,
    mergeCurrentSourceIdx, setMergeCurrentSourceIdx,
    mergeCommonColumns, setMergeCommonColumns,
    mergeSelectedKeys, setMergeSelectedKeys,
    mergePullColumns, setMergePullColumns,
    mergeSimulation, setMergeSimulation,
    mergeValidationReport, setMergeValidationReport,
    mergeResult, setMergeResult,
    mergeApprovedSources, setMergeApprovedSources,
    mergeHistory, setMergeHistory,
    onRegisterMergedGroup,
  } = props;

  const [basePreview, setBasePreview] = useState<{ columns: string[]; rows: any[] } | null>(null);
  const [sourcePreview, setSourcePreview] = useState<{ columns: string[]; rows: any[] } | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [executingMerge, setExecutingMerge] = useState(false);
  const [mergeProgress, setMergeProgress] = useState(0);
  const [mergeProgressMessage, setMergeProgressMessage] = useState("");
  const [allBaseColumns, setAllBaseColumns] = useState<string[]>([]);
  const [allSourceColumns, setAllSourceColumns] = useState<string[]>([]);
  const [baseColClasses, setBaseColClasses] = useState<Record<string, { category: string; eligibility: string; color: string }>>({});
  const [sourceColClasses, setSourceColClasses] = useState<Record<string, { category: string; eligibility: string; color: string }>>({});
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [pendingBaseCols, setPendingBaseCols] = useState<string[]>([]);
  const simDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastBasePreviewId = useRef<string>("");

  const currentSourceGroupId = mergeSourceGroupIds[mergeCurrentSourceIdx] || "";

  // Build lookup for common columns
  const commonByBase = new Map<string, any>();
  const commonBySource = new Map<string, any>();
  for (const cc of mergeCommonColumns) {
    commonByBase.set(cc.base_col, cc);
    commonBySource.set(cc.source_col, cc);
  }

  // --- Section A: Recommend Base ---

  const fetchRecommendation = useCallback(async () => {
    if (!sessionId || groupSchema.length === 0) return;
    setAiLoading(true);
    setLoadingMessage("AI is recommending the best base table...");
    try {
      const res = await fetch("/api/merge/recommend-base", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, apiKey }),
      });
      if (!res.ok) throw new Error((await res.json()).error || "Failed to recommend base");
      const data = await res.json();
      setMergeBaseRecommendation(data);
      if (data.recommended && !mergeBaseGroupId) {
        setMergeBaseGroupId(data.recommended);
        const others = groupSchema.map((g: any) => g.group_id).filter((id: string) => id !== data.recommended);
        setMergeSourceGroupIds(others);
      }
      addLog("Merge", "success", `Recommended base: ${groupNameMap[data.recommended] || data.recommended}`);
    } catch (err: any) {
      addLog("Merge", "error", err.message);
    } finally {
      setAiLoading(false);
    }
  }, [sessionId, apiKey, groupSchema, mergeBaseGroupId, groupNameMap, addLog, setAiLoading, setLoadingMessage, setMergeBaseGroupId, setMergeBaseRecommendation, setMergeSourceGroupIds]);

  useEffect(() => {
    if (step === 6 && !mergeBaseRecommendation && groupSchema.length > 0) {
      fetchRecommendation();
    }
  }, [step, mergeBaseRecommendation, groupSchema.length, fetchRecommendation]);

  const handleBaseChange = useCallback((newBaseId: string) => {
    setMergeBaseGroupId(newBaseId);
    const others = groupSchema.map((g: any) => g.group_id).filter((id: string) => id !== newBaseId);
    setMergeSourceGroupIds(others);
    setMergeCurrentSourceIdx(0);
    setMergeCommonColumns([]);
    setMergeSelectedKeys([]);
    setMergePullColumns([]);
    setMergeSimulation(null);
    setMergeValidationReport(null);
    setBasePreview(null);
    setSourcePreview(null);
    setBaseColClasses({});
    setSourceColClasses({});
    setPendingBaseCols([]);
  }, [groupSchema, setMergeBaseGroupId, setMergeSourceGroupIds, setMergeCurrentSourceIdx, setMergeCommonColumns, setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeValidationReport]);

  const handleSourceChange = useCallback((newSourceId: string) => {
    const idx = mergeSourceGroupIds.indexOf(newSourceId);
    if (idx >= 0) {
      setMergeCurrentSourceIdx(idx);
    } else {
      const allOthers = groupSchema.map((g: any) => g.group_id).filter((id: string) => id !== mergeBaseGroupId);
      const newIdx = allOthers.indexOf(newSourceId);
      setMergeSourceGroupIds(allOthers);
      setMergeCurrentSourceIdx(newIdx >= 0 ? newIdx : 0);
    }
    // Keep base preview & base classes intact — only source changed
    setMergeCommonColumns([]);
    setMergeSelectedKeys([]);
    setMergePullColumns([]);
    setMergeSimulation(null);
    setMergeValidationReport(null);
    setSourcePreview(null);
    setSourceColClasses({});
    setPendingBaseCols([]);
  }, [groupSchema, mergeBaseGroupId, mergeSourceGroupIds, setMergeCurrentSourceIdx, setMergeSourceGroupIds, setMergeCommonColumns, setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeValidationReport]);

  // --- Section B: Fetch Common Columns + Preview ---

  const fetchColumnsAndPreviews = useCallback(async () => {
    if (!sessionId || !mergeBaseGroupId || !currentSourceGroupId) return;
    setColumnsLoading(true);
    setLoadingMessage("Analyzing columns & loading previews...");
    try {
      // Single round-trip: column analysis + previews in parallel on the backend
      const res = await fetch("/api/merge/common-columns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId,
          baseGroupId: mergeBaseGroupId,
          sourceGroupId: currentSourceGroupId,
          apiKey,
          includePreview: true,
        }),
      });
      if (!res.ok) throw new Error((await res.json()).error || "Failed to analyze columns");
      const data = await res.json();
      setMergeCommonColumns(data.common_columns || []);
      setAllBaseColumns(data.base_columns || []);
      setAllSourceColumns(data.source_columns || []);
      setBaseColClasses(data.base_column_classes || {});
      setSourceColClasses(data.source_column_classes || {});
      if (data.base_preview) {
        setBasePreview({ columns: data.base_preview.columns, rows: data.base_preview.rows });
        lastBasePreviewId.current = mergeBaseGroupId;
      }
      if (data.source_preview) {
        setSourcePreview({ columns: data.source_preview.columns, rows: data.source_preview.rows });
      }
      addLog("Merge", "info", `Found ${(data.common_columns || []).length} common column(s)`);
    } catch (err: any) {
      setError(err.message);
      addLog("Merge", "error", err.message);
    } finally {
      setColumnsLoading(false);
    }
  }, [sessionId, mergeBaseGroupId, currentSourceGroupId, apiKey, addLog, setError, setLoadingMessage, setMergeCommonColumns]);

  useEffect(() => {
    if (mergeBaseGroupId && currentSourceGroupId && step === 6) {
      setMergeSelectedKeys([]);
      setMergePullColumns([]);
      setMergeSimulation(null);
      setMergeValidationReport(null);
      setPendingBaseCols([]);
      fetchColumnsAndPreviews();
    }
  }, [mergeBaseGroupId, currentSourceGroupId, step]); // eslint-disable-line react-hooks/exhaustive-deps

  // --- Key Selection ---
  // Base checkbox: toggles column in pendingBaseCols queue, or removes existing pair
  // Source checkbox: pairs with the oldest pending base col (FIFO), or removes existing pair
  // Supports composite keys — check multiple base cols then multiple source cols

  const handleBaseKeyCheck = useCallback((col: string) => {
    const existingPair = mergeSelectedKeys.find((k) => k.base_col === col);
    if (existingPair) {
      setMergeSelectedKeys((prev) => prev.filter((k) => k.base_col !== col));
      return;
    }
    setPendingBaseCols((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  }, [mergeSelectedKeys, setMergeSelectedKeys]);

  const handleSourceKeyCheck = useCallback((col: string) => {
    const existingPair = mergeSelectedKeys.find((k) => k.source_col === col);
    if (existingPair) {
      setMergeSelectedKeys((prev) => prev.filter((k) => k.source_col !== col));
      return;
    }
    if (pendingBaseCols.length > 0) {
      const baseCol = pendingBaseCols[0];
      setMergeSelectedKeys((prev) => [...prev, { base_col: baseCol, source_col: col }]);
      setPendingBaseCols((prev) => prev.slice(1));
    }
  }, [pendingBaseCols, mergeSelectedKeys, setMergeSelectedKeys]);

  const handleSourceHeaderClick = useCallback((col: string) => {
    const isKey = mergeSelectedKeys.some((k) => k.source_col === col);
    if (isKey) return;
    setMergePullColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  }, [mergeSelectedKeys, setMergePullColumns]);

  const removeKeyPair = useCallback((baseCol: string, sourceCol: string) => {
    setMergeSelectedKeys((prev) => prev.filter((k) => !(k.base_col === baseCol && k.source_col === sourceCol)));
  }, [setMergeSelectedKeys]);

  const removePullColumn = useCallback((col: string) => {
    setMergePullColumns((prev) => prev.filter((c) => c !== col));
  }, [setMergePullColumns]);

  // --- Section C: Simulation (debounced) ---

  const [simLoading, setSimLoading] = useState(false);
  const [simError, setSimError] = useState<string | null>(null);

  useEffect(() => {
    if (simDebounceRef.current) clearTimeout(simDebounceRef.current);
    if (mergeSelectedKeys.length === 0 || !mergeBaseGroupId || !currentSourceGroupId) {
      setMergeSimulation(null);
      setSimError(null);
      return;
    }
    setSimLoading(true);
    setSimError(null);
    simDebounceRef.current = setTimeout(async () => {
      try {
        const res = await fetch("/api/merge/simulate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId,
            baseGroupId: mergeBaseGroupId,
            sourceGroupId: currentSourceGroupId,
            keyPairs: mergeSelectedKeys,
            pullColumns: mergePullColumns,
          }),
        });
        const data = await res.json();
        if (res.ok) {
          setMergeSimulation(data);
          setSimError(null);
        } else {
          setSimError(data.error || "Simulation failed");
          setMergeSimulation(null);
        }
      } catch (err: any) {
        setSimError(err.message || "Simulation request failed");
        setMergeSimulation(null);
      } finally {
        setSimLoading(false);
      }
    }, 500);
    return () => { if (simDebounceRef.current) clearTimeout(simDebounceRef.current); };
  }, [mergeSelectedKeys, mergePullColumns, sessionId, mergeBaseGroupId, currentSourceGroupId, setMergeSimulation]);

  // --- Section D: Execute ---

  const handleExecute = useCallback(async () => {
    if (!sessionId || !mergeBaseGroupId || !currentSourceGroupId || mergeSelectedKeys.length === 0) return;
    setExecutingMerge(true);
    setMergeProgress(5);
    setMergeProgressMessage("Preparing merge...");
    setError(null);
    addLog("Merge", "info", `Executing merge for source: ${groupNameMap[currentSourceGroupId] || currentSourceGroupId}...`);
    try {
      const res = await fetch("/api/merge/execute", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId,
          baseGroupId: mergeBaseGroupId,
          sourceGroupId: currentSourceGroupId,
          keyPairs: mergeSelectedKeys,
          pullColumns: mergePullColumns,
        }),
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.error || "Merge execution failed");
      }

      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const events = buffer.split("\n\n");
        buffer = events.pop() || "";

        for (const event of events) {
          const dataLine = event.split("\n").find((l) => l.startsWith("data: "));
          if (!dataLine) continue;
          try {
            const data = JSON.parse(dataLine.slice(6));
            setMergeProgress(data.progress);
            setMergeProgressMessage(data.message);

            if (data.stage === "done" && data.result) {
              const vr = { ...data.result.validation_report, _execution_plan: data.result.merge_log?.execution_plan };
              setMergeValidationReport(vr);
              addLog("Merge", "success", `Merged: ${data.result.merge_log?.rows || 0} rows, ${data.result.merge_log?.columns_pulled?.length || 0} columns pulled`);
            } else if (data.stage === "error") {
              throw new Error(data.message);
            }
          } catch (parseErr: any) {
            if (parseErr.message && !parseErr.message.includes("JSON")) throw parseErr;
          }
        }
      }
      reader.releaseLock();
    } catch (err: any) {
      setError(err.message);
      addLog("Merge", "error", err.message);
    } finally {
      setExecutingMerge(false);
      setMergeProgress(0);
      setMergeProgressMessage("");
    }
  }, [sessionId, mergeBaseGroupId, currentSourceGroupId, mergeSelectedKeys, mergePullColumns, groupNameMap, addLog, setError, setMergeValidationReport]);

  // --- Approve & Continue / Finalize ---

  const handleApprove = useCallback(async () => {
    try {
      const newApproved = [
        ...mergeApprovedSources,
        {
          base_group_id: mergeBaseGroupId,
          source_group_id: currentSourceGroupId,
          result_table: `_merge_step_${currentSourceGroupId}`,
          key_pairs: mergeSelectedKeys,
          pull_columns: mergePullColumns,
          validation_report: mergeValidationReport,
        },
      ];
      setMergeApprovedSources(newApproved);

      const nextIdx = mergeCurrentSourceIdx + 1;
      if (nextIdx < mergeSourceGroupIds.length) {
        setMergeCurrentSourceIdx(nextIdx);
        setMergeCommonColumns([]);
        setMergeSelectedKeys([]);
        setMergePullColumns([]);
        setMergeSimulation(null);
        setMergeValidationReport(null);
        setSourcePreview(null);
        setSourceColClasses({});
        setPendingBaseCols([]);
        addLog("Merge", "info", `Approved source ${mergeCurrentSourceIdx + 1}/${mergeSourceGroupIds.length}. Moving to next source.`);
      } else {
        setAiLoading(true);
        setLoadingMessage("Finalizing merge — building final_merged table...");
        try {
          const payload = newApproved.map(({ validation_report: _vr, ...rest }) => rest);
          const res = await fetch("/api/merge/finalize", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sessionId, approvedMerges: payload }),
          });
          if (!res.ok) throw new Error((await res.json()).error || "Finalization failed");
          const data = await res.json();
          setMergeResult(data);
          if (data.merge_history) setMergeHistory(data.merge_history);
          addLog("Merge", "success", `Final merged table: ${data.rows} rows × ${data.cols} columns`);
          setStep(7);
        } finally {
          setAiLoading(false);
        }
      }
    } catch (err: any) {
      setError(err.message || "Approve & finalize failed");
      addLog("Merge", "error", err.message || "Approve & finalize failed");
      setAiLoading(false);
    }
  }, [mergeApprovedSources, mergeBaseGroupId, currentSourceGroupId, mergeSelectedKeys, mergePullColumns, mergeValidationReport, mergeCurrentSourceIdx, mergeSourceGroupIds, sessionId, addLog, setError, setAiLoading, setLoadingMessage, setMergeApprovedSources, setMergeCurrentSourceIdx, setMergeCommonColumns, setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeValidationReport, setMergeResult, setMergeHistory, setStep]);

  const handleRedo = useCallback(() => {
    setMergeSelectedKeys([]);
    setMergePullColumns([]);
    setMergeSimulation(null);
    setMergeValidationReport(null);
    setPendingBaseCols([]);
  }, [setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeValidationReport]);

  // --- Skip Merge ---

  const handleSkipMerge = useCallback(async () => {
    if (!sessionId || groupSchema.length === 0) return;
    const baseId = mergeBaseGroupId || groupSchema[0].group_id;
    setAiLoading(true);
    setLoadingMessage("Using single table as final file...");
    addLog("Merge", "info", "Skipping merge — using single table as final file.");
    try {
      const res = await fetch("/api/merge/skip", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, baseGroupId: baseId }),
      });
      if (!res.ok) throw new Error((await res.json()).error || "Skip merge failed");
      const data = await res.json();
      setMergeResult(data);
      if (data.merge_history) setMergeHistory(data.merge_history);
      addLog("Merge", "success", `Final file: ${data.rows} rows × ${data.cols} columns (no merge)`);
      setStep(7);
    } catch (err: any) {
      setError(err.message || "Skip merge failed");
      addLog("Merge", "error", err.message || "Skip merge failed");
    } finally {
      setAiLoading(false);
    }
  }, [sessionId, groupSchema, mergeBaseGroupId, addLog, setError, setAiLoading, setLoadingMessage, setMergeResult, setStep]);

  // --- Download helpers ---

  const downloadStepCsv = useCallback(async () => {
    if (!sessionId || !currentSourceGroupId) {
      setError("Cannot download — no active merge session or source group.");
      return;
    }
    try {
      const res = await fetch(`/api/merge/download-step-csv?sessionId=${encodeURIComponent(sessionId)}&sourceGroupId=${encodeURIComponent(currentSourceGroupId)}`);
      if (!res.ok) {
        let msg = "Failed to download step CSV";
        try { const j = await res.json(); msg = j.error || msg; } catch { /* not json */ }
        throw new Error(msg);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      const srcName = groupNameMap[currentSourceGroupId] || currentSourceGroupId;
      a.download = `step_merge_${srcName}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err: any) {
      setError(err.message || "Step CSV download failed");
    }
  }, [sessionId, currentSourceGroupId, groupNameMap, setError]);

  const downloadXlsx = useCallback(async (version?: number) => {
    try {
      let url = `/api/merge/download-xlsx?sessionId=${encodeURIComponent(sessionId)}`;
      if (version !== undefined) url += `&version=${version}`;
      const res = await fetch(url);
      if (!res.ok) {
        let msg = "Failed to download xlsx";
        try { const j = await res.json(); msg = j.error || msg; } catch { /* not json */ }
        throw new Error(msg);
      }
      const blob = await res.blob();
      const blobUrl = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = blobUrl;
      a.download = `merge_output${version !== undefined ? `_v${version}` : ""}.xlsx`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(blobUrl);
    } catch (err: any) {
      setError(err.message || "XLSX download failed");
    }
  }, [sessionId, setError]);

  const downloadAllZip = useCallback(async () => {
    try {
      const res = await fetch(`/api/merge/download-all?sessionId=${encodeURIComponent(sessionId)}`);
      if (!res.ok) {
        let msg = "Failed to download ZIP";
        try { const j = await res.json(); msg = j.error || msg; } catch { /* not json */ }
        throw new Error(msg);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "all_merge_outputs.zip";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err: any) {
      setError(err.message || "ZIP download failed");
    }
  }, [sessionId, setError]);

  const downloadCsv = useCallback(async () => {
    try {
      const res = await fetch(`/api/merge/download-csv?sessionId=${encodeURIComponent(sessionId)}`);
      if (!res.ok) throw new Error("Failed to download CSV");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "final_merged.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err: any) {
      setError(err.message || "CSV download failed");
    }
  }, [sessionId, setError]);

  const downloadReport = useCallback(() => {
    if (!mergeResult) return;
    const blob = new Blob([JSON.stringify(mergeResult, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "merge_audit_report.json";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [mergeResult]);

  // --- Post-merge actions ---

  const handleRedoMerge = useCallback(() => {
    setMergeResult(null);
    setMergeApprovedSources([]);
    setMergeValidationReport(null);
    setMergeSelectedKeys([]);
    setMergePullColumns([]);
    setMergeSimulation(null);
    setMergeCurrentSourceIdx(0);
    setBasePreview(null);
    setSourcePreview(null);
    setBaseColClasses({});
    setSourceColClasses({});
    setPendingBaseCols([]);
    addLog("Merge", "info", "Redo merge — returning to key selection.");
    setStep(6);
  }, [addLog, setMergeResult, setMergeApprovedSources, setMergeValidationReport, setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeCurrentSourceIdx, setStep]);

  const [mergeAgainLoading, setMergeAgainLoading] = useState(false);

  const handleMergeAgain = useCallback(async () => {
    if (!sessionId) return;
    setMergeAgainLoading(true);
    try {
      const baseName = groupNameMap[mergeBaseGroupId] || mergeBaseGroupId;
      const sourceNames = mergeApprovedSources.map((s) => groupNameMap[s.source_group_id] || s.source_group_id);
      const newGroupName = `Merged ${baseName}-${sourceNames.join(",")}`;

      const res = await fetch("/api/merge/register-merged-group", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, groupName: newGroupName }),
      });
      if (!res.ok) throw new Error((await res.json()).error || "Failed to register merged group");
      const data = await res.json();

      onRegisterMergedGroup(data.group_id, data.group_name, data.group_row);

      setMergeResult(null);
      setMergeApprovedSources([]);
      setMergeValidationReport(null);
      setMergeSelectedKeys([]);
      setMergePullColumns([]);
      setMergeSimulation(null);
      setMergeCurrentSourceIdx(0);
      setMergeBaseGroupId("");
      setMergeSourceGroupIds([]);
      setMergeBaseRecommendation(null);
      setBasePreview(null);
      setSourcePreview(null);
      setBaseColClasses({});
      setSourceColClasses({});
      setPendingBaseCols([]);

      addLog("Merge", "info", `Registered "${newGroupName}" as a new group — starting fresh merge workflow.`);
      setStep(6);
    } catch (err: any) {
      setError(err.message);
      addLog("Merge", "error", err.message);
    } finally {
      setMergeAgainLoading(false);
    }
  }, [sessionId, mergeBaseGroupId, mergeApprovedSources, groupNameMap, onRegisterMergedGroup, addLog, setError, setStep, setMergeResult, setMergeApprovedSources, setMergeValidationReport, setMergeSelectedKeys, setMergePullColumns, setMergeSimulation, setMergeCurrentSourceIdx, setMergeBaseGroupId, setMergeSourceGroupIds, setMergeBaseRecommendation]);

  // ===================== COLUMN COLOR HELPERS =====================

  function getColColor(col: string, side: "base" | "source"): string {
    const cc = side === "base" ? commonByBase.get(col) : commonBySource.get(col);
    if (cc?.color) return cc.color;
    const cls = side === "base" ? baseColClasses[col] : sourceColClasses[col];
    return cls?.color || "grey";
  }

  function getColBgClass(col: string, side: "base" | "source"): string {
    const cc = side === "base" ? commonByBase.get(col) : commonBySource.get(col);
    if (cc) return COLOR_MAP[cc.color] || COLOR_MAP.grey;
    const cls = side === "base" ? baseColClasses[col] : sourceColClasses[col];
    if (cls?.color && COLOR_MAP[cls.color]) return COLOR_MAP[cls.color];
    return "bg-neutral-50 border-neutral-200 text-neutral-600 dark:bg-neutral-800 dark:border-neutral-700 dark:text-neutral-400";
  }

  // ===================== RENDER =====================

  if (step !== 6 && step !== 7) return null;

  if (step === 7) {
    return (
      <MergeReport
        mergeResult={mergeResult}
        mergeApprovedSources={mergeApprovedSources}
        mergeHistory={mergeHistory}
        groupNameMap={groupNameMap}
        onDownloadXlsx={downloadXlsx}
        onDownloadCsv={downloadCsv}
        onDownloadAllZip={downloadAllZip}
        onDownloadReport={downloadReport}
        onRedoMerge={handleRedoMerge}
        onMergeAgain={handleMergeAgain}
        mergeAgainLoading={mergeAgainLoading}
      />
    );
  }

  const hasMultipleGroups = groupSchema.length > 1;
  const canExecute = mergeSelectedKeys.length > 0;
  const availableSources = groupSchema.filter((g: any) => g.group_id !== mergeBaseGroupId);

  // --- Full-screen Overlay (Portal) ---
  const fullscreenOverlay = expanded ? ReactDOM.createPortal(
    <div className="fixed inset-0 z-[9999] bg-white dark:bg-neutral-900 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-neutral-200 dark:border-neutral-700 shrink-0">
        <h3 className="text-lg font-bold text-neutral-900 dark:text-white">
          Column Preview — Base vs Source
        </h3>
        <button
          onClick={() => setExpanded(false)}
          className="flex items-center gap-2 px-4 py-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 hover:bg-neutral-200 dark:hover:bg-neutral-700 text-sm font-medium transition-colors"
        >
          <Minimize2 className="w-4 h-4" /> Close
        </button>
      </div>
      <div className="flex-1 grid grid-cols-2 gap-4 p-6 overflow-hidden">
        {renderTable("base", true)}
        {renderTable("source", true)}
      </div>
    </div>,
    document.body
  ) : null;

  function renderTable(side: "base" | "source", isExpanded: boolean) {
    const preview = side === "base" ? basePreview : sourcePreview;
    const columns = preview?.columns || (side === "base" ? allBaseColumns : allSourceColumns);
    const rows = preview?.rows || [];
    const groupId = side === "base" ? mergeBaseGroupId : currentSourceGroupId;
    const label = side === "base" ? "Base" : "Source";

    return (
      <div className="flex flex-col min-h-0">
        <h4 className="text-xs font-bold text-neutral-500 dark:text-neutral-400 mb-2 uppercase tracking-wider shrink-0">
          {label}: {groupNameMap[groupId] || groupId}
        </h4>
        <div className={`overflow-auto rounded-xl border border-neutral-200 dark:border-neutral-700 ${isExpanded ? "flex-1" : "max-h-[400px]"}`}>
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 z-10">
              {/* Row 1: Key checkboxes above the headers */}
              <tr className="bg-neutral-100/80 dark:bg-neutral-800/80">
                {columns.map((col) => {
                  const isKey = side === "base"
                    ? mergeSelectedKeys.some((k) => k.base_col === col)
                    : mergeSelectedKeys.some((k) => k.source_col === col);
                  const isPending = side === "base" && pendingBaseCols.includes(col);

                  return (
                    <td
                      key={col}
                      className="px-2 py-1.5 text-center border-b border-neutral-200 dark:border-neutral-700"
                    >
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          if (side === "base") handleBaseKeyCheck(col);
                          else handleSourceKeyCheck(col);
                        }}
                        className="mx-auto block"
                        title={
                          isKey
                            ? "Remove key"
                            : side === "base"
                            ? isPending ? "Click again to deselect" : "Select as base key column"
                            : pendingBaseCols.length > 0 ? `Pair with base "${pendingBaseCols[0]}"` : "Select a base column first"
                        }
                      >
                        <span className={`inline-flex items-center justify-center w-5 h-5 rounded border-2 transition-colors cursor-pointer ${
                          isKey
                            ? "bg-red-500 border-red-500 text-white"
                            : isPending
                            ? "border-blue-400 bg-blue-50 dark:bg-blue-950/30"
                            : "border-neutral-300 dark:border-neutral-600 hover:border-red-400 hover:bg-red-50 dark:hover:bg-red-950/20"
                        }`}>
                          {isKey && <Key className="w-3 h-3" />}
                          {isPending && !isKey && <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />}
                        </span>
                      </button>
                    </td>
                  );
                })}
              </tr>

              {/* Row 2: Column headers — source side headers are clickable for pull */}
              <tr>
                {columns.map((col) => {
                  const cc = side === "base" ? commonByBase.get(col) : commonBySource.get(col);
                  const cls = side === "base" ? baseColClasses[col] : sourceColClasses[col];
                  const isKey = side === "base"
                    ? mergeSelectedKeys.some((k) => k.base_col === col)
                    : mergeSelectedKeys.some((k) => k.source_col === col);
                  const isPull = side === "source" && mergePullColumns.includes(col);
                  const bgClass = getColBgClass(col, side);
                  const color = getColColor(col, side);
                  const isSourceClickable = side === "source" && !isKey;
                  const categoryLabel = cc?.category || cls?.category || "";

                  return (
                    <th
                      key={col}
                      className={`px-2 py-2 font-bold whitespace-nowrap border-b select-none transition-all ${bgClass} ${
                        isSourceClickable ? "cursor-pointer hover:opacity-80" : ""
                      } ${isPull ? "ring-2 ring-blue-500 ring-inset" : ""} ${isKey ? "ring-2 ring-red-500 ring-inset" : ""}`}
                      onClick={() => { if (side === "source") handleSourceHeaderClick(col); }}
                      title={
                        side === "source"
                          ? isKey
                            ? `Key column — paired for join`
                            : isPull
                            ? `Click to un-pull "${col}"`
                            : `Click to pull "${col}" into merged output`
                          : categoryLabel
                          ? `${categoryLabel} (${cc?.eligibility || cls?.eligibility || ""})`
                          : col
                      }
                    >
                      <span className="flex items-center gap-1.5">
                        {/* Pull indicator on source headers */}
                        {side === "source" && (
                          <span className={`inline-flex items-center justify-center w-4 h-4 rounded border-2 shrink-0 transition-colors ${
                            isPull
                              ? "bg-blue-500 border-blue-500 text-white"
                              : isKey
                              ? "bg-red-500/20 border-red-500/40 text-red-500"
                              : "border-neutral-300 dark:border-neutral-600"
                          }`}>
                            {isPull && <Download className="w-2.5 h-2.5" />}
                            {isKey && !isPull && <Key className="w-2.5 h-2.5" />}
                          </span>
                        )}

                        {/* Color badge for ALL classified columns */}
                        {color !== "grey" && (
                          <span className={`inline-block w-2.5 h-2.5 rounded-full shrink-0 ${BADGE_COLOR_MAP[color] || BADGE_COLOR_MAP.grey}`} />
                        )}

                        <span className="truncate">{col}</span>

                        {cc && <Link2 className="w-3 h-3 opacity-40 shrink-0" />}
                      </span>

                      {cc ? (
                        <span className="block text-[9px] font-normal opacity-60 mt-0.5">
                          {cc.category} · {Math.round(cc.overlap_pct ?? 0)}% overlap
                        </span>
                      ) : categoryLabel && categoryLabel !== "unknown" ? (
                        <span className="block text-[9px] font-normal opacity-60 mt-0.5">
                          {categoryLabel}
                        </span>
                      ) : null}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, isExpanded ? 200 : 50).map((row: any, ri: number) => (
                <tr key={ri} className="hover:bg-neutral-50 dark:hover:bg-neutral-800/50">
                  {columns.map((col) => (
                    <td key={col} className="px-2 py-1 border-b border-neutral-100 dark:border-neutral-800 whitespace-nowrap max-w-[180px] truncate">
                      {String(row[col] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={columns.length} className="px-4 py-8 text-center text-neutral-400 text-xs">
                    No preview data available
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {fullscreenOverlay}

      {/* Section A: Base & Source Selection */}
      <SurfaceCard title="Base & Source Selection" icon={Sparkles} subtitle={hasMultipleGroups ? `Merging source ${mergeCurrentSourceIdx + 1} of ${mergeSourceGroupIds.length}` : undefined}>
        {!hasMultipleGroups && groupSchema.length === 1 && (
          <div className="text-sm text-neutral-500 dark:text-neutral-400 mb-4">
            Only one table available.{" "}
            <button onClick={handleSkipMerge} className="text-red-600 dark:text-red-400 font-semibold hover:underline">
              Skip Merge
            </button>
          </div>
        )}
        {hasMultipleGroups && (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
              {/* Base dropdown */}
              <div>
                <label className="block text-xs font-semibold text-neutral-500 dark:text-neutral-400 mb-1.5">
                  Base Table (left joins from here)
                </label>
                <select
                  value={mergeBaseGroupId}
                  onChange={(e) => handleBaseChange(e.target.value)}
                  className="w-full rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-3 py-2.5 text-sm text-neutral-900 dark:text-neutral-100 focus:ring-2 focus:ring-red-500 focus:border-transparent"
                >
                  <option value="">Select base table...</option>
                  {groupSchema.map((g: any) => (
                    <option key={g.group_id} value={g.group_id}>
                      {groupNameMap[g.group_id] || g.group_id} ({g.rows} rows, {g.columns?.length || 0} cols)
                    </option>
                  ))}
                </select>
                {mergeBaseRecommendation?.reasoning && mergeBaseGroupId === mergeBaseRecommendation?.recommended && (
                  <p className="mt-1.5 text-[11px] text-emerald-600 dark:text-emerald-400 flex items-center gap-1">
                    <Sparkles className="w-3 h-3" /> AI Recommended: {mergeBaseRecommendation.reasoning}
                  </p>
                )}
              </div>

              {/* Source dropdown */}
              <div>
                <label className="block text-xs font-semibold text-neutral-500 dark:text-neutral-400 mb-1.5">
                  Source Table ({mergeCurrentSourceIdx + 1}/{mergeSourceGroupIds.length})
                </label>
                <select
                  value={currentSourceGroupId}
                  onChange={(e) => handleSourceChange(e.target.value)}
                  className="w-full rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 px-3 py-2.5 text-sm text-neutral-900 dark:text-neutral-100 focus:ring-2 focus:ring-red-500 focus:border-transparent"
                >
                  <option value="">Select source table...</option>
                  {availableSources.map((g: any) => (
                    <option key={g.group_id} value={g.group_id}>
                      {groupNameMap[g.group_id] || g.group_id} ({g.rows} rows, {g.columns?.length || 0} cols)
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <button onClick={handleSkipMerge} className="text-xs text-neutral-400 hover:text-red-500 transition-colors">
              <SkipForward className="w-3 h-3 inline mr-1" />Skip merge entirely
            </button>
          </>
        )}
      </SurfaceCard>

      {/* Section B: Side-by-Side Preview */}
      {mergeBaseGroupId && currentSourceGroupId && (
        <SurfaceCard
          title="Column Matching & Preview"
          icon={Link2}
          subtitle={
            pendingBaseCols.length > 0
              ? `Pairing: ${pendingBaseCols.map(c => `"${c}"`).join(", ")} selected — check source key checkbox(es) to pair`
              : "Use checkboxes above headers for keys. Click source headers to pull columns."
          }
          right={
            <button
              onClick={() => setExpanded(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-neutral-100 dark:bg-neutral-800 hover:bg-neutral-200 dark:hover:bg-neutral-700 text-neutral-600 dark:text-neutral-300 text-xs font-medium transition-colors"
              title="Expand to full screen"
            >
              <Maximize2 className="w-3.5 h-3.5" /> Full Screen
            </button>
          }
        >
          {/* Loading overlay while columns are being analyzed */}
          {columnsLoading && (
            <div className="flex items-center gap-3 rounded-xl bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 px-4 py-4 text-sm text-blue-700 dark:text-blue-300 mb-4">
              <Loader2 className="w-5 h-5 animate-spin shrink-0" />
              <div>
                <p className="font-semibold">Analyzing columns...</p>
                <p className="text-xs opacity-70">Classifying all columns, finding common matches, and computing value overlap. This may take a moment.</p>
              </div>
            </div>
          )}

          {/* No common columns warning */}
          {mergeCommonColumns.length === 0 && !columnsLoading && allBaseColumns.length > 0 && (
            <div className="flex items-center gap-2 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 px-4 py-3 text-xs text-amber-700 dark:text-amber-300 mb-4">
              <AlertTriangle className="w-4 h-4 shrink-0" />
              No common columns found automatically. Check a base key checkbox, then check the corresponding source key checkbox to manually pair them.
            </div>
          )}

          {/* Pending pairing indicator */}
          {pendingBaseCols.length > 0 && (
            <div className="flex items-center gap-2 rounded-xl bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 px-4 py-3 text-xs text-blue-700 dark:text-blue-300 mb-4">
              <Key className="w-4 h-4 shrink-0" />
              Base column{pendingBaseCols.length > 1 ? "s" : ""} {pendingBaseCols.map(c => `"${c}"`).join(", ")} selected — now check source column key checkbox(es) to create pair(s).
              <button onClick={() => setPendingBaseCols([])} className="ml-auto text-blue-500 hover:text-blue-700"><X className="w-3.5 h-3.5" /></button>
            </div>
          )}

          {/* Color legend — always visible once columns are loaded */}
          {!columnsLoading && allBaseColumns.length > 0 && (
            <div className="flex flex-wrap items-center gap-3 mb-4 text-[10px]">
              <span className="font-semibold text-neutral-400 uppercase tracking-wider">Legend (directional only):</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-emerald-500" /> Identifier (high)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-amber-500" /> Descriptor (medium)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-red-500" /> Metric/Weak (low/never)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-neutral-400" /> Unclassified</span>
              <span className="flex items-center gap-1 ml-2 pl-2 border-l border-neutral-200 dark:border-neutral-700">
                <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded border-2 bg-red-500 border-red-500 text-white"><Key className="w-2 h-2" /></span> Key
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded border-2 bg-blue-500 border-blue-500 text-white"><Download className="w-2 h-2" /></span> Pull
              </span>
              {mergeCommonColumns.length > 0 && (
                <span className="flex items-center gap-1">
                  <Link2 className="w-3 h-3 text-neutral-500" /> Common column
                </span>
              )}
            </div>
          )}

          {/* Key pairs summary */}
          {mergeSelectedKeys.length > 0 && (
            <div className="flex flex-wrap gap-2 mb-4">
              <span className="text-[10px] font-semibold text-neutral-400 uppercase tracking-wider self-center mr-1">Keys:</span>
              {mergeSelectedKeys.map((kp, i) => (
                <span key={i} className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 text-xs font-semibold text-red-700 dark:text-red-300">
                  <Key className="w-3 h-3" />
                  {kp.base_col} ↔ {kp.source_col}
                  <button onClick={() => removeKeyPair(kp.base_col, kp.source_col)} className="ml-1 hover:text-red-500">
                    <X className="w-3 h-3" />
                  </button>
                </span>
              ))}
            </div>
          )}

          {/* Pull columns summary */}
          {mergePullColumns.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mb-4">
              <span className="text-[10px] font-semibold text-neutral-400 uppercase tracking-wider self-center mr-1">Pull:</span>
              {mergePullColumns.map((col) => (
                <span key={col} className="inline-flex items-center gap-1 px-2 py-1 rounded-full bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800 text-[11px] font-medium text-blue-700 dark:text-blue-300">
                  <Download className="w-2.5 h-2.5" />
                  {col}
                  <button onClick={() => removePullColumn(col)} className="ml-0.5 hover:text-blue-500"><X className="w-2.5 h-2.5" /></button>
                </span>
              ))}
            </div>
          )}

          {/* Side-by-side tables (inline, not expanded) */}
          <div className="grid grid-cols-2 gap-4">
            {renderTable("base", false)}
            {renderTable("source", false)}
          </div>
        </SurfaceCard>
      )}

      {/* Section C: Simulation Stats */}
      {mergeSelectedKeys.length > 0 && (
        <SurfaceCard title="Join Simulation" subtitle="Auto-updates when keys change">
          {simLoading && !mergeSimulation && (
            <div className="flex items-center gap-2 text-sm text-neutral-500">
              <Loader2 className="w-4 h-4 animate-spin" /> Simulating join...
            </div>
          )}
          {simError && (
            <div className="flex items-center gap-2 rounded-xl bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800 px-4 py-3 text-xs text-red-700 dark:text-red-300">
              <AlertTriangle className="w-4 h-4 shrink-0" /> {simError}
            </div>
          )}
          {mergeSimulation && (
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
              {[
                { label: "Match Rate", value: `${mergeSimulation.match_rate}%`, metric: "match_rate", raw: mergeSimulation.match_rate },
                { label: "Row Explosion", value: `${mergeSimulation.row_explosion_factor}×`, metric: "row_explosion_factor", raw: mergeSimulation.row_explosion_factor },
                { label: "Unmatched Base", value: mergeSimulation.unmatched_base_count, metric: "unmatched", raw: 0 },
                { label: "Unmatched Source", value: mergeSimulation.unmatched_source_count, metric: "unmatched", raw: 0 },
                { label: "Dup Source Keys", value: mergeSimulation.duplicate_source_keys, metric: "dup", raw: 0 },
                { label: "Est. Null Rate", value: `${mergeSimulation.estimated_null_rate}%`, metric: "estimated_null_rate", raw: mergeSimulation.estimated_null_rate },
              ].map((s) => (
                <div key={s.label} className="rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-3 text-center">
                  <p className="text-[10px] font-semibold text-neutral-400 dark:text-neutral-500 uppercase tracking-wider mb-1">{s.label}</p>
                  <p className={`text-lg font-bold tabular-nums ${getStatColor(s.metric, s.raw)}`}>{s.value}</p>
                </div>
              ))}
            </div>
          )}
        </SurfaceCard>
      )}

      {/* Section D: Execute & Validate */}
      {mergeBaseGroupId && currentSourceGroupId && !mergeValidationReport && (
        <>
          {executingMerge && (
            <SurfaceCard>
              <div className="space-y-4">
                <div className="flex items-center gap-3">
                  <Loader2 className="w-5 h-5 animate-spin text-red-500 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-2">
                      <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                        {mergeProgressMessage || "Preparing merge..."}
                      </p>
                      <span className="text-xs font-bold text-red-600 dark:text-red-400 tabular-nums ml-2">{mergeProgress}%</span>
                    </div>
                    <div className="h-2.5 rounded-full bg-neutral-200 dark:bg-neutral-700 overflow-hidden">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-red-500 to-red-600 transition-all duration-700 ease-out"
                        style={{ width: `${mergeProgress}%` }}
                      />
                    </div>
                  </div>
                </div>
                <div className="flex gap-6 text-[10px] font-medium">
                  {[
                    { label: "Prepare", min: 0, done: 15 },
                    { label: "Dedup & Merge", min: 15, done: 55 },
                    { label: "Validate", min: 55, done: 100 },
                  ].map(({ label, min, done: doneAt }) => {
                    const isDone = mergeProgress >= doneAt;
                    const isActive = mergeProgress >= min && mergeProgress < doneAt;
                    return (
                      <span
                        key={label}
                        className={`flex items-center gap-1.5 ${
                          isDone ? "text-emerald-500" : isActive ? "text-red-500" : "text-neutral-400"
                        }`}
                      >
                        {isDone ? (
                          <Check className="w-3 h-3" />
                        ) : isActive ? (
                          <span className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
                        ) : (
                          <span className="w-2 h-2 rounded-full bg-neutral-300 dark:bg-neutral-600" />
                        )}
                        {label}
                      </span>
                    );
                  })}
                </div>
              </div>
            </SurfaceCard>
          )}
          {!executingMerge && (
            <div className="flex items-center gap-3">
              <PrimaryButton
                onClick={handleExecute}
                disabled={!canExecute || loading}
              >
                Execute Merge <ArrowRight className="w-4 h-4" />
              </PrimaryButton>
              {!canExecute && (
                <p className="text-xs text-neutral-400">Select at least one key pair to execute.</p>
              )}
            </div>
          )}
        </>
      )}

      {/* Validation Report */}
      {mergeValidationReport && (
        <SurfaceCard title="Merge Validation" icon={Check}>
          <div className="rounded-xl bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800 px-4 py-3 mb-4">
            <p className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">
              Source "{groupNameMap[currentSourceGroupId] || currentSourceGroupId}" merged successfully
            </p>
            <p className="text-xs text-emerald-600 dark:text-emerald-400 mt-1">
              {mergeValidationReport.result_rows} rows · {mergeValidationReport.columns_pulled?.length || 0} columns pulled · Explosion factor: {mergeValidationReport.explosion_factor}×
            </p>
          </div>

          {mergeValidationReport.column_stats && mergeValidationReport.column_stats.length > 0 && (
            <div className="overflow-auto max-h-[300px] rounded-xl border border-neutral-200 dark:border-neutral-700 mb-4">
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
                  {mergeValidationReport.column_stats.map((cs: any) => (
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
          )}

          {mergeValidationReport.preview && mergeValidationReport.preview.length > 0 && (
            <details className="mb-4">
              <summary className="text-xs font-semibold text-neutral-500 cursor-pointer hover:text-neutral-700 dark:hover:text-neutral-300 mb-2">
                Preview merged data ({mergeValidationReport.preview.length} rows)
              </summary>
              <div className="overflow-auto max-h-[250px] rounded-xl border border-neutral-200 dark:border-neutral-700">
                <table className="w-full text-[11px]">
                  <thead className="sticky top-0 bg-neutral-50 dark:bg-neutral-800">
                    <tr>
                      {Object.keys(mergeValidationReport.preview[0] || {}).map((col) => (
                        <th key={col} className="px-2 py-1.5 font-bold text-neutral-500 whitespace-nowrap border-b">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {mergeValidationReport.preview.slice(0, 30).map((row: any, ri: number) => (
                      <tr key={ri} className="hover:bg-neutral-50 dark:hover:bg-neutral-800/50">
                        {Object.values(row).map((val: any, ci: number) => (
                          <td key={ci} className="px-2 py-1 border-b border-neutral-100 dark:border-neutral-800 whitespace-nowrap max-w-[180px] truncate">{String(val ?? "")}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}

          {(mergeValidationReport.unmatched_base_preview?.length > 0 || mergeValidationReport.unmatched_source_preview?.length > 0) && (
            <details className="mb-4">
              <summary className="text-xs font-semibold text-amber-600 cursor-pointer hover:text-amber-700 mb-2">
                Unmatched rows ({mergeValidationReport.unmatched_base_preview?.length || 0} base, {mergeValidationReport.unmatched_source_preview?.length || 0} source)
              </summary>
              {mergeValidationReport.unmatched_base_preview?.length > 0 && (
                <>
                  <p className="text-[11px] text-neutral-400 mb-2">Base rows without a match in source:</p>
                  <div className="overflow-auto max-h-[150px] rounded-lg border border-amber-200 dark:border-amber-800 mb-2">
                    <table className="w-full text-[10px]">
                      <thead className="sticky top-0 bg-amber-50 dark:bg-amber-950/30">
                        <tr>
                          {Object.keys(mergeValidationReport.unmatched_base_preview[0]).map((col) => (
                            <th key={col} className="px-2 py-1 font-bold text-amber-600 whitespace-nowrap">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {mergeValidationReport.unmatched_base_preview.slice(0, 10).map((row: any, ri: number) => (
                          <tr key={ri}>
                            {Object.values(row).map((val: any, ci: number) => (
                              <td key={ci} className="px-2 py-0.5 whitespace-nowrap max-w-[150px] truncate text-neutral-600 dark:text-neutral-400">{String(val ?? "")}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
              {mergeValidationReport.unmatched_source_preview?.length > 0 && (
                <>
                  <p className="text-[11px] text-neutral-400 mb-2">Source rows without a match in base:</p>
                  <div className="overflow-auto max-h-[150px] rounded-lg border border-amber-200 dark:border-amber-800">
                    <table className="w-full text-[10px]">
                      <thead className="sticky top-0 bg-amber-50 dark:bg-amber-950/30">
                        <tr>
                          {Object.keys(mergeValidationReport.unmatched_source_preview[0]).map((col) => (
                            <th key={col} className="px-2 py-1 font-bold text-amber-600 whitespace-nowrap">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {mergeValidationReport.unmatched_source_preview.slice(0, 10).map((row: any, ri: number) => (
                          <tr key={ri}>
                            {Object.values(row).map((val: any, ci: number) => (
                              <td key={ci} className="px-2 py-0.5 whitespace-nowrap max-w-[150px] truncate text-neutral-600 dark:text-neutral-400">{String(val ?? "")}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </details>
          )}

          {/* Execution Plan Summary */}
          {mergeValidationReport._execution_plan && (
            <details className="mb-4">
              <summary className="text-xs font-semibold text-neutral-500 cursor-pointer hover:text-neutral-700 dark:hover:text-neutral-300 mb-2">
                Execution Plan {mergeValidationReport._execution_plan.index_used ? "(Index Used)" : "(No Index)"}
              </summary>
              <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 p-3 text-[11px] font-mono space-y-1">
                {mergeValidationReport._execution_plan.details?.map((d: string, i: number) => (
                  <p key={i} className="text-neutral-600 dark:text-neutral-400">{d}</p>
                ))}
                {mergeValidationReport._execution_plan.warnings?.length > 0 && (
                  <div className="mt-2 text-amber-600 dark:text-amber-400">
                    {mergeValidationReport._execution_plan.warnings.map((w: string, i: number) => (
                      <p key={i} className="flex items-center gap-1"><AlertTriangle className="w-3 h-3" />{w}</p>
                    ))}
                  </div>
                )}
              </div>
            </details>
          )}

          <div className="flex items-center gap-3 pt-2">
            <PrimaryButton onClick={handleApprove}>
              <Check className="w-4 h-4" />
              {mergeCurrentSourceIdx + 1 < mergeSourceGroupIds.length
                ? `Approve & Continue (${mergeCurrentSourceIdx + 2}/${mergeSourceGroupIds.length})`
                : "Approve & Finalize"}
            </PrimaryButton>
            <SecondaryButton onClick={downloadStepCsv}>
              <Download className="w-4 h-4" />
              Download This Result
            </SecondaryButton>
            <SecondaryButton onClick={handleRedo}>
              <RotateCcw className="w-4 h-4" />
              Redo with Different Keys
            </SecondaryButton>
          </div>
        </SurfaceCard>
      )}
    </motion.div>
  );
}
