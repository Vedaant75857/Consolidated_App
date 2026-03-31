import { useState } from "react";
import { motion } from "motion/react";
import { Download, Package, Trash2, X, Loader2, FileSpreadsheet, BarChart3, CheckCircle2, AlertCircle } from "lucide-react";

import type { MergeOutput } from "../../types";

const ANALYZER_BE = "http://localhost:3005";
const ANALYZER_FE = "http://localhost:3004";

interface MergeOutputsPanelProps {
  mergeOutputs: MergeOutput[];
  sessionId: string;
  onClose: () => void;
  onDeleteOutput: (version: number) => Promise<void>;
}

export default function MergeOutputsPanel({
  mergeOutputs,
  sessionId,
  onClose,
  onDeleteOutput,
}: MergeOutputsPanelProps) {
  const [downloadingVersion, setDownloadingVersion] = useState<number | null>(null);
  const [downloadingAll, setDownloadingAll] = useState(false);
  const [deletingVersion, setDeletingVersion] = useState<number | null>(null);
  const [confirmDeleteVersion, setConfirmDeleteVersion] = useState<number | null>(null);

  const [selectingForAnalyzer, setSelectingForAnalyzer] = useState(false);
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [sending, setSending] = useState(false);
  const [sendResult, setSendResult] = useState<{ ok: boolean; message: string } | null>(null);

  const handleDownloadCsv = async (version: number) => {
    setDownloadingVersion(version);
    try {
      const res = await fetch(
        `/api/merge/download-csv?sessionId=${encodeURIComponent(sessionId)}&version=${version}`
      );
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      const output = mergeOutputs.find((o) => o.version === version);
      const safeName = (output?.label || `merge_v${version}`)
        .replace(/[^a-zA-Z0-9._\- ]/g, "_");
      a.download = `${safeName}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch {
      /* silently fail */
    } finally {
      setDownloadingVersion(null);
    }
  };

  const handleDownloadAll = async () => {
    setDownloadingAll(true);
    try {
      const res = await fetch(
        `/api/merge/download-all-csv?sessionId=${encodeURIComponent(sessionId)}`
      );
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "all_merge_outputs.zip";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch {
      /* silently fail */
    } finally {
      setDownloadingAll(false);
    }
  };

  const handleDelete = async (version: number) => {
    setDeletingVersion(version);
    try {
      await onDeleteOutput(version);
    } catch {
      /* handled by parent */
    } finally {
      setDeletingVersion(null);
      setConfirmDeleteVersion(null);
    }
  };

  const handleSendToAnalyzer = async () => {
    if (selectedVersion === null) return;
    setSending(true);
    setSendResult(null);

    // Open a blank tab immediately so the browser treats it as a user gesture
    // (avoids popup-blocker after long async work).
    const analyzerTab = window.open("about:blank", "_blank");

    try {
      const csvRes = await fetch(
        `/api/merge/download-csv?sessionId=${encodeURIComponent(sessionId)}&version=${selectedVersion}`
      );
      if (!csvRes.ok) throw new Error("Failed to download merge output");
      const blob = await csvRes.blob();

      const output = mergeOutputs.find((o) => o.version === selectedVersion);
      const safeName = (output?.label || `merge_v${selectedVersion}`)
        .replace(/[^a-zA-Z0-9._\- ]/g, "_");
      const file = new File([blob], `${safeName}.csv`, { type: "text/csv" });

      const fd = new FormData();
      fd.append("file", file);
      const uploadRes = await fetch(`${ANALYZER_BE}/api/upload`, { method: "POST", body: fd });
      if (!uploadRes.ok) {
        const err = await uploadRes.json().catch(() => ({ error: uploadRes.statusText }));
        throw new Error(err.error || "Upload to Spend Analyzer failed");
      }
      const data = await uploadRes.json();
      const analyzerSessionId: string = data.sessionId;

      if (analyzerTab) {
        analyzerTab.location.href = `${ANALYZER_FE}?sessionId=${encodeURIComponent(analyzerSessionId)}`;
      } else {
        window.open(`${ANALYZER_FE}?sessionId=${encodeURIComponent(analyzerSessionId)}`, "_blank");
      }
      setSendResult({ ok: true, message: "Opened Spend Analyzer in a new tab" });
      setSelectingForAnalyzer(false);
      setSelectedVersion(null);
    } catch (err: any) {
      if (analyzerTab) analyzerTab.close();
      setSendResult({ ok: false, message: err.message || "Send failed" });
    } finally {
      setSending(false);
    }
  };

  return (
    <motion.aside
      initial={{ x: 384, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      exit={{ x: 384, opacity: 0 }}
      transition={{ type: "spring", damping: 26, stiffness: 300 }}
      className="w-96 bg-white/95 dark:bg-neutral-900/95 backdrop-blur-xl border-l border-neutral-200/80 dark:border-neutral-700/80 flex flex-col z-10 shrink-0"
    >
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-neutral-200/80 dark:border-neutral-700/80">
        <div className="flex items-center gap-2.5">
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-emerald-500 to-teal-500 flex items-center justify-center">
            <Package className="w-4 h-4 text-white" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-neutral-900 dark:text-white">
              Merge Outputs
            </h2>
            <p className="text-[10px] text-neutral-400">
              {mergeOutputs.length} output{mergeOutputs.length !== 1 ? "s" : ""}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {!selectingForAnalyzer && (
            <>
              <button
                onClick={() => { setSelectingForAnalyzer(true); setSendResult(null); }}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold bg-red-50 dark:bg-red-950/30 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors"
                title="Send an output to the Spend Analyzer"
              >
                <BarChart3 className="w-3.5 h-3.5" />
                Send to Analyzer
              </button>
              <button
                onClick={handleDownloadAll}
                disabled={downloadingAll}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800 hover:bg-emerald-100 dark:hover:bg-emerald-900/40 transition-colors disabled:opacity-50"
                title="Download all outputs as ZIP of CSVs"
              >
                {downloadingAll ? (
                  <Loader2 className="w-3.5 h-3.5 animate-spin" />
                ) : (
                  <Download className="w-3.5 h-3.5" />
                )}
                Download All
              </button>
            </>
          )}
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-200 hover:bg-neutral-100 dark:hover:bg-neutral-800 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Selection mode banner */}
      {selectingForAnalyzer && (
        <div className="px-5 py-2.5 bg-red-50 dark:bg-red-950/30 border-b border-red-200/80 dark:border-red-800/60">
          <p className="text-xs font-medium text-red-700 dark:text-red-300">
            Select an output to send to the Spend Analyzer
          </p>
        </div>
      )}

      {/* Send result feedback */}
      {sendResult && (
        <div className={`px-5 py-2.5 flex items-center gap-2 border-b text-xs font-medium ${
          sendResult.ok
            ? "bg-emerald-50 dark:bg-emerald-950/30 border-emerald-200/80 dark:border-emerald-800/60 text-emerald-700 dark:text-emerald-300"
            : "bg-red-50 dark:bg-red-950/30 border-red-200/80 dark:border-red-800/60 text-red-700 dark:text-red-300"
        }`}>
          {sendResult.ok ? <CheckCircle2 className="w-3.5 h-3.5 shrink-0" /> : <AlertCircle className="w-3.5 h-3.5 shrink-0" />}
          <span className="truncate">{sendResult.message}</span>
          <button onClick={() => setSendResult(null)} className="ml-auto p-0.5 rounded hover:bg-black/5 dark:hover:bg-white/5">
            <X className="w-3 h-3" />
          </button>
        </div>
      )}

      {/* Output List */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-2">
        {mergeOutputs.map((output) => (
          <div
            key={output.version}
            onClick={selectingForAnalyzer ? () => setSelectedVersion(output.version) : undefined}
            className={`rounded-xl border p-3.5 transition-colors ${
              selectingForAnalyzer
                ? selectedVersion === output.version
                  ? "border-red-400 dark:border-red-600 bg-red-50 dark:bg-red-950/30 ring-1 ring-red-300 dark:ring-red-700 cursor-pointer"
                  : "border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 hover:border-red-300 dark:hover:border-red-700 cursor-pointer"
                : "border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 hover:border-emerald-300 dark:hover:border-emerald-700"
            }`}
          >
            <div className="flex items-start justify-between gap-2">
              <div className="flex items-start gap-2 min-w-0 flex-1">
                {selectingForAnalyzer && (
                  <span className={`mt-0.5 w-4 h-4 rounded-full border-2 shrink-0 flex items-center justify-center ${
                    selectedVersion === output.version
                      ? "border-red-500 bg-red-500"
                      : "border-neutral-300 dark:border-neutral-600"
                  }`}>
                    {selectedVersion === output.version && (
                      <span className="w-1.5 h-1.5 rounded-full bg-white" />
                    )}
                  </span>
                )}
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <FileSpreadsheet className="w-4 h-4 text-emerald-500 shrink-0" />
                    <p className="text-xs font-semibold text-neutral-800 dark:text-neutral-200 truncate">
                      {output.label}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-neutral-500 dark:text-neutral-400 ml-6">
                    <span>{output.rows.toLocaleString()} rows</span>
                    <span>{output.cols} cols</span>
                    {output.sourcesCount > 0 && (
                      <span>
                        {output.sourcesCount} source{output.sourcesCount !== 1 ? "s" : ""}
                      </span>
                    )}
                  </div>
                  <p className="text-[9px] text-neutral-400 dark:text-neutral-500 mt-1 ml-6">
                    {new Date(output.timestamp).toLocaleString()}
                  </p>
                </div>
              </div>
              {!selectingForAnalyzer && (
                <div className="flex items-center gap-1 shrink-0">
                  <button
                    onClick={() => handleDownloadCsv(output.version)}
                    disabled={downloadingVersion === output.version}
                    className="p-2 rounded-lg text-neutral-400 hover:text-emerald-600 dark:hover:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950/30 transition-colors disabled:opacity-50"
                    title="Download as CSV"
                  >
                    {downloadingVersion === output.version ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <Download className="w-4 h-4" />
                    )}
                  </button>
                  {confirmDeleteVersion === output.version ? (
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => handleDelete(output.version)}
                        disabled={deletingVersion === output.version}
                        className="px-2 py-1 text-[10px] font-bold bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors disabled:opacity-50"
                      >
                        {deletingVersion === output.version ? (
                          <Loader2 className="w-3 h-3 animate-spin" />
                        ) : (
                          "Confirm"
                        )}
                      </button>
                      <button
                        onClick={() => setConfirmDeleteVersion(null)}
                        className="px-2 py-1 text-[10px] font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 rounded-md hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                      >
                        Cancel
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => setConfirmDeleteVersion(output.version)}
                      className="p-2 rounded-lg text-neutral-400 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/30 transition-colors"
                      title="Delete output"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  )}
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Selection mode footer */}
      {selectingForAnalyzer && (
        <div className="px-5 py-3 border-t border-neutral-200/80 dark:border-neutral-700/80 flex items-center gap-2">
          <button
            onClick={handleSendToAnalyzer}
            disabled={selectedVersion === null || sending}
            className="flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {sending ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
            ) : (
              <BarChart3 className="w-3.5 h-3.5" />
            )}
            {sending ? "Sending..." : "Send"}
          </button>
          <button
            onClick={() => { setSelectingForAnalyzer(false); setSelectedVersion(null); setSendResult(null); }}
            disabled={sending}
            className="px-4 py-2 rounded-xl text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
        </div>
      )}
    </motion.aside>
  );
}
