import { useState } from "react";
import { motion } from "motion/react";
import { Download, Package, Trash2, X, Loader2, FileSpreadsheet, BarChart3, Database, CheckCircle2, AlertCircle } from "lucide-react";

import type { MergeOutput } from "../../types";
import TransferOverlay from "../common/TransferOverlay";

const NORMALIZER_FE = import.meta.env.VITE_NORMALIZER_FE ?? "http://localhost:3003";
const ANALYZER_FE = import.meta.env.VITE_ANALYZER_FE ?? "http://localhost:3004";

type SelectionTarget = "analyzer" | "normalizer";

interface MergeOutputsPanelProps {
  mergeOutputs: MergeOutput[];
  sessionId: string;
  apiKey: string;
  onClose: () => void;
  onDeleteOutput: (version: number) => Promise<void>;
}

export default function MergeOutputsPanel({
  mergeOutputs,
  sessionId,
  apiKey,
  onClose,
  onDeleteOutput,
}: MergeOutputsPanelProps) {
  const [downloadingVersion, setDownloadingVersion] = useState<number | null>(null);
  const [downloadingAll, setDownloadingAll] = useState(false);
  const [deletingVersion, setDeletingVersion] = useState<number | null>(null);
  const [confirmDeleteVersion, setConfirmDeleteVersion] = useState<number | null>(null);

  const [selectionTarget, setSelectionTarget] = useState<SelectionTarget | null>(null);
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [sending, setSending] = useState(false);
  const [sendResult, setSendResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [transferOverlay, setTransferOverlay] = useState<{ visible: boolean; destination: string } | null>(null);

  const isSelecting = selectionTarget !== null;

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

  const handleFallbackDownload = () => {
    if (selectedVersion !== null) handleDownloadCsv(selectedVersion);
  };

  const handleSendToAnalyzer = async () => {
    if (selectedVersion === null) return;
    setSending(true);
    setSendResult(null);
    setTransferOverlay({ visible: true, destination: "Spend Summarizer" });

    try {
      const res = await fetch("/api/merge/transfer-to-analyzer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, version: selectedVersion }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || "Transfer failed");

      const analyzerSessionId: string = data.analyzerSessionId;
      if (!analyzerSessionId || typeof analyzerSessionId !== "string") {
        throw new Error("Transfer succeeded but no session ID was returned by the Analyzer.");
      }
      localStorage.setItem("summarizer_api_key", apiKey);
      const url = `${ANALYZER_FE}?sessionId=${encodeURIComponent(analyzerSessionId)}&source=stitcher`;
      window.open(url, "_blank");
      setSendResult({ ok: true, message: "Opened Spend Summarizer in a new tab" });
      setSelectionTarget(null);
      setSelectedVersion(null);
    } catch (err: any) {
      setSendResult({ ok: false, message: err.message || "Send failed" });
    } finally {
      setSending(false);
      setTransferOverlay(null);
    }
  };

  const handleSendToNormalizer = async () => {
    if (selectedVersion === null) return;
    setSending(true);
    setSendResult(null);
    setTransferOverlay({ visible: true, destination: "Data Normalizer" });

    try {
      const res = await fetch("/api/merge/transfer-to-normalizer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, version: selectedVersion }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || "Transfer failed");

      const normalizerSessionId: string = data.normalizerSessionId || "";
      localStorage.setItem("datastitcher_apikey", apiKey);
      const url = `${NORMALIZER_FE}?imported=true&source=stitcher${normalizerSessionId ? `&sessionId=${encodeURIComponent(normalizerSessionId)}` : ""}`;
      window.open(url, "_blank");
      setSendResult({ ok: true, message: "Opened Data Normalizer in a new tab" });
      setSelectionTarget(null);
      setSelectedVersion(null);
    } catch (err: any) {
      setSendResult({ ok: false, message: err.message || "Send failed" });
    } finally {
      setSending(false);
      setTransferOverlay(null);
    }
  };

  const handleSend = () => {
    if (selectionTarget === "analyzer") handleSendToAnalyzer();
    else if (selectionTarget === "normalizer") handleSendToNormalizer();
  };

  const cancelSelection = () => {
    setSelectionTarget(null);
    setSelectedVersion(null);
    setSendResult(null);
  };

  const accentColor = selectionTarget === "normalizer" ? "blue" : "red";

  const accentClasses = {
    banner: accentColor === "blue"
      ? "bg-blue-50 dark:bg-blue-950/30 border-b border-blue-200/80 dark:border-blue-800/60"
      : "bg-red-50 dark:bg-red-950/30 border-b border-red-200/80 dark:border-red-800/60",
    bannerText: accentColor === "blue"
      ? "text-blue-700 dark:text-blue-300"
      : "text-red-700 dark:text-red-300",
    selectedCard: accentColor === "blue"
      ? "border-blue-400 dark:border-blue-600 bg-blue-50 dark:bg-blue-950/30 ring-1 ring-blue-300 dark:ring-blue-700 cursor-pointer"
      : "border-red-400 dark:border-red-600 bg-red-50 dark:bg-red-950/30 ring-1 ring-red-300 dark:ring-red-700 cursor-pointer",
    hoverCard: accentColor === "blue"
      ? "border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 hover:border-blue-300 dark:hover:border-blue-700 cursor-pointer"
      : "border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 hover:border-red-300 dark:hover:border-red-700 cursor-pointer",
    radio: accentColor === "blue"
      ? "border-blue-500 bg-blue-500"
      : "border-red-500 bg-red-500",
    sendBtn: accentColor === "blue"
      ? "bg-blue-600 text-white hover:bg-blue-700"
      : "bg-red-600 text-white hover:bg-red-700",
  };

  const targetLabel = selectionTarget === "normalizer" ? "Data Normalizer" : "Spend Summarizer";

  return (
    <motion.aside
      initial={{ x: 384, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      exit={{ x: 384, opacity: 0 }}
      transition={{ type: "spring", damping: 26, stiffness: 300 }}
      className="w-96 bg-white/95 dark:bg-neutral-900/95 backdrop-blur-xl border-l border-neutral-200/80 dark:border-neutral-700/80 flex flex-col z-10 shrink-0"
    >
      <TransferOverlay
        visible={!!transferOverlay?.visible}
        destinationName={transferOverlay?.destination ?? ""}
        sourceName="Data Stitcher"
      />
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
        <div className="flex items-center gap-1.5">
          {!isSelecting && (
            <>
              <button
                onClick={() => { setSelectionTarget("normalizer"); setSendResult(null); }}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-xl text-xs font-semibold bg-blue-50 dark:bg-blue-950/30 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800 hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors"
                title="Send an output to the Data Normalizer"
              >
                <Database className="w-3.5 h-3.5" />
                Send to Normalizer
              </button>
              <button
                onClick={() => { setSelectionTarget("analyzer"); setSendResult(null); }}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-xl text-xs font-semibold bg-red-50 dark:bg-red-950/30 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors"
                title="Send an output to the Spend Summarizer"
              >
                <BarChart3 className="w-3.5 h-3.5" />
                Send to Summarizer
              </button>
              <button
                onClick={handleDownloadAll}
                disabled={downloadingAll}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-xl text-xs font-semibold bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800 hover:bg-emerald-100 dark:hover:bg-emerald-900/40 transition-colors disabled:opacity-50"
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
      {isSelecting && (
        <div className={`px-5 py-2.5 ${accentClasses.banner}`}>
          <p className={`text-xs font-medium ${accentClasses.bannerText}`}>
            Select an output to send to the {targetLabel}
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
          {!sendResult.ok && (
            <button
              onClick={handleFallbackDownload}
              className="ml-1 underline whitespace-nowrap hover:no-underline"
            >
              Download instead
            </button>
          )}
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
            onClick={isSelecting ? () => setSelectedVersion(output.version) : undefined}
            className={`rounded-xl border p-3.5 transition-colors ${
              isSelecting
                ? selectedVersion === output.version
                  ? accentClasses.selectedCard
                  : accentClasses.hoverCard
                : "border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 hover:border-emerald-300 dark:hover:border-emerald-700"
            }`}
          >
            <div className="flex items-start justify-between gap-2">
              <div className="flex items-start gap-2 min-w-0 flex-1">
                {isSelecting && (
                  <span className={`mt-0.5 w-4 h-4 rounded-full border-2 shrink-0 flex items-center justify-center ${
                    selectedVersion === output.version
                      ? accentClasses.radio
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
              {!isSelecting && (
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
      {isSelecting && (
        <div className="px-5 py-3 border-t border-neutral-200/80 dark:border-neutral-700/80 flex items-center gap-2">
          <button
            onClick={handleSend}
            disabled={selectedVersion === null || sending}
            className={`flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${accentClasses.sendBtn}`}
          >
            {sending ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
            ) : selectionTarget === "normalizer" ? (
              <Database className="w-3.5 h-3.5" />
            ) : (
              <BarChart3 className="w-3.5 h-3.5" />
            )}
            {sending ? "Sending..." : "Send"}
          </button>
          <button
            onClick={cancelSelection}
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
