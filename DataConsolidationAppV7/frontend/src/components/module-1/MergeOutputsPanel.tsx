import { useState } from "react";
import { motion } from "motion/react";
import { Download, Package, Trash2, X, Loader2, FileSpreadsheet } from "lucide-react";

import type { MergeOutput } from "../../types";

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
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-200 hover:bg-neutral-100 dark:hover:bg-neutral-800 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Output List */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-2">
        {mergeOutputs.map((output) => (
          <div
            key={output.version}
            className="rounded-xl border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800/50 p-3.5 transition-colors hover:border-emerald-300 dark:hover:border-emerald-700"
          >
            <div className="flex items-start justify-between gap-2">
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
            </div>
          </div>
        ))}
      </div>
    </motion.aside>
  );
}
