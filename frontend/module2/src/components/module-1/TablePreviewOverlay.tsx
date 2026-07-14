import React, { useEffect, useState, useRef } from "react";
import { X, Loader2, Database, ArrowLeft, Columns } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";

interface PreviewTarget {
  factGroupId: string;
  dimGroupId: string;
  factKey: string | null;
  dimKey: string | null;
  extraKeys?: Array<{ fact_key: string; dim_key: string }>;
}

interface GroupData {
  columns: string[];
  rows: Record<string, any>[];
  totalRows: number;
}

interface TablePreviewOverlayProps {
  sessionId: string;
  target: PreviewTarget;
  onClose: () => void;
}

function TablePane({
  label,
  role,
  data,
  loading,
  highlightCols,
}: {
  label: string;
  role: "Fact" | "Dimension";
  data: GroupData | null;
  loading: boolean;
  highlightCols: Set<string>;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);

  if (loading) {
    return (
      <div className="flex-1 min-w-0 flex flex-col border border-neutral-200 dark:border-neutral-700 rounded-2xl overflow-hidden bg-white dark:bg-neutral-900">
        <div className="px-5 py-3 bg-neutral-50 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-neutral-400" />
            <span className="text-sm font-bold text-neutral-700 dark:text-neutral-300">{role}: {label}</span>
          </div>
        </div>
        <div className="flex-1 flex items-center justify-center p-12">
          <Loader2 className="w-6 h-6 animate-spin text-red-500" />
          <span className="ml-3 text-sm text-neutral-500">Loading preview...</span>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex-1 min-w-0 flex flex-col border border-neutral-200 dark:border-neutral-700 rounded-2xl overflow-hidden bg-white dark:bg-neutral-900">
        <div className="px-5 py-3 bg-neutral-50 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-neutral-400" />
            <span className="text-sm font-bold text-neutral-700 dark:text-neutral-300">{role}: {label}</span>
          </div>
        </div>
        <div className="flex-1 flex items-center justify-center p-12">
          <span className="text-sm text-neutral-400">No data available</span>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 min-w-0 flex flex-col border border-neutral-200 dark:border-neutral-700 rounded-2xl overflow-hidden bg-white dark:bg-neutral-900">
      <div className="px-5 py-3 bg-neutral-50 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Database className="w-4 h-4 text-neutral-400" />
          <span className="text-sm font-bold text-neutral-700 dark:text-neutral-300">{role}: {label}</span>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-[10px] font-bold text-neutral-500 bg-neutral-200 dark:bg-neutral-700 px-2 py-0.5 rounded-full">
            {data.totalRows.toLocaleString()} rows
          </span>
          <span className="text-[10px] font-bold text-neutral-500 bg-neutral-200 dark:bg-neutral-700 px-2 py-0.5 rounded-full">
            {data.columns.length} cols
          </span>
        </div>
      </div>

      <div ref={scrollRef} className="flex-1 overflow-auto">
        <table className="min-w-full text-xs border-collapse">
          <thead className="sticky top-0 z-10">
            <tr>
              <th className="px-3 py-2 text-left font-bold text-neutral-400 dark:text-neutral-500 uppercase text-[10px] tracking-wider whitespace-nowrap bg-neutral-100 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700 w-10">
                #
              </th>
              {data.columns.map((c) => (
                <th
                  key={c}
                  className={`px-3 py-2 text-left font-bold uppercase text-[10px] tracking-wider whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700 ${
                    highlightCols.has(c)
                      ? "bg-red-50 dark:bg-red-950/40 text-red-600 dark:text-red-400"
                      : "bg-neutral-100 dark:bg-neutral-800 text-neutral-400 dark:text-neutral-500"
                  }`}
                >
                  {c}
                  {highlightCols.has(c) && (
                    <span className="ml-1 text-[8px] font-bold bg-red-100 dark:bg-red-900/50 text-red-500 dark:text-red-400 px-1 py-px rounded">
                      KEY
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {data.rows.map((row, i) => (
              <tr key={i} className="hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors">
                <td className="px-3 py-1.5 whitespace-nowrap text-neutral-300 dark:text-neutral-600 font-mono bg-neutral-50/50 dark:bg-neutral-850 border-r border-neutral-100 dark:border-neutral-800">
                  {i + 1}
                </td>
                {data.columns.map((c) => (
                  <td
                    key={c}
                    className={`px-3 py-1.5 whitespace-nowrap max-w-[220px] truncate ${
                      highlightCols.has(c)
                        ? "bg-red-50/40 dark:bg-red-950/20 text-neutral-800 dark:text-neutral-200 font-medium"
                        : "text-neutral-700 dark:text-neutral-300"
                    }`}
                    title={row[c] != null ? String(row[c]) : ""}
                  >
                    {row[c] != null ? String(row[c]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data.totalRows > data.rows.length && (
        <div className="px-5 py-2 bg-neutral-50 dark:bg-neutral-800 border-t border-neutral-200 dark:border-neutral-700 text-[10px] text-neutral-400 font-bold">
          Showing first {data.rows.length} of {data.totalRows.toLocaleString()} rows
        </div>
      )}
    </div>
  );
}

export default function TablePreviewOverlay({ sessionId, target, onClose }: TablePreviewOverlayProps) {
  const [factData, setFactData] = useState<GroupData | null>(null);
  const [dimData, setDimData] = useState<GroupData | null>(null);
  const [factLoading, setFactLoading] = useState(true);
  const [dimLoading, setDimLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function fetchGroup(groupId: string, setter: (d: GroupData) => void, setLoading: (l: boolean) => void) {
      setLoading(true);
      try {
        const resp = await fetch("/api/group-preview", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sessionId, groupId }),
          signal: controller.signal,
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.error || `HTTP ${resp.status}`);
        }
        const data = await resp.json();
        setter(data);
      } catch (e: any) {
        if (e.name !== "AbortError") setError(e.message);
      } finally {
        setLoading(false);
      }
    }

    fetchGroup(target.factGroupId, setFactData, setFactLoading);
    fetchGroup(target.dimGroupId, setDimData, setDimLoading);

    return () => controller.abort();
  }, [sessionId, target.factGroupId, target.dimGroupId]);

  const factHighlight = new Set<string>();
  const dimHighlight = new Set<string>();
  if (target.factKey) factHighlight.add(target.factKey);
  if (target.dimKey) dimHighlight.add(target.dimKey);
  if (target.extraKeys) {
    for (const ek of target.extraKeys) {
      if (ek.fact_key) factHighlight.add(ek.fact_key);
      if (ek.dim_key) dimHighlight.add(ek.dim_key);
    }
  }

  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleEsc);
    return () => window.removeEventListener("keydown", handleEsc);
  }, [onClose]);

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="fixed inset-0 z-50 flex flex-col bg-white dark:bg-neutral-950"
      >
        {/* Top bar */}
        <div className="flex items-center justify-between px-6 py-3 border-b border-neutral-200 dark:border-neutral-800 bg-white dark:bg-neutral-950 shrink-0">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={onClose}
              className="flex items-center gap-1.5 text-sm font-bold text-neutral-500 hover:text-red-600 dark:hover:text-red-400 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back
            </button>
            <div className="h-5 w-px bg-neutral-200 dark:bg-neutral-700" />
            <div className="flex items-center gap-2">
              <Columns className="w-4 h-4 text-red-500" />
              <h2 className="text-sm font-bold text-neutral-900 dark:text-white">
                Table Preview
              </h2>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-neutral-100 dark:hover:bg-neutral-800 text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-300 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {error && (
          <div className="px-6 py-3 bg-red-50 dark:bg-red-950/30 border-b border-red-200 dark:border-red-800 text-sm text-red-600 dark:text-red-400">
            {error}
          </div>
        )}

        {/* Two panes */}
        <div className="flex-1 flex gap-4 p-4 min-h-0 overflow-hidden">
          <TablePane
            label={target.factGroupId}
            role="Fact"
            data={factData}
            loading={factLoading}
            highlightCols={factHighlight}
          />
          <TablePane
            label={target.dimGroupId}
            role="Dimension"
            data={dimData}
            loading={dimLoading}
            highlightCols={dimHighlight}
          />
        </div>
      </motion.div>
    </AnimatePresence>
  );
}
