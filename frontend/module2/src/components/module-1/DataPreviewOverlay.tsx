import React, { useState, useEffect, useRef } from "react";
import { X, ArrowLeft, Table2, FileSpreadsheet } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";

interface InventoryItem {
  table_key: string;
  rows: number;
  cols: number;
  [key: string]: any;
}

interface DataPreviewOverlayProps {
  previews: Record<string, { columns: string[]; rows: any[] }>;
  inventory: InventoryItem[];
  onClose: () => void;
  title?: string;
}

function shortLabel(tableKey: string): string {
  const parts = tableKey.split("::");
  const file = parts[0].split(/[/\\]/).pop() || parts[0];
  const sheet = parts[1] || "";
  if (sheet) return `${file} — ${sheet}`;
  return file;
}

export default function DataPreviewOverlay({ previews, inventory, onClose, title = "Data Preview" }: DataPreviewOverlayProps) {
  const tableKeys = Object.keys(previews);
  const [activeKey, setActiveKey] = useState<string>(tableKeys[0] || "");
  const tabBarRef = useRef<HTMLDivElement>(null);

  const activeData = activeKey ? previews[activeKey] : undefined;
  const activeMeta = activeKey ? inventory.find((inv) => inv.table_key === activeKey) : undefined;

  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleEsc);
    return () => window.removeEventListener("keydown", handleEsc);
  }, [onClose]);

  useEffect(() => {
    if (tableKeys.length > 0 && !previews[activeKey]) {
      setActiveKey(tableKeys[0]);
    }
  }, [tableKeys, activeKey, previews]);

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="fixed inset-0 z-[9999] flex flex-col bg-white dark:bg-neutral-950"
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
              <Table2 className="w-4 h-4 text-red-500" />
              <h2 className="text-sm font-bold text-neutral-900 dark:text-white">{title}</h2>
            </div>
            {activeMeta && (
              <div className="flex items-center gap-2 ml-2">
                <span className="text-[10px] font-bold text-neutral-500 bg-neutral-100 dark:bg-neutral-800 px-2 py-0.5 rounded-full">
                  {activeMeta.rows.toLocaleString()} rows
                </span>
                <span className="text-[10px] font-bold text-neutral-500 bg-neutral-100 dark:bg-neutral-800 px-2 py-0.5 rounded-full">
                  {activeMeta.cols} cols
                </span>
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-neutral-100 dark:hover:bg-neutral-800 text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-300 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Tab bar at the top */}
        {tableKeys.length > 0 && (
          <div className="shrink-0 border-b border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-900">
            <div
              ref={tabBarRef}
              className="flex overflow-x-auto scrollbar-thin px-2 py-1 gap-0.5"
            >
              {tableKeys.map((key) => {
                const isActive = key === activeKey;
                return (
                  <button
                    key={key}
                    type="button"
                    onClick={() => setActiveKey(key)}
                    className={`flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-lg whitespace-nowrap transition-colors ${
                      isActive
                        ? "bg-white dark:bg-neutral-950 text-red-600 dark:text-red-400 border border-neutral-200 dark:border-neutral-700 shadow-sm"
                        : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400 border border-transparent hover:bg-neutral-200 dark:hover:bg-neutral-700 hover:text-neutral-700 dark:hover:text-neutral-300"
                    }`}
                  >
                    <FileSpreadsheet className="w-3 h-3" />
                    {shortLabel(key)}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Spreadsheet area */}
        <div className="flex-1 min-h-0 overflow-auto">
          {activeData && activeData.columns.length > 0 ? (
            <table className="min-w-full text-xs border-collapse">
              <thead className="sticky top-0 z-10">
                <tr>
                  <th className="px-3 py-2 text-left font-bold text-neutral-400 dark:text-neutral-500 uppercase text-[10px] tracking-wider whitespace-nowrap bg-neutral-100 dark:bg-neutral-800 border-b border-r border-neutral-200 dark:border-neutral-700 w-10">
                    #
                  </th>
                  {activeData.columns.map((c) => (
                    <th
                      key={c}
                      className="px-3 py-2 text-left font-bold text-neutral-500 dark:text-neutral-400 uppercase text-[10px] tracking-wider whitespace-nowrap bg-neutral-100 dark:bg-neutral-800 border-b border-neutral-200 dark:border-neutral-700"
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                {activeData.rows.map((row, i) => (
                  <tr key={i} className="hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors">
                    <td className="px-3 py-1.5 whitespace-nowrap text-neutral-300 dark:text-neutral-600 font-mono bg-neutral-50/50 dark:bg-neutral-900 border-r border-neutral-100 dark:border-neutral-800 text-[10px]">
                      {i + 1}
                    </td>
                    {activeData.columns.map((c) => (
                      <td
                        key={c}
                        className="px-3 py-1.5 whitespace-nowrap max-w-[240px] truncate text-neutral-700 dark:text-neutral-300"
                        title={row[c] != null ? String(row[c]) : ""}
                      >
                        {row[c] != null ? (
                          String(row[c])
                        ) : (
                          <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="flex items-center justify-center h-full text-sm text-neutral-400">
              {tableKeys.length === 0 ? "No tables loaded" : "No data in this table"}
            </div>
          )}
        </div>

        {/* Row limit footer */}
        {activeData && activeMeta && activeMeta.rows > activeData.rows.length && (
          <div className="px-6 py-1.5 border-t border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-900 text-[10px] text-neutral-400 font-bold shrink-0">
            Showing first {activeData.rows.length} of {activeMeta.rows.toLocaleString()} rows
          </div>
        )}
      </motion.div>
    </AnimatePresence>
  );
}
