import React, { useEffect, useRef, useState } from "react";
import { ChevronUp, ChevronDown, AlertCircle, CheckCircle2, Info, Trash2 } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";

export type LogEntry = {
  id: number;
  timestamp: Date;
  step: string;
  type: "info" | "success" | "error";
  message: string;
};

interface StatusLogProps {
  entries: LogEntry[];
  onClear: () => void;
}

const TYPE_CONFIG = {
  info: { icon: Info, text: "text-neutral-700 dark:text-neutral-300", bg: "bg-neutral-50/30 dark:bg-neutral-800/30", accent: "text-red-600 dark:text-red-400", dot: "bg-neutral-400", badge: "bg-neutral-50 dark:bg-neutral-800/50" },
  success: { icon: CheckCircle2, text: "text-emerald-700 dark:text-emerald-400", bg: "bg-emerald-50/30 dark:bg-emerald-950/20", accent: "text-emerald-600 dark:text-emerald-400", dot: "bg-emerald-500", badge: "bg-emerald-50 dark:bg-emerald-950/30" },
  error: { icon: AlertCircle, text: "text-red-700 dark:text-red-400", bg: "bg-red-50/30 dark:bg-red-950/20", accent: "text-red-600 dark:text-red-400", dot: "bg-red-500", badge: "bg-red-50 dark:bg-red-950/30" },
};

function formatTime(date: Date) {
  return date.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export default function StatusLog({ entries, onClear }: StatusLogProps) {
  const [collapsed, setCollapsed] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!collapsed && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, collapsed]);

  const errorCount = entries.filter((e) => e.type === "error").length;
  const successCount = entries.filter((e) => e.type === "success").length;

  const prevErrorCountRef = useRef(errorCount);
  useEffect(() => {
    if (errorCount > prevErrorCountRef.current) {
      setCollapsed(false);
    }
    prevErrorCountRef.current = errorCount;
  }, [errorCount]);

  const latestId = entries.length > 0 ? entries[entries.length - 1].id : -1;

  return (
    <div className="border-t border-neutral-200/80 dark:border-neutral-700/80 bg-white/80 dark:bg-neutral-900/80 backdrop-blur-sm flex-shrink-0 rounded-t-2xl shadow-[0_-2px_12px_rgba(0,0,0,0.03)]">
      <button
        type="button"
        aria-label={collapsed ? "Expand status log" : "Collapse status log"}
        className="w-full flex items-center justify-between px-5 py-2.5 cursor-pointer select-none hover:bg-neutral-50/50 dark:hover:bg-neutral-800/50 transition-colors"
        onClick={() => setCollapsed((c) => !c)}
      >
        <div className="flex items-center gap-3">
          {entries.length > 0 && (
            <span className="relative flex h-2.5 w-2.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-60" />
              <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-red-500" />
            </span>
          )}
          <span className="text-xs font-semibold tracking-tight text-neutral-600 dark:text-neutral-300">Live pipeline activity</span>
          <span className="text-[10px] font-medium text-neutral-400 dark:text-neutral-500">{entries.length} entries</span>
          {errorCount > 0 && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-red-100 text-red-600 dark:bg-red-950/40 dark:text-red-400">
              {errorCount} error{errorCount !== 1 ? "s" : ""}
            </span>
          )}
          {successCount > 0 && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-100 text-emerald-600 dark:bg-emerald-950/40 dark:text-emerald-400">
              {successCount} done
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {entries.length > 0 && (
            <button
              onClick={(e) => { e.stopPropagation(); if (window.confirm("Clear the entire status log?")) onClear(); }}
              className="p-1 rounded-lg hover:bg-neutral-200 dark:hover:bg-neutral-700 text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-300 transition-colors"
              title="Clear log"
            >
              <Trash2 className="w-3.5 h-3.5" />
            </button>
          )}
          {collapsed ? <ChevronUp className="w-4 h-4 text-neutral-400 dark:text-neutral-500" /> : <ChevronDown className="w-4 h-4 text-neutral-400 dark:text-neutral-500" />}
        </div>
      </button>

      {!collapsed && (
        <div ref={scrollRef} className="max-h-44 overflow-y-auto border-t border-neutral-100 dark:border-neutral-800">
          {entries.length === 0 ? (
            <div className="px-5 py-6 text-center text-xs text-neutral-400 dark:text-neutral-500">No activity yet. Logs will appear here as you work through the steps.</div>
          ) : (
            <div className="divide-y divide-neutral-50/80 dark:divide-neutral-800/80">
              <AnimatePresence initial={false}>
                {entries.map((entry) => {
                  const cfg = TYPE_CONFIG[entry.type];
                  const Icon = cfg.icon;
                  const isNewest = entry.id === latestId;
                  return (
                    <motion.div
                      key={entry.id}
                      initial={{ opacity: 0, y: 8 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ duration: 0.2 }}
                      className={`flex items-start gap-3 px-5 py-2.5 ${cfg.bg}`}
                    >
                      <span className="text-[10px] tabular-nums font-mono text-neutral-400 dark:text-neutral-500 pt-0.5 flex-shrink-0 w-16">
                        {formatTime(entry.timestamp)}
                      </span>
                      <div className="relative mt-1.5 shrink-0">
                        {isNewest && (
                          <span className="absolute -left-0.5 -top-0.5 flex h-3 w-3">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-50" style={{ backgroundColor: entry.type === "error" ? "#ef4444" : entry.type === "success" ? "#10b981" : "#ef4444" }} />
                          </span>
                        )}
                        <span className={`block w-2 h-2 rounded-full ${cfg.dot}`} />
                      </div>
                      <Icon className={`w-3.5 h-3.5 mt-0.5 flex-shrink-0 ${cfg.accent}`} />
                      <span className={`text-[10px] font-bold uppercase tracking-[0.12em] px-2 py-0.5 rounded-full flex-shrink-0 ${cfg.badge} ${cfg.text}`}>
                        {entry.step}
                      </span>
                      <span className="text-xs text-neutral-600 dark:text-neutral-300 leading-relaxed">{entry.message}</span>
                    </motion.div>
                  );
                })}
              </AnimatePresence>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
