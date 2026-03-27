import { useState } from "react";
import { motion } from "motion/react";
import { CheckCircle2, ChevronDown, ChevronRight, Clock, Package } from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import type { MergeOutput } from "../../types";

interface MergeReportProps {
  mergeHistory: any[];
  mergeOutputs: MergeOutput[];
}

export default function MergeReport({
  mergeHistory,
  mergeOutputs,
}: MergeReportProps) {
  const [historyOpen, setHistoryOpen] = useState(true);

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      <SurfaceCard noPadding>
        <div className="rounded-3xl bg-gradient-to-r from-emerald-600 to-teal-600 p-7 text-white">
          <div className="flex items-start justify-between gap-6">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <CheckCircle2 className="w-6 h-6" />
                <h2 className="text-xl font-semibold tracking-tight">Merge Step Complete</h2>
              </div>
              <p className="text-emerald-50/90 text-sm max-w-xl">
                {mergeOutputs.length > 0
                  ? `You have ${mergeOutputs.length} merge output${mergeOutputs.length !== 1 ? "s" : ""} available. Use the Merge Outputs panel to download or manage them.`
                  : "No merge outputs created yet."}
              </p>
            </div>
            <div className="flex gap-3 text-center shrink-0">
              <div className="rounded-xl bg-white/15 px-4 py-3 backdrop-blur">
                <p className="text-[10px] uppercase tracking-wider text-emerald-200">Outputs</p>
                <p className="text-lg font-bold tabular-nums mt-0.5">{mergeOutputs.length}</p>
              </div>
            </div>
          </div>
        </div>
      </SurfaceCard>

      {mergeHistory && mergeHistory.length > 0 && (
        <SurfaceCard noPadding>
          <button
            onClick={() => setHistoryOpen((p) => !p)}
            className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-3xl"
          >
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 text-neutral-400" />
              <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                Merge History ({mergeHistory.length} version{mergeHistory.length !== 1 ? "s" : ""})
              </h3>
            </div>
            {historyOpen ? <ChevronDown className="w-4 h-4 text-neutral-400" /> : <ChevronRight className="w-4 h-4 text-neutral-400" />}
          </button>
          {historyOpen && (
            <div className="px-6 pb-5 space-y-2">
              {mergeHistory.map((entry: any) => (
                <div
                  key={entry.version}
                  className="flex items-center justify-between rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 p-3 transition-colors"
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-neutral-100 text-neutral-600 dark:bg-neutral-700 dark:text-neutral-300">
                      v{entry.version}
                    </span>
                    <div className="min-w-0">
                      <p className="text-xs font-medium text-neutral-700 dark:text-neutral-300 truncate">
                        {entry.file_label || entry.table_name}
                      </p>
                      <p className="text-[10px] text-neutral-400">
                        {entry.rows?.toLocaleString()} rows &middot; {entry.cols} cols &middot; {new Date(entry.timestamp).toLocaleTimeString()}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </SurfaceCard>
      )}

      <SurfaceCard>
        <div className="flex items-center gap-3 text-neutral-500 dark:text-neutral-400">
          <Package className="w-5 h-5 shrink-0" />
          <p className="text-sm">
            Navigate back to <span className="font-semibold text-neutral-700 dark:text-neutral-200">Merge (Step 6)</span> using the sidebar to perform additional merges. All outputs are accessible from the Merge Outputs panel.
          </p>
        </div>
      </SurfaceCard>
    </motion.div>
  );
}
