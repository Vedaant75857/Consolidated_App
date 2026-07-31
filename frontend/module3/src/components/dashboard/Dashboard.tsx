import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ChevronDown, ChevronUp, ArrowRight, Download, Loader2 } from "lucide-react";
import type { ViewResult, ViewConfig } from "../../types";
import ViewPanel from "./ViewPanel";

interface Props {
  views: ViewResult[];
  onExportCsv: (viewId: string) => void;
  onExportCompleteAnalysis?: () => Promise<void>;
  onRecomputeView?: (viewId: string, config: ViewConfig) => Promise<ViewResult>;
  onViewProcurementFeasibility?: () => void;
}

/** Collapsible accordion-style panel for grouping dashboard sections. */
function CollapsiblePanel({
  title,
  defaultExpanded,
  children,
}: {
  title: string;
  defaultExpanded: boolean;
  children: React.ReactNode;
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors"
      >
        <h3 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">
          {title}
        </h3>
        {expanded ? (
          <ChevronUp className="w-5 h-5 text-neutral-400" />
        ) : (
          <ChevronDown className="w-5 h-5 text-neutral-400" />
        )}
      </button>
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="border-t border-neutral-100 dark:border-neutral-800">
              {children}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default function Dashboard({
  views,
  onExportCsv,
  onExportCompleteAnalysis,
  onRecomputeView,
  onViewProcurementFeasibility,
}: Props) {
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  const handleExportCompleteAnalysis = async () => {
    if (!onExportCompleteAnalysis || exporting) return;
    setExporting(true);
    setExportError(null);
    try {
      await onExportCompleteAnalysis();
    } catch (err: any) {
      setExportError(err?.message || "Excel export failed.");
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">
            Analysis Dashboard
          </h2>
          <p className="text-xs text-neutral-500">
            {views.length} view{views.length !== 1 ? "s" : ""} generated
          </p>
        </div>
        {onExportCompleteAnalysis && (
          <button
            type="button"
            onClick={handleExportCompleteAnalysis}
            disabled={exporting || views.length === 0}
            aria-busy={exporting}
            className="inline-flex items-center gap-2 rounded-xl bg-primary px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-primary-hover disabled:cursor-not-allowed disabled:opacity-60"
          >
            {exporting ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
            ) : (
              <Download className="h-4 w-4" aria-hidden="true" />
            )}
            {exporting ? "Preparing Excel…" : "Export complete analysis"}
          </button>
        )}
      </div>

      {exportError && (
        <p role="alert" className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-300">
          {exportError}
        </p>
      )}

      <CollapsiblePanel title="Detailed Summary" defaultExpanded={true}>
        <div className="space-y-6 p-4">
          {views.map((view) => (
            <ViewPanel
              key={view.viewId}
              view={view}
              onExportCsv={() => onExportCsv(view.viewId)}
              onRecomputeView={onRecomputeView}
            />
          ))}

          {onViewProcurementFeasibility && (
            <div className="flex justify-center pt-2 pb-4">
              <button
                onClick={onViewProcurementFeasibility}
                className="flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors shadow-sm"
              >
                View Procurement Feasibility
                <ArrowRight className="w-4 h-4" />
              </button>
            </div>
          )}
        </div>
      </CollapsiblePanel>
    </div>
  );
}
