import React, { useState, useCallback, useEffect } from "react";
import {
  Loader2, ArrowRight, CheckCircle2,
  Download, Sparkles, Building2, Globe, Calendar, DollarSign,
  MapPin, ClipboardList, CheckSquare, Square
} from "lucide-react";
import { motion } from "motion/react";
import { SurfaceCard, PrimaryButton } from "../common/ui";
import type { LogEntry } from "../module-1/StatusLog";

const OPERATIONS = [
  { id: "supplier_name", label: "Supplier Names", icon: Building2, desc: "Clean & deduplicate supplier names" },
  { id: "supplier_country", label: "Supplier Country", icon: Globe, desc: "Standardize country names" },
  { id: "date", label: "Dates", icon: Calendar, desc: "Normalize date formats" },
  { id: "payment_terms", label: "Payment Terms", icon: ClipboardList, desc: "Extract numeric payment terms" },
  { id: "region", label: "Regions", icon: MapPin, desc: "Classify into NA/EMEA/APAC/LATAM" },
  { id: "plant", label: "Plant/Site", icon: Building2, desc: "Standardize plant codes & names" },
  { id: "currency_conversion", label: "Currency Conversion", icon: DollarSign, desc: "Convert local spend to USD" },
];

interface NormDashboardProps {
  apiKey: string;
  activeTab?: string;
  setActiveTab?: (tab: string) => void;
  addLog?: (stepName: string, type: LogEntry["type"], message: string) => void;
}

export default function NormDashboard({ apiKey, activeTab = "pipeline", setActiveTab, addLog }: NormDashboardProps) {
  const [completedOps, setCompletedOps] = useState<Set<string>>(new Set());
  const [activeOp, setActiveOp] = useState<string | null>(null);
  const [opResults, setOpResults] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [operationPreview, setOperationPreview] = useState<{ columns: string[]; rows: any[] }>({ columns: [], rows: [] });
  const [operationPreviewLoading, setOperationPreviewLoading] = useState(false);
  const [operationPreviewError, setOperationPreviewError] = useState<string | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<Set<string>>(
    new Set(OPERATIONS.map(op => op.id))
  );

  const log = useCallback((type: LogEntry["type"], message: string) => {
    addLog?.("NORMALIZE", type, message);
  }, [addLog]);

  const togglePipelineSelect = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setSelectedPipeline(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const fetchOperationPreview = useCallback(async () => {
    setOperationPreviewLoading(true);
    setOperationPreviewError(null);
    try {
      const res = await fetch("/api/current-preview");
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed to load preview");
      setOperationPreview({
        columns: data.columns || [],
        rows: data.rows || [],
      });
    } catch (err: any) {
      setOperationPreviewError(err.message || "Failed to load preview");
    } finally {
      setOperationPreviewLoading(false);
    }
  }, []);

  const handleRunOperation = useCallback(async (agentId: string) => {
    setActiveOp(agentId);
    const opLabel = OPERATIONS.find(o => o.id === agentId)?.label || agentId;
    log("info", "Running " + opLabel + "…");
    try {
      const res = await fetch("/api/run-normalization", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_id: agentId, kwargs: {}, apiKey }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);

      setOpResults(prev => ({ ...prev, [agentId]: data.message }));
      setCompletedOps(prev => new Set([...prev, agentId]));
      await fetchOperationPreview();
      log("success", opLabel + ": " + data.message);
    } catch (err: any) {
      setOpResults(prev => ({ ...prev, [agentId]: "Error: " + err.message }));
      log("error", opLabel + ": " + err.message);
    } finally {
      setActiveOp(null);
    }
  }, [apiKey, fetchOperationPreview, log]);

  const handleRunPipeline = useCallback(async () => {
    if (selectedPipeline.size === 0) return;
    setLoading(true);
    const agentIds = Array.from(selectedPipeline);
    log("info", "Running pipeline with " + agentIds.length + " agents…");
    try {
      const res = await fetch("/api/run-pipeline", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_ids: agentIds, kwargs: {}, apiKey }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);

      data.results?.forEach((resItem: any) => {
        if (resItem.message) {
          setOpResults(prev => ({ ...prev, [resItem.agent]: resItem.message }));
          setCompletedOps(prev => new Set([...prev, resItem.agent]));
          log("success", resItem.agent + ": " + resItem.message);
        } else if (resItem.error) {
          setOpResults(prev => ({ ...prev, [resItem.agent]: "Error: " + resItem.error }));
          log("error", resItem.agent + ": " + resItem.error);
        }
      });
      if (data.results?.some((resItem: any) => resItem.message)) {
        await fetchOperationPreview();
      }
      log("success", "Pipeline complete.");
    } catch (err: any) {
      log("error", "Pipeline error: " + err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedPipeline, apiKey, fetchOperationPreview, log]);

  useEffect(() => {
    if (
      activeTab !== "pipeline" &&
      activeTab !== "download" &&
      completedOps.has(activeTab) &&
      operationPreview.columns.length === 0 &&
      !operationPreviewLoading
    ) {
      fetchOperationPreview();
    }
  }, [activeTab, completedOps, operationPreview.columns.length, operationPreviewLoading, fetchOperationPreview]);

  const handleDownload = useCallback(() => {
    log("info", "Downloading normalised data…");
    window.location.href = "/api/download";
  }, [log]);

  /* ── Render based on activeTab from sidebar ── */

  // If a specific operation tab is selected from the sidebar, show just that one
  const singleOp = OPERATIONS.find(op => op.id === activeTab);

  if (activeTab === "download") {
    return (
      <SurfaceCard title="Download Normalized Data" subtitle="Export your cleaned and standardized dataset" icon={Download}>
        <div className="space-y-4">
          <p className="text-sm text-neutral-600 dark:text-neutral-400">
            Download your fully normalized dataset as an Excel file. All completed normalizations will be included.
          </p>
          <div className="flex items-center gap-3">
            <PrimaryButton onClick={handleDownload} disabled={loading || activeOp !== null}>
              <Download className="w-4 h-4 mr-2" />
              Download Excel
            </PrimaryButton>
            {completedOps.size > 0 && (
              <span className="text-xs text-emerald-600 dark:text-emerald-400 font-medium">
                {completedOps.size} / {OPERATIONS.length} normalizations complete
              </span>
            )}
          </div>
        </div>
      </SurfaceCard>
    );
  }

  if (singleOp) {
    const Icon = singleOp.icon;
    const isCompleted = completedOps.has(singleOp.id);
    const isRunning = activeOp === singleOp.id;
    const showOperationPreview = isCompleted;
    const highlightedOperationColumns = new Set(
      operationPreview.columns.filter((col) => {
        switch (singleOp.id) {
          case "supplier_name":
            return col.startsWith("NORMALIZED SUPPLIER_NAME_BAIN");
          case "supplier_country":
            return col.startsWith("SUPPLIER COUNTRY NORMALIZED");
          case "date":
            return col.startsWith("Norm_Date_");
          case "payment_terms":
            return (
              col.startsWith("PAYMENT TERMS_NORMALIZED") ||
              col.startsWith("Discount_Payment_Terms") ||
              col.startsWith("Payment_Terms_Doubt")
            );
          case "region":
            return col.startsWith("Norm_Region_");
          case "plant":
            return col.startsWith("Norm_Plant_");
          case "currency_conversion":
            return col.startsWith("SPEND AMOUNT CONVERTED_");
          default:
            return false;
        }
      })
    );
    return (
      <SurfaceCard
        title={singleOp.label}
        subtitle={singleOp.desc}
        icon={Icon}
      >
        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <PrimaryButton
              onClick={() => handleRunOperation(singleOp.id)}
              disabled={isRunning || loading}
            >
              {isRunning ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <ArrowRight className="w-4 h-4 mr-2" />}
              {isRunning ? "Running…" : isCompleted ? "Re-run" : "Run"} {singleOp.label}
            </PrimaryButton>
            {isCompleted && (
              <span className="inline-flex items-center gap-1 text-xs font-medium text-emerald-600">
                <CheckCircle2 className="w-3.5 h-3.5" /> Completed
              </span>
            )}
          </div>
          {opResults[singleOp.id] && (
            <div className={`rounded-xl p-3 text-sm ${
              opResults[singleOp.id].startsWith("Error")
                ? "bg-red-50 dark:bg-red-950/20 text-red-700 dark:text-red-400 border border-red-100 dark:border-red-900/50"
                : "bg-emerald-50 dark:bg-emerald-950/20 text-emerald-700 dark:text-emerald-400 border border-emerald-100 dark:border-emerald-900/50"
            }`}>
              {opResults[singleOp.id]}
            </div>
          )}
          {showOperationPreview && (
            <div className="rounded-2xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900/60 overflow-hidden">
              <div className="flex items-center justify-between gap-3 px-4 py-3 border-b border-neutral-200 dark:border-neutral-700">
                <div>
                  <p className="text-sm font-semibold text-neutral-900 dark:text-neutral-100">{singleOp.label} Preview</p>
                  <p className="text-xs text-neutral-500 dark:text-neutral-400">Top rows from the current working dataset after {singleOp.label.toLowerCase()} runs</p>
                </div>
                <button
                  type="button"
                  onClick={fetchOperationPreview}
                  disabled={operationPreviewLoading}
                  className="text-xs font-medium text-red-600 dark:text-red-400 hover:text-red-700 disabled:opacity-50"
                >
                  {operationPreviewLoading ? "Refreshing..." : "Refresh"}
                </button>
              </div>
              {operationPreviewError ? (
                <div className="px-4 py-4 text-sm text-red-600 dark:text-red-400">{operationPreviewError}</div>
              ) : operationPreviewLoading && operationPreview.columns.length === 0 ? (
                <div className="px-4 py-6 text-sm text-neutral-500 dark:text-neutral-400">Loading preview...</div>
              ) : operationPreview.columns.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead className="bg-neutral-50 dark:bg-neutral-800/80">
                      <tr>
                        <th className="px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wide text-neutral-500 dark:text-neutral-400 border-b border-neutral-200 dark:border-neutral-700">#</th>
                        {operationPreview.columns.map((col) => (
                          <th
                            key={col}
                            className={`px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wide border-b border-neutral-200 dark:border-neutral-700 whitespace-nowrap ${
                              highlightedOperationColumns.has(col)
                                ? "bg-neutral-200/80 dark:bg-neutral-700/80 text-neutral-700 dark:text-neutral-200"
                                : "text-neutral-500 dark:text-neutral-400"
                            }`}
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {operationPreview.rows.map((row, index) => (
                        <tr key={index} className="odd:bg-white even:bg-neutral-50/60 dark:odd:bg-neutral-900/30 dark:even:bg-neutral-800/30">
                          <td className="px-3 py-2 text-xs text-neutral-400 dark:text-neutral-500 border-b border-neutral-100 dark:border-neutral-800">{index + 1}</td>
                          {operationPreview.columns.map((col) => (
                            <td
                              key={`${index}-${col}`}
                              className={`px-3 py-2 border-b border-neutral-100 dark:border-neutral-800 text-neutral-700 dark:text-neutral-300 whitespace-nowrap ${
                                highlightedOperationColumns.has(col)
                                  ? "bg-neutral-100 dark:bg-neutral-800/80"
                                  : ""
                              }`}
                            >
                              {row[col] || "—"}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="px-4 py-6 text-sm text-neutral-500 dark:text-neutral-400">No preview data available yet.</div>
              )}
            </div>
          )}
        </div>
      </SurfaceCard>
    );
  }

  /* ── Default: Pipeline view (all operations grid) ── */
  return (
    <div className="space-y-6">
      <SurfaceCard title="Normalization Dashboard" subtitle="Run AI-powered data cleaning agents individually or as a pipeline." icon={Sparkles}>

        <div className="flex justify-between items-center mb-4 p-4 bg-red-50 dark:bg-red-950/20 rounded-xl border border-red-100 dark:border-red-900/50">
          <div className="text-sm font-semibold text-red-900 dark:text-red-300">
            Pipeline Mode: Select agents to run sequentially
          </div>
          <PrimaryButton onClick={handleRunPipeline} disabled={loading || selectedPipeline.size === 0}>
            {loading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <ArrowRight className="w-4 h-4 mr-2" />}
            Run Pipeline ({selectedPipeline.size} selected)
          </PrimaryButton>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {OPERATIONS.map(op => {
            const Icon = op.icon;
            const isCompleted = completedOps.has(op.id);
            const isRunning = activeOp === op.id;
            const isSelected = selectedPipeline.has(op.id);

            return (
              <motion.button
                key={op.id}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                onClick={() => !isRunning && handleRunOperation(op.id)}
                disabled={isRunning || loading}
                className={`relative p-4 rounded-2xl border text-left transition-all ${
                  isCompleted
                    ? "border-emerald-200 dark:border-emerald-800 bg-emerald-50/50 dark:bg-emerald-950/20"
                    : isRunning
                    ? "border-amber-200 dark:border-amber-800 bg-amber-50/50 dark:bg-amber-950/20"
                    : "border-neutral-200 dark:border-neutral-700 hover:border-red-200 dark:hover:border-red-800 hover:shadow-sm bg-white dark:bg-neutral-800/50"
                }`}
              >
                <div
                  className="absolute top-4 right-4 text-neutral-400 hover:text-red-500 z-10"
                  onClick={(e) => togglePipelineSelect(op.id, e)}
                  title="Select for Pipeline"
                >
                  {isSelected ? <CheckSquare className="w-5 h-5 text-red-500" /> : <Square className="w-5 h-5" />}
                </div>

                <div className="flex items-start gap-3 mt-1">
                  <div className={`w-9 h-9 rounded-xl flex items-center justify-center shrink-0 ${
                    isCompleted ? "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-600 dark:text-emerald-400" :
                    isRunning ? "bg-amber-100 dark:bg-amber-900/40 text-amber-600 dark:text-amber-400" :
                    "bg-neutral-100 dark:bg-neutral-700 text-neutral-500 dark:text-neutral-400"
                  }`}>
                    {isRunning ? <Loader2 className="w-4 h-4 animate-spin" /> :
                     isCompleted ? <CheckCircle2 className="w-4 h-4" /> :
                     <Icon className="w-4 h-4" />}
                  </div>
                  <div className="min-w-0 pr-6">
                    <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">{op.label}</p>
                    <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-0.5">{op.desc}</p>
                  </div>
                </div>
                {opResults[op.id] && (
                  <p className={`text-xs mt-2 pl-12 ${opResults[op.id].startsWith('Error') ? 'text-red-600 dark:text-red-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
                    {opResults[op.id]}
                  </p>
                )}
              </motion.button>
            );
          })}
        </div>
      </SurfaceCard>

      <SurfaceCard title="Download Normalized Data" subtitle="Export your cleaned and standardized dataset" icon={Download}>
        <div className="space-y-4">
          <div className="flex gap-3">
            <PrimaryButton onClick={handleDownload} disabled={loading || activeOp !== null}>
              <Download className="w-4 h-4 mr-2" />
              Download Excel
            </PrimaryButton>
          </div>
        </div>
      </SurfaceCard>
    </div>
  );
}
