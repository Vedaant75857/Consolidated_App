import { useState, useCallback } from "react";
import { Loader2, FlaskConical, RefreshCw, AlertCircle } from "lucide-react";
import type { QualityMetrics, QualityPhase } from "./types";
import { runQualityAnalysis, getQualityAnalysisSummary } from "./api";
import MetricsPanel from "./MetricsPanel";
import QualitySummary from "./QualitySummary";

interface Props {
  sessionId: string;
  mapping: Record<string, string | null>;
  apiKey: string;
}

export default function QualityAnalysisSection({
  sessionId,
  mapping,
  apiKey,
}: Props) {
  const [phase, setPhase] = useState<QualityPhase>("idle");
  const [metrics, setMetrics] = useState<QualityMetrics | null>(null);
  const [summary, setSummary] = useState<string | null>(null);
  const [error, setError] = useState("");

  const descriptionMapped = Boolean(mapping?.po_material_description);

  const handleRun = useCallback(async () => {
    setPhase("computing");
    setError("");
    setSummary(null);
    setMetrics(null);

    try {
      const { metrics: m } = await runQualityAnalysis(sessionId);
      setMetrics(m);
      setPhase("summarizing");

      try {
        const { summary: s } = await getQualityAnalysisSummary(
          sessionId,
          m,
          apiKey,
        );
        setSummary(s);
        setPhase("done");
      } catch (aiErr: any) {
        setSummary(null);
        setPhase("done");
      }
    } catch (err: any) {
      setError(err.message || "Quality analysis failed");
      setPhase("error");
    }
  }, [sessionId, apiKey]);

  return (
    <div className="mt-8 rounded-2xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 shadow-sm overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-neutral-200 dark:border-neutral-700">
        <div className="flex items-center gap-2.5">
          <FlaskConical className="w-5 h-5 text-primary" />
          <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
            Quality Analysis
          </h3>
        </div>

        {(phase === "done" || phase === "error") && (
          <button
            onClick={handleRun}
            className="flex items-center gap-1.5 px-4 py-1.5 rounded-lg text-xs font-semibold text-primary hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Re-run Analysis
          </button>
        )}
      </div>

      <div className="px-6 py-5">
        {/* Idle / placeholder state */}
        {phase === "idle" && (
          <div className="text-center py-6">
            <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-4 max-w-md mx-auto">
              Analyse the quality of your description and supplier data to
              determine the best categorisation approach for your spend cube.
            </p>

            {!descriptionMapped ? (
              <div className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 text-xs text-amber-700 dark:text-amber-400">
                <AlertCircle className="w-3.5 h-3.5" />
                PO Material Description must be mapped to run this analysis
              </div>
            ) : (
              <button
                onClick={handleRun}
                className="px-6 py-2.5 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors shadow-md shadow-red-200/30 dark:shadow-red-900/20"
              >
                Run Quality Analysis
              </button>
            )}
          </div>
        )}

        {/* Computing metrics */}
        {phase === "computing" && (
          <div className="flex flex-col items-center py-8 gap-3">
            <Loader2 className="w-8 h-8 text-primary animate-spin" />
            <p className="text-sm text-neutral-500 dark:text-neutral-400">
              Computing metrics...
            </p>
          </div>
        )}

        {/* Generating AI summary */}
        {phase === "summarizing" && (
          <div className="space-y-6">
            <div className="flex flex-col items-center py-4 gap-3">
              <Loader2 className="w-6 h-6 text-primary animate-spin" />
              <p className="text-sm text-neutral-500 dark:text-neutral-400">
                Generating AI summary...
              </p>
            </div>
            {metrics && <MetricsPanel metrics={metrics} />}
          </div>
        )}

        {/* Error state */}
        {phase === "error" && (
          <div className="flex items-start gap-3 py-4">
            <AlertCircle className="w-5 h-5 text-red-500 mt-0.5 shrink-0" />
            <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
          </div>
        )}

        {/* Done state */}
        {phase === "done" && metrics && (
          <div className="space-y-6">
            {summary && <QualitySummary summary={summary} />}
            <MetricsPanel metrics={metrics} />
          </div>
        )}
      </div>
    </div>
  );
}
