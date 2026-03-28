import { useEffect, useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ArrowLeft } from "lucide-react";
import type {
  AppStep,
  ColumnInfo,
  FileInventoryItem,
  ViewDefinition,
  ViewResult,
  CastReport,
  ViewConfig,
} from "./types";
import {
  uploadFile,
  getSessionState,
  mapColumns,
  confirmMapping,
  getAvailableViews,
  computeViews,
  exportCsv,
  exportPdf,
} from "./api/client";
import Header from "./components/layout/Header";
import UploadStep from "./components/upload/UploadStep";
import ColumnMappingStep from "./components/mapping/ColumnMappingStep";
import ViewSelectionStep from "./components/views/ViewSelectionStep";
import Dashboard from "./components/dashboard/Dashboard";

const LS_SESSION_KEY = "summarizer_session_id";
const LS_API_KEY = "summarizer_api_key";

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export default function App() {
  const [step, setStep] = useState<AppStep>(1);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [inventory, setInventory] = useState<FileInventoryItem[]>([]);
  const [castReport, setCastReport] = useState<CastReport | null>(null);
  const [availableViews, setAvailableViews] = useState<ViewDefinition[]>([]);
  const [viewResults, setViewResults] = useState<ViewResult[]>([]);

  const apiKey = () => localStorage.getItem(LS_API_KEY) || "";

  useEffect(() => {
    const savedSession = localStorage.getItem(LS_SESSION_KEY);
    if (savedSession) {
      getSessionState(savedSession)
        .then((state) => {
          setSessionId(savedSession);
          if (state.columns) setColumns(state.columns);
          if (state.fileInventory) setInventory(state.fileInventory);
          if (state.castReport) setCastReport(state.castReport);
          if (state.viewResults) setViewResults(state.viewResults);
          setStep((state.step || 1) as AppStep);
        })
        .catch(() => {
          localStorage.removeItem(LS_SESSION_KEY);
        });
    }
  }, []);

  const handleUpload = useCallback(async (file: File, key: string) => {
    setLoading(true);
    setError("");
    try {
      localStorage.setItem(LS_API_KEY, key);
      const result = await uploadFile(file);
      setSessionId(result.sessionId);
      localStorage.setItem(LS_SESSION_KEY, result.sessionId);
      setColumns(result.columns);
      setInventory(result.fileInventory);
      setStep(2);
    } catch (err: any) {
      setError(err.message || "Upload failed");
    } finally {
      setLoading(false);
    }
  }, []);

  const handleRequestMapping = useCallback(async () => {
    if (!sessionId) throw new Error("No session");
    setLoading(true);
    try {
      const result = await mapColumns(sessionId, apiKey());
      return result;
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  const handleConfirmMapping = useCallback(
    async (mapping: Record<string, string | null>) => {
      if (!sessionId) throw new Error("No session");
      setLoading(true);
      try {
        const result = await confirmMapping(sessionId, mapping);
        setCastReport(result.castReport);
        const viewsResult = await getAvailableViews(sessionId);
        setAvailableViews(viewsResult.views);
        setStep(3);
        return result.castReport;
      } finally {
        setLoading(false);
      }
    },
    [sessionId]
  );

  const handleComputeViews = useCallback(
    async (selectedViews: string[], config: ViewConfig) => {
      if (!sessionId) throw new Error("No session");
      setLoading(true);
      setError("");
      try {
        const result = await computeViews(sessionId, selectedViews, config, apiKey());
        setViewResults(result.views);
        setStep(4);
      } catch (err: any) {
        setError(err.message || "Computation failed");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [sessionId]
  );

  const handleExportCsv = useCallback(
    async (viewId: string) => {
      if (!sessionId) return;
      try {
        const blob = await exportCsv(sessionId, viewId);
        downloadBlob(blob, `${viewId}.csv`);
      } catch (err: any) {
        setError(err.message || "CSV export failed");
      }
    },
    [sessionId]
  );

  const handleExportPdf = useCallback(
    async (chartImages: Record<string, string>) => {
      if (!sessionId) return;
      try {
        const blob = await exportPdf(sessionId, chartImages);
        downloadBlob(blob, "procurement_analysis.pdf");
      } catch (err: any) {
        setError(err.message || "PDF export failed");
      }
    },
    [sessionId]
  );

  const handleNewAnalysis = () => {
    localStorage.removeItem(LS_SESSION_KEY);
    setSessionId(null);
    setColumns([]);
    setInventory([]);
    setCastReport(null);
    setAvailableViews([]);
    setViewResults([]);
    setStep(1);
    setError("");
  };

  return (
    <div className="min-h-screen bg-surface-secondary dark:bg-neutral-950 transition-colors">
      <div className="h-10 bg-white/90 dark:bg-neutral-900/90 backdrop-blur-sm border-b border-neutral-200/80 dark:border-neutral-700/80 flex items-center px-4 shrink-0 z-50">
        <a
          href="http://localhost:3000"
          className="flex items-center gap-2 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" />
          Back to Home
        </a>
      </div>
      <Header currentStep={step} />

      <main className="max-w-[1400px] mx-auto px-6 py-8">
        {error && step !== 4 && (
          <div className="mb-6 px-4 py-3 rounded-xl bg-primary-50 dark:bg-primary-900/20 border border-primary-100 dark:border-primary-900/30 text-sm text-primary">
            {error}
          </div>
        )}

        <AnimatePresence mode="wait">
          <motion.div
            key={step}
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -12 }}
            transition={{ duration: 0.2, ease: "easeOut" }}
          >
            {step === 1 && <UploadStep onUpload={handleUpload} loading={loading} />}

            {step === 2 && (
              <ColumnMappingStep
                columns={columns}
                onRequestMapping={handleRequestMapping}
                onConfirm={handleConfirmMapping}
                loading={loading}
              />
            )}

            {step === 3 && (
              <ViewSelectionStep
                views={availableViews}
                onCompute={handleComputeViews}
                loading={loading}
              />
            )}

            {step === 4 && sessionId && (
              <Dashboard
                views={viewResults}
                sessionId={sessionId}
                onExportCsv={handleExportCsv}
                onExportPdf={handleExportPdf}
              />
            )}
          </motion.div>
        </AnimatePresence>

        {step > 1 && (
          <div className="mt-8 text-center">
            <button
              onClick={handleNewAnalysis}
              className="text-xs text-neutral-400 hover:text-primary transition-colors underline underline-offset-2"
            >
              Start New Analysis
            </button>
          </div>
        )}
      </main>
    </div>
  );
}
