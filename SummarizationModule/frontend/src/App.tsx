import { useEffect, useState, useCallback, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  ArrowLeft,
  BarChart3,
  CheckCircle2,
  Sun,
  Moon,
  AlertCircle,
} from "lucide-react";
import { useTheme } from "./theme/ThemeProvider";
import type {
  AppStep,
  AIMapping,
  StandardField,
  ColumnInfo,
  FileInventoryItem,
  UploadWarning,
  PreviewData,
  ViewDefinition,
  ViewResult,
  CastReport,
  ViewConfig,
  EmailContext,
} from "./types";
import {
  uploadFile,
  getSessionState,
  mapColumns,
  confirmMapping,
  getAvailableViews,
  computeViews,
  recomputeView,
  generateSummary,
  generateEmail,
  cleanupSession,
  exportCsv,
  deleteTable,
  setHeaderRow,
  deleteRows,
  getProcurementViews,
} from "./api/client";
import { ErrorBoundary } from "./components/common/ui";
import DataLoading from "./components/data_preview/DataLoading";
import ColumnMappingStep from "./components/mapping/ColumnMappingStep";
import ViewSelectionStep from "./components/views/ViewSelectionStep";
import DataQualityStep from "./components/spend_quality_assessment/DataQualityStep";
import Dashboard from "./components/dashboard/Dashboard";
import ProcurementViewsStep from "./components/procurement/ProcurementViewsStep";
import ContextModal from "./components/email/ContextModal";
import EmailStep from "./components/email/EmailStep";
import LoadingOverlay from "./components/common/LoadingOverlay";
import StepChangeWarningDialog from "./components/common/StepChangeWarningDialog";

const SIDEBAR_ITEMS = [
  { name: "Upload", steps: [1] as AppStep[] },
  { name: "Data Preview", steps: [2] as AppStep[] },
  { name: "Map Columns", steps: [3] as AppStep[] },
  { name: "Spend Quality Assessment", steps: [4] as AppStep[] },
  { name: "Select Views", steps: [5] as AppStep[] },
  { name: "Dashboard", steps: [6] as AppStep[] },
  { name: "Procurement Views", steps: [7] as AppStep[] },
  { name: "Email", steps: [8] as AppStep[] },
];

const STEP_META: Record<number, { title: string; description: string }> = {
  1: { title: "Upload", description: "Upload your procurement data files to begin analysis." },
  2: { title: "Data Preview", description: "Review extracted tables, adjust headers, and remove unwanted files." },
  3: { title: "Map Columns", description: "AI maps your columns to the standard procurement fields." },
  4: { title: "Spend Quality Assessment", description: "Assess whether your data is ready for accurate spend analysis and cube creation by evaluating coverage, structure, and key quality signals." },
  5: { title: "Select Views", description: "Choose which analyses to generate from your data." },
  6: { title: "Dashboard", description: "Review detailed view summaries and export results." },
  7: { title: "Spend X-ray Feasibility", description: "Check which procurement analysis views your data can support." },
  8: { title: "Email", description: "Generate and edit a client-ready email summary." },
};

const pageVariants = {
  initial: (dir: number) => ({ opacity: 0, x: dir > 0 ? 60 : -60, filter: "blur(4px)" }),
  animate: { opacity: 1, x: 0, filter: "blur(0px)", transition: { duration: 0.3, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] } },
  exit: (dir: number) => ({ opacity: 0, x: dir > 0 ? -60 : 60, filter: "blur(4px)", transition: { duration: 0.2 } }),
};

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
  const { theme, toggle: toggleTheme } = useTheme();
  const [step, setStep] = useState<AppStep>(1);
  const [maxStepReached, setMaxStepReached] = useState<AppStep>(1);
  const [slideDirection, setSlideDirection] = useState(1);
  const prevStepRef = useRef<AppStep>(1);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMessage, setLoadingMessage] = useState("");
  const [error, setError] = useState("");

  const [file, setFile] = useState<File | null>(null);
  const [apiKey, setApiKey] = useState(() => localStorage.getItem(LS_API_KEY) || "");

  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [inventory, setInventory] = useState<FileInventoryItem[]>([]);
  const [previews, setPreviews] = useState<Record<string, PreviewData>>({});
  const [uploadWarnings, setUploadWarnings] = useState<UploadWarning[]>([]);
  const [castReport, setCastReport] = useState<CastReport | null>(null);
  const [availableViews, setAvailableViews] = useState<ViewDefinition[]>([]);
  const [viewResults, setViewResults] = useState<ViewResult[]>([]);
  const [savedAiMappings, setSavedAiMappings] = useState<AIMapping[] | null>(null);
  const [savedStandardFields, setSavedStandardFields] = useState<StandardField[] | null>(null);
  const [confirmedMapping, setConfirmedMapping] = useState<Record<string, string | null>>({});

  const [showContextModal, setShowContextModal] = useState(false);
  const [emailContext, setEmailContext] = useState<EmailContext | null>(null);
  const [generatedEmail, setGeneratedEmail] = useState<string | null>(null);
  const [emailSubject, setEmailSubject] = useState("");
  const [emailFallback, setEmailFallback] = useState<string | null>(null);
  const [emailError, setEmailError] = useState<string | null>(null);
  const [emailLoading, setEmailLoading] = useState(false);
  const [importSource, setImportSource] = useState<string | null>(null);

  const navigateToStep = useCallback((target: AppStep) => {
    setSlideDirection(target > step ? 1 : -1);
    setStep(target);
  }, [step]);

  useEffect(() => {
    if (step > maxStepReached) setMaxStepReached(step);
    prevStepRef.current = step;
  }, [step, maxStepReached]);

  useEffect(() => {
    if (apiKey) localStorage.setItem(LS_API_KEY, apiKey);
    else localStorage.removeItem(LS_API_KEY);
  }, [apiKey]);

  // ── Step-aware cache invalidation ──────────────────────────────────
  const [pendingInvalidation, setPendingInvalidation] = useState<{
    step: number;
    action: () => void;
  } | null>(null);

  const invalidateDownstream = useCallback(
    async (fromStep: number) => {
      if (fromStep < 3) {
        setSavedAiMappings(null);
        setSavedStandardFields(null);
        setConfirmedMapping({});
        setCastReport(null);
      }
      if (fromStep < 5) {
        setAvailableViews([]);
        setViewResults([]);
      }
      if (fromStep < 8) {
        setEmailContext(null);
        setGeneratedEmail(null);
        setEmailSubject("");
        setEmailFallback(null);
        setEmailError(null);
      }
      setMaxStepReached(fromStep as AppStep);
      // Clear backend artifacts
      if (sessionId) {
        try {
          await fetch("/api/invalidate-downstream", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sessionId, fromStep }),
          });
        } catch (err) {
          console.error("Backend invalidation failed:", err);
        }
      }
    },
    [sessionId],
  );

  const guardedAction = useCallback(
    (targetStep: number, action: () => void) => {
      if (maxStepReached > targetStep) {
        setPendingInvalidation({ step: targetStep, action });
      } else {
        action();
      }
    },
    [maxStepReached],
  );

  useEffect(() => {
    localStorage.removeItem(LS_SESSION_KEY);

    const urlParams = new URLSearchParams(window.location.search);
    const urlSessionId = urlParams.get("sessionId");
    const urlApiKey = urlParams.get("apiKey");
    const urlSource = urlParams.get("source");

    const restoreTarget = urlSessionId || sessionStorage.getItem(LS_SESSION_KEY);

    if (urlSessionId) {
      window.history.replaceState({}, "", window.location.pathname);
    }

    if (urlApiKey) {
      setApiKey(urlApiKey);
      localStorage.setItem(LS_API_KEY, urlApiKey);
    }
    if (urlSource) {
      setImportSource(urlSource);
    }

    if (restoreTarget) {
      getSessionState(restoreTarget)
        .then((state) => {
          setSessionId(restoreTarget);
          sessionStorage.setItem(LS_SESSION_KEY, restoreTarget);
          if (state.columns) setColumns(state.columns);
          if (state.fileInventory) setInventory(state.fileInventory);
          if (state.previews) setPreviews(state.previews);
          if (state.castReport) setCastReport(state.castReport);
          if (state.viewResults) setViewResults(state.viewResults);
          if (state.aiMappings) setSavedAiMappings(state.aiMappings);
          if (state.standardFields) setSavedStandardFields(state.standardFields);
          if (state.mapping) setConfirmedMapping(state.mapping);
          setStep((state.step || 1) as AppStep);
          setMaxStepReached((state.step || 1) as AppStep);
        })
        .catch(() => {
          sessionStorage.removeItem(LS_SESSION_KEY);
        });
    }
  }, []);

  useEffect(() => {
    const handleUnload = () => {
      const sid = sessionStorage.getItem(LS_SESSION_KEY);
      if (sid) {
        cleanupSession(sid);
        sessionStorage.removeItem(LS_SESSION_KEY);
      }
    };
    window.addEventListener("beforeunload", handleUnload);
    return () => window.removeEventListener("beforeunload", handleUnload);
  }, []);

  /* ──── Upload (step 1 -> 2) ──── */

  const doUpload = useCallback(async () => {
    if (!file) {
      setError("Please select a file or folder to upload.");
      return;
    }
    // Re-upload: wipe existing session cache first
    if (sessionId) {
      await invalidateDownstream(0);
    }
    setLoading(true);
    setLoadingMessage("Uploading and extracting your data…");
    setError("");
    try {
      const result = await uploadFile(file);
      setSessionId(result.sessionId);
      sessionStorage.setItem(LS_SESSION_KEY, result.sessionId);
      setColumns(result.columns);
      setInventory(result.fileInventory);
      setPreviews(result.previews);
      setUploadWarnings(result.warnings || []);
      setStep(2);
    } catch (err: any) {
      setError(err.message || "Upload failed");
    } finally {
      setLoading(false);
      setLoadingMessage("");
    }
  }, [file, sessionId, invalidateDownstream]);

  const handleUpload = useCallback(() => {
    if (sessionId && maxStepReached > 1) {
      guardedAction(1, doUpload);
    } else {
      doUpload();
    }
  }, [sessionId, maxStepReached, guardedAction, doUpload]);

  /* ──── Inventory table operations ──── */

  const doDeleteTable = useCallback(
    async (tableKey: string) => {
      if (!sessionId) return;
      if (maxStepReached > 2) await invalidateDownstream(2);
      setLoading(true);
      setError("");
      try {
        const data = await deleteTable(sessionId, tableKey);
        setInventory(data.inventory);
        setPreviews(data.previews || {});
      } catch (err: any) {
        setError(err.message || "Failed to delete table");
      } finally {
        setLoading(false);
      }
    },
    [sessionId, maxStepReached, invalidateDownstream]
  );

  const handleDeleteTable = useCallback(
    (tableKey: string) => {
      if (maxStepReached > 2) {
        guardedAction(2, () => doDeleteTable(tableKey));
      } else {
        doDeleteTable(tableKey);
      }
    },
    [maxStepReached, guardedAction, doDeleteTable]
  );

  const doSetHeaderRow = useCallback(
    async (
      tableKey: string,
      rowIndex: number,
      customNames?: Record<number, string>
    ) => {
      if (!sessionId) return;
      if (maxStepReached > 2) await invalidateDownstream(2);
      setLoading(true);
      setError("");
      try {
        const data = await setHeaderRow(sessionId, tableKey, rowIndex, customNames);
        setInventory(data.inventory);
        setPreviews(data.previews || {});
        if (data.columns) setColumns(data.columns);
      } catch (err: any) {
        setError(err.message || "Failed to set header row");
      } finally {
        setLoading(false);
      }
    },
    [sessionId, maxStepReached, invalidateDownstream]
  );

  const handleSetHeaderRow = useCallback(
    (tableKey: string, rowIndex: number, customNames?: Record<number, string>) => {
      if (maxStepReached > 2) {
        guardedAction(2, () => doSetHeaderRow(tableKey, rowIndex, customNames));
      } else {
        doSetHeaderRow(tableKey, rowIndex, customNames);
      }
    },
    [maxStepReached, guardedAction, doSetHeaderRow]
  );

  const doDeleteRows = useCallback(
    async (tableKey: string, rowIds: (string | number)[]) => {
      if (!sessionId) return;
      if (maxStepReached > 2) await invalidateDownstream(2);
      setLoading(true);
      setError("");
      try {
        const data = await deleteRows(sessionId, tableKey, rowIds);
        if (data.preview)
          setPreviews((prev) => ({ ...prev, [tableKey]: data.preview }));
        if (data.inventoryRow)
          setInventory((prev) =>
            prev.map((inv) =>
              inv.table_key === tableKey ? data.inventoryRow : inv
            )
          );
      } catch (err: any) {
        setError(err.message || "Failed to delete rows");
      } finally {
        setLoading(false);
      }
    },
    [sessionId, maxStepReached, invalidateDownstream]
  );

  const handleDeleteRows = useCallback(
    (tableKey: string, rowIds: (string | number)[]) => {
      if (maxStepReached > 2) {
        guardedAction(2, () => doDeleteRows(tableKey, rowIds));
      } else {
        doDeleteRows(tableKey, rowIds);
      }
    },
    [maxStepReached, guardedAction, doDeleteRows]
  );

  /* ──── Column mapping (step 3) ──── */

  const handleRequestMapping = useCallback(async () => {
    if (!sessionId) throw new Error("No session");
    setLoading(true);
    setLoadingMessage("AI is detecting your column mappings…");
    try {
      return await mapColumns(sessionId, apiKey);
    } finally {
      setLoading(false);
      setLoadingMessage("");
    }
  }, [sessionId, apiKey]);

  const doConfirmMapping = useCallback(
    async (mapping: Record<string, string | null>) => {
      if (!sessionId) throw new Error("No session");
      if (maxStepReached > 3) await invalidateDownstream(3);
      setLoading(true);
      setLoadingMessage("Confirming mappings and casting data…");
      try {
        setConfirmedMapping(mapping);
        const result = await confirmMapping(sessionId, mapping);
        setCastReport(result.castReport);
        const viewsResult = await getAvailableViews(sessionId);
        setAvailableViews(viewsResult.views);
        setStep(4);
        return result.castReport;
      } finally {
        setLoading(false);
        setLoadingMessage("");
      }
    },
    [sessionId, maxStepReached, invalidateDownstream]
  );

  const handleConfirmMapping = useCallback(
    (mapping: Record<string, string | null>) => {
      if (maxStepReached > 3) {
        return new Promise<CastReport>((resolve) => {
          guardedAction(3, async () => {
            const result = await doConfirmMapping(mapping);
            resolve(result);
          });
        });
      }
      return doConfirmMapping(mapping);
    },
    [maxStepReached, guardedAction, doConfirmMapping]
  );

  /* ──── Compute views (step 5 -> 6) ──── */

  const doComputeViews = useCallback(
    async (selectedViews: string[], config: ViewConfig) => {
      if (!sessionId) throw new Error("No session");
      if (maxStepReached > 5) await invalidateDownstream(5);
      setLoading(true);
      setLoadingMessage("Computing views and generating charts…");
      setError("");
      try {
        const result = await computeViews(sessionId, selectedViews, config);
        setViewResults(result.views);
        setStep(6);

        if (apiKey && apiKey.trim()) {
          const summaryViews = result.views.filter(
            (v: ViewResult) => !v.error && v.viewId !== "category_drilldown"
          );
          summaryViews.forEach((v: ViewResult) => {
            generateSummary(sessionId, v.viewId, apiKey)
              .then(({ viewId, summary }: { viewId: string; summary: string }) => {
                setViewResults((prev) =>
                  prev.map((vr) =>
                    vr.viewId === viewId ? { ...vr, aiSummary: summary } : vr
                  )
                );
              })
              .catch(() => {});
          });
        }
      } catch (err: any) {
        setError(err.message || "Computation failed");
        throw err;
      } finally {
        setLoading(false);
        setLoadingMessage("");
      }
    },
    [sessionId, apiKey, maxStepReached, invalidateDownstream]
  );

  const handleComputeViews = useCallback(
    (selectedViews: string[], config: ViewConfig) => {
      if (maxStepReached > 5) {
        return new Promise<void>((resolve, reject) => {
          guardedAction(5, async () => {
            try {
              await doComputeViews(selectedViews, config);
              resolve();
            } catch (err) {
              reject(err);
            }
          });
        });
      }
      return doComputeViews(selectedViews, config);
    },
    [maxStepReached, guardedAction, doComputeViews]
  );

  /* ──── Recompute single view (dashboard slider changes) ──── */

  const handleRecomputeView = useCallback(
    async (viewId: string, config: ViewConfig): Promise<ViewResult> => {
      if (!sessionId) throw new Error("No session");
      if (maxStepReached > 6) await invalidateDownstream(6);
      const result = await recomputeView(sessionId, viewId, config);
      setViewResults((prev) =>
        prev.map((v) => (v.viewId === viewId ? result.view : v))
      );
      return result.view;
    },
    [sessionId, maxStepReached, invalidateDownstream]
  );

  /* ──── Exports ──── */

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

  /* ──── Procurement views (step 6 -> 7) ──── */

  const handleViewProcurementFeasibility = useCallback(() => {
    setStep(7);
  }, []);

  const handleFetchProcurementViews = useCallback(async () => {
    if (!sessionId) throw new Error("No session");
    return getProcurementViews(sessionId);
  }, [sessionId]);

  /* ──── Email generation (step 6 -> 7) ──── */

  const handleOpenEmailModal = useCallback(() => {
    setShowContextModal(true);
  }, []);

  const handleEmailGenerate = useCallback(
    async (context: EmailContext) => {
      if (!sessionId) return;
      setShowContextModal(false);
      setEmailContext(context);
      setEmailLoading(true);
      setLoadingMessage("Generating email with AI…");
      setEmailError(null);
      setEmailFallback(null);
      setGeneratedEmail(null);
      setStep(8);

      try {
        const result = await generateEmail(sessionId, apiKey, context);
        if (result.email) {
          setGeneratedEmail(result.email);
          setEmailSubject(result.subject || "");
        } else {
          setEmailError(result.error || "Email generation failed");
          setEmailFallback(result.fallback || null);
        }
      } catch (err: any) {
        setEmailError(err.message || "Email generation failed");
      } finally {
        setEmailLoading(false);
        setLoadingMessage("");
      }
    },
    [sessionId, apiKey]
  );

  const handleEmailRegenerate = useCallback(() => {
    if (emailContext) {
      handleEmailGenerate(emailContext);
    }
  }, [emailContext, handleEmailGenerate]);

  const handleBackToProcurementViews = useCallback(() => {
    setStep(7);
  }, []);

  /* ──── Reset ──── */

  const handleNewAnalysis = () => {
    const oldSession = sessionStorage.getItem(LS_SESSION_KEY);
    if (oldSession) cleanupSession(oldSession);
    sessionStorage.removeItem(LS_SESSION_KEY);
    setSessionId(null);
    setFile(null);
    setColumns([]);
    setInventory([]);
    setPreviews({});
    setUploadWarnings([]);
    setCastReport(null);
    setAvailableViews([]);
    setViewResults([]);
    setSavedAiMappings(null);
    setSavedStandardFields(null);
    setConfirmedMapping({});
    setShowContextModal(false);
    setEmailContext(null);
    setGeneratedEmail(null);
    setEmailSubject("");
    setEmailFallback(null);
    setEmailError(null);
    setStep(1);
    setMaxStepReached(1);
    setSlideDirection(-1);
    setError("");
  };

  const stepMeta = STEP_META[step] || { title: `Step ${step}`, description: "" };

  return (
    <div className="h-screen flex flex-col overflow-hidden">
      {/* Back to Home bar */}
      <div className="h-10 bg-white/90 dark:bg-neutral-900/90 backdrop-blur-sm border-b border-neutral-200/80 dark:border-neutral-700/80 flex items-center px-4 shrink-0 z-50">
        <a
          href={import.meta.env.VITE_HOME_URL ?? "http://localhost:3000"}
          className="flex items-center gap-2 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" />
          Back to Home
        </a>
      </div>

      <div className="flex-1 bg-gradient-to-br from-neutral-50 via-white to-neutral-100 dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 text-neutral-900 dark:text-neutral-100 font-sans flex relative overflow-hidden">
        {/* Decorative background blurs */}
        <div className="pointer-events-none fixed inset-0 z-0">
          <div className="absolute -top-40 -right-40 h-[600px] w-[600px] rounded-full bg-red-100/40 dark:bg-red-950/20 blur-3xl" />
          <div className="absolute -bottom-60 -left-40 h-[500px] w-[500px] rounded-full bg-rose-100/30 dark:bg-rose-950/15 blur-3xl" />
        </div>

        {/* Sidebar */}
        <aside className="w-72 bg-white/90 dark:bg-neutral-900/90 backdrop-blur-xl border-r border-neutral-200/80 dark:border-neutral-700/80 flex-shrink-0 flex flex-col z-10">
          <div className="p-6 border-b border-neutral-200/80 dark:border-neutral-700/80 flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-red-600 to-rose-600 flex items-center justify-center shadow-md shadow-red-200/40">
              <BarChart3 className="w-5 h-5 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white">Spend Summarizer</h1>
              <p className="text-[10px] text-neutral-400 dark:text-neutral-500 font-medium tracking-wide">Procurement analytics</p>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto px-4 py-3">
            <p className="text-[10px] uppercase tracking-[0.16em] text-neutral-400 dark:text-neutral-500 font-semibold mb-4 px-2">Workflow</p>
            <nav className="relative">
              <div className="absolute left-[18px] top-5 bottom-5 w-px bg-neutral-200 dark:bg-neutral-700 z-0" />
              <div className="space-y-1 relative z-10">
                {SIDEBAR_ITEMS.map((s, idx) => {
                  const displayNum = idx + 1;
                  const firstStep = s.steps[0];
                  const lastStep = s.steps[s.steps.length - 1];
                  const isActive = s.steps.includes(step);
                  const isCompleted = step > lastStep;
                  const isReachable = firstStep <= maxStepReached;
                  const targetStep = s.steps.filter(st => st <= maxStepReached).pop() || firstStep;
                  return (
                    <motion.div
                      key={displayNum}
                      whileHover={isReachable ? { x: 2 } : undefined}
                      className={`flex items-center gap-3 px-3 py-2.5 rounded-2xl transition-all ${
                        isActive ? "bg-red-50 dark:bg-red-950/30 ring-1 ring-red-200 dark:ring-red-800 shadow-sm cursor-pointer" :
                        isCompleted ? "text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer" :
                        isReachable ? "text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer" :
                        "text-neutral-300 dark:text-neutral-600 cursor-not-allowed"
                      }`}
                      onClick={() => { if (isReachable) navigateToStep(targetStep as AppStep); }}
                      title={!isReachable ? "Complete previous steps first" : undefined}
                    >
                      <span className={`h-9 w-9 rounded-xl flex items-center justify-center text-xs font-bold shrink-0 ${
                        isActive ? "bg-red-600 text-white shadow-md shadow-red-200 dark:shadow-red-900/30" :
                        isCompleted ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-400" :
                        isReachable ? "bg-neutral-100 text-neutral-500 dark:bg-neutral-800 dark:text-neutral-400" :
                        "bg-neutral-50 text-neutral-300 dark:bg-neutral-800/50 dark:text-neutral-600"
                      }`}>
                        {isCompleted ? <CheckCircle2 className="w-4 h-4" /> : displayNum}
                      </span>
                      <div className="min-w-0 flex-1">
                        <p className={`text-sm font-semibold truncate ${isActive ? "text-red-700 dark:text-red-400" : ""}`}>{s.name}</p>
                      </div>
                    </motion.div>
                  );
                })}
              </div>
            </nav>
          </div>

          {step > 1 && (
            <div
              className="p-4 border-t border-neutral-200/80 dark:border-neutral-700/80 flex items-center gap-2.5 text-xs font-medium cursor-pointer hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors text-neutral-500 dark:text-neutral-400"
              onClick={handleNewAnalysis}
            >
              <ArrowLeft className="w-4 h-4" />
              Start New Analysis
            </div>
          )}
        </aside>

        {/* Main Content */}
        <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative z-10">
          <div className="flex-1 overflow-y-auto p-8">
            <div className="max-w-6xl mx-auto space-y-6">

              {/* Top-right theme toggle */}
              <div className="flex justify-end">
                <motion.button
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={toggleTheme}
                  className="p-2.5 rounded-xl bg-white/80 dark:bg-neutral-800/80 border border-neutral-200 dark:border-neutral-700 shadow-sm backdrop-blur-sm text-neutral-600 dark:text-neutral-300 hover:text-neutral-900 dark:hover:text-white transition-colors"
                  title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
                >
                  <AnimatePresence mode="wait" initial={false}>
                    {theme === "dark" ? (
                      <motion.span key="sun" initial={{ rotate: -90, opacity: 0 }} animate={{ rotate: 0, opacity: 1 }} exit={{ rotate: 90, opacity: 0 }} transition={{ duration: 0.2 }}>
                        <Sun className="w-4 h-4" />
                      </motion.span>
                    ) : (
                      <motion.span key="moon" initial={{ rotate: 90, opacity: 0 }} animate={{ rotate: 0, opacity: 1 }} exit={{ rotate: -90, opacity: 0 }} transition={{ duration: 0.2 }}>
                        <Moon className="w-4 h-4" />
                      </motion.span>
                    )}
                  </AnimatePresence>
                </motion.button>
              </div>

              {/* StepHero banner */}
              <motion.div
                key={step}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.32, ease: [0.22, 1, 0.36, 1] }}
                className="rounded-3xl border border-red-200/60 dark:border-red-900/60 bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white shadow-xl shadow-red-200/20 dark:shadow-red-900/20"
              >
                <div className="flex items-start justify-between gap-6">
                  <div className="min-w-0">
                    <p className="text-xs font-semibold uppercase tracking-[0.2em] text-red-200">
                      Spend Summarizer
                    </p>
                    <h2 className="mt-2 text-2xl font-semibold tracking-tight">{stepMeta.title}</h2>
                    <p className="mt-2 max-w-2xl text-sm text-red-50/90 leading-relaxed">{stepMeta.description}</p>
                  </div>
                  <div className="rounded-2xl bg-white/10 px-4 py-3 backdrop-blur shrink-0">
                    <p className="text-[10px] uppercase tracking-wider text-red-200">Current step</p>
                    <p className="mt-1 text-lg font-semibold tabular-nums">
                      {step} <span className="text-red-200/70 text-sm font-normal">of 7</span>
                    </p>
                  </div>
                </div>
              </motion.div>

              {/* Error banner */}
              {error && step !== 5 && step !== 6 && step !== 7 && (
                <motion.div
                  initial={{ opacity: 0, y: -8 }}
                  animate={{ opacity: 1, y: 0 }}
                  className="rounded-2xl border border-red-200 dark:border-red-900/50 bg-gradient-to-r from-red-50 to-white dark:from-red-950/30 dark:to-neutral-900 shadow-sm p-4 flex items-start gap-3"
                >
                  <AlertCircle className="w-5 h-5 text-red-500 mt-0.5 shrink-0" />
                  <p className="text-sm font-medium text-red-700 dark:text-red-400 flex-1">{error}</p>
                </motion.div>
              )}

              {/* Step content */}
              <AnimatePresence mode="wait" custom={slideDirection}>
                <motion.div
                  key={step}
                  custom={slideDirection}
                  variants={pageVariants}
                  initial="initial"
                  animate="animate"
                  exit="exit"
                >
                  <ErrorBoundary>
                    {(step === 1 || step === 2) && (
                      <DataLoading
                        step={step}
                        file={file}
                        setFile={setFile}
                        apiKey={apiKey}
                        setApiKey={setApiKey}
                        handleUpload={handleUpload}
                        loading={loading}
                        sessionId={sessionId || ""}
                        inventory={inventory}
                        previews={previews}
                        uploadWarnings={uploadWarnings}
                        onProceedToMapping={() => setStep(3)}
                        onDeleteTable={handleDeleteTable}
                        onSetHeaderRow={handleSetHeaderRow}
                        onDeleteRows={handleDeleteRows}
                        importSource={importSource}
                      />
                    )}

                    {step === 3 && (
                      <ColumnMappingStep
                        columns={columns}
                        onRequestMapping={handleRequestMapping}
                        onConfirm={handleConfirmMapping}
                        loading={loading}
                        initialMappings={savedAiMappings}
                        initialStandardFields={savedStandardFields}
                      />
                    )}

                    {step === 4 && sessionId && (
                      <DataQualityStep
                        sessionId={sessionId}
                        apiKey={apiKey}
                        onProceed={() => setStep(5)}
                      />
                    )}

                    {step === 5 && (
                      <ViewSelectionStep
                        views={availableViews}
                        onCompute={handleComputeViews}
                        loading={loading}
                      />
                    )}

                    {step === 6 && sessionId && (
                      <Dashboard
                        views={viewResults}
                        onExportCsv={handleExportCsv}
                        onRecomputeView={handleRecomputeView}
                        onViewProcurementFeasibility={handleViewProcurementFeasibility}
                      />
                    )}

                    {step === 7 && sessionId && (
                      <ProcurementViewsStep
                        sessionId={sessionId}
                        onFetchViews={handleFetchProcurementViews}
                        onGenerateEmail={apiKey.trim() ? handleOpenEmailModal : undefined}
                      />
                    )}

                    {step === 8 && (
                      <EmailStep
                        email={generatedEmail}
                        subject={emailSubject}
                        fallback={emailFallback}
                        error={emailError}
                        loading={emailLoading}
                        onRegenerate={handleEmailRegenerate}
                        onBack={handleBackToProcurementViews}
                      />
                    )}
                  </ErrorBoundary>
                </motion.div>
              </AnimatePresence>
            </div>
          </div>
          <LoadingOverlay isLoading={!!loadingMessage} message={loadingMessage} />
        </main>
      </div>

      {showContextModal && (
        <ContextModal
          onSubmit={handleEmailGenerate}
          onCancel={() => setShowContextModal(false)}
        />
      )}

      <StepChangeWarningDialog
        open={!!pendingInvalidation}
        onCancel={() => setPendingInvalidation(null)}
        onConfirm={async () => {
          if (pendingInvalidation) {
            await invalidateDownstream(pendingInvalidation.step);
            const action = pendingInvalidation.action;
            setPendingInvalidation(null);
            action();
          }
        }}
      />
    </div>
  );
}
