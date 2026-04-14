import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Database, AlertCircle, RefreshCw, CheckCircle2, KeyRound,
  Building2, Globe, Calendar, DollarSign, MapPin,
  ClipboardList, Download, Sun, Moon, ArrowLeft,
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import DataLoading from "./components/module-1/DataLoading";
import DataInventory from "./components/module-1/DataInventory";
import NormDashboard from "./components/module-2/NormDashboard";
import LoadingOverlay from "./components/module-2/LoadingOverlay";
import StepChangeWarningDialog from "./components/module-2/StepChangeWarningDialog";
import StatusLog, { type LogEntry } from "./components/module-1/StatusLog";
import { useTheme } from "./components/common/ThemeProvider";

/* ── Normalization sub-step definitions (mirrored in sidebar + NormDashboard) ── */
const NORM_OPS = [
  { id: "supplier_name",       label: "Supplier Names",      icon: Building2 },
  { id: "supplier_country",    label: "Supplier Country",    icon: Globe },
  { id: "date",                label: "Dates",               icon: Calendar },
  { id: "currency_conversion", label: "Currency Conversion", icon: DollarSign },
  { id: "payment_terms",       label: "Payment Terms",       icon: ClipboardList },
  { id: "region",              label: "Regions",             icon: MapPin },
  { id: "plant",               label: "Plant / Site",        icon: Building2 },
];

const DISABLED_OPS = new Set(["supplier_name"]);

/* ── Sidebar step descriptor ── */
const SIDEBAR_STEPS = [
  { num: 1, name: "Upload Data",           ai: false },
  { num: 2, name: "Data Preview",           ai: false },
  { num: 3, name: "Normalization",         ai: true  },
];

export default function App() {
  const { theme, toggleTheme } = useTheme();

  /* ── Session ID (persisted for the lifetime of this browser tab) ── */
  const [sessionId, setSessionId] = useState<string>(() => crypto.randomUUID());

  /* ── Core wizard state ── */
  const [step, setStep]                       = useState<number>(1);
  const [maxStepReached, setMaxStepReached]   = useState<number>(1);
  const [normActiveTab, setNormActiveTab]     = useState<string>("supplier_country");
  const [apiKey, setApiKey]                   = useState(() => sessionStorage.getItem("normalizer_apiKey") || "");
  const [file, setFile]                       = useState<File | null>(null);
  const [filename, setFilename]               = useState<string | null>(null);
  const [loading, setLoading]                 = useState(false);
  const [loadingMessage, setLoadingMessage]   = useState("");
  const [uploadProgress, setUploadProgress]   = useState<number | null>(null);
  const [loadingOnCancel, setLoadingOnCancel] = useState<(() => void) | null>(null);
  const [error, setError]                     = useState<string | null>(null);

  /* ── Data state ── */
  const [inventory, setInventory]             = useState<any[]>([]);
  const [importSource, setImportSource]       = useState<string | null>(null);
  const [importSessionId, setImportSessionId] = useState<string | null>(null);

  /* ── Live pipeline activity log ── */
  const [statusLog, setStatusLog] = useState<LogEntry[]>([]);
  const logIdRef = useRef(0);
  const addLog = useCallback((stepName: string, type: LogEntry["type"], message: string) => {
    setStatusLog((prev) => [
      ...prev,
      { id: ++logIdRef.current, timestamp: new Date(), step: stepName, type, message },
    ]);
  }, []);

  

  // Persist apiKey to sessionStorage so it survives page refreshes
  useEffect(() => {
    if (apiKey) sessionStorage.setItem("normalizer_apiKey", apiKey);
  }, [apiKey]);

  /* ── Handle cross-module import via URL params ── */
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const imported = params.get("imported");
    const urlApiKey = params.get("apiKey");
    const urlSessionId = params.get("sessionId");
    const source = params.get("source");

    if (imported === "true") {
      window.history.replaceState({}, "", window.location.pathname);

      if (urlApiKey) {
        setApiKey(urlApiKey);
      }
      if (urlSessionId) {
        setImportSessionId(urlSessionId);
        setSessionId(urlSessionId);
      }
      if (source) setImportSource(source);

      const sid = urlSessionId || sessionId;
      fetch(`/api/current-inventory?sessionId=${encodeURIComponent(sid)}`)
        .then((res) => res.json())
        .then((data) => {
          if (data.inventory?.length) {
            setInventory(data.inventory);
            setStep(2);
            setMaxStepReached(2);
            addLog("IMPORT", "success", `Data imported from ${source === "stitcher" ? "DataStitcher" : "external module"} — ${data.inventory.length} table(s) ready.`);
          }
        })
        .catch(() => {
          addLog("IMPORT", "error", "Failed to load imported data from backend.");
        });
    }
  }, [addLog]);

  useEffect(() => {
    setMaxStepReached((prev) => Math.max(prev, step));
  }, [step]);

  // ── Step-aware cache invalidation ──────────────────────────────────
  const [pendingInvalidation, setPendingInvalidation] = useState<{
    step: number;
    action: () => void;
  } | null>(null);
  const [normResetKey, setNormResetKey] = useState(0);

  const invalidateDownstream = useCallback(
    async (fromStep: number) => {
      if (fromStep < 2) {
        setInventory([]);
        setFilename(null);
        try {
          await fetch("/api/reset-state", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sessionId }) });
        } catch (err) {
          console.error("Backend reset-state failed:", err);
        }
      }
      if (fromStep < 3) {
        setNormResetKey((k: number) => k + 1);
        try {
          await fetch("/api/reset-normalization", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sessionId }),
          });
        } catch (err) {
          console.error("Backend reset-normalization failed:", err);
        }
      }
      setMaxStepReached(fromStep as number);
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

  /* ── Step 1 → Upload ── */
  const doUpload = async () => {
    if (!file) return;
    setLoading(true);
    setUploadProgress(0);
    setLoadingMessage("Uploading your data…");
    setError(null);
    addLog("UPLOAD", "info", "Uploading and extracting " + file.name + "…");
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("sessionId", sessionId);

      const data: any = await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open("POST", "/api/upload");
        xhr.upload.onprogress = (e) => {
          if (e.lengthComputable) {
            setUploadProgress(Math.round((e.loaded / e.total) * 100));
          }
        };
        xhr.upload.onload = () => {
          setUploadProgress(null);
          setLoadingMessage("Processing and extracting your data…");
        };
        xhr.onload = () => {
          try {
            const json = JSON.parse(xhr.responseText);
            if (xhr.status >= 400) reject(new Error(json.error || "Upload failed"));
            else resolve(json);
          } catch { reject(new Error("Invalid server response")); }
        };
        xhr.onerror = () => reject(new Error("Network error during upload"));
        xhr.send(formData);
      });

      setInventory(data.inventory || []);
      setFilename(file.name);
      addLog("UPLOAD", "success", "Extracted " + (data.inventory?.length || 0) + " table(s) successfully.");
      setStep(2);
    } catch (err: any) {
      setError(err.message);
      addLog("UPLOAD", "error", err.message);
    } finally {
      setLoading(false);
      setLoadingMessage("");
      setUploadProgress(null);
    }
  };

  const handleUpload = () => {
    if (maxStepReached > 1) {
      guardedAction(1, doUpload);
    } else {
      doUpload();
    }
  };

  /* ── Step 2 → Select table from inventory ── */
  const doProceedFromInventory = async (tableKey: string) => {
    if (maxStepReached > 2) {
      setNormResetKey((k: number) => k + 1);
      setMaxStepReached(2);
    }
    setLoading(true);
    setLoadingMessage("Locking table into pipeline…");
    setError(null);
    addLog("INVENTORY", "info", "Locking table " + tableKey + " into pipeline…");
    try {
      const res = await fetch("/api/select-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tableKey, sessionId }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
      addLog("INVENTORY", "success", "Locked dataset with " + data.rows + " rows.");
      setStep(3);
    } catch (err: any) {
      setError(err.message);
      addLog("INVENTORY", "error", err.message);
    } finally {
      setLoading(false);
      setLoadingMessage("");
    }
  };

  const handleProceedFromInventory = (tableKey: string) => {
    if (maxStepReached > 2) {
      guardedAction(2, () => doProceedFromInventory(tableKey));
    } else {
      doProceedFromInventory(tableKey);
    }
  };

  /* ── Step router ── */
  const renderCurrentStep = () => {
    switch (step) {
      case 1:
        return (
          <DataLoading
            step={1} file={file} setFile={setFile} setFilename={setFilename}
            apiKey={apiKey} setApiKey={setApiKey}
            handleUpload={handleUpload} loading={loading} filename={filename}
          />
        );
      case 2:
        return (
          <DataInventory
            inventory={inventory} onProceed={handleProceedFromInventory}
            loading={loading} setLoading={setLoading} setError={setError}
            importSource={importSource}
            onBeforeMutate={maxStepReached > 2 ? () => invalidateDownstream(2) : undefined}
            sessionId={sessionId}
          />
        );
      case 3:
        return (
          <div key={normResetKey}>
            <NormDashboard
              apiKey={apiKey}
              activeTab={normActiveTab}
              setActiveTab={setNormActiveTab}
              addLog={addLog}
              setLoadingMessage={setLoadingMessage}
              setLoadingOnCancel={setLoadingOnCancel}
              sessionId={sessionId}
            />
          </div>
        );
      default:
        return null;
    }
  };

  /* ───────────────────────────────── JSX ───────────────────────────────── */
  return (
    <div className={`h-screen flex flex-col overflow-hidden ${theme}`}>
      {/* Back to Home bar */}
      <div className="h-10 bg-white dark:bg-neutral-900 border-b border-neutral-200 dark:border-neutral-800 flex items-center px-4 shrink-0 z-50">
        <a
          href={import.meta.env.VITE_HOME_URL ?? "http://localhost:3000"}
          className="flex items-center gap-2 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" />
          Back to Home
        </a>
      </div>

      <div className="flex flex-1 bg-neutral-100 dark:bg-neutral-950 font-sans text-neutral-900 dark:text-neutral-100 selection:bg-red-200 dark:selection:bg-red-900/40 overflow-hidden">

      {/* ═══════════ LEFT SIDEBAR ═══════════ */}
      <aside className="w-[280px] bg-white dark:bg-neutral-900 border-r border-neutral-200 dark:border-neutral-800 flex flex-col h-full flex-shrink-0 relative z-20 shadow-sm">

        {/* ─ Brand header ─ */}
        <div className="p-5 flex items-center gap-3 border-b border-neutral-100 dark:border-neutral-800">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-red-600 to-red-700 flex items-center justify-center shadow-lg shadow-red-600/20 text-white shrink-0">
            <Database className="w-4 h-4" />
          </div>
          <div className="min-w-0">
            <h1 className="text-sm font-bold tracking-tight text-neutral-900 dark:text-white leading-tight">Data Normalizer</h1>
            <p className="text-[10px] font-semibold uppercase tracking-widest text-red-600 dark:text-red-400">Processing Engine</p>
          </div>
          <motion.button
            whileHover={{ scale: 1.1 }} whileTap={{ scale: 0.9 }}
            onClick={toggleTheme}
            className="ml-auto p-2 rounded-xl bg-neutral-100 dark:bg-neutral-800 text-neutral-500 hover:text-neutral-900 dark:hover:text-white transition-colors"
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

        {/* ─ Workflow nav ─ */}
        <div className="flex-1 overflow-y-auto px-4 py-5">
          <p className="text-[10px] uppercase tracking-[0.16em] text-neutral-400 dark:text-neutral-500 font-semibold mb-4 px-2">
            Workflow
          </p>

          <nav className="relative">
            {/* Vertical connector line */}
            <div className="absolute left-[18px] top-5 bottom-5 w-px bg-neutral-200 dark:bg-neutral-700 z-0" />

            <div className="space-y-1 relative z-10">
              {SIDEBAR_STEPS.map((s) => {
                const isActive    = step === s.num;
                const isCompleted = step > s.num;
                const isReachable = s.num <= maxStepReached;
                return (
                  <div key={s.num}>
                    {/* Main step button */}
                    <motion.div
                      whileHover={isReachable ? { x: 2 } : undefined}
                      className={`flex items-center gap-3 px-3 py-2.5 rounded-2xl transition-all ${
                        isActive
                          ? "bg-red-50 dark:bg-red-950/30 ring-1 ring-red-200 dark:ring-red-800 shadow-sm cursor-pointer"
                          : isCompleted
                          ? "text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer"
                          : isReachable
                          ? "text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 cursor-pointer"
                          : "text-neutral-300 dark:text-neutral-600 cursor-not-allowed"
                      }`}
                      onClick={() => { if (isReachable) setStep(s.num); }}
                      title={!isReachable ? "Complete previous steps first" : undefined}
                    >
                      <span
                        className={`h-9 w-9 rounded-xl flex items-center justify-center text-xs font-bold shrink-0 ${
                          isActive
                            ? "bg-red-600 text-white shadow-md shadow-red-200 dark:shadow-red-900/30"
                            : isCompleted
                            ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-400"
                            : isReachable
                            ? "bg-neutral-100 text-neutral-500 dark:bg-neutral-800 dark:text-neutral-400"
                            : "bg-neutral-50 text-neutral-300 dark:bg-neutral-800/50 dark:text-neutral-600"
                        }`}
                      >
                        {isCompleted ? <CheckCircle2 className="w-4 h-4" /> : s.num}
                      </span>
                      <div className="min-w-0 flex-1">
                        <p className={`text-sm font-semibold truncate ${isActive ? "text-red-700 dark:text-red-400" : ""}`}>
                          {s.name}
                        </p>
                        <p className="text-[10px] text-neutral-400 dark:text-neutral-500">
                          {s.ai ? "AI-assisted" : "Manual"}
                        </p>
                      </div>
                    </motion.div>

                    {/* ── Normalization sub-steps (always visible for step 3) ── */}
                    {s.num === 3 && (
                      <motion.div
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={{ opacity: 0, height: 0 }}
                        className="ml-[42px] mt-1 mb-2 space-y-0.5 pl-3"
                      >
                        {/* Individual operations */}
                        {NORM_OPS.map((op) => {
                          const Icon = op.icon;
                          const isDisabled = DISABLED_OPS.has(op.id);
                          return (
                            <button
                              key={op.id}
                              disabled={isDisabled}
                              onClick={isDisabled ? undefined : () => { setStep(3); setNormActiveTab(op.id); }}
                              className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors text-left ${
                                isDisabled
                                  ? "opacity-40 cursor-not-allowed text-neutral-400 dark:text-neutral-600"
                                  : isActive && normActiveTab === op.id
                                    ? "bg-red-50 dark:bg-red-950/30 text-red-600 dark:text-red-400 font-bold"
                                    : "text-neutral-500 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800/50"
                              }`}
                            >
                              <Icon className="w-3.5 h-3.5 shrink-0 opacity-70" />
                              {op.label}
                            </button>
                          );
                        })}

                        {/* Download */}
                        <button
                          onClick={() => { setStep(3); setNormActiveTab("download"); }}
                          className={`w-full flex items-center gap-2 px-2.5 py-1.5 mt-1 rounded-lg text-xs font-medium transition-colors text-left ${
                            isActive && normActiveTab === "download"
                              ? "bg-emerald-50 dark:bg-emerald-950/30 text-emerald-600 dark:text-emerald-400 font-bold"
                              : "text-emerald-600/60 dark:text-emerald-500/60 hover:bg-emerald-50/50 dark:hover:bg-emerald-950/20"
                          }`}
                        >
                          <Download className="w-3.5 h-3.5 shrink-0" />
                          Download Pack
                        </button>
                      </motion.div>
                    )}
                  </div>
                );
              })}
            </div>
          </nav>
        </div>

        {/* ─ API key status (sticky bottom) ─ */}
        <div
          className={`p-4 border-t border-neutral-200/80 dark:border-neutral-700/80 flex items-center gap-2.5 text-xs font-medium cursor-pointer hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors ${
            apiKey ? "text-emerald-600" : "text-red-500"
          }`}
          onClick={() => setStep(1)}
          title={apiKey ? "API key is configured" : "Click to set API key"}
        >
          <KeyRound className="w-4 h-4" />
          {apiKey ? "API Key Set" : "API Key Missing"}
        </div>
      </aside>

      {/* ═══════════ MAIN CONTENT ═══════════ */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative z-10">

        {/* Scrollable content area */}
        <div className="flex-1 overflow-y-auto p-8">
          <div className="max-w-6xl mx-auto space-y-6">

            {/* Error banner */}
            <AnimatePresence>
              {error && (
                <motion.div
                  initial={{ opacity: 0, y: -8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  className="rounded-2xl border border-red-200 dark:border-red-900/50 bg-gradient-to-r from-red-50 to-white dark:from-red-950/30 dark:to-neutral-900 shadow-sm p-4 flex items-start gap-3"
                >
                  <AlertCircle className="w-5 h-5 text-red-500 mt-0.5 shrink-0" />
                  <p className="text-sm font-medium text-red-700 dark:text-red-400 flex-1">{error}</p>
                  <motion.button
                    whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}
                    onClick={() => setError(null)}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-xl bg-red-600 text-white hover:bg-red-700 transition-colors shrink-0 shadow-md shadow-red-200"
                  >
                    <RefreshCw className="w-3 h-3" />
                    Dismiss
                  </motion.button>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Active step component */}
            <div className="pb-32">
              {renderCurrentStep()}
            </div>
          </div>
        </div>

        {/* ─ Live pipeline activity (sticky bottom bar) ─ */}
        <StatusLog entries={statusLog} onClear={() => setStatusLog([])} />
        <LoadingOverlay isLoading={!!loadingMessage} message={loadingMessage} onCancel={loadingOnCancel || undefined} progress={uploadProgress} />

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
      </main>
      </div>
    </div>
  );
}
