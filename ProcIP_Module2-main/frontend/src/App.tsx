import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Database, AlertCircle, RefreshCw, CheckCircle2, KeyRound,
  Sparkles, Building2, Globe, Calendar, DollarSign, MapPin,
  ClipboardList, Download, Sun, Moon, ArrowLeft,
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import DataLoading from "./components/module-1/DataLoading";
import DataInventory from "./components/module-1/DataInventory";
import NormDashboard from "./components/module-2/NormDashboard";
import StatusLog, { type LogEntry } from "./components/module-1/StatusLog";
import { useTheme } from "./components/common/ThemeProvider";

/* ── Normalization sub-step definitions (mirrored in sidebar + NormDashboard) ── */
const NORM_OPS = [
  { id: "supplier_name",       label: "Supplier Names",      icon: Building2 },
  { id: "supplier_country",    label: "Supplier Country",    icon: Globe },
  { id: "date",                label: "Dates",               icon: Calendar },
  { id: "payment_terms",       label: "Payment Terms",       icon: ClipboardList },
  { id: "region",              label: "Regions",             icon: MapPin },
  { id: "plant",               label: "Plant / Site",        icon: Building2 },
  { id: "currency_conversion", label: "Currency Conversion", icon: DollarSign },
];

/* ── Sidebar step descriptor ── */
const SIDEBAR_STEPS = [
  { num: 1, name: "Upload Data",           ai: false },
  { num: 2, name: "Data Preview",           ai: false },
  { num: 3, name: "Normalization",         ai: true  },
];

export default function App() {
  const { theme, toggleTheme } = useTheme();

  /* ── Core wizard state ── */
  const [step, setStep]                       = useState<number>(1);
  const [maxStepReached, setMaxStepReached]   = useState<number>(1);
  const [normActiveTab, setNormActiveTab]     = useState<string>("pipeline");
  const [apiKey, setApiKey]                   = useState("");
  const [file, setFile]                       = useState<File | null>(null);
  const [filename, setFilename]               = useState<string | null>(null);
  const [loading, setLoading]                 = useState(false);
  const [error, setError]                     = useState<string | null>(null);

  /* ── Data state ── */
  const [inventory, setInventory]             = useState<any[]>([]);

  /* ── Live pipeline activity log ── */
  const [statusLog, setStatusLog] = useState<LogEntry[]>([]);
  const logIdRef = useRef(0);
  const addLog = useCallback((stepName: string, type: LogEntry["type"], message: string) => {
    setStatusLog((prev) => [
      ...prev,
      { id: ++logIdRef.current, timestamp: new Date(), step: stepName, type, message },
    ]);
  }, []);

  /* ── Persist / restore API key ── */
  useEffect(() => {
    const saved = localStorage.getItem("datastitcher_apikey");
    if (saved) {
      setApiKey(saved);
      addLog("SYSTEM", "info", "Restored API key from storage.");
    }
  }, [addLog]);

  useEffect(() => {
    if (apiKey) localStorage.setItem("datastitcher_apikey", apiKey);
  }, [apiKey]);

  useEffect(() => {
    setMaxStepReached((prev) => Math.max(prev, step));
  }, [step]);

  /* ── Step 1 → Upload ── */
  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    addLog("UPLOAD", "info", "Uploading and extracting " + file.name + "…");
    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch("/api/upload", { method: "POST", body: formData });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
      setInventory(data.inventory || []);
      setFilename(file.name);
      addLog("UPLOAD", "success", "Extracted " + (data.inventory?.length || 0) + " table(s) successfully.");
      setStep(2);
    } catch (err: any) {
      setError(err.message);
      addLog("UPLOAD", "error", err.message);
    } finally {
      setLoading(false);
    }
  };

  /* ── Step 2 → Select table from inventory ── */
  const handleProceedFromInventory = async (tableKey: string) => {
    setLoading(true);
    setError(null);
    addLog("INVENTORY", "info", "Locking table " + tableKey + " into pipeline…");
    try {
      const res = await fetch("/api/select-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tableKey }),
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
    }
  };

  /* ── Step router ── */
  const renderCurrentStep = () => {
    switch (step) {
      case 1:
        return (
          <DataLoading
            step={1} file={file} setFile={setFile}
            apiKey={apiKey} setApiKey={setApiKey}
            handleUpload={handleUpload} loading={loading} filename={filename}
          />
        );
      case 2:
        return (
          <DataInventory
            inventory={inventory} onProceed={handleProceedFromInventory}
            loading={loading} setLoading={setLoading} setError={setError}
          />
        );
      case 3:
        return (
          <NormDashboard
            apiKey={apiKey}
            activeTab={normActiveTab}
            setActiveTab={setNormActiveTab}
            addLog={addLog}
          />
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
          href="http://localhost:3000"
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

                    {/* ── Normalization sub-steps (expand when step 3 is active/completed) ── */}
                    {s.num === 3 && (isActive || isCompleted) && (
                      <motion.div
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={{ opacity: 0, height: 0 }}
                        className="ml-[42px] mt-1 mb-2 space-y-0.5 border-l-2 border-neutral-200 dark:border-neutral-700 pl-3"
                      >
                        {/* Run Pipeline */}
                        <button
                          onClick={() => { setStep(3); setNormActiveTab("pipeline"); }}
                          className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors text-left ${
                            isActive && normActiveTab === "pipeline"
                              ? "bg-red-50 dark:bg-red-950/30 text-red-600 dark:text-red-400 font-bold"
                              : "text-neutral-500 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800/50"
                          }`}
                        >
                          <Sparkles className="w-3.5 h-3.5 shrink-0" />
                          Run Pipeline
                        </button>

                        {/* Individual operations */}
                        {NORM_OPS.map((op) => {
                          const Icon = op.icon;
                          return (
                            <button
                              key={op.id}
                              onClick={() => { setStep(3); setNormActiveTab(op.id); }}
                              className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors text-left ${
                                isActive && normActiveTab === op.id
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
      </main>
      </div>
    </div>
  );
}
