import React, { useState, useCallback, useEffect, useRef } from "react";
import {
  Loader2, ArrowRight, CheckCircle2,
  Download, Sparkles, Building2, Globe, Calendar, DollarSign,
  MapPin, ClipboardList, Info, AlertTriangle, BarChart3, AlertCircle, X
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { SurfaceCard, PrimaryButton } from "../common/ui";
import TransferOverlay from "../common/TransferOverlay";
import UnsupportedCurrencyPanel, {
  hasEmptyOverrides,
  type UnsupportedCurrency,
  type FxOverrideMode,
  type YearlyOverrides,
  type MonthlyOverrides,
} from "./UnsupportedCurrencyPanel";
import type { LogEntry } from "../module-1/StatusLog";

const ANALYZER_FE = import.meta.env.VITE_ANALYZER_FE ?? "http://localhost:3004";

const OPERATIONS = [
  { id: "supplier_name", label: "Supplier Names", icon: Building2, desc: "Clean & deduplicate supplier names" },
  { id: "supplier_country", label: "Supplier Country", icon: Globe, desc: "Standardize country names" },
  { id: "date", label: "Dates", icon: Calendar, desc: "Normalize date formats" },
  { id: "currency_conversion", label: "Currency Conversion", icon: DollarSign, desc: "Convert local spend to USD" },
  { id: "payment_terms", label: "Payment Terms", icon: ClipboardList, desc: "Extract numeric payment terms" },
  { id: "region", label: "Regions", icon: MapPin, desc: "Classify into NA/EMEA/APAC/LATAM" },
  { id: "plant", label: "Plant/Site", icon: Building2, desc: "Standardize plant codes & names" },
];

interface PopulationInfo {
  n_populated: number;
  n_total: number;
  pct_populated: number;
  warn: boolean;
}
interface AssessResult {
  needs_confirmation: boolean;
  warnings: string[];
  population: PopulationInfo | null;
  unsupported_currencies: UnsupportedCurrency[];
}
interface ConversionMetrics {
  spend_col: string;
  status_col: string;
  n_converted: number;
  n_fallback: number;
  n_currency_missing: number;
  n_unsupported: number;
  n_spend_invalid: number;
  n_date_unparseable: number;
  unsupported: string[];
}

interface CountryNormMetrics {
  n_total: number;
  n_empty: number;
  n_normalized: number;
  n_deterministic: number;
  n_ai: number;
  n_unresolved: number;
  n_distinct: number;
  ai_errors: string[];
}

interface RegionNormMetrics {
  n_total: number;
  n_empty: number;
  n_normalized: number;
  n_deterministic: number;
  n_ai: number;
  n_from_country: number;
  n_unresolved: number;
  ai_errors: string[];
}

interface NormDashboardProps {
  apiKey: string;
  activeTab?: string;
  setActiveTab?: (tab: string) => void;
  addLog?: (stepName: string, type: LogEntry["type"], message: string) => void;
  setLoadingMessage?: (msg: string) => void;
  setLoadingOnCancel?: (cb: (() => void) | null) => void;
}

export default function NormDashboard({ apiKey, activeTab = "supplier_name", setActiveTab, addLog, setLoadingMessage, setLoadingOnCancel }: NormDashboardProps) {
  const [completedOps, setCompletedOps] = useState<Set<string>>(new Set());
  const [activeOp, setActiveOp] = useState<string | null>(null);
  const [opResults, setOpResults] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [operationPreview, setOperationPreview] = useState<{ columns: string[]; rows: any[] }>({ columns: [], rows: [] });
  const [operationPreviewLoading, setOperationPreviewLoading] = useState(false);
  const [operationPreviewError, setOperationPreviewError] = useState<string | null>(null);
  const [dateFormat, setDateFormat] = useState("%d-%m-%Y");
  const [currencySpendColumn, setCurrencySpendColumn] = useState("");
  const [currencyCodeColumn, setCurrencyCodeColumn] = useState("");
  const [currencyDateColumn, setCurrencyDateColumn] = useState("No date col");
  const [scopeYear, setScopeYear] = useState("2024");

  // Supplier country assess state
  const [supplierCountryColumn, setSupplierCountryColumn] = useState("");
  const [scAssessLoading, setScAssessLoading] = useState(false);
  const [scAssessResult, setScAssessResult] = useState<{ population: PopulationInfo | null } | null>(null);
  const [scAssessError, setScAssessError] = useState<string | null>(null);
  const [countryNormMetrics, setCountryNormMetrics] = useState<CountryNormMetrics | null>(null);

  // Region assess state
  const [regionColumn, setRegionColumn] = useState("");
  const [regionAssessLoading, setRegionAssessLoading] = useState(false);
  const [regionAssessResult, setRegionAssessResult] = useState<{ population: PopulationInfo | null } | null>(null);
  const [regionAssessError, setRegionAssessError] = useState<string | null>(null);
  const [regionNormMetrics, setRegionNormMetrics] = useState<RegionNormMetrics | null>(null);

  // Currency conversion assess state
  const [assessLoading, setAssessLoading] = useState(false);
  const [assessResult, setAssessResult] = useState<AssessResult | null>(null);
  const [assessError, setAssessError] = useState<string | null>(null);
  const [fxOverrideMode, setFxOverrideMode] = useState<FxOverrideMode>("yearly");
  const [fxOverridesYearly, setFxOverridesYearly] = useState<YearlyOverrides>({});
  const [fxOverridesMonthly, setFxOverridesMonthly] = useState<MonthlyOverrides>({});
  const [showFxValidation, setShowFxValidation] = useState(false);
  const [conversionMetrics, setConversionMetrics] = useState<ConversionMetrics | null>(null);

  // Smart column auto-selection: call backend scoring endpoint when columns load.
  // Uses functional state updates so the effect only depends on operationPreview.columns,
  // avoiding stale-closure issues and preventing re-runs on every user dropdown change.
  useEffect(() => {
    const cols = operationPreview.columns;
    if (!cols.length) return;

    const apply = async () => {
      try {
        const res = await fetch("/api/suggest-columns");
        if (!res.ok) throw new Error("suggest-columns failed");
        const s = await res.json();

        setCurrencyCodeColumn(prev => {
          if (prev && cols.includes(prev)) return prev; // keep valid user selection
          return s.currency_col && cols.includes(s.currency_col) ? s.currency_col : "";
        });

        setCurrencySpendColumn(prev => {
          if (prev && cols.includes(prev)) return prev;
          return s.spend_col && cols.includes(s.spend_col) ? s.spend_col : "";
        });

        setCurrencyDateColumn(prev => {
          if (prev === "No date col" || (prev && cols.includes(prev))) return prev;
          if (s.date_col && cols.includes(s.date_col)) return s.date_col;
          return "No date col";
        });

        // Auto-detect supplier country column (client-side keyword match)
        setSupplierCountryColumn(prev => {
          if (prev && cols.includes(prev)) return prev;
          return cols.find(c => /(supplier.?country|vendor.?country|^country$)/i.test(c)) ?? "";
        });

        // Auto-detect region column (priority: region > SUPPLIER COUNTRY NORMALIZED > country)
        setRegionColumn((prev: string) => {
          if (prev && cols.includes(prev)) return prev;
          const regionMatch = cols.find((c: string) => /region/i.test(c));
          if (regionMatch) return regionMatch;
          if (cols.includes("SUPPLIER COUNTRY NORMALIZED")) return "SUPPLIER COUNTRY NORMALIZED";
          const scnMatch = cols.find((c: string) => /supplier.?country.?normalized/i.test(c));
          if (scnMatch) return scnMatch;
          return cols.find((c: string) => /(supplier.?country|vendor.?country|^country$)/i.test(c)) ?? "";
        });
      } catch {
        // Fallback: replicate previous header-only logic so the UI is never left blank
        // if the backend is unavailable or /api/suggest-columns returns an error.
        setSupplierCountryColumn(prev => {
          if (prev && cols.includes(prev)) return prev;
          return cols.find(c => /(supplier.?country|vendor.?country|^country$)/i.test(c)) ?? "";
        });
        setCurrencyCodeColumn(prev => {
          if (prev && cols.includes(prev)) return prev;
          return cols.find(c => /(currency|curr|ccy)/i.test(c)) ?? "";
        });
        setCurrencySpendColumn(prev => {
          if (prev && cols.includes(prev)) return prev;
          return cols.find(c => /(spend|amount|cost|price|total|value|charge|fee|payment|invoice)/i.test(c)) ?? "";
        });
        setCurrencyDateColumn(prev => {
          if (prev === "No date col" || (prev && cols.includes(prev))) return prev;
          const fallback = cols.find(c =>
            (c.toLowerCase().includes("date") || c.toLowerCase().includes("dob") || c.toLowerCase().includes("time"))
            && !c.startsWith("Norm_Date_")
          );
          return fallback ?? "No date col";
        });
        setRegionColumn((prev: string) => {
          if (prev && cols.includes(prev)) return prev;
          const regionMatch = cols.find((c: string) => /region/i.test(c));
          if (regionMatch) return regionMatch;
          if (cols.includes("SUPPLIER COUNTRY NORMALIZED")) return "SUPPLIER COUNTRY NORMALIZED";
          const scnMatch = cols.find((c: string) => /supplier.?country.?normalized/i.test(c));
          if (scnMatch) return scnMatch;
          return cols.find((c: string) => /(supplier.?country|vendor.?country|^country$)/i.test(c)) ?? "";
        });
      }
    };

    apply();
  }, [operationPreview.columns]);

  const log = useCallback((type: LogEntry["type"], message: string) => {
    addLog?.("NORMALIZE", type, message);
  }, [addLog]);

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

  const normControllerRef = useRef<AbortController | null>(null);

  const handleRunOperation = useCallback(async (agentId: string) => {
    if (agentId === "currency_conversion") {
      if (!currencySpendColumn || !currencyCodeColumn) {
        log("error", "Select both a currency column and a spend column before running Currency Conversion.");
        return;
      }
      // Nothing here — validation is handled by the button UI layer
    }
    setActiveOp(agentId);
    const opLabel = OPERATIONS.find(o => o.id === agentId)?.label || agentId;
    setLoadingMessage?.("Running " + opLabel + "…");
    log("info", "Running " + opLabel + "…");

    const controller = new AbortController();
    normControllerRef.current = controller;
    setLoadingOnCancel?.(() => () => {
      controller.abort();
      normControllerRef.current = null;
      setActiveOp(null);
      setLoadingMessage?.("");
      setLoadingOnCancel?.(null);
      log("info", opLabel + " cancelled.");
    });

    const agentKwargs: any = {};
    if (agentId === "supplier_country" && supplierCountryColumn) {
      agentKwargs.country_col = supplierCountryColumn;
    }
    if (agentId === "region" && regionColumn) {
      agentKwargs.region_col = regionColumn;
    }
    if (agentId === "date") {
      agentKwargs.user_format = dateFormat;
    }
    if (agentId === "currency_conversion") {
      agentKwargs.spend_cols = [currencySpendColumn];
      agentKwargs.currency_col = currencyCodeColumn;
      agentKwargs.date_col = currencyDateColumn;
      agentKwargs.scope_year = scopeYear;
      agentKwargs.target_currency = "USD";
      if (fxOverrideMode === "yearly") {
        const parsed: Record<string, Record<string, number>> = {};
        for (const [ccy, years] of Object.entries(fxOverridesYearly) as [string, Record<string, string>][]) {
          parsed[ccy] = {};
          for (const [yr, val] of Object.entries(years) as [string, string][]) {
            const n = parseFloat(val);
            if (!isNaN(n) && n > 0) parsed[ccy][yr] = n;
          }
        }
        agentKwargs.fx_overrides = parsed;
        agentKwargs.fx_override_mode = "yearly";
      } else {
        const parsed: Record<string, Record<string, Record<string, number>>> = {};
        for (const [ccy, years] of Object.entries(fxOverridesMonthly) as [string, Record<string, Record<string, string>>][]) {
          parsed[ccy] = {};
          for (const [yr, months] of Object.entries(years) as [string, Record<string, string>][]) {
            parsed[ccy][yr] = {};
            for (const [mo, val] of Object.entries(months) as [string, string][]) {
              const n = parseFloat(val);
              if (!isNaN(n) && n > 0) parsed[ccy][yr][mo] = n;
            }
          }
        }
        agentKwargs.fx_overrides = parsed;
        agentKwargs.fx_override_mode = "monthly";
      }
    }

    try {
      const res = await fetch("/api/run-normalization", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_id: agentId, kwargs: agentKwargs, apiKey }),
        signal: controller.signal,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);

      setOpResults(prev => ({ ...prev, [agentId]: data.message }));
      setCompletedOps(prev => new Set([...prev, agentId]));
      if (data.conversion_metrics?.length > 0) {
        setConversionMetrics(data.conversion_metrics[0]);
      }
      if (data.country_norm_metrics) {
        setCountryNormMetrics(data.country_norm_metrics);
      }
      if (data.region_norm_metrics) {
        setRegionNormMetrics(data.region_norm_metrics);
      }
      await fetchOperationPreview();
      log("success", opLabel + ": " + data.message);
    } catch (err: any) {
      if (err.name !== "AbortError") {
        setOpResults(prev => ({ ...prev, [agentId]: "Error: " + err.message }));
        log("error", opLabel + ": " + err.message);
      }
    } finally {
      normControllerRef.current = null;
      setActiveOp(null);
      setShowFxValidation(false);
      setLoadingMessage?.("");
      setLoadingOnCancel?.(null);
    }
  }, [apiKey, fetchOperationPreview, log, supplierCountryColumn, regionColumn, dateFormat, currencySpendColumn, currencyCodeColumn, currencyDateColumn, scopeYear, fxOverrideMode, fxOverridesYearly, fxOverridesMonthly, setLoadingMessage, setLoadingOnCancel]);

  const handleAssess = useCallback(async () => {
    if (!currencyCodeColumn) {
      setAssessError("Select a currency column first.");
      return;
    }
    setAssessLoading(true);
    setLoadingMessage?.("Assessing your data…");
    setAssessResult(null);
    setAssessError(null);
    setConversionMetrics(null);
    setFxOverridesYearly({});
    setFxOverridesMonthly({});
    setShowFxValidation(false);
    try {
      const res = await fetch("/api/assess-currency-conversion", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kwargs: {
            currency_col: currencyCodeColumn,
            spend_col: currencySpendColumn || undefined,
            date_col: currencyDateColumn !== "No date col" ? currencyDateColumn : undefined,
          },
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Assessment failed");
      setAssessResult(data as AssessResult);
      // Initialize empty yearly/monthly override structures from assessment data
      const initYearly: YearlyOverrides = {};
      const initMonthly: MonthlyOverrides = {};
      const dateColSelected = currencyDateColumn !== "No date col";
      (data.unsupported_currencies || []).forEach((u: UnsupportedCurrency) => {
        const years = dateColSelected
          ? Object.keys(u.year_month_breakdown || {})
          : [scopeYear];
        initYearly[u.code] = {};
        initMonthly[u.code] = {};
        for (const yr of years) {
          initYearly[u.code][yr] = "";
          initMonthly[u.code][yr] = {};
          const months = u.year_month_breakdown?.[yr] || [];
          for (const mo of months) {
            initMonthly[u.code][yr][mo] = "";
          }
        }
      });
      setFxOverridesYearly(initYearly);
      setFxOverridesMonthly(initMonthly);
    } catch (err: any) {
      setAssessError(err.message || "Assessment failed");
    } finally {
      setAssessLoading(false);
      setLoadingMessage?.("");
    }
  }, [currencyCodeColumn, currencySpendColumn, currencyDateColumn, scopeYear, setLoadingMessage]);

  const handleAssessSupplierCountry = useCallback(async () => {
    if (!supplierCountryColumn) {
      setScAssessError("Select a supplier country column first.");
      return;
    }
    setScAssessLoading(true);
    setLoadingMessage?.("Assessing your data…");
    setScAssessResult(null);
    setScAssessError(null);
    setCountryNormMetrics(null);
    try {
      const res = await fetch("/api/assess-supplier-country", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kwargs: { country_col: supplierCountryColumn } }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Assessment failed");
      setScAssessResult(data);
    } catch (err: any) {
      setScAssessError(err.message || "Assessment failed");
    } finally {
      setScAssessLoading(false);
      setLoadingMessage?.("");
    }
  }, [supplierCountryColumn, setLoadingMessage]);

  const handleAssessRegion = useCallback(async () => {
    if (!regionColumn) {
      setRegionAssessError("Select a region column first.");
      return;
    }
    setRegionAssessLoading(true);
    setLoadingMessage?.("Assessing your data…");
    setRegionAssessResult(null);
    setRegionAssessError(null);
    setRegionNormMetrics(null);
    try {
      const res = await fetch("/api/assess-region", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kwargs: { region_col: regionColumn } }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Assessment failed");
      setRegionAssessResult(data);
    } catch (err: any) {
      setRegionAssessError(err.message || "Assessment failed");
    } finally {
      setRegionAssessLoading(false);
      setLoadingMessage?.("");
    }
  }, [regionColumn, setLoadingMessage]);

  useEffect(() => {
    if (
      activeTab !== "pipeline" &&
      activeTab !== "download" &&
      operationPreview.columns.length === 0 &&
      !operationPreviewLoading
    ) {
      fetchOperationPreview();
    }
  }, [activeTab, operationPreview.columns.length, operationPreviewLoading, fetchOperationPreview]);

  const downloadControllerRef = useRef<AbortController | null>(null);

  const handleDownload = useCallback(async () => {
    log("info", "Downloading Excel…");
    setLoadingMessage?.("Preparing Excel download…");

    const controller = new AbortController();
    downloadControllerRef.current = controller;
    setLoadingOnCancel?.(() => () => {
      controller.abort();
      downloadControllerRef.current = null;
      setLoadingMessage?.("");
      setLoadingOnCancel?.(null);
      log("info", "Download cancelled.");
    });

    try {
      const res = await fetch("/api/download", { signal: controller.signal });
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const disposition = res.headers.get("content-disposition");
      const match = disposition?.match(/filename="?([^"]+)"?/);
      const filename = match?.[1] || "normalised_data.xlsx";
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
      log("success", "Download complete.");
    } catch (err: any) {
      if (err.name !== "AbortError") {
        log("error", "Download failed: " + err.message);
      }
    } finally {
      downloadControllerRef.current = null;
      setLoadingMessage?.("");
      setLoadingOnCancel?.(null);
    }
  }, [log, setLoadingMessage, setLoadingOnCancel]);


  // Send to Analyzer state
  const [showAnalyzerConfirm, setShowAnalyzerConfirm] = useState(false);
  const [sendingToAnalyzer, setSendingToAnalyzer] = useState(false);
  const [analyzerSendResult, setAnalyzerSendResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [showTransferOverlay, setShowTransferOverlay] = useState(false);

  const handleSendToAnalyzer = useCallback(async () => {
    setSendingToAnalyzer(true);
    setAnalyzerSendResult(null);
    setShowAnalyzerConfirm(false);
    setShowTransferOverlay(true);

    try {
      const res = await fetch("/api/transfer-to-analyzer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apiKey }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || "Transfer failed");

      const analyzerSessionId: string = data.analyzerSessionId;
      if (!analyzerSessionId || typeof analyzerSessionId !== "string") {
        throw new Error("Transfer succeeded but no session ID was returned by the Summarizer.");
      }
      localStorage.setItem("summarizer_api_key", apiKey);
      const url = `${ANALYZER_FE}?sessionId=${encodeURIComponent(analyzerSessionId)}&source=normalizer`;
      window.open(url, "_blank");
      setAnalyzerSendResult({ ok: true, message: "Opened Spend Summarizer in a new tab" });
      log("success", "Data sent to Spend Summarizer successfully.");
    } catch (err: any) {
      setAnalyzerSendResult({ ok: false, message: err.message || "Send failed" });
      log("error", "Failed to send to Spend Summarizer: " + (err.message || "Unknown error"));
    } finally {
      setSendingToAnalyzer(false);
      setShowTransferOverlay(false);
    }
  }, [apiKey, log]);

  /* ── Transfer overlay (shows during Module 2 → Module 3 transfer) ── */
  const transferOverlayEl = (
    <TransferOverlay
      visible={showTransferOverlay}
      destinationName="Spend Summarizer"
      sourceName="Data Normalizer"
    />
  );

  /* ── Render based on activeTab from sidebar ── */

  // If a specific operation tab is selected from the sidebar, show just that one
  const singleOp = OPERATIONS.find(op => op.id === activeTab);

  if (activeTab === "download") {
    return (
      <>{transferOverlayEl}
      <div className="space-y-4">
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

        <SurfaceCard title="Send to Spend Summarizer" subtitle="Transfer your normalized data to the Summarization Module" icon={BarChart3}>
          <div className="space-y-4">
            <p className="text-sm text-neutral-600 dark:text-neutral-400">
              Send the current normalized dataset directly to the Spend Summarizer for procurement analysis, views, and email generation.
            </p>

            {/* Confirmation / action area */}
            <AnimatePresence mode="wait">
              {showAnalyzerConfirm ? (
                <motion.div
                  key="confirm"
                  initial={{ opacity: 0, y: 4 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -4 }}
                  className="flex items-center gap-3 p-3 rounded-xl bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800"
                >
                  <p className="text-sm font-medium text-red-700 dark:text-red-300 flex-1">
                    Send normalized data to Spend Summarizer?
                  </p>
                  <button
                    onClick={handleSendToAnalyzer}
                    disabled={sendingToAnalyzer}
                    className="px-4 py-1.5 rounded-lg text-xs font-semibold bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50"
                  >
                    {sendingToAnalyzer ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : "Send"}
                  </button>
                  <button
                    onClick={() => setShowAnalyzerConfirm(false)}
                    disabled={sendingToAnalyzer}
                    className="px-3 py-1.5 rounded-lg text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors disabled:opacity-50"
                  >
                    Cancel
                  </button>
                </motion.div>
              ) : (
                <motion.div key="button" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <PrimaryButton
                    onClick={() => { setShowAnalyzerConfirm(true); setAnalyzerSendResult(null); }}
                    disabled={sendingToAnalyzer}
                  >
                    {sendingToAnalyzer ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <BarChart3 className="w-4 h-4 mr-2" />}
                    {sendingToAnalyzer ? "Sending..." : "Send to Summarizer"}
                  </PrimaryButton>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Result feedback */}
            {analyzerSendResult && (
              <div className={`flex items-center gap-2 p-3 rounded-xl text-sm font-medium ${
                analyzerSendResult.ok
                  ? "bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800 text-emerald-700 dark:text-emerald-300"
                  : "bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800 text-red-700 dark:text-red-300"
              }`}>
                {analyzerSendResult.ok ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <AlertCircle className="w-4 h-4 shrink-0" />}
                <span className="flex-1">{analyzerSendResult.message}</span>
                {!analyzerSendResult.ok && (
                  <button
                    onClick={handleDownload}
                    className="underline text-xs whitespace-nowrap hover:no-underline"
                  >
                    Download instead
                  </button>
                )}
                <button onClick={() => setAnalyzerSendResult(null)} className="p-0.5 rounded hover:bg-black/5 dark:hover:bg-white/5">
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            )}
          </div>
        </SurfaceCard>
      </div>
      </>
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
            return col.startsWith("SUPPLIER COUNTRY NORMALIZED") || col === "NORMALIZED_VIA";
          case "date":
            return col.startsWith("Norm_Date_");
          case "payment_terms":
            return (
              col.endsWith("_Normalized") ||
              col.startsWith("Payment term duration") ||
              col.startsWith("Discount_Payment_Terms") ||
              col.startsWith("Payment_Terms_Doubt")
            );
          case "region":
            return col.startsWith("Norm_Region_");
          case "plant":
            return col.startsWith("Norm_Plant_");
          case "currency_conversion":
            return col.includes("FX_rate_used_") || col.includes("_converted_inUSD") || col.includes("_conversion_status");
          default:
            return false;
        }
      })
    );
    return (
      <>{transferOverlayEl}
      <SurfaceCard
        title={singleOp.label}
        subtitle={singleOp.desc}
        icon={Icon}
      >
        <div className="space-y-4">
          {/* Generic Run button — hidden for currency_conversion, supplier_country, and region which use Assess flow */}
          {singleOp.id !== "currency_conversion" && singleOp.id !== "supplier_country" && singleOp.id !== "region" && (
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
          )}

          {/* Date format selector — shown only for the date agent */}
          {singleOp.id === "date" && (
            <div className="flex items-center gap-3 p-3 rounded-xl bg-neutral-50 dark:bg-neutral-800/60 border border-neutral-200 dark:border-neutral-700">
              <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300 whitespace-nowrap">Target format</label>
              <select
                value={dateFormat}
                onChange={(e) => setDateFormat(e.target.value)}
                disabled={isRunning || loading}
                className="text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
              >
                <option value="%d-%m-%Y">dd-mm-yyyy</option>
                <option value="%m-%d-%Y">mm-dd-yyyy</option>
              </select>
            </div>
          )}

          {/* Supplier Country — Assess-first workflow */}
          {singleOp.id === "supplier_country" && (
            <div className="space-y-4">
              {/* Column selection */}
              <div className="flex flex-col gap-3 p-4 rounded-xl bg-neutral-50 dark:bg-neutral-800/60 border border-neutral-200 dark:border-neutral-700">
                <div className="flex-1 space-y-1">
                  <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Supplier Country Column <span className="text-red-500">*</span></label>
                  <select
                    value={supplierCountryColumn}
                    onChange={(e) => { setSupplierCountryColumn(e.target.value); setScAssessResult(null); setScAssessError(null); }}
                    disabled={isRunning || loading}
                    className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                  >
                    <option value="" disabled>Select Column</option>
                    {operationPreview.columns.map(col => (
                      <option key={col} value={col}>{col}</option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Assess button */}
              <div className="flex items-center gap-3">
                <PrimaryButton onClick={handleAssessSupplierCountry} disabled={scAssessLoading || isRunning || loading}>
                  {scAssessLoading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Sparkles className="w-4 h-4 mr-2" />}
                  {scAssessLoading ? "Assessing…" : "Assess"}
                </PrimaryButton>
                {isCompleted && (
                  <span className="inline-flex items-center gap-1 text-xs font-medium text-emerald-600">
                    <CheckCircle2 className="w-3.5 h-3.5" /> Completed
                  </span>
                )}
              </div>

              {/* Assess error */}
              {scAssessError && (
                <div className="flex gap-3 p-3 rounded-xl bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800">
                  <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-red-500 dark:text-red-400" />
                  <p className="text-xs text-red-700 dark:text-red-300">{scAssessError}</p>
                </div>
              )}

              {/* Assessment panel */}
              {scAssessResult && scAssessResult.population && (
                <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 p-4 space-y-4 bg-neutral-50 dark:bg-neutral-800/60">
                  {/* Population */}
                  <div className="flex flex-wrap items-center gap-2 text-sm">
                    <span className="font-medium text-neutral-700 dark:text-neutral-300">Country column population:</span>
                    <span className={`font-semibold ${scAssessResult.population.warn ? "text-amber-600" : "text-emerald-600"}`}>
                      {scAssessResult.population.pct_populated}%
                    </span>
                    <span className="text-neutral-500">
                      ({scAssessResult.population.n_populated} / {scAssessResult.population.n_total} rows populated)
                    </span>
                    {scAssessResult.population.warn && (
                      <span className="text-xs text-amber-600 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 px-2 py-0.5 rounded-full">
                        Below 60% threshold
                      </span>
                    )}
                  </div>

                  {!scAssessResult.population.warn && (
                    <div className="flex gap-3 p-3 rounded-xl bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800">
                      <CheckCircle2 className="w-4 h-4 mt-0.5 flex-shrink-0 text-emerald-500 dark:text-emerald-400" />
                      <p className="text-sm text-emerald-700 dark:text-emerald-300">Column is well populated. Ready to normalize.</p>
                    </div>
                  )}

                  {/* Confirm & Run */}
                  <PrimaryButton
                    onClick={() => handleRunOperation(singleOp.id)}
                    disabled={isRunning || loading}
                  >
                    {isRunning ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <ArrowRight className="w-4 h-4 mr-2" />}
                    {isRunning ? "Normalizing…" : "Confirm & Run"}
                  </PrimaryButton>
                </div>
              )}

              {/* Post-normalization summary */}
              {countryNormMetrics && (
                <div className="rounded-xl border border-emerald-200 dark:border-emerald-800 p-4 bg-emerald-50 dark:bg-emerald-950/20 space-y-1 text-sm">
                  <p className="font-semibold text-emerald-800 dark:text-emerald-300 mb-2">Normalization Summary</p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Total rows normalized: <strong>{countryNormMetrics.n_normalized}</strong> / {countryNormMetrics.n_total}
                  </p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Rows via direct ISO lookup: <strong>{countryNormMetrics.n_deterministic}</strong>
                  </p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Rows normalized via AI: <strong>{countryNormMetrics.n_ai}</strong>
                  </p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Distinct supplier countries: <strong>{countryNormMetrics.n_distinct}</strong>
                  </p>
                  {(countryNormMetrics.n_empty + countryNormMetrics.n_unresolved) > 0 && (
                    <div className="mt-2 space-y-0.5 text-neutral-600 dark:text-neutral-400">
                      <p className="font-medium">Rows not normalized:</p>
                      {countryNormMetrics.n_empty > 0 && (
                        <p className="ml-3">• Missing or empty values: {countryNormMetrics.n_empty}</p>
                      )}
                      {countryNormMetrics.n_unresolved > 0 && (
                        <p className="ml-3">• Unrecognized country values: {countryNormMetrics.n_unresolved}</p>
                      )}
                    </div>
                  )}
                  {countryNormMetrics.ai_errors?.length > 0 && (
                    <div className="flex gap-3 p-3 mt-2 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800">
                      <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-amber-500 dark:text-amber-400" />
                      <div className="space-y-1">
                        <p className="text-xs font-medium text-amber-700 dark:text-amber-300">AI normalization errors:</p>
                        {countryNormMetrics.ai_errors.map((err, i) => (
                          <p key={i} className="text-xs text-amber-600 dark:text-amber-400">{err}</p>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="pt-3 mt-2 border-t border-emerald-200 dark:border-emerald-800 flex items-center gap-2">
                    <PrimaryButton onClick={handleDownload}>
                      <Download className="w-4 h-4 mr-2" />
                      Download Excel
                    </PrimaryButton>
                    <button
                      onClick={handleSendToAnalyzer}
                      disabled={sendingToAnalyzer}
                      className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-semibold text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors disabled:opacity-50"
                    >
                      {sendingToAnalyzer ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <BarChart3 className="w-3.5 h-3.5" />}
                      Send to Summarizer
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Region — Assess-first workflow */}
          {singleOp.id === "region" && (
            <div className="space-y-4">
              {/* Recommendation callout */}
              <div className="flex gap-3 p-3 rounded-xl bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 text-sm text-blue-700 dark:text-blue-300">
                <Info className="w-4 h-4 mt-0.5 flex-shrink-0 text-blue-500 dark:text-blue-400" />
                <p>It is recommended to complete <strong>Supplier Country</strong> normalization before performing Region normalization to ensure better accuracy.</p>
              </div>

              {/* Column selection */}
              <div className="flex flex-col gap-3 p-4 rounded-xl bg-neutral-50 dark:bg-neutral-800/60 border border-neutral-200 dark:border-neutral-700">
                <div className="flex-1 space-y-1">
                  <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Region / Country Column <span className="text-red-500">*</span></label>
                  <select
                    value={regionColumn}
                    onChange={(e) => { setRegionColumn(e.target.value); setRegionAssessResult(null); setRegionAssessError(null); }}
                    disabled={isRunning || loading}
                    className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                  >
                    <option value="" disabled>Select Column</option>
                    {operationPreview.columns.map(col => (
                      <option key={col} value={col}>{col}</option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Assess button */}
              <div className="flex items-center gap-3">
                <PrimaryButton onClick={handleAssessRegion} disabled={regionAssessLoading || isRunning || loading}>
                  {regionAssessLoading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Sparkles className="w-4 h-4 mr-2" />}
                  {regionAssessLoading ? "Assessing…" : "Assess"}
                </PrimaryButton>
                {isCompleted && (
                  <span className="inline-flex items-center gap-1 text-xs font-medium text-emerald-600">
                    <CheckCircle2 className="w-3.5 h-3.5" /> Completed
                  </span>
                )}
              </div>

              {/* Assess error */}
              {regionAssessError && (
                <div className="flex gap-3 p-3 rounded-xl bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800">
                  <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-red-500 dark:text-red-400" />
                  <p className="text-xs text-red-700 dark:text-red-300">{regionAssessError}</p>
                </div>
              )}

              {/* Assessment panel */}
              {regionAssessResult && regionAssessResult.population && (
                <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 p-4 space-y-4 bg-neutral-50 dark:bg-neutral-800/60">
                  {/* Population */}
                  <div className="flex flex-wrap items-center gap-2 text-sm">
                    <span className="font-medium text-neutral-700 dark:text-neutral-300">Column population:</span>
                    <span className={`font-semibold ${regionAssessResult.population.warn ? "text-amber-600" : "text-emerald-600"}`}>
                      {regionAssessResult.population.pct_populated}%
                    </span>
                    <span className="text-neutral-500">
                      ({regionAssessResult.population.n_populated} / {regionAssessResult.population.n_total} rows populated)
                    </span>
                    {regionAssessResult.population.warn && (
                      <span className="text-xs text-amber-600 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 px-2 py-0.5 rounded-full">
                        Below 60% threshold
                      </span>
                    )}
                  </div>

                  {/* Column type message */}
                  <div className="flex gap-3 p-3 rounded-xl bg-neutral-100 dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700">
                    <Info className="w-4 h-4 mt-0.5 flex-shrink-0 text-neutral-500" />
                    <p className="text-sm text-neutral-600 dark:text-neutral-400">
                      {/region/i.test(regionColumn)
                        ? "Region will be normalized using the selected Region column."
                        : "Region will be derived using the selected Country column."}
                    </p>
                  </div>

                  {!regionAssessResult.population.warn && (
                    <div className="flex gap-3 p-3 rounded-xl bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800">
                      <CheckCircle2 className="w-4 h-4 mt-0.5 flex-shrink-0 text-emerald-500 dark:text-emerald-400" />
                      <p className="text-sm text-emerald-700 dark:text-emerald-300">Column is well populated. Ready to normalize.</p>
                    </div>
                  )}

                  {/* Confirm & Run */}
                  <PrimaryButton
                    onClick={() => handleRunOperation(singleOp.id)}
                    disabled={isRunning || loading}
                  >
                    {isRunning ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <ArrowRight className="w-4 h-4 mr-2" />}
                    {isRunning ? "Normalizing…" : "Confirm & Run"}
                  </PrimaryButton>
                </div>
              )}

              {/* Post-normalization summary */}
              {regionNormMetrics && (
                <div className="rounded-xl border border-emerald-200 dark:border-emerald-800 p-4 bg-emerald-50 dark:bg-emerald-950/20 space-y-1 text-sm">
                  <p className="font-semibold text-emerald-800 dark:text-emerald-300 mb-2">Normalization Summary</p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Total rows normalized: <strong>{regionNormMetrics.n_normalized}</strong> / {regionNormMetrics.n_total}
                  </p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Rows via deterministic lookup: <strong>{regionNormMetrics.n_deterministic}</strong>
                  </p>
                  <p className="text-emerald-700 dark:text-emerald-400">
                    Rows normalized via AI: <strong>{regionNormMetrics.n_ai}</strong>
                  </p>
                  {regionNormMetrics.n_from_country > 0 && (
                    <p className="text-emerald-700 dark:text-emerald-400">
                      Blank regions filled from Country column: <strong>{regionNormMetrics.n_from_country}</strong>
                    </p>
                  )}
                  {(regionNormMetrics.n_empty + regionNormMetrics.n_unresolved) > 0 && (
                    <div className="mt-2 space-y-0.5 text-neutral-600 dark:text-neutral-400">
                      <p className="font-medium">Rows not normalized:</p>
                      {regionNormMetrics.n_empty > 0 && (
                        <p className="ml-3">• Missing or empty values: {regionNormMetrics.n_empty}</p>
                      )}
                      {regionNormMetrics.n_unresolved > 0 && (
                        <p className="ml-3">• Unrecognized region values: {regionNormMetrics.n_unresolved}</p>
                      )}
                    </div>
                  )}
                  {regionNormMetrics.ai_errors?.length > 0 && (
                    <div className="flex gap-3 p-3 mt-2 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800">
                      <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-amber-500 dark:text-amber-400" />
                      <div className="space-y-1">
                        <p className="text-xs font-medium text-amber-700 dark:text-amber-300">AI normalization errors:</p>
                        {regionNormMetrics.ai_errors.map((err: string, i: number) => (
                          <p key={i} className="text-xs text-amber-600 dark:text-amber-400">{err}</p>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="pt-3 mt-2 border-t border-emerald-200 dark:border-emerald-800 flex items-center gap-2">
                    <PrimaryButton onClick={handleDownload}>
                      <Download className="w-4 h-4 mr-2" />
                      Download Excel
                    </PrimaryButton>
                    <button
                      onClick={handleSendToAnalyzer}
                      disabled={sendingToAnalyzer}
                      className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-semibold text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors disabled:opacity-50"
                    >
                      {sendingToAnalyzer ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <BarChart3 className="w-3.5 h-3.5" />}
                      Send to Summarizer
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Currency Conversion — Assess-first workflow */}
          {singleOp.id === "currency_conversion" && (
            <div className="space-y-4">
              {/* Recommendation callout */}
              <div className="flex gap-3 p-3 rounded-xl bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 text-sm text-blue-700 dark:text-blue-300">
                <Info className="w-4 h-4 mt-0.5 flex-shrink-0 text-blue-500 dark:text-blue-400" />
                <p>We recommend applying the <strong>Date Normalization</strong> step before currency conversion, as standardized date formats improve FX rate mapping accuracy.</p>
              </div>

              {/* Column selection */}
              <div className="flex flex-col gap-3 p-4 rounded-xl bg-neutral-50 dark:bg-neutral-800/60 border border-neutral-200 dark:border-neutral-700">
                <div className="flex flex-col sm:flex-row gap-4">
                  <div className="flex-1 space-y-1">
                    <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Currency Column <span className="text-red-500">*</span></label>
                    <select
                      value={currencyCodeColumn}
                      onChange={(e) => { setCurrencyCodeColumn(e.target.value); setAssessResult(null); setConversionMetrics(null); setFxOverridesYearly({}); setFxOverridesMonthly({}); setShowFxValidation(false); }}
                      disabled={isRunning || loading}
                      className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                    >
                      <option value="" disabled>Select Column</option>
                      {operationPreview.columns.map(col => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                    </select>
                  </div>

                  <div className="flex-1 space-y-1">
                    <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Spend Column <span className="text-red-500">*</span></label>
                    <select
                      value={currencySpendColumn}
                      onChange={(e) => { setCurrencySpendColumn(e.target.value); setAssessResult(null); setConversionMetrics(null); setFxOverridesYearly({}); setFxOverridesMonthly({}); setShowFxValidation(false); }}
                      disabled={isRunning || loading}
                      className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                    >
                      <option value="" disabled>Select Column</option>
                      {operationPreview.columns.map(col => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="flex flex-col sm:flex-row gap-4">
                  <div className="flex-1 space-y-1">
                    <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Date Column</label>
                    <select
                      value={currencyDateColumn}
                      onChange={(e) => { setCurrencyDateColumn(e.target.value); setAssessResult(null); setConversionMetrics(null); setFxOverridesYearly({}); setFxOverridesMonthly({}); setShowFxValidation(false); }}
                      disabled={isRunning || loading}
                      className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                    >
                      {operationPreview.columns.map(col => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                      <option value="No date col">No Date col</option>
                    </select>
                  </div>

                  {currencyDateColumn === "No date col" && (
                    <div className="flex-1 space-y-1">
                      <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Scope Year <span className="text-red-500">*</span></label>
                      <select
                        value={scopeYear}
                        onChange={(e) => setScopeYear(e.target.value)}
                        disabled={isRunning || loading}
                        className="w-full text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-red-500/40"
                      >
                        <option value="2023">2023</option>
                        <option value="2024">2024</option>
                        <option value="2025">2025</option>
                        <option value="2026">2026</option>
                      </select>
                    </div>
                  )}
                </div>
              </div>

              {/* Assess button */}
              <div className="flex items-center gap-3">
                <PrimaryButton onClick={handleAssess} disabled={assessLoading || isRunning || loading}>
                  {assessLoading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Sparkles className="w-4 h-4 mr-2" />}
                  {assessLoading ? "Assessing…" : "Assess"}
                </PrimaryButton>
                {isCompleted && (
                  <span className="inline-flex items-center gap-1 text-xs font-medium text-emerald-600">
                    <CheckCircle2 className="w-3.5 h-3.5" /> Completed
                  </span>
                )}
              </div>

              {/* Assess error */}
              {assessError && (
                <div className="flex gap-3 p-3 rounded-xl bg-red-50 dark:bg-red-950/20 border border-red-200 dark:border-red-800">
                  <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-red-500 dark:text-red-400" />
                  <p className="text-xs text-red-700 dark:text-red-300">{assessError}</p>
                </div>
              )}

              {/* Assessment panel */}
              {assessResult && (
                <div className="rounded-xl border border-neutral-200 dark:border-neutral-700 p-4 space-y-4 bg-neutral-50 dark:bg-neutral-800/60">
                  {/* Population */}
                  {assessResult.population && (
                    <div className="flex flex-wrap items-center gap-2 text-sm">
                      <span className="font-medium text-neutral-700 dark:text-neutral-300">Currency column population:</span>
                      <span className={`font-semibold ${assessResult.population.warn ? "text-amber-600" : "text-emerald-600"}`}>
                        {assessResult.population.pct_populated}%
                      </span>
                      <span className="text-neutral-500">
                        ({assessResult.population.n_populated} / {assessResult.population.n_total} rows populated)
                      </span>
                      {assessResult.population.warn && (
                        <span className="text-xs text-amber-600 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 px-2 py-0.5 rounded-full">
                          Below 60% threshold
                        </span>
                      )}
                    </div>
                  )}

                  {/* Warnings */}
                  {assessResult.warnings.length > 0 && (
                    <div className="flex gap-3 p-3 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800">
                      <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-amber-500 dark:text-amber-400" />
                      <ul className="space-y-1">
                        {assessResult.warnings.map((w, i) => (
                          <li key={i} className="text-xs text-amber-700 dark:text-amber-300">{w}</li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* Unsupported currencies panel */}
                  {assessResult.unsupported_currencies.length > 0 && (
                    <UnsupportedCurrencyPanel
                      currencies={assessResult.unsupported_currencies}
                      dateColumnSelected={currencyDateColumn !== "No date col"}
                      scopeYear={scopeYear}
                      overrideMode={fxOverrideMode}
                      onOverrideModeChange={(mode) => {
                        setFxOverrideMode(mode);
                        setFxOverridesYearly({});
                        setFxOverridesMonthly({});
                        setShowFxValidation(false);
                      }}
                      yearlyOverrides={fxOverridesYearly}
                      onYearlyOverridesChange={setFxOverridesYearly}
                      monthlyOverrides={fxOverridesMonthly}
                      onMonthlyOverridesChange={setFxOverridesMonthly}
                      showValidation={showFxValidation}
                      disabled={isRunning || loading}
                    />
                  )}

                  {assessResult.unsupported_currencies.length === 0 && assessResult.warnings.length === 0 && (
                    <div className="flex gap-3 p-3 rounded-xl bg-emerald-50 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800">
                      <CheckCircle2 className="w-4 h-4 mt-0.5 flex-shrink-0 text-emerald-500 dark:text-emerald-400" />
                      <p className="text-sm text-emerald-700 dark:text-emerald-300">All currencies are supported. Ready to convert.</p>
                    </div>
                  )}

                  {/* Missing entries warning */}
                  {showFxValidation && (
                    <div className="flex gap-3 p-3 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800">
                      <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0 text-amber-500 dark:text-amber-400" />
                      <p className="text-sm text-amber-700 dark:text-amber-300">
                        Missing entries — rows with missing rates will not be converted.
                      </p>
                    </div>
                  )}

                  {/* Confirm & Run / Proceed anyway */}
                  {showFxValidation ? (
                    <button
                      onClick={() => handleRunOperation(singleOp.id)}
                      disabled={isRunning || loading}
                      className="inline-flex items-center justify-center gap-2 px-4 py-2 text-sm font-medium rounded-xl bg-amber-500 hover:bg-amber-600 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {isRunning ? <Loader2 className="w-4 h-4 animate-spin" /> : <AlertTriangle className="w-4 h-4" />}
                      {isRunning ? "Converting…" : "Proceed anyway"}
                    </button>
                  ) : (
                    <PrimaryButton
                      onClick={() => {
                        // Check for missing entries — if found, show warning + rename button instead of running
                        if (assessResult && assessResult.unsupported_currencies.length > 0) {
                          const dateColSelected = currencyDateColumn !== "No date col";
                          const empty = hasEmptyOverrides(fxOverrideMode, assessResult.unsupported_currencies, fxOverridesYearly, fxOverridesMonthly, dateColSelected, scopeYear);
                          if (empty) {
                            setShowFxValidation(true);
                            return;
                          }
                        }
                        handleRunOperation(singleOp.id);
                      }}
                      disabled={isRunning || loading}
                    >
                      {isRunning ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <ArrowRight className="w-4 h-4 mr-2" />}
                      {isRunning ? "Converting…" : "Confirm & Run"}
                    </PrimaryButton>
                  )}
                </div>
              )}

              {/* Post-conversion summary */}
              {conversionMetrics && (
                <div className="rounded-xl border border-emerald-200 dark:border-emerald-800 p-4 bg-emerald-50 dark:bg-emerald-950/20 space-y-1 text-sm">
                  <p className="font-semibold text-emerald-800 dark:text-emerald-300 mb-2">Conversion Summary</p>
                  <p className="text-emerald-700 dark:text-emerald-400">Rows converted: <strong>{conversionMetrics.n_converted}</strong></p>
                  <p className="text-emerald-700 dark:text-emerald-400">Rows via fallback: <strong>{conversionMetrics.n_fallback}</strong></p>
                  {(conversionMetrics.n_currency_missing + conversionMetrics.n_unsupported + conversionMetrics.n_spend_invalid + conversionMetrics.n_date_unparseable) > 0 && (
                    <div className="mt-2 space-y-0.5 text-neutral-600 dark:text-neutral-400">
                      <p className="font-medium">Rows not converted:</p>
                      {conversionMetrics.n_currency_missing > 0 && (
                        <p className="ml-3">• Currency missing: {conversionMetrics.n_currency_missing}</p>
                      )}
                      {conversionMetrics.n_unsupported > 0 && (
                        <p className="ml-3">• Unsupported currency (no rate provided): {conversionMetrics.n_unsupported}</p>
                      )}
                      {conversionMetrics.n_spend_invalid > 0 && (
                        <p className="ml-3">• Spend value invalid: {conversionMetrics.n_spend_invalid}</p>
                      )}
                      {conversionMetrics.n_date_unparseable > 0 && (
                        <p className="ml-3">• Date unparseable: {conversionMetrics.n_date_unparseable}</p>
                      )}
                    </div>
                  )}
                  <div className="pt-3 mt-2 border-t border-emerald-200 dark:border-emerald-800 flex items-center gap-2">
                    <PrimaryButton onClick={handleDownload}>
                      <Download className="w-4 h-4 mr-2" />
                      Download Excel
                    </PrimaryButton>
                    <button
                      onClick={handleSendToAnalyzer}
                      disabled={sendingToAnalyzer}
                      className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-semibold text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors disabled:opacity-50"
                    >
                      {sendingToAnalyzer ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <BarChart3 className="w-3.5 h-3.5" />}
                      Send to Summarizer
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {opResults[singleOp.id] && singleOp.id !== "currency_conversion" && singleOp.id !== "supplier_country" && (
            <div className="space-y-3">
              <div className={`whitespace-pre-wrap rounded-xl p-3 text-sm ${
                opResults[singleOp.id].startsWith("Error")
                  ? "bg-red-50 dark:bg-red-950/20 text-red-700 dark:text-red-400 border border-red-200 dark:border-red-800"
                  : "bg-emerald-50 dark:bg-emerald-950/20 text-emerald-700 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-800"
              }`}>
                {opResults[singleOp.id]}
              </div>
              {!opResults[singleOp.id].startsWith("Error") && (
                <div className="flex items-center gap-2">
                  <PrimaryButton onClick={handleDownload}>
                    <Download className="w-4 h-4 mr-2" />
                    Download Excel
                  </PrimaryButton>
                  <button
                    onClick={handleSendToAnalyzer}
                    disabled={sendingToAnalyzer}
                    className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-semibold text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors disabled:opacity-50"
                  >
                    {sendingToAnalyzer ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <BarChart3 className="w-3.5 h-3.5" />}
                    Send to Analyzer
                  </button>
                </div>
              )}
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
      </>
    );
  }

  /* ── Default: Download view (fallback) ── */
  return (
    <>{transferOverlayEl}
    <div className="space-y-6">
      <SurfaceCard title="Download Normalized Data" subtitle="Export your cleaned and standardized dataset" icon={Download}>
        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <PrimaryButton onClick={handleDownload} disabled={loading || activeOp !== null}>
              <Download className="w-4 h-4 mr-2" />
              Download Excel
            </PrimaryButton>
          </div>
        </div>
      </SurfaceCard>
    </div>
    </>
  );
}
