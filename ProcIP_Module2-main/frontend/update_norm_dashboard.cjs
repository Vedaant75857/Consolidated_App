const fs = require('fs');
const path = require('path');

const targetFile = 'c:\\Users\\78464\\Downloads\\Consolidated_App\\ProcIP_Module2-main\\frontend\\src\\components\\module-2\\NormDashboard.tsx';
let content = fs.readFileSync(targetFile, 'utf8');

// 1. Add new states
content = content.replace(
  'const [currencySpendColumn, setCurrencySpendColumn] = useState<string>("");',
  `const [currencySpendColumn, setCurrencySpendColumn] = useState<string>("");
  const [currencyCodeColumn, setCurrencyCodeColumn] = useState<string>("");
  const [currencyDateColumn, setCurrencyDateColumn] = useState<string>("");
  const [scopeYear, setScopeYear] = useState<string>("2024");
  const [conversionWarnings, setConversionWarnings] = useState<string[]>([]);
  const [showConversionModal, setShowConversionModal] = useState(false);`
);

// 2. Pre-fill logic correctly inside useEffect
const useEffectStr = `  useEffect(() => {
    const cols = operationPreview.columns;
    if (cols.length === 0) return;
    if (currencySpendColumn && cols.includes(currencySpendColumn)) return;

    const preferred = cols.find((col) =>
      /(spend|amount|cost|price|total|value|charge|fee|payment|pay|invoice)/i.test(col)
    );
    setCurrencySpendColumn(preferred || "");
  }, [operationPreview.columns, currencySpendColumn]);`;

const replacementUseEffect = `  useEffect(() => {
    const cols = operationPreview.columns;
    if (cols.length === 0) return;
    
    if (!currencySpendColumn || !cols.includes(currencySpendColumn)) {
      const preferredSpend = cols.find((col) => /(spend|amount|cost|price|total|value|charge|fee|payment|pay|invoice)/i.test(col));
      if (preferredSpend) setCurrencySpendColumn(preferredSpend);
    }
    
    if (!currencyCodeColumn || !cols.includes(currencyCodeColumn)) {
      const preferredCode = cols.find((col) => /(currency|curr|code)/i.test(col)) || "";
      setCurrencyCodeColumn(preferredCode);
    }
    
    if (!currencyDateColumn || (!cols.includes(currencyDateColumn) && currencyDateColumn !== "No date col")) {
      const candidateDates = cols.filter(c => (c.toLowerCase().includes("date") || c.toLowerCase().includes("dob") || c.toLowerCase().includes("time")) && !c.startsWith("Norm_Date_"));
      setCurrencyDateColumn(candidateDates.length > 0 ? candidateDates[0] : "No date col");
    }
  }, [operationPreview.columns, currencySpendColumn, currencyCodeColumn, currencyDateColumn]);`;

content = content.replace(useEffectStr, replacementUseEffect);

// 3. Update handleRunOperation
const runOpOld = `  const handleRunOperation = useCallback(async (agentId: string) => {
    if (agentId === "currency_conversion" && !currencySpendColumn) {
      log("error", "Select a spend column before running Currency Conversion.");
      return;
    }
    setActiveOp(agentId);
    const opLabel = OPERATIONS.find(o => o.id === agentId)?.label || agentId;
    log("info", "Running " + opLabel + "…");
    // Build agent-specific kwargs
    const agentKwargs: Record<string, any> = {};
    if (agentId === "date") {
      agentKwargs.user_format = dateFormat;
    }
    if (agentId === "currency_conversion") {
      agentKwargs.spend_cols = [currencySpendColumn];
      agentKwargs.target_currency = "USD";
    }`;

const runOpNew = `  const executeAgent = async (agentId: string, agentKwargs: any, opLabel: string) => {
    try {
      const res = await fetch("/api/run-normalization", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_id: agentId, kwargs: agentKwargs, apiKey }),
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
  };

  type RunOperationFn = (agentId: string, bypassWarning?: boolean) => Promise<void>;
  
  const handleRunOperation: RunOperationFn = useCallback(async (agentId, bypassWarning = false) => {
    if (agentId === "currency_conversion" && (!currencySpendColumn || !currencyCodeColumn)) {
      log("error", "Select both a spend column and a currency column before running Currency Conversion.");
      return;
    }
    
    const opLabel = OPERATIONS.find(o => o.id === agentId)?.label || agentId;
    const agentKwargs: Record<string, any> = {};
    
    if (agentId === "date") {
      agentKwargs.user_format = dateFormat;
    }
    if (agentId === "currency_conversion") {
      agentKwargs.spend_cols = [currencySpendColumn];
      agentKwargs.currency_col = currencyCodeColumn;
      agentKwargs.date_col = currencyDateColumn;
      agentKwargs.scope_year = scopeYear;
      agentKwargs.target_currency = "USD";
      
      if (!bypassWarning) {
        log("info", "Assessing currency properties...");
        try {
            const assessRes = await fetch("/api/assess-currency-conversion", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ kwargs: { currency_col: currencyCodeColumn } })
            });
            const assessData = await assessRes.json();
            
            if (assessData.needs_confirmation) {
                setConversionWarnings(assessData.warnings || []);
                setShowConversionModal(true);
                return; // Stop execution, await modal confirmation
            }
        } catch(err: any) {
            log("error", "Assessment failed: " + err.message);
            return;
        }
      }
    }
    
    setActiveOp(agentId);
    log("info", "Running " + opLabel + "…");
    await executeAgent(agentId, agentKwargs, opLabel);
  }, [apiKey, currencySpendColumn, currencyCodeColumn, currencyDateColumn, scopeYear, fetchOperationPreview, log]);`;

content = content.replace(runOpOld, runOpNew);

// Strip out old try/catch logic and remove old useCallback deps
const tryCatchBlockOld = `    try {
      const res = await fetch("/api/run-normalization", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_id: agentId, kwargs: agentKwargs, apiKey }),
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
  }, [apiKey, currencySpendColumn, dateFormat, fetchOperationPreview, log]);`;

content = content.replace(tryCatchBlockOld, ``);

// 4. Also fix handleRunPipeline to pass the updated kwargs
content = content.replace(
  `        pipelineKwargs.currency_conversion = {
          spend_cols: [currencySpendColumn],
          target_currency: "USD",
        };`,
  `        pipelineKwargs.currency_conversion = {
          spend_cols: [currencySpendColumn],
          currency_col: currencyCodeColumn,
          date_col: currencyDateColumn,
          scope_year: scopeYear,
          target_currency: "USD",
        };`
);


// 5. Update UI for Currency Agent
const dateAgentUI = `{/* Date format selector — shown only for the date agent */}
          {singleOp.id === "date" && (`;

const newCurrencyUI = `{singleOp.id === "currency_conversion" && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 p-4 rounded-xl bg-neutral-50 dark:bg-neutral-800/60 border border-neutral-200 dark:border-neutral-700">
              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Spend Column <span className="text-red-500">*</span></label>
                <select value={currencySpendColumn} onChange={(e) => setCurrencySpendColumn(e.target.value)} disabled={isRunning || loading} className="text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-2 w-full">
                  <option value="">Select Spend...</option>
                  {operationPreview.columns.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Currency Column <span className="text-red-500">*</span></label>
                <select value={currencyCodeColumn} onChange={(e) => setCurrencyCodeColumn(e.target.value)} disabled={isRunning || loading} className="text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-2 w-full">
                  <option value="">Select Currency...</option>
                  {operationPreview.columns.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Date Column</label>
                <select value={currencyDateColumn} onChange={(e) => setCurrencyDateColumn(e.target.value)} disabled={isRunning || loading} className="text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-2 w-full">
                  {operationPreview.columns.filter(c => (c.toLowerCase().includes("date") || c.toLowerCase().includes("dob") || c.toLowerCase().includes("time")) && !c.startsWith("Norm_Date_")).map(c => <option key={c} value={c}>{c}</option>)}
                  <option value="No date col">No date col</option>
                </select>
              </div>
              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-neutral-700 dark:text-neutral-300">Scope Year</label>
                <select value={scopeYear} onChange={(e) => setScopeYear(e.target.value)} disabled={isRunning || loading} className="text-sm rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-3 py-2 w-full">
                  {[2023, 2024, 2025, 2026].map(y => <option key={y} value={y}>{y}</option>)}
                </select>
              </div>
            </div>
          )}

          {/* Date format selector — shown only for the date agent */}
          {singleOp.id === "date" && (`

content = content.replace(dateAgentUI, newCurrencyUI);

content = content.replace(
    '      <SurfaceCard title="Normalization Dashboard"',
    `      
      {showConversionModal && (
        <div className="fixed justify-center z-[100] inset-0 flex items-center p-4 bg-black/60 backdrop-blur-sm shadow-xl animate-in fade-in" style={{zIndex: 9999}}>
          <div className="w-full max-w-lg bg-white dark:bg-neutral-900 rounded-2xl shadow-xl overflow-hidden animate-in fade-in zoom-in-95 duration-200 border border-neutral-200 dark:border-neutral-800">
            <div className="p-6">
              <h3 className="text-lg font-semibold text-neutral-900 dark:text-white mb-2">Currency Conversion Warnings</h3>
              <p className="text-sm text-neutral-600 dark:text-neutral-400 mb-4">
                We detected the following issues which may result in some rows not being converted (they will output NaN):
              </p>
              <ul className="space-y-2 mb-6 text-sm text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/30 p-4 rounded-xl border border-amber-200 dark:border-amber-900/50">
                {conversionWarnings.map((w, i) => (
                  <li key={i} className="flex gap-2 items-start"><span className="shrink-0 mt-0.5">•</span> <span>{w}</span></li>
                ))}
              </ul>
              <div className="flex justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setShowConversionModal(false)}
                  className="px-4 py-2 text-sm font-medium text-neutral-700 dark:text-neutral-300 border border-neutral-200 dark:border-neutral-700 hover:bg-neutral-100 dark:hover:bg-neutral-800 rounded-lg transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowConversionModal(false);
                    handleRunOperation("currency_conversion", true);
                  }}
                  className="px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg transition-colors border border-red-700"
                >
                  Proceed Anyway
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      <SurfaceCard title="Normalization Dashboard"`
);

fs.writeFileSync(targetFile, content);
console.log('Successfully updated NormDashboard.tsx');
