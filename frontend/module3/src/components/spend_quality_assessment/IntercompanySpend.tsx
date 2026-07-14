import React, { useCallback, useState } from "react";
import { motion } from "framer-motion";
import {
  AlertCircle,
  Building2,
  Loader2,
  Play,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  detectIntercompanySpend,
  type IntercompanyVendorMatch,
} from "../../api/client";

function fmtNumber(val: number): string {
  return Math.round(val).toLocaleString();
}

interface IntercompanySpendProps {
  sessionId: string;
}

export default function IntercompanySpend({
  sessionId,
}: IntercompanySpendProps) {
  const [clientName, setClientName] = useState("");
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [feasible, setFeasible] = useState<boolean | null>(null);
  const [summaryRows, setSummaryRows] = useState<number | null>(null);
  const [summarySpend, setSummarySpend] = useState<number | null>(null);
  const [vendors, setVendors] = useState<IntercompanyVendorMatch[]>([]);

  const runScan = useCallback(async () => {
    const trimmed = clientName.trim();
    if (!trimmed) {
      setError("Enter your client company name.");
      return;
    }

    setScanning(true);
    setError(null);
    setMessage(null);
    try {
      const data = await detectIntercompanySpend(sessionId, trimmed);
      setFeasible(data.feasible);
      setMessage(data.message);
      if (data.feasible && data.summary) {
        setSummaryRows(data.summary.matchingRows);
        setSummarySpend(data.summary.totalSpend);
        setVendors(data.vendors);
      } else if (data.feasible) {
        setSummaryRows(0);
        setSummarySpend(0);
        setVendors([]);
      } else {
        setSummaryRows(null);
        setSummarySpend(null);
        setVendors([]);
      }
    } catch (err: any) {
      setError(err?.message || "Scan failed");
    } finally {
      setScanning(false);
    }
  }, [clientName, sessionId]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      runScan();
    }
  };

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      <SurfaceCard noPadding>
        <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-center gap-2 mb-2">
            <Building2 className="w-6 h-6" />
            <h2 className="text-xl font-semibold tracking-tight">
              Intercompany Spend
            </h2>
          </div>
          <p className="text-red-50/90 text-sm max-w-xl">
            Enter your client company name to scan vendor names for likely
            intercompany suppliers.
          </p>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-3">
          Client Company Name
        </h3>
        <div className="flex gap-2">
          <input
            type="text"
            value={clientName}
            onChange={(e) => {
              setClientName(e.target.value);
              setError(null);
            }}
            onKeyDown={handleKeyDown}
            placeholder="e.g. Acme Corp"
            disabled={scanning}
            className="flex-1 px-4 py-2.5 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm focus:outline-none focus:ring-2 focus:ring-red-500/40 disabled:opacity-50"
          />
          <button
            onClick={runScan}
            disabled={scanning || !clientName.trim()}
            className="inline-flex items-center gap-1.5 px-5 py-2.5 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors disabled:opacity-50 shrink-0"
          >
            {scanning ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Play className="w-4 h-4" />
            )}
            Scan Vendors
          </button>
        </div>
        <p className="text-xs text-neutral-400 mt-2">
          Searches the mapped <strong>Vendor Name</strong> column only. Vendors
          whose name contains your client name are flagged as intercompany.
        </p>
        {error && <p className="text-xs text-red-500 mt-2">{error}</p>}
      </SurfaceCard>

      {feasible === false && message && !scanning && (
        <SurfaceCard>
          <div className="flex gap-3 items-start">
            <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-1">
                Scan not available
              </p>
              <p className="text-sm text-neutral-600 dark:text-neutral-400">
                {message}
              </p>
            </div>
          </div>
        </SurfaceCard>
      )}

      {feasible && (message || summaryRows != null) && (
        <SurfaceCard noPadding>
          {message && (
            <div className="px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
              <p className="text-sm text-neutral-600 dark:text-neutral-400">
                {message}
              </p>
            </div>
          )}
          {summaryRows != null && summaryRows > 0 && (
            <div className="px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
              <p className="text-sm text-neutral-700 dark:text-neutral-300">
                Vendors matching{" "}
                <span className="font-semibold text-indigo-600 dark:text-indigo-400">
                  {clientName.trim()}
                </span>
                :{" "}
                <span className="font-semibold">
                  {fmtNumber(summaryRows)} rows
                </span>
                ,{" "}
                <span className="font-semibold">
                  {fmtNumber(summarySpend ?? 0)} spend
                </span>
              </p>
            </div>
          )}
          {vendors.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500">
                    <th className="px-6 py-3">Vendor Name</th>
                    <th className="px-4 py-3 text-right">Rows</th>
                    <th className="px-4 py-3 text-right">Spend</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                  {vendors.map((v) => (
                    <tr key={v.vendor}>
                      <td className="px-6 py-3 font-medium">{v.vendor}</td>
                      <td className="px-4 py-3 text-right tabular-nums">
                        {fmtNumber(v.matchingRows)}
                      </td>
                      <td className="px-4 py-3 text-right tabular-nums">
                        {fmtNumber(v.totalSpend)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="px-6 py-8 text-sm text-neutral-500 text-center">
              No vendor names matched this client name.
            </p>
          )}
        </SurfaceCard>
      )}
    </motion.div>
  );
}
