import React, { useState, useEffect } from "react";
import { Loader2, ArrowRight, Trash2, RowsIcon, Check, Database, ChevronDown, ChevronRight, X, ExternalLink } from "lucide-react";
import { motion } from "motion/react";
import { SurfaceCard, PrimaryButton } from "../common/ui";

interface DataInventoryProps {
  inventory: any[];
  onProceed: (tableKey: string) => void;
  loading: boolean;
  setLoading: (l: boolean) => void;
  setError: (e: string | null) => void;
  importSource?: string | null;
}

function HeaderRowEditor({
  tableKey,
  onSetHeaderRow,
  onCancel,
  setError,
}: {
  tableKey: string;
  onSetHeaderRow: (tableKey: string, rowIndex: number, customNames?: Record<number, string>) => Promise<void>;
  onCancel: () => void;
  setError: (e: string | null) => void;
}) {
  const [rawPreview, setRawPreview] = useState<any[][] | null>(null);
  const [loadingRaw, setLoadingRaw] = useState(false);
  const [selectedRow, setSelectedRow] = useState<number | null>(null);
  const [customNames, setCustomNames] = useState<Record<number, string>>({});
  const [submitting, setSubmitting] = useState(false);

  const fetchRaw = async () => {
    setLoadingRaw(true);
    try {
      const res = await fetch(`/api/get-raw-preview?tableKey=${encodeURIComponent(tableKey)}`);
      if (!res.ok) throw new Error("Failed to fetch raw data");
      const data = await res.json();
      setRawPreview(data.rawPreview || []);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoadingRaw(false);
    }
  };

  useEffect(() => { fetchRaw(); }, [tableKey]);

  const candidateHeaders = selectedRow !== null && rawPreview ? rawPreview[selectedRow] : null;

  const handleConfirm = async () => {
    if (selectedRow === null) return;
    setSubmitting(true);
    try {
      await onSetHeaderRow(tableKey, selectedRow, Object.keys(customNames).length > 0 ? customNames : undefined);
    } finally {
      setSubmitting(false);
    }
  };

  if (loadingRaw) {
    return (
      <div className="px-6 py-8 flex items-center justify-center gap-2 text-sm text-neutral-500">
        <Loader2 className="w-5 h-5 animate-spin" /> Loading raw tabular data...
      </div>
    );
  }

  if (!rawPreview || rawPreview.length === 0) {
    return (
      <div className="px-6 py-6 text-sm text-neutral-400 italic text-center">No raw data available for this table.</div>
    );
  }

  const maxCols = Math.max(...rawPreview.slice(0, 50).map((r) => r.length));

  return (
    <div className="bg-neutral-50/50 pt-2 pb-6 px-4 border-t border-neutral-100">
      <div className="flex items-center justify-between mb-4 mt-2 px-2">
        <p className="text-sm font-bold text-neutral-700">
          Select a row to use as headers. Click a row number on the left.
        </p>
        <div className="flex gap-2">
          {selectedRow !== null && (
            <button
              type="button"
              onClick={handleConfirm}
              disabled={submitting}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-lg bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50"
            >
              {submitting ? <Loader2 className="w-3.5 h-3.5 animate-spin"/> : <Check className="w-3.5 h-3.5" />}
              Confirm Row {selectedRow}
            </button>
          )}
          <button
            type="button"
            onClick={onCancel}
            disabled={submitting}
            className="inline-flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-lg border border-neutral-200 text-neutral-600 hover:bg-neutral-100 transition-colors disabled:opacity-50"
          >
            <X className="w-3.5 h-3.5" /> Cancel
          </button>
        </div>
      </div>

      {candidateHeaders && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-3 mb-4 space-y-2 max-w-full overflow-hidden">
          <p className="text-[10px] font-bold text-blue-700 uppercase">New headers from row {selectedRow}:</p>
          <div className="flex flex-wrap gap-1.5">
            {candidateHeaders.map((cell: any, i: number) => {
              const isEmpty = cell == null || String(cell).trim() === "";
              return (
                <div key={i} className="flex items-center gap-1">
                  {isEmpty ? (
                    <input
                      type="text"
                      value={customNames[i] || ""}
                      onChange={(e) => setCustomNames((prev) => ({ ...prev, [i]: e.target.value }))}
                      placeholder={`Col ${i + 1}`}
                      className="px-2 py-1 text-xs border border-amber-300 bg-amber-50 rounded-lg w-24 focus:outline-none focus:ring-2 focus:ring-amber-400"
                    />
                  ) : (
                    <span className="px-2 py-1 text-xs font-medium bg-blue-100 text-blue-800 rounded-lg">
                      {String(cell).trim()}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
          {candidateHeaders.some((c: any) => c == null || String(c).trim() === "") && (
            <p className="text-[10px] text-amber-600">Empty cells highlighted -- type custom names above.</p>
          )}
        </div>
      )}

      <div className="overflow-x-auto border border-neutral-200 rounded-lg max-h-80 bg-white">
        <table className="min-w-full text-xs font-mono">
          <thead className="bg-neutral-50 sticky top-0 z-10 shadow-sm">
            <tr>
              <th className="px-2 py-2 text-center font-bold text-neutral-500 border-b border-r border-neutral-200 w-12 whitespace-nowrap">
                Row
              </th>
              {Array.from({ length: maxCols }, (_, i) => (
                <th key={i} className="px-3 py-2 text-left font-bold text-neutral-400 whitespace-nowrap border-b border-neutral-200">
                  Col {i}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100">
            {rawPreview.slice(0, 50).map((row, ri) => (
              <tr
                key={ri}
                className={`transition-colors ${
                  selectedRow === ri
                    ? "bg-blue-50 ring-1 ring-blue-400 ring-inset"
                    : "hover:bg-neutral-50/50"
                }`}
              >
                <td className="px-2 py-1.5 text-center border-r border-neutral-100">
                  <button
                    type="button"
                    onClick={() => { setSelectedRow(ri); setCustomNames({}); }}
                    className={`w-7 h-6 rounded text-[10px] font-bold transition-colors ${
                      selectedRow === ri
                        ? "bg-blue-600 text-white"
                        : "bg-neutral-100 text-neutral-500 hover:bg-red-100 hover:text-red-700"
                    }`}
                    title={`Use row ${ri} as header`}
                  >
                    {ri}
                  </button>
                </td>
                {Array.from({ length: maxCols }, (_, ci) => (
                  <td key={ci} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 max-w-[200px] truncate">
                    {row[ci] != null && row[ci] !== "" ? String(row[ci]) : <span className="text-neutral-300 italic">null</span>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const FormattedTable: React.FC<{ tableKey: string, setError: any, setLoading: any, onUpdated: () => void }> = ({ tableKey, setError, setLoading, onUpdated }) => {
  const [preview, setPreview] = useState<{columns: string[], rows: any[]}>({ columns: [], rows: [] });
  const [fetching, setFetching] = useState(true);
  const [selectedRowIds, setSelectedRowIds] = useState<Set<string | number>>(new Set());

  const fetchPreview = async () => {
    setFetching(true);
    try {
      const res = await fetch(`/api/get-preview?tableKey=${encodeURIComponent(tableKey)}`);
      if (!res.ok) throw new Error("Failed to fetch formatted preview");
      const data = await res.json();
      setPreview(data);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setFetching(false);
    }
  };

  useEffect(() => { fetchPreview(); }, [tableKey]);

  const toggleRowSelection = (ri: string | number) => {
    setSelectedRowIds(prev => {
      const next = new Set(prev);
      if (next.has(ri)) next.delete(ri); else next.add(ri);
      return next;
    });
  };

  const handleDeleteSelected = async () => {
    if (selectedRowIds.size === 0) return;
    if (!window.confirm(`Delete ${selectedRowIds.size} selected row(s)?`)) return;
    
    setLoading(true);
    try {
      const res = await fetch("/api/delete-rows", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tableKey, rowIds: Array.from(selectedRowIds) }),
      });
      if (!res.ok) throw new Error("Failed to delete rows");
      
      setSelectedRowIds(new Set());
      await fetchPreview();
      onUpdated();
    } catch (err: any) { setError(err.message); } finally { setLoading(false); }
  };

  if (fetching) return <div className="p-6 text-center"><Loader2 className="w-5 h-5 mx-auto animate-spin text-neutral-400" /></div>;

  return (
    <div className="bg-white border-t border-neutral-100">
      {selectedRowIds.size > 0 && (
        <div className="px-6 py-3 bg-red-50 border-b border-red-100 flex items-center justify-between">
          <span className="text-xs font-bold text-red-600">{selectedRowIds.size} row(s) selected</span>
          <div className="flex gap-2 items-center">
            <button
              onClick={() => setSelectedRowIds(new Set())}
              className="px-3 py-1 text-[11px] font-medium text-neutral-600 hover:text-neutral-900 transition-colors"
            >
              Clear
            </button>
            <button
              onClick={handleDeleteSelected}
              className="px-3 py-1.5 text-[11px] font-bold bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors shadow-sm"
            >
              Delete Selected Rows
            </button>
          </div>
        </div>
      )}
      <div className="p-4">
        <div className="overflow-x-auto border border-neutral-200 rounded-lg max-h-[500px] shadow-sm">
          <table className="min-w-full text-xs font-mono bg-white">
            <thead className="bg-neutral-50 sticky top-0 z-10 shadow-sm border-b border-neutral-200">
              <tr>
                <th className="px-2 py-2 text-center w-8 border-r border-neutral-200 bg-neutral-50 sticky left-0 z-20 shadow-[1px_0_0_#e5e5e5]"></th>
                <th className="px-2 py-2 text-center font-bold text-neutral-500 border-r border-neutral-200 w-12 whitespace-nowrap hidden md:table-cell">#</th>
                {preview.columns.map((col, i) => (
                  <th key={col + i} className="px-3 py-2 text-left font-bold text-neutral-500 whitespace-nowrap border-r border-neutral-200 last:border-r-0">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-100">
              {preview.rows.map((row, ri) => (
                <tr key={ri} className={`transition-colors ${selectedRowIds.has(ri) ? "bg-red-50/50" : "hover:bg-neutral-50"}`}>
                  <td className="px-2 py-1.5 text-center border-r border-neutral-100 bg-inherit sticky left-0 z-10 shadow-[1px_0_0_#f5f5f5]">
                    <input type="checkbox" checked={selectedRowIds.has(ri)} onChange={() => toggleRowSelection(ri)} className="w-3.5 h-3.5 text-red-500 rounded border-neutral-300 focus:ring-red-500 cursor-pointer" />
                  </td>
                  <td className="px-2 py-1.5 text-center text-[10px] text-neutral-400 border-r border-neutral-100 hidden md:table-cell bg-inherit">
                    {ri}
                  </td>
                  {preview.columns.map((col, ci) => (
                    <td key={col + ci} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 max-w-[200px] truncate border-r border-neutral-100 last:border-r-0">
                      {row[col] != null && row[col] !== "" ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                    </td>
                  ))}
                </tr>
              ))}
              {preview.rows.length === 0 && (
                <tr>
                  <td colSpan={preview.columns.length + 2} className="px-3 py-8 text-center text-neutral-400 italic">
                    No rows currently exist.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="mt-2 text-[10px] text-neutral-500 text-right">Previewing top {preview.rows.length} rows</div>
      </div>
    </div>
  );
}

export default function DataInventory({ inventory, onProceed, loading, setLoading, setError, importSource }: DataInventoryProps) {
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [headerEditTable, setHeaderEditTable] = useState<string | null>(null);
  const [localInventory, setLocalInventory] = useState(inventory);
  const [updates, setUpdates] = useState(0);
  
  useEffect(() => { setLocalInventory(inventory); }, [inventory]);

  const handleDeleteTable = async (tableKey: string) => {
    if (!window.confirm("Delete this table completely?")) return;
    setLoading(true);
    try {
      await fetch("/api/delete-table", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ tableKey }) });
      setLocalInventory(prev => prev.filter(i => i.table_key !== tableKey));
      if (expandedTable === tableKey) setExpandedTable(null);
    } catch (err: any) { setError(err.message); } finally { setLoading(false); }
  };

  const handleSetHeaderRow = async (tableKey: string, rowIndex: number, customNames?: Record<number, string>) => {
    setError(null);
    const payload = { tableKey, rowIndex, customNames };
    const res = await fetch("/api/set-header-row", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error("Failed to set header");
      
    const responseBody = await res.json();
      
    setLocalInventory(prev => prev.map(inv => {
         if (inv.table_key === tableKey) {
             return { ...inv, rows: responseBody.rows, cols: responseBody.columns.length };
         }
         return inv;
    }));

    setHeaderEditTable(null); // Return to formatted table
    setUpdates(u => u + 1); // trigger refresh
  };

  const forceUpdate = () => setUpdates(u => u + 1);

  return (
    <motion.section initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
      <SurfaceCard title="Data Preview" subtitle={`${localInventory.length} table${localInventory.length !== 1 ? "s" : ""} securely extracted from upload.`} icon={Database} noPadding>
        {importSource && (
          <div className="mx-6 mt-4 mb-2 inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-blue-50 dark:bg-blue-950/30 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800">
            <ExternalLink className="w-3 h-3" />
            Imported from {importSource === "stitcher" ? "DataStitcher" : "external module"}
          </div>
        )}
        <div className="divide-y divide-neutral-100 bg-white rounded-b-xl">
          {localInventory.map((inv, i) => {
            const isExpanded = expandedTable === inv.table_key;
            const isHeaderEdit = headerEditTable === inv.table_key;
            return (
              <div key={i} className="group">
                <div className="flex items-center px-6 py-4 hover:bg-neutral-50/60 cursor-pointer transition-colors" onClick={() => { setExpandedTable(isExpanded ? null : inv.table_key); setHeaderEditTable(null); }}>
                  <div className="w-10 h-10 rounded-xl bg-red-50 text-red-600 flex items-center justify-center text-sm font-bold shrink-0 mr-4 shadow-[0_1px_2px_rgba(0,0,0,0.05)]">
                    {inv.table_key.split('.').pop()?.substring(0, 2).toUpperCase() || 'TB'}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-[15px] font-bold text-neutral-900 truncate">{inv.table_key}</p>
                    <p className="text-[11px] text-neutral-500 font-medium mt-0.5 tracking-wide uppercase">{inv.rows.toLocaleString()} rows • {inv.cols.toLocaleString()} cols</p>
                  </div>
                  
                  <div className="flex items-center gap-1.5" onClick={e => e.stopPropagation()}>
                    {isExpanded && !isHeaderEdit && (
                       <button onClick={() => setHeaderEditTable(inv.table_key)} title="Redefine Header Row" className="p-2 text-neutral-400 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors">
                           <RowsIcon className="w-4 h-4" />
                       </button>
                    )}
                    <button onClick={() => handleDeleteTable(inv.table_key)} title="Delete Table" className="p-2 opacity-0 group-hover:opacity-100 transition-opacity text-neutral-400 hover:text-red-600 hover:bg-red-50 rounded-lg">
                      <Trash2 className="w-4 h-4" />
                    </button>
                    <div className="p-2" onClick={() => { setExpandedTable(isExpanded ? null : inv.table_key); setHeaderEditTable(null); }}>
                      {isExpanded ? <ChevronDown className="w-4 h-4 text-neutral-700" /> : <ChevronRight className="w-4 h-4 text-neutral-400" />}
                    </div>
                  </div>
                </div>
                
                {isExpanded && isHeaderEdit && (
                    <HeaderRowEditor 
                        tableKey={inv.table_key} 
                        onSetHeaderRow={handleSetHeaderRow} 
                        onCancel={() => setHeaderEditTable(null)} 
                        setError={setError} 
                    />
                )}

                {isExpanded && !isHeaderEdit && (
                  <div className="bg-neutral-50/30">
                    <FormattedTable key={`${inv.table_key}-${updates}`} tableKey={inv.table_key} setError={setError} setLoading={setLoading} onUpdated={forceUpdate} />
                    <div className="p-4 bg-white border-t border-neutral-100 flex justify-end items-center shadow-[0_-4px_6px_-6px_rgba(0,0,0,0.02)]">
                       <span className="text-xs text-neutral-500 font-medium mr-4">Ready to lock in this specific table?</span>
                       <PrimaryButton onClick={() => onProceed(inv.table_key)} disabled={loading}>
                           Proceed with this Table <ArrowRight className="w-4 h-4 ml-1" />
                       </PrimaryButton>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
          {localInventory.length === 0 && (
            <div className="p-16 text-center border-t border-neutral-100 text-sm font-semibold tracking-wide text-neutral-400 rounded-b-xl">
              No tables found. Please return and upload a package.
            </div>
          )}
        </div>
      </SurfaceCard>
    </motion.section>
  );
}
