import React, { useState, useRef, useCallback } from "react";
import { Upload, Loader2, FileText, Database, ArrowRight, FolderOpen, X, KeyRound, ChevronDown, ChevronRight, Trash2, RowsIcon, Check, CheckCircle2, Zap } from "lucide-react";
import { motion } from "motion/react";
import JSZip from "jszip";
import { SurfaceCard, EmptyState, PrimaryButton, SecondaryButton, itemVariants } from "../common/ui";

const ACCEPTED_EXTENSIONS = [".csv", ".xlsx", ".xlsm", ".xltx", ".xltm", ".zip"];

function fileHasAcceptedExt(name: string) {
  const lower = name.toLowerCase();
  return ACCEPTED_EXTENSIONS.some((ext) => lower.endsWith(ext));
}

async function readEntryAsFile(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

async function readDirectoryEntries(dirEntry: FileSystemDirectoryEntry): Promise<FileSystemEntry[]> {
  return new Promise((resolve, reject) => {
    const reader = dirEntry.createReader();
    const allEntries: FileSystemEntry[] = [];
    const readBatch = () => {
      reader.readEntries((entries) => {
        if (entries.length === 0) {
          resolve(allEntries);
        } else {
          allEntries.push(...entries);
          readBatch();
        }
      }, reject);
    };
    readBatch();
  });
}

async function collectFilesFromEntry(entry: FileSystemEntry, path = ""): Promise<{ path: string; file: File }[]> {
  if (entry.isFile) {
    const file = await readEntryAsFile(entry as FileSystemFileEntry);
    if (fileHasAcceptedExt(file.name)) {
      return [{ path: path + file.name, file }];
    }
    return [];
  }
  if (entry.isDirectory) {
    const dirEntries = await readDirectoryEntries(entry as FileSystemDirectoryEntry);
    const results: { path: string; file: File }[] = [];
    for (const child of dirEntries) {
      results.push(...(await collectFilesFromEntry(child, path + entry.name + "/")));
    }
    return results;
  }
  return [];
}

async function buildZipFromFiles(files: { path: string; file: File }[]): Promise<File> {
  const zip = new JSZip();
  for (const { path, file } of files) {
    zip.file(path, await file.arrayBuffer());
  }
  const blob = await zip.generateAsync({ type: "blob" });
  return new File([blob], "upload.zip", { type: "application/zip" });
}

async function extractZipEntries(zipFile: File): Promise<{ path: string; file: File }[]> {
  const zip = await JSZip.loadAsync(await zipFile.arrayBuffer());
  const results: { path: string; file: File }[] = [];
  for (const [path, entry] of Object.entries(zip.files)) {
    if (entry.dir) continue;
    if (!fileHasAcceptedExt(path)) continue;
    const blob = await entry.async("blob");
    const name = path.split("/").pop() || path;
    results.push({ path, file: new File([blob], name) });
  }
  return results;
}

interface DataLoadingProps {
  step: number;
  file: File | null;
  setFile: (file: File | null) => void;
  apiKey: string;
  setApiKey: (key: string) => void;
  handleUpload: () => void;
  loading: boolean;
  sessionId: string;
  inventory: any[];
  previews: Record<string, { columns: string[]; rows: any[] }>;
  uploadWarnings: { file: string; message: string }[];
  handleGenerateAppendPlan: () => void;
  onProceedToAppend: () => void;
  onProceedSingleTable?: () => void;
  onDeleteTable: (tableKey: string) => void;
  onSetHeaderRow: (tableKey: string, rowIndex: number, customNames?: Record<number, string>) => void;
  onDeleteRows?: (tableKey: string, rowIds: (string | number)[]) => void;
}

function HeaderRowEditor({
  sessionId,
  tableKey,
  onSetHeaderRow,
  onCancel,
}: {
  sessionId: string;
  tableKey: string;
  onSetHeaderRow: (tableKey: string, rowIndex: number, customNames?: Record<number, string>) => void;
  onCancel: () => void;
}) {
  const [rawPreview, setRawPreview] = useState<any[][] | null>(null);
  const [loadingRaw, setLoadingRaw] = useState(false);
  const [selectedRow, setSelectedRow] = useState<number | null>(null);
  const [customNames, setCustomNames] = useState<Record<number, string>>({});

  const fetchRaw = async () => {
    setLoadingRaw(true);
    try {
      const res = await fetch("/api/get-raw-preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId, tableKey }),
      });
      if (!res.ok) throw new Error("Failed to fetch raw data");
      const data = await res.json();
      setRawPreview(data.rawPreview || []);
    } catch {
      setRawPreview([]);
    } finally {
      setLoadingRaw(false);
    }
  };

  // eslint-disable-next-line react-hooks/exhaustive-deps
  React.useEffect(() => { fetchRaw(); }, [sessionId, tableKey]);

  const candidateHeaders = selectedRow !== null && rawPreview ? rawPreview[selectedRow] : null;

  const handleConfirm = () => {
    if (selectedRow === null) return;
    onSetHeaderRow(tableKey, selectedRow, Object.keys(customNames).length > 0 ? customNames : undefined);
  };

  if (loadingRaw) {
    return (
      <div className="px-6 pb-4 flex items-center gap-2 text-xs text-neutral-500 dark:text-neutral-400">
        <Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading raw data...
      </div>
    );
  }

  if (!rawPreview || rawPreview.length === 0) {
    return (
      <div className="px-6 pb-4 text-xs text-neutral-400 dark:text-neutral-500 italic">No raw data available for this table.</div>
    );
  }

  const maxCols = Math.max(...rawPreview.slice(0, 50).map((r) => r.length));

  return (
    <div className="px-6 pb-4 space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-xs font-bold text-neutral-700 dark:text-neutral-300">
          Select a row to use as headers. Click a row number on the left.
        </p>
        <div className="flex gap-2">
          {selectedRow !== null && (
            <button
              type="button"
              onClick={handleConfirm}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-lg bg-red-600 text-white hover:bg-red-700 transition-colors"
            >
              <Check className="w-3 h-3" /> Confirm Row {selectedRow}
            </button>
          )}
          <button
            type="button"
            onClick={onCancel}
            className="inline-flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-lg border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
          >
            <X className="w-3 h-3" /> Cancel
          </button>
        </div>
      </div>

      {candidateHeaders && (
        <div className="bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800 rounded-xl p-3 space-y-2">
          <p className="text-[10px] font-bold text-blue-700 dark:text-blue-400 uppercase">New headers from row {selectedRow}:</p>
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
                      className="px-2 py-1 text-xs border border-amber-300 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/30 rounded-lg w-24 focus:outline-none focus:ring-2 focus:ring-amber-400"
                    />
                  ) : (
                    <span className="px-2 py-1 text-xs font-medium bg-blue-100 dark:bg-blue-950/40 text-blue-800 dark:text-blue-300 rounded-lg">
                      {String(cell).trim()}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
          {candidateHeaders.some((c: any) => c == null || String(c).trim() === "") && (
            <p className="text-[10px] text-amber-600 dark:text-amber-400">Empty cells highlighted -- type custom names above.</p>
          )}
        </div>
      )}

      <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-lg max-h-80">
        <table className="min-w-full text-xs">
          <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0 z-10">
            <tr>
              <th className="px-2 py-2 text-center font-bold text-neutral-500 dark:text-neutral-400 border-b border-r border-neutral-200 dark:border-neutral-700 w-12 whitespace-nowrap">
                Row
              </th>
              {Array.from({ length: maxCols }, (_, i) => (
                <th key={i} className="px-3 py-2 text-left font-bold text-neutral-400 dark:text-neutral-500 whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700">
                  Col {i}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {rawPreview.slice(0, 50).map((row, ri) => (
              <tr
                key={ri}
                className={`transition-colors ${
                  selectedRow === ri
                    ? "bg-blue-100 dark:bg-blue-950/40 ring-1 ring-blue-400 ring-inset"
                    : "hover:bg-neutral-50/50 dark:hover:bg-neutral-800"
                }`}
              >
                <td className="px-2 py-1.5 text-center border-r border-neutral-100 dark:border-neutral-800">
                  <button
                    type="button"
                    onClick={() => { setSelectedRow(ri); setCustomNames({}); }}
                    className={`w-7 h-6 rounded text-[10px] font-bold transition-colors ${
                      selectedRow === ri
                        ? "bg-blue-600 text-white"
                        : "bg-neutral-100 dark:bg-neutral-700 text-neutral-500 dark:text-neutral-400 hover:bg-red-100 dark:hover:bg-red-950/30 hover:text-red-700 dark:hover:text-red-400"
                    }`}
                    title={`Use row ${ri} as header`}
                  >
                    {ri}
                  </button>
                </td>
                {Array.from({ length: maxCols }, (_, ci) => (
                  <td key={ci} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate">
                    {row[ci] != null ? String(row[ci]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
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

export default function DataLoading({
  step,
  file,
  setFile,
  apiKey,
  setApiKey,
  handleUpload,
  loading,
  sessionId,
  inventory,
  previews,
  uploadWarnings,
  handleGenerateAppendPlan,
  onProceedToAppend,
  onProceedSingleTable,
  onDeleteTable,
  onSetHeaderRow,
  onDeleteRows,
}: DataLoadingProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const [fileLabel, setFileLabel] = useState<string | null>(null);
  const [fileList, setFileList] = useState<{ path: string; file: File }[]>([]);
  const [zipping, setZipping] = useState(false);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [headerEditTable, setHeaderEditTable] = useState<string | null>(null);
  const [confirmDeleteKey, setConfirmDeleteKey] = useState<string | null>(null);
  const [selectedRowIds, setSelectedRowIds] = useState<Record<string, Set<string | number>>>({});
  const dragCounter = useRef(0);
  const zipInputRef = useRef<HTMLInputElement>(null);
  const folderInputRef = useRef<HTMLInputElement>(null);
  const accumulatedFilesRef = useRef<{ path: string; file: File }[]>([]);

  const rebuildFromAccumulated = useCallback(async () => {
    const all = accumulatedFilesRef.current;
    if (all.length === 0) return;
    const zipFile = await buildZipFromFiles(all);
    setFile(zipFile);
    setFileLabel(`${all.length} file${all.length !== 1 ? "s" : ""} ready for upload`);
    setFileList([...all]);
  }, [setFile]);

  const toggleRowSelection = (tableKey: string, rowId: string | number) => {
    setSelectedRowIds(prev => {
      const current = new Set(prev[tableKey] || []);
      if (current.has(rowId)) current.delete(rowId); else current.add(rowId);
      return { ...prev, [tableKey]: current };
    });
  };

  const handleDeleteSelectedRows = (tableKey: string) => {
    const ids = Array.from(selectedRowIds[tableKey] || []);
    if (ids.length === 0 || !onDeleteRows) return;
    if (!window.confirm(`Delete ${ids.length} selected row(s)?`)) return;
    onDeleteRows(tableKey, ids);
    setSelectedRowIds(prev => { const n = { ...prev }; delete n[tableKey]; return n; });
  };

  const processDroppedItems = useCallback(async (items: DataTransferItemList) => {
    const entries: FileSystemEntry[] = [];
    for (let i = 0; i < items.length; i++) {
      const entry = items[i].webkitGetAsEntry?.();
      if (entry) entries.push(entry);
    }
    if (entries.length === 0) return;

    setZipping(true);
    try {
      const newFiles: { path: string; file: File }[] = [];

      for (const entry of entries) {
        if (entry.isFile && entry.name.toLowerCase().endsWith(".zip")) {
          const f = await readEntryAsFile(entry as FileSystemFileEntry);
          newFiles.push(...(await extractZipEntries(f)));
        } else {
          newFiles.push(...(await collectFilesFromEntry(entry)));
        }
      }

      if (newFiles.length === 0) return;

      const existingPaths = new Set(accumulatedFilesRef.current.map((f) => f.path));
      const deduped = newFiles.filter((f) => !existingPaths.has(f.path));
      accumulatedFilesRef.current = [...accumulatedFilesRef.current, ...deduped];
      await rebuildFromAccumulated();
    } finally {
      setZipping(false);
    }
  }, [rebuildFromAccumulated]);

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current++;
    if (dragCounter.current === 1) setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current--;
    if (dragCounter.current === 0) setIsDragOver(false);
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback(async (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current = 0;
    setIsDragOver(false);
    if (e.dataTransfer.items) {
      await processDroppedItems(e.dataTransfer.items);
    }
  }, [processDroppedItems]);

  const handleZipInputChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files || e.target.files.length === 0) return;

    setZipping(true);
    try {
      const newFiles: { path: string; file: File }[] = [];
      for (let i = 0; i < e.target.files.length; i++) {
        const f = e.target.files[i];
        if (f.name.toLowerCase().endsWith(".zip")) {
          newFiles.push(...(await extractZipEntries(f)));
        } else if (fileHasAcceptedExt(f.name)) {
          newFiles.push({ path: f.name, file: f });
        }
      }
      if (newFiles.length === 0) return;

      const existingPaths = new Set(accumulatedFilesRef.current.map((f) => f.path));
      const deduped = newFiles.filter((f) => !existingPaths.has(f.path));
      accumulatedFilesRef.current = [...accumulatedFilesRef.current, ...deduped];
      await rebuildFromAccumulated();
    } finally {
      setZipping(false);
      if (zipInputRef.current) zipInputRef.current.value = "";
    }
  }, [rebuildFromAccumulated]);

  const handleFolderInputChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const fileList = e.target.files;
    if (!fileList || fileList.length === 0) return;

    setZipping(true);
    try {
      const newFiles: { path: string; file: File }[] = [];
      for (let i = 0; i < fileList.length; i++) {
        const f = fileList[i];
        if (fileHasAcceptedExt(f.name)) {
          const path = (f as any).webkitRelativePath || f.name;
          newFiles.push({ path, file: f });
        }
      }
      if (newFiles.length === 0) return;

      const existingPaths = new Set(accumulatedFilesRef.current.map((f) => f.path));
      const deduped = newFiles.filter((f) => !existingPaths.has(f.path));
      accumulatedFilesRef.current = [...accumulatedFilesRef.current, ...deduped];
      await rebuildFromAccumulated();
    } finally {
      setZipping(false);
      if (folderInputRef.current) folderInputRef.current.value = "";
    }
  }, [rebuildFromAccumulated]);

  const removeFile = useCallback(async (path: string) => {
    accumulatedFilesRef.current = accumulatedFilesRef.current.filter((f) => f.path !== path);
    const all = accumulatedFilesRef.current;
    if (all.length === 0) {
      setFile(null);
      setFileLabel(null);
      setFileList([]);
      return;
    }
    const zipFile = await buildZipFromFiles(all);
    setFile(zipFile);
    setFileLabel(`${all.length} file${all.length !== 1 ? "s" : ""} ready for upload`);
    setFileList([...all]);
  }, [setFile]);

  const clearFile = () => {
    setFile(null);
    setFileLabel(null);
    setFileList([]);
    accumulatedFilesRef.current = [];
    if (zipInputRef.current) zipInputRef.current.value = "";
    if (folderInputRef.current) folderInputRef.current.value = "";
  };

  return (
    <div className="space-y-6">
      {/* Step 1: Upload */}
      {step === 1 && (
        <SurfaceCard
          title="Source Data Ingestion"
          subtitle="Upload a ZIP archive, a folder, or individual CSV / Excel files to begin."
          icon={Upload}
        >
          <div className="space-y-8">
            <div className="space-y-4">
              <label className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-neutral-400 dark:text-neutral-500 font-semibold">
                <FileText className="w-4 h-4" />
                Data Package
              </label>

              <input
                ref={zipInputRef}
                type="file"
                className="sr-only"
                onChange={handleZipInputChange}
                accept=".zip,.csv,.xlsx,.xlsm,.xltx,.xltm"
                multiple
              />
              <input
                ref={folderInputRef}
                type="file"
                className="sr-only"
                onChange={handleFolderInputChange}
                {...({ webkitdirectory: "", directory: "" } as any)}
              />

              <div
                onDragEnter={handleDragEnter}
                onDragLeave={handleDragLeave}
                onDragOver={handleDragOver}
                onDrop={handleDrop}
                className={`relative border-2 border-dashed rounded-2xl p-10 text-center transition-all ${
                  isDragOver
                    ? "border-red-400 bg-red-50/40 dark:bg-red-950/30 scale-[1.01]"
                    : file
                      ? "border-emerald-200 bg-emerald-50/30 dark:bg-emerald-950/30"
                      : "border-neutral-200 dark:border-neutral-700 hover:border-red-300 hover:bg-neutral-50/50 dark:hover:bg-neutral-800"
                }`}
              >
                {zipping ? (
                  <div className="flex flex-col items-center gap-3">
                    <Loader2 className="w-10 h-10 text-red-500 animate-spin" />
                    <p className="text-sm font-medium text-neutral-600 dark:text-neutral-300">Zipping files...</p>
                  </div>
                ) : file ? (
                  <div className="flex flex-col items-center gap-2">
                    <div className="w-16 h-16 rounded-2xl bg-emerald-100 dark:bg-emerald-950/40 text-emerald-600 dark:text-emerald-400 flex items-center justify-center">
                      <Upload className="w-8 h-8" />
                    </div>
                    <p className="text-sm font-bold text-neutral-900 dark:text-white">{fileLabel}</p>
                    {fileList.length > 0 && (
                      <div className="w-full max-h-40 overflow-y-auto px-6 py-2">
                        <div className="flex flex-col gap-1">
                          {fileList.map((f) => {
                            const basename = f.path.split("/").pop() || f.path;
                            return (
                              <div
                                key={f.path}
                                title={f.path}
                                className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 text-[11px] font-medium text-emerald-700 dark:text-emerald-300"
                              >
                                <FileText className="w-3 h-3 shrink-0" />
                                <span className="truncate flex-1 text-left">{basename}</span>
                                <button
                                  type="button"
                                  onClick={(e) => { e.stopPropagation(); removeFile(f.path); }}
                                  className="ml-auto shrink-0 hover:text-red-500 transition-colors"
                                >
                                  <X className="w-3 h-3" />
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}
                    <p className="text-xs text-neutral-400 dark:text-neutral-500">Drop or browse more files to add them</p>
                    <div className="flex items-center gap-3">
                      <button
                        type="button"
                        onClick={() => zipInputRef.current?.click()}
                        className="inline-flex items-center gap-1 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                      >
                        <FileText className="w-3 h-3" /> Add Files
                      </button>
                      <span className="text-neutral-300 dark:text-neutral-600">|</span>
                      <button
                        type="button"
                        onClick={clearFile}
                        className="inline-flex items-center gap-1 text-xs text-neutral-500 dark:text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                      >
                        <X className="w-3 h-3" /> Remove All
                      </button>
                    </div>
                    <motion.div
                      initial={{ scale: 0 }}
                      animate={{ scale: 1 }}
                      className="absolute -top-2 -right-2 bg-emerald-500 text-white p-1 rounded-full shadow-lg"
                    >
                      <CheckCircle2 className="w-4 h-4" />
                    </motion.div>
                  </div>
                ) : (
                  <div className="space-y-4">
                    <div className="w-16 h-16 rounded-2xl bg-neutral-100 dark:bg-neutral-700 text-neutral-400 dark:text-neutral-500 flex items-center justify-center mx-auto">
                      <Upload className="w-8 h-8" />
                    </div>
                    <div className="space-y-1">
                      <p className="text-sm font-bold text-neutral-900 dark:text-white">
                        {isDragOver ? "Drop files here" : "Drag & drop files or a folder here"}
                      </p>
                      <p className="text-xs text-neutral-500 dark:text-neutral-400">ZIP, CSV, or Excel files (Max 300MB)</p>
                    </div>
                    <div className="flex items-center justify-center gap-3 pt-2">
                      <button
                        type="button"
                        onClick={() => zipInputRef.current?.click()}
                        className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                      >
                        <FileText className="w-3.5 h-3.5" /> Browse Files
                      </button>
                      <button
                        type="button"
                        onClick={() => folderInputRef.current?.click()}
                        className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                      >
                        <FolderOpen className="w-3.5 h-3.5" /> Browse Folder
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="space-y-2">
              <label className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-neutral-400 dark:text-neutral-500 font-semibold">
                <KeyRound className="w-4 h-4" />
                Portkey API Key
              </label>
              <input
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="Paste your Portkey API key here"
                className="w-full px-4 py-2.5 text-sm border border-neutral-200 dark:border-neutral-700 rounded-xl bg-white dark:bg-neutral-900 focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-transparent placeholder:text-neutral-400 dark:placeholder:text-neutral-500 transition-shadow"
              />
              <p className="text-xs text-neutral-400 dark:text-neutral-500">Required for AI-powered steps (append plan, mapping, procurement).</p>
            </div>
          </div>

          <div className="pt-6 border-t border-neutral-100 dark:border-neutral-800 flex justify-end">
            <PrimaryButton
              onClick={handleUpload}
              disabled={!file || loading || zipping}
            >
              {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Initialize Workspace"}
              <ArrowRight className="w-4 h-4" />
            </PrimaryButton>
          </div>
        </SurfaceCard>
      )}

      {/* Step 2: Data Preview */}
      {step === 2 && (
        <SurfaceCard
          title="Data Preview"
          subtitle={`${inventory.length} table${inventory.length !== 1 ? "s" : ""} extracted. Expand to preview, delete unwanted tables, or redefine headers.`}
          icon={Database}
          noPadding
        >

          {uploadWarnings.length > 0 && (
            <div className="mx-6 mt-4 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-xl p-4 space-y-2">
              <p className="text-sm font-bold text-amber-700 dark:text-amber-400">
                {uploadWarnings.length} file{uploadWarnings.length !== 1 ? "s" : ""} failed to parse:
              </p>
              {uploadWarnings.map((w, i) => (
                <p key={i} className="text-xs text-amber-600 dark:text-amber-400">
                  <span className="font-semibold">{w.file}</span>: {w.message}
                </p>
              ))}
            </div>
          )}

          <div className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {inventory.map((inv, i) => {
              const isExpanded = expandedTable === inv.table_key;
              const isHeaderEdit = headerEditTable === inv.table_key;
              const preview = previews[inv.table_key];
              const isConfirmingDelete = confirmDeleteKey === inv.table_key;
              return (
                <div key={i}>
                  <div className="flex items-center px-6 py-4 hover:bg-neutral-50/50 dark:hover:bg-neutral-800 transition-colors">
                    <button
                      type="button"
                      onClick={() => { setExpandedTable(isExpanded ? null : inv.table_key); setHeaderEditTable(null); }}
                      className="flex items-center gap-3 flex-1 min-w-0 text-left"
                    >
                      <div className="w-8 h-8 rounded-lg bg-red-50 dark:bg-red-950/30 text-red-600 dark:text-red-400 flex items-center justify-center text-xs font-bold shrink-0">
                        {inv.table_key.split('.').pop()?.substring(0, 2).toUpperCase()}
                      </div>
                      <span className="text-sm font-bold text-neutral-900 dark:text-white truncate">{inv.table_key}</span>
                    </button>

                    <span className="text-xs font-medium text-neutral-500 dark:text-neutral-400 px-3 whitespace-nowrap">
                      {inv.rows.toLocaleString()} rows &times; {inv.cols.toLocaleString()} cols
                    </span>

                    <div className="flex items-center gap-1.5 shrink-0">
                      {isExpanded && !isHeaderEdit && (
                        <button
                          type="button"
                          onClick={(e) => { e.stopPropagation(); setHeaderEditTable(inv.table_key); }}
                          title="Redefine header row"
                          className="p-1.5 rounded-lg text-neutral-400 dark:text-neutral-500 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-950/30 transition-colors"
                        >
                          <RowsIcon className="w-4 h-4" />
                        </button>
                      )}

                      {!isConfirmingDelete ? (
                        <button
                          type="button"
                          onClick={(e) => { e.stopPropagation(); setConfirmDeleteKey(inv.table_key); }}
                          title="Delete table"
                          className="p-1.5 rounded-lg text-neutral-400 dark:text-neutral-500 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/30 transition-colors"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      ) : (
                        <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                          <button
                            type="button"
                            onClick={() => { onDeleteTable(inv.table_key); setConfirmDeleteKey(null); if (expandedTable === inv.table_key) setExpandedTable(null); }}
                            className="px-2 py-1 text-[10px] font-bold bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
                          >
                            Confirm
                          </button>
                          <button
                            type="button"
                            onClick={() => setConfirmDeleteKey(null)}
                            className="px-2 py-1 text-[10px] font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 rounded-md hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                          >
                            Cancel
                          </button>
                        </div>
                      )}

                      <button
                        type="button"
                        onClick={() => { setExpandedTable(isExpanded ? null : inv.table_key); setHeaderEditTable(null); }}
                      >
                        {isExpanded
                          ? <ChevronDown className="w-4 h-4 text-neutral-400 dark:text-neutral-500" />
                          : <ChevronRight className="w-4 h-4 text-neutral-400 dark:text-neutral-500" />}
                      </button>
                    </div>
                  </div>

                  {isExpanded && isHeaderEdit && (
                    <HeaderRowEditor
                      sessionId={sessionId}
                      tableKey={inv.table_key}
                      onSetHeaderRow={(tk, ri, cn) => { onSetHeaderRow(tk, ri, cn); setHeaderEditTable(null); }}
                      onCancel={() => setHeaderEditTable(null)}
                    />
                  )}

                  {isExpanded && !isHeaderEdit && preview && preview.columns.length > 0 && (() => {
                    const tableSelectedRows = selectedRowIds[inv.table_key] || new Set();
                    const hasRecordId = preview.columns.includes("RECORD_ID");
                    return (
                    <div className="px-6 pb-4">
                      {onDeleteRows && tableSelectedRows.size > 0 && (
                        <div className="flex items-center gap-2 mb-2 px-2 py-1.5 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 rounded-lg">
                          <span className="text-xs font-bold text-red-600 dark:text-red-400">{tableSelectedRows.size} row{tableSelectedRows.size !== 1 ? "s" : ""} selected</span>
                          <button
                            type="button"
                            onClick={() => handleDeleteSelectedRows(inv.table_key)}
                            className="ml-auto px-3 py-1 text-[11px] font-bold bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
                          >
                            Delete Selected Rows
                          </button>
                          <button
                            type="button"
                            onClick={() => setSelectedRowIds(prev => { const n = { ...prev }; delete n[inv.table_key]; return n; })}
                            className="px-2 py-1 text-[11px] font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-300 rounded-md hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
                          >
                            Clear
                          </button>
                        </div>
                      )}
                      <div className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-lg max-h-80">
                        <table className="min-w-full text-xs">
                          <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0 z-10">
                            <tr>
                              {onDeleteRows && (
                                <th className="px-2 py-2 text-center font-bold text-neutral-400 dark:text-neutral-500 whitespace-nowrap border-b border-r border-neutral-200 dark:border-neutral-700 w-8" />
                              )}
                              <th className="px-2 py-2 text-center font-bold text-neutral-400 dark:text-neutral-500 whitespace-nowrap border-b border-r border-neutral-200 dark:border-neutral-700 w-10">
                                #
                              </th>
                              {preview.columns.map((col) => (
                                <th key={col} className="px-3 py-2 text-left font-bold text-neutral-500 dark:text-neutral-400 whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700">
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                            {preview.rows.map((row, ri) => {
                              const rowId = hasRecordId ? row["RECORD_ID"] : ri;
                              const isRowSelected = tableSelectedRows.has(rowId);
                              return (
                              <tr key={ri} className={`transition-colors ${isRowSelected ? "bg-red-50 dark:bg-red-950/30" : "hover:bg-red-50/30"}`}>
                                {onDeleteRows && (
                                  <td className="px-2 py-1.5 text-center border-r border-neutral-100 dark:border-neutral-800">
                                    <input
                                      type="checkbox"
                                      checked={isRowSelected}
                                      onChange={() => toggleRowSelection(inv.table_key, rowId)}
                                      className="w-3 h-3 text-red-600 rounded border-neutral-300 focus:ring-red-500"
                                    />
                                  </td>
                                )}
                                <td className="px-2 py-1.5 text-center text-[10px] text-neutral-400 dark:text-neutral-500 font-mono border-r border-neutral-100 dark:border-neutral-800">
                                  {hasRecordId ? row["RECORD_ID"] : ri}
                                </td>
                                {preview.columns.map((col) => (
                                  <td key={col} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 dark:text-neutral-300 max-w-[200px] truncate">
                                    {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                                  </td>
                                ))}
                              </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      <p className="text-[10px] text-neutral-400 dark:text-neutral-500 mt-1.5">
                        Showing first {preview.rows.length} of {inv.rows.toLocaleString()} rows
                      </p>
                    </div>
                    );
                  })()}

                  {isExpanded && !isHeaderEdit && (!preview || preview.columns.length === 0) && (
                    <div className="px-6 pb-4">
                      <p className="text-xs text-neutral-400 dark:text-neutral-500 italic">Empty table — no rows to preview.</p>
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          <div className="p-6 border-t border-neutral-100 dark:border-neutral-800 flex justify-end gap-3">
            {inventory.length === 1 && onProceedSingleTable && (
              <PrimaryButton
                onClick={onProceedSingleTable}
                disabled={loading}
              >
                <Zap className="w-4 h-4" />
                {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Single Table Flow"}
                <ArrowRight className="w-4 h-4" />
              </PrimaryButton>
            )}
            <SecondaryButton
              onClick={onProceedToAppend}
              disabled={loading}
            >
              {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Proceed to Append Strategy"}
              <ArrowRight className="w-4 h-4" />
            </SecondaryButton>
          </div>
        </SurfaceCard>
      )}
    </div>
  );
}
