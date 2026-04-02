import React, { useState, useRef, useCallback } from "react";
import { Upload, Loader2, FileText, ArrowRight, FolderOpen, X, KeyRound, CheckCircle2 } from "lucide-react";
import { motion } from "motion/react";
import JSZip from "jszip";
import { SurfaceCard, PrimaryButton } from "../common/ui";

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
        if (entries.length === 0) resolve(allEntries);
        else { allEntries.push(...entries); readBatch(); }
      }, reject);
    };
    readBatch();
  });
}

async function collectFilesFromEntry(entry: FileSystemEntry, path = ""): Promise<{ path: string; file: File }[]> {
  if (entry.isFile) {
    const file = await readEntryAsFile(entry as FileSystemFileEntry);
    if (fileHasAcceptedExt(file.name)) return [{ path: path + file.name, file }];
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
  for (const { path, file } of files) zip.file(path, await file.arrayBuffer());
  const blob = await zip.generateAsync({ type: "blob" });
  return new File([blob], "upload.zip", { type: "application/zip" });
}

interface DataLoadingProps {
  step: number;
  file: File | null;
  setFile: (file: File | null) => void;
  setFilename: (name: string | null) => void;
  apiKey: string;
  setApiKey: (key: string) => void;
  handleUpload: () => void;
  loading: boolean;
  filename: string | null;
}

export default function DataLoading({
  step,
  file,
  setFile,
  setFilename,
  apiKey,
  setApiKey,
  handleUpload,
  loading,
  filename
}: DataLoadingProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const [fileLabel, setFileLabel] = useState<string | null>(null);
  const [zipping, setZipping] = useState(false);
  
  const dragCounter = useRef(0);
  const zipInputRef = useRef<HTMLInputElement>(null);
  const folderInputRef = useRef<HTMLInputElement>(null);

  const processDroppedItems = useCallback(async (items: DataTransferItemList) => {
    const entries: FileSystemEntry[] = [];
    for (let i = 0; i < items.length; i++) {
      const entry = items[i].webkitGetAsEntry?.();
      if (entry) entries.push(entry);
    }
    if (entries.length === 0) return;

    if (entries.length === 1 && entries[0].isFile && entries[0].name.toLowerCase().endsWith(".zip")) {
      const f = await readEntryAsFile(entries[0] as FileSystemFileEntry);
      setFile(f);
      setFileLabel(f.name);
      return;
    }

    setZipping(true);
    try {
      const allFiles: { path: string; file: File }[] = [];
      for (const entry of entries) {
        allFiles.push(...(await collectFilesFromEntry(entry)));
      }
      if (allFiles.length === 0) return;
      const zipFile = await buildZipFromFiles(allFiles);
      setFile(zipFile);
      setFileLabel(`${allFiles.length} file${allFiles.length !== 1 ? "s" : ""} zipped for upload`);
    } finally {
      setZipping(false);
    }
  }, [setFile]);

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation();
    dragCounter.current++;
    if (dragCounter.current === 1) setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation();
    dragCounter.current--;
    if (dragCounter.current === 0) setIsDragOver(false);
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => { e.preventDefault(); e.stopPropagation(); }, []);

  const handleDrop = useCallback(async (e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation();
    dragCounter.current = 0; setIsDragOver(false);
    if (e.dataTransfer.items) await processDroppedItems(e.dataTransfer.items);
  }, [processDroppedItems]);

  const handleZipInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const fileList = e.target.files;
    if (!fileList || fileList.length === 0) return;

    if (fileList.length === 1 && fileList[0].name.toLowerCase().endsWith(".zip")) {
      setFile(fileList[0]);
      setFileLabel(fileList[0].name);
      return;
    }

    setZipping(true);
    try {
      const collected: { path: string; file: File }[] = [];
      for (let i = 0; i < fileList.length; i++) {
        if (fileHasAcceptedExt(fileList[i].name)) {
          collected.push({ path: fileList[i].name, file: fileList[i] });
        }
      }
      if (collected.length === 0) return;
      const zipFile = await buildZipFromFiles(collected);
      setFile(zipFile);
      setFileLabel(`${collected.length} file${collected.length !== 1 ? "s" : ""} zipped for upload`);
    } finally {
      setZipping(false);
    }
  };

  const handleFolderInputChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const fileList = e.target.files;
    if (!fileList || fileList.length === 0) return;

    setZipping(true);
    try {
      const collected: { path: string; file: File }[] = [];
      for (let i = 0; i < fileList.length; i++) {
        const f = fileList[i];
        if (fileHasAcceptedExt(f.name)) {
          const path = (f as any).webkitRelativePath || f.name;
          collected.push({ path, file: f });
        }
      }
      if (collected.length === 0) return;
      const zipFile = await buildZipFromFiles(collected);
      setFile(zipFile);
      setFileLabel(`${collected.length} file${collected.length !== 1 ? "s" : ""} zipped for upload`);
    } finally {
      setZipping(false);
    }
  }, [setFile]);

  const clearFile = () => {
    setFile(null);
    setFilename(null);
    setFileLabel(null);
    if (zipInputRef.current) zipInputRef.current.value = "";
    if (folderInputRef.current) folderInputRef.current.value = "";
  };

  if (step !== 1) return null;

  return (
    <SurfaceCard title="Upload Data" subtitle="Upload a ZIP archive, folder, or Excel sheet to begin." icon={Upload}>
      <div className="space-y-8">
        <div className="space-y-4">
          <input ref={zipInputRef} type="file" className="sr-only" onChange={handleZipInputChange} accept=".zip,.csv,.xlsx,.xlsm,.xltx,.xltm" multiple />
          <input ref={folderInputRef} type="file" className="sr-only" onChange={handleFolderInputChange} {...({ webkitdirectory: "", directory: "" } as any)} />
          
          <div
            onDragEnter={handleDragEnter} onDragLeave={handleDragLeave} onDragOver={handleDragOver} onDrop={handleDrop}
            className={`relative border-2 border-dashed rounded-2xl p-10 text-center transition-all ${
              isDragOver ? "border-red-400 bg-red-50/40 scale-[1.01]" : file
              ? "border-emerald-200 bg-emerald-50/30"
              : "border-neutral-200 hover:border-red-300 hover:bg-neutral-50/50"
            }`}
          >
            {zipping ? (
              <div className="flex flex-col items-center gap-3">
                <Loader2 className="w-10 h-10 text-red-500 animate-spin" />
                <p className="text-sm font-medium text-neutral-600">Zipping files...</p>
              </div>
            ) : file || filename ? (
              <div className="flex flex-col items-center gap-2">
                <div className="w-16 h-16 rounded-2xl bg-emerald-100 text-emerald-600 flex items-center justify-center">
                  <Upload className="w-8 h-8" />
                </div>
                <p className="text-sm font-bold text-neutral-900">{fileLabel || filename}</p>
                <button type="button" onClick={clearFile} className="mt-1 inline-flex items-center gap-1 text-xs text-neutral-500 hover:text-red-600 transition-colors">
                  <X className="w-3 h-3" /> Remove
                </button>
                <motion.div initial={{ scale: 0 }} animate={{ scale: 1 }} className="absolute -top-2 -right-2 bg-emerald-500 text-white p-1 rounded-full shadow-lg">
                  <CheckCircle2 className="w-4 h-4" />
                </motion.div>
              </div>
            ) : (
              <div className="space-y-4">
                <div className="w-16 h-16 rounded-2xl bg-neutral-100 text-neutral-400 flex items-center justify-center mx-auto">
                  <Upload className="w-8 h-8" />
                </div>
                <div className="space-y-1">
                  <p className="text-sm font-bold text-neutral-900">{isDragOver ? "Drop files here" : "Drag & drop files or a folder here"}</p>
                  <p className="text-xs text-neutral-500">ZIP, CSV, or Excel files</p>
                </div>
                <div className="flex items-center justify-center gap-3 pt-2">
                  <button type="button" onClick={() => zipInputRef.current?.click()} className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-neutral-200 bg-white text-neutral-700 hover:bg-neutral-50">
                    <FileText className="w-3.5 h-3.5" /> Browse Files
                  </button>
                  <button type="button" onClick={() => folderInputRef.current?.click()} className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-neutral-200 bg-white text-neutral-700 hover:bg-neutral-50">
                    <FolderOpen className="w-3.5 h-3.5" /> Browse Folder
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        <div className="space-y-2">
          <label className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-neutral-400 font-semibold">
            <KeyRound className="w-4 h-4" /> OpenAI API Key
          </label>
          <input
            type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)}
            placeholder="Paste your API key here"
            className="w-full px-4 py-2.5 text-sm border border-neutral-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-red-500"
          />
        </div>
      </div>

      <div className="pt-6 border-t border-neutral-100 flex justify-end">
        <PrimaryButton onClick={handleUpload} disabled={(!file && !filename) || loading || !apiKey.trim() || zipping}>
          {loading ? <Loader2 className="animate-spin w-4 h-4" /> : "Upload & Analyze"}
          <ArrowRight className="w-4 h-4" />
        </PrimaryButton>
      </div>
    </SurfaceCard>
  );
}
