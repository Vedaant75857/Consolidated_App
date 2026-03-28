import { useCallback, useRef, useState } from "react";
import { Upload, FileSpreadsheet, X, KeyRound, ArrowRight, Loader2, FolderOpen, File as FileIcon } from "lucide-react";
import JSZip from "jszip";

interface Props {
  onUpload: (file: File, apiKey: string) => Promise<void>;
  loading: boolean;
}

interface AccumulatedFile {
  path: string;
  file: File;
}

const ACCEPT = ".csv,.xlsx,.xlsm,.xltx,.xltm,.zip";

async function buildZipFromFiles(files: AccumulatedFile[]): Promise<File> {
  const zip = new JSZip();
  for (const { path, file } of files) {
    zip.file(path, await file.arrayBuffer());
  }
  const blob = await zip.generateAsync({ type: "blob" });
  return new File([blob], "upload.zip", { type: "application/zip" });
}

export default function UploadStep({ onUpload, loading }: Props) {
  const [files, setFiles] = useState<AccumulatedFile[]>([]);
  const [apiKey, setApiKey] = useState(() => localStorage.getItem("summarizer_api_key") || "");
  const [dragOver, setDragOver] = useState(false);
  const [zipping, setZipping] = useState(false);
  const [error, setError] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);
  const folderRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback(async (incoming: File[]) => {
    const newFiles: AccumulatedFile[] = [];
    for (const f of incoming) {
      const ext = f.name.split(".").pop()?.toLowerCase() || "";
      if (ext === "zip") {
        try {
          const zip = await JSZip.loadAsync(await f.arrayBuffer());
          for (const [path, entry] of Object.entries(zip.files)) {
            if (entry.dir || path.startsWith("__MACOSX") || path.startsWith(".")) continue;
            const innerExt = path.split(".").pop()?.toLowerCase() || "";
            if (["csv", "xlsx", "xlsm", "xltx", "xltm"].includes(innerExt)) {
              const blob = await entry.async("blob");
              const innerFile = new File([blob], path.split("/").pop() || path);
              newFiles.push({ path, file: innerFile });
            }
          }
        } catch {
          setError("Failed to read ZIP file");
        }
      } else if (["csv", "xlsx", "xlsm", "xltx", "xltm"].includes(ext)) {
        newFiles.push({ path: f.name, file: f });
      }
    }
    setFiles((prev) => {
      const existing = new Set(prev.map((p) => p.path));
      return [...prev, ...newFiles.filter((n) => !existing.has(n.path))];
    });
    setError("");
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      const items = Array.from(e.dataTransfer.files);
      addFiles(items);
    },
    [addFiles]
  );

  const removeFile = (path: string) => {
    setFiles((prev) => prev.filter((f) => f.path !== path));
  };

  const handleSubmit = async () => {
    if (!files.length) {
      setError("Please add at least one file.");
      return;
    }
    if (!apiKey.trim()) {
      setError("Portkey API key is required.");
      return;
    }
    localStorage.setItem("summarizer_api_key", apiKey);
    setZipping(true);
    try {
      const zipFile = await buildZipFromFiles(files);
      setZipping(false);
      await onUpload(zipFile, apiKey);
    } catch (err: any) {
      setZipping(false);
      setError(err.message || "Upload failed");
    }
  };

  return (
    <div className="max-w-2xl mx-auto">
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
        <div className="px-8 pt-8 pb-4">
          <div className="flex items-center gap-3 mb-1">
            <div className="w-10 h-10 rounded-xl bg-primary-50 dark:bg-primary-900/20 flex items-center justify-center">
              <Upload className="w-5 h-5 text-primary" />
            </div>
            <div>
              <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Source Data</h2>
              <p className="text-xs text-neutral-500">Upload CSV, Excel, or ZIP files to begin analysis</p>
            </div>
          </div>
        </div>

        <div className="px-8 pb-8 space-y-6">
          <div
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={handleDrop}
            className={`relative border-2 border-dashed rounded-xl p-10 text-center transition-all cursor-pointer
              ${dragOver
                ? "border-primary bg-primary-50/50 dark:bg-primary-900/10"
                : "border-neutral-300 dark:border-neutral-700 hover:border-primary/50"
              }`}
            onClick={() => fileRef.current?.click()}
          >
            <FileSpreadsheet className="w-10 h-10 mx-auto mb-3 text-neutral-400" />
            <p className="text-sm font-medium text-neutral-700 dark:text-neutral-300">
              Drag & drop files here
            </p>
            <p className="text-xs text-neutral-400 mt-1">or click to browse</p>
            <input
              ref={fileRef}
              type="file"
              multiple
              accept={ACCEPT}
              className="hidden"
              onChange={(e) => addFiles(Array.from(e.target.files || []))}
            />
          </div>

          <div className="flex gap-2">
            <button
              onClick={() => fileRef.current?.click()}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg border border-neutral-200 dark:border-neutral-700 text-sm font-medium text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
            >
              <FileIcon className="w-4 h-4" /> Browse Files
            </button>
            <button
              onClick={() => folderRef.current?.click()}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg border border-neutral-200 dark:border-neutral-700 text-sm font-medium text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
            >
              <FolderOpen className="w-4 h-4" /> Browse Folder
            </button>
            <input
              ref={folderRef}
              type="file"
              /* @ts-expect-error webkitdirectory */
              webkitdirectory=""
              className="hidden"
              onChange={(e) => addFiles(Array.from(e.target.files || []))}
            />
          </div>

          {files.length > 0 && (
            <div className="space-y-1.5 max-h-48 overflow-y-auto">
              {files.map((f) => (
                <div
                  key={f.path}
                  className="flex items-center justify-between px-3 py-2 rounded-lg bg-neutral-50 dark:bg-neutral-800/50 text-sm"
                >
                  <span className="truncate text-neutral-700 dark:text-neutral-300">{f.path}</span>
                  <button
                    onClick={() => removeFile(f.path)}
                    className="ml-2 text-neutral-400 hover:text-primary transition-colors shrink-0"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              ))}
              <p className="text-xs text-neutral-400 pt-1">
                {files.length} file{files.length > 1 ? "s" : ""} selected
              </p>
            </div>
          )}

          <div className="space-y-2">
            <label className="flex items-center gap-2 text-xs uppercase tracking-widest text-neutral-500 dark:text-neutral-400 font-semibold">
              <KeyRound className="w-3.5 h-3.5" />
              Portkey API Key
            </label>
            <input
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder="Paste your Portkey API key here"
              className="w-full px-4 py-2.5 rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm text-neutral-900 dark:text-neutral-100 placeholder:text-neutral-400 focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
            />
            <p className="text-xs text-neutral-400">Required for AI-powered column mapping and summaries.</p>
          </div>

          {error && (
            <p className="text-sm text-primary font-medium bg-primary-50 dark:bg-primary-900/20 px-4 py-2 rounded-lg">
              {error}
            </p>
          )}

          <div className="pt-4 border-t border-neutral-100 dark:border-neutral-800 flex justify-end">
            <button
              onClick={handleSubmit}
              disabled={!files.length || loading || zipping}
              className="flex items-center gap-2 px-6 py-2.5 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {loading || zipping ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : null}
              {zipping ? "Preparing..." : loading ? "Uploading..." : "Start Analysis"}
              {!loading && !zipping && <ArrowRight className="w-4 h-4" />}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
