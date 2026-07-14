import { useState, useEffect } from "react";
import ReactMarkdown from "react-markdown";
import {
  Copy,
  Check,
  RefreshCw,
  ArrowLeft,
  Loader2,
  AlertTriangle,
  Edit3,
  Eye,
} from "lucide-react";

interface Props {
  email: string | null;
  subject: string;
  fallback: string | null;
  error: string | null;
  loading: boolean;
  onRegenerate: () => void;
  onBack: () => void;
}

export default function EmailStep({
  email,
  subject: initialSubject,
  fallback,
  error,
  loading,
  onRegenerate,
  onBack,
}: Props) {
  const [editedSubject, setEditedSubject] = useState(initialSubject);
  const [editedBody, setEditedBody] = useState(email || "");
  const [copied, setCopied] = useState(false);
  const [useFallback, setUseFallback] = useState(false);
  const [mode, setMode] = useState<"edit" | "preview">("edit");

  useEffect(() => {
    if (email !== null) {
      setEditedBody(email);
      setUseFallback(false);
    }
  }, [email]);

  useEffect(() => {
    setEditedSubject(initialSubject);
  }, [initialSubject]);

  const displayBody = useFallback && fallback ? fallback : editedBody;

  const handleCopy = async () => {
    const full = editedSubject ? `Subject: ${editedSubject}\n\n${displayBody}` : displayBody;
    await navigator.clipboard.writeText(full);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleUseFallback = () => {
    if (fallback) {
      setEditedBody(fallback);
      setUseFallback(true);
    }
  };

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto">
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm p-8">
          <div className="flex flex-col items-center gap-4 py-12">
            <Loader2 className="w-8 h-8 text-primary animate-spin" />
            <p className="text-sm font-medium text-neutral-600 dark:text-neutral-400">
              Generating your email...
            </p>
            <p className="text-xs text-neutral-400 dark:text-neutral-500">
              This may take 15-30 seconds
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-3xl mx-auto space-y-4">
      {/* Header bar */}
      <div className="flex items-center justify-between bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm px-6 py-4">
        <div className="flex items-center gap-3">
          <button
            onClick={onBack}
            className="flex items-center gap-1.5 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:text-primary transition-colors"
          >
            <ArrowLeft className="w-3.5 h-3.5" />
            Back to Procurement Views
          </button>
        </div>
        <div className="flex items-center gap-2">
          {/* Edit / Preview toggle */}
          <div className="flex rounded-lg border border-neutral-200 dark:border-neutral-700 overflow-hidden">
            <button
              onClick={() => setMode("edit")}
              className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === "edit"
                  ? "bg-primary text-white"
                  : "bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-700"
              }`}
            >
              <Edit3 className="w-3 h-3" />
              Edit
            </button>
            <button
              onClick={() => setMode("preview")}
              className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === "preview"
                  ? "bg-primary text-white"
                  : "bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-700"
              }`}
            >
              <Eye className="w-3 h-3" />
              Preview
            </button>
          </div>
          <button
            onClick={onRegenerate}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Regenerate
          </button>
          <button
            onClick={handleCopy}
            className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold bg-primary text-white hover:bg-primary-hover transition-colors"
          >
            {copied ? (
              <>
                <Check className="w-3.5 h-3.5" />
                Copied!
              </>
            ) : (
              <>
                <Copy className="w-3.5 h-3.5" />
                Copy to Clipboard
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error banner */}
      {error && !useFallback && (
        <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800/40 rounded-xl px-5 py-4">
          <div className="flex items-start gap-3">
            <AlertTriangle className="w-4 h-4 text-amber-600 dark:text-amber-400 mt-0.5 shrink-0" />
            <div className="flex-1">
              <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                AI generation failed
              </p>
              <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                {error}
              </p>
              <div className="flex items-center gap-3 mt-3">
                <button
                  onClick={onRegenerate}
                  className="text-xs font-medium text-amber-700 dark:text-amber-300 hover:underline"
                >
                  Retry
                </button>
                {fallback && (
                  <button
                    onClick={handleUseFallback}
                    className="text-xs font-medium text-amber-700 dark:text-amber-300 hover:underline"
                  >
                    Use Template Fallback
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Email editor / preview */}
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
        <div className="px-6 py-3 border-b border-neutral-100 dark:border-neutral-800">
          <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1.5">
            Subject
          </label>
          {mode === "edit" ? (
            <input
              type="text"
              value={editedSubject}
              onChange={(e) => setEditedSubject(e.target.value)}
              className="w-full px-3 py-2 text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 focus:outline-none focus:ring-2 focus:ring-primary/40"
            />
          ) : (
            <p className="text-sm font-semibold text-neutral-900 dark:text-neutral-100 py-2">
              {editedSubject || "(No subject)"}
            </p>
          )}
        </div>

        <div className="p-6">
          {mode === "edit" ? (
            <textarea
              value={displayBody}
              onChange={(e) => {
                setEditedBody(e.target.value);
                if (useFallback) setUseFallback(false);
              }}
              rows={30}
              className="w-full px-4 py-3 text-sm leading-relaxed font-mono rounded-lg border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 focus:outline-none focus:ring-2 focus:ring-primary/40 resize-y"
            />
          ) : (
            <div className="prose prose-sm dark:prose-invert max-w-none px-4 py-3 rounded-lg border border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-800 min-h-[400px]">
              <ReactMarkdown>{displayBody}</ReactMarkdown>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
