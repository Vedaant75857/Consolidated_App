import { useState } from "react";
import { Loader2, ArrowRight, Check, Sparkles } from "lucide-react";
import type { ColumnInfo, AIMapping, StandardField, CastReport } from "../../types";

interface Props {
  columns: ColumnInfo[];
  onRequestMapping: () => Promise<{ mappings: AIMapping[]; standardFields: StandardField[] }>;
  onConfirm: (mapping: Record<string, string | null>) => Promise<CastReport>;
  loading: boolean;
  initialMappings?: AIMapping[] | null;
  initialStandardFields?: StandardField[] | null;
}

function strVal(v: unknown): string | null {
  if (typeof v === "string") return v;
  if (v && typeof v === "object") return (v as any).column ?? (v as any).name ?? null;
  return null;
}

const TYPE_COLORS: Record<string, string> = {
  numeric: "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300 border border-blue-200 dark:border-blue-800",
  datetime: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300 border border-amber-200 dark:border-amber-800",
  string: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800",
};

function confidenceColor(c: number): string {
  if (c >= 0.8) return "bg-emerald-500";
  if (c >= 0.5) return "bg-amber-400";
  return "bg-red-400";
}

export default function ColumnMappingStep({
  columns, onRequestMapping, onConfirm, loading,
  initialMappings, initialStandardFields,
}: Props) {
  const hasInitial = Array.isArray(initialMappings) && initialMappings.length > 0;
  const [aiMappings, setAiMappings] = useState<AIMapping[] | null>(hasInitial ? initialMappings! : null);
  const [standardFields, setStandardFields] = useState<StandardField[]>(
    Array.isArray(initialStandardFields) ? initialStandardFields : []
  );
  const [userMapping, setUserMapping] = useState<Record<string, string | null>>(() => {
    if (!hasInitial) return {};
    const init: Record<string, string | null> = {};
    for (const m of initialMappings!) init[m.fieldKey] = strVal(m.bestMatch);
    return init;
  });
  const [castReport, setCastReport] = useState<CastReport | null>(null);
  const [phase, setPhase] = useState<"idle" | "detecting" | "review" | "confirming" | "done">(
    hasInitial ? "review" : "idle"
  );
  const [error, setError] = useState("");
  const [showPreview, setShowPreview] = useState(false);

  const handleDetect = async () => {
    setPhase("detecting");
    setError("");
    try {
      const result = await onRequestMapping();
      const mappings = Array.isArray(result.mappings) ? result.mappings : [];
      const fields = Array.isArray(result.standardFields) ? result.standardFields : [];
      if (mappings.length === 0) {
        setError("AI returned no column mappings. Please try again.");
        setPhase("idle");
        return;
      }
      setAiMappings(mappings);
      setStandardFields(fields);
      const initial: Record<string, string | null> = {};
      for (const m of mappings) {
        initial[m.fieldKey] = strVal(m.bestMatch);
      }
      setUserMapping(initial);
      setPhase("review");
    } catch (err: any) {
      setError(err.message || "AI detection failed");
      setPhase("idle");
    }
  };

  const handleConfirm = async () => {
    setPhase("confirming");
    setError("");
    try {
      const report = await onConfirm(userMapping);
      setCastReport(report);
      setPhase("done");
    } catch (err: any) {
      setError(err.message || "Confirmation failed");
      setPhase("review");
    }
  };

  if (phase === "idle") {
    return (
      <div className="max-w-2xl mx-auto">
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm p-8 text-center">
          <div className="w-14 h-14 rounded-2xl bg-primary-50 dark:bg-primary-900/20 flex items-center justify-center mx-auto mb-4">
            <Sparkles className="w-7 h-7 text-primary" />
          </div>
          <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100 mb-2">
            AI Column Detection
          </h2>
          <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-6 max-w-md mx-auto">
            The AI will analyze your column names and sample values to automatically map them
            to the required procurement fields.
          </p>
          {error && (
            <p className="text-sm text-primary mb-4 bg-primary-50 dark:bg-primary-900/20 px-4 py-2 rounded-lg">
              {error}
            </p>
          )}
          <button
            onClick={handleDetect}
            disabled={loading}
            className="inline-flex items-center gap-2 px-6 py-2.5 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 transition-colors"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
            Detect Columns
          </button>
        </div>
      </div>
    );
  }

  if (phase === "detecting") {
    return (
      <div className="max-w-2xl mx-auto">
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm p-12 text-center">
          <Loader2 className="w-10 h-10 text-primary animate-spin mx-auto mb-4" />
          <p className="text-sm text-neutral-600 dark:text-neutral-400">AI is analyzing your columns...</p>
        </div>
      </div>
    );
  }

  const mappedCount = aiMappings?.filter(m => userMapping[m.fieldKey]).length ?? 0;
  const totalCount = aiMappings?.length ?? 0;

  return (
    <div className="space-y-4">
      {/* Mapping table */}
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-300 dark:border-neutral-700 shadow-sm overflow-hidden">
        <div className="px-6 py-5 flex items-center justify-between border-b border-neutral-200 dark:border-neutral-700">
          <div>
            <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Column Mapping</h2>
            <p className="text-xs text-neutral-500 dark:text-neutral-400 mt-1">
              Review and adjust the AI-suggested mappings below.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs font-medium text-neutral-500 dark:text-neutral-400 tabular-nums">
              {mappedCount} / {totalCount} mapped
            </span>
            <button
              onClick={() => setShowPreview(p => !p)}
              className="text-xs font-medium px-3 py-1.5 rounded-lg border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
            >
              {showPreview ? "Hide Preview" : "Show Data Preview"}
            </button>
          </div>
        </div>

        <div className="overflow-x-auto max-h-[520px] overflow-y-auto">
          <table className="w-full text-[12px] font-mono border-collapse">
            <thead className="sticky top-0 z-20">
              <tr className="bg-emerald-50 dark:bg-emerald-950/30">
                <th className="sticky left-0 z-30 bg-emerald-50 dark:bg-emerald-950/30 text-left py-2.5 px-4 text-[10px] font-bold text-emerald-800 dark:text-emerald-300 uppercase tracking-wider border-b border-emerald-200 dark:border-emerald-900 w-[200px]">
                  Field
                </th>
                <th className="text-left py-2.5 px-3 text-[10px] font-bold text-emerald-800 dark:text-emerald-300 uppercase tracking-wider border-b border-emerald-200 dark:border-emerald-900 w-[80px]">
                  Type
                </th>
                <th className="text-left py-2.5 px-3 text-[10px] font-bold text-emerald-800 dark:text-emerald-300 uppercase tracking-wider border-b border-emerald-200 dark:border-emerald-900 min-w-[220px]">
                  Mapped Column
                </th>
                <th className="text-left py-2.5 px-3 text-[10px] font-bold text-emerald-800 dark:text-emerald-300 uppercase tracking-wider border-b border-emerald-200 dark:border-emerald-900 w-[140px]">
                  Confidence
                </th>
                <th className="text-center py-2.5 px-3 text-[10px] font-bold text-emerald-800 dark:text-emerald-300 uppercase tracking-wider border-b border-emerald-200 dark:border-emerald-900 w-[70px]">
                  Status
                </th>
              </tr>
            </thead>
            <tbody>
              {aiMappings?.map((m, idx) => {
                const selectedCol = userMapping[m.fieldKey];
                const field = standardFields.find((f) => f.fieldKey === m.fieldKey);
                const expectedType = field?.expectedType || m.expectedType;
                const isEven = idx % 2 === 0;

                return (
                  <tr
                    key={m.fieldKey}
                    className={`border-b border-neutral-100 dark:border-neutral-800 transition-colors ${
                      isEven ? "bg-white dark:bg-neutral-900" : "bg-neutral-50/50 dark:bg-neutral-800/30"
                    } hover:bg-neutral-50 dark:hover:bg-neutral-800/50`}
                  >
                    <td className="sticky left-0 z-10 py-3 px-4 bg-inherit">
                      <div className="font-semibold text-neutral-900 dark:text-neutral-100 text-[12px]">
                        {field?.displayName || m.fieldKey}
                      </div>
                      {field?.description && (
                        <div className="text-[10px] text-neutral-400 dark:text-neutral-500 mt-0.5 font-normal truncate max-w-[180px]" title={field.description}>
                          {field.description}
                        </div>
                      )}
                    </td>
                    <td className="py-3 px-3">
                      <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold ${TYPE_COLORS[expectedType] || TYPE_COLORS.string}`}>
                        {expectedType}
                      </span>
                    </td>
                    <td className="py-3 px-3">
                      <select
                        value={selectedCol || ""}
                        onChange={(e) =>
                          setUserMapping((prev) => ({
                            ...prev,
                            [m.fieldKey]: e.target.value || null,
                          }))
                        }
                        disabled={phase === "confirming" || phase === "done"}
                        className="w-full px-3 py-1.5 rounded-lg border border-neutral-300 dark:border-neutral-600 bg-white dark:bg-neutral-800 text-[12px] font-mono text-neutral-900 dark:text-neutral-100 focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all disabled:opacity-50"
                      >
                        <option value="">-- Not mapped --</option>
                        {(() => { const bm = strVal(m.bestMatch); return bm ? <option value={bm}>&#9733; {bm} (AI pick)</option> : null; })()}
                        {(m.alternatives ?? [])
                          .map((a) => strVal(a) ?? "")
                          .filter((a): a is string => a.length > 0 && a !== strVal(m.bestMatch))
                          .map((alt) => (
                            <option key={alt} value={alt}>
                              {alt} (alternative)
                            </option>
                          ))}
                        <optgroup label="All columns">
                          {columns.map((c) => (
                            <option key={c.name} value={c.name}>
                              {c.name}
                            </option>
                          ))}
                        </optgroup>
                      </select>
                    </td>
                    <td className="py-3 px-3">
                      {strVal(m.bestMatch) && (
                        <div className="flex items-center gap-2">
                          <div className="w-20 h-2 rounded-full bg-neutral-200 dark:bg-neutral-700 overflow-hidden">
                            <div
                              className={`h-full rounded-full ${confidenceColor(m.confidence || 0)}`}
                              style={{ width: `${(m.confidence || 0) * 100}%` }}
                            />
                          </div>
                          <span className="text-[11px] font-bold text-neutral-600 dark:text-neutral-300 tabular-nums w-10 text-right">
                            {Math.round((m.confidence || 0) * 100)}%
                          </span>
                        </div>
                      )}
                    </td>
                    <td className="py-3 px-3 text-center">
                      {selectedCol ? (
                        <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-emerald-100 dark:bg-emerald-950/40">
                          <Check className="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
                        </span>
                      ) : (
                        <span className="inline-block text-[10px] font-bold text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-950/30 px-2 py-0.5 rounded">
                          Unmapped
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="px-4 py-2 bg-neutral-50 dark:bg-neutral-800/50 border-t border-neutral-200 dark:border-neutral-700 text-[10px] text-neutral-400 dark:text-neutral-500 font-medium">
          {totalCount} fields &middot; {mappedCount} mapped
        </div>
      </div>

      {/* Data preview */}
      {showPreview && columns.length > 0 && (
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-300 dark:border-neutral-700 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-neutral-200 dark:border-neutral-700">
            <h3 className="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Sample Data Preview</h3>
            <p className="text-[10px] text-neutral-400 dark:text-neutral-500 mt-0.5">Sample values from your uploaded columns</p>
          </div>
          <div className="overflow-x-auto max-h-[300px] overflow-y-auto">
            <table className="w-full text-[11px] font-mono border-collapse">
              <thead className="sticky top-0 z-10">
                <tr className="bg-neutral-700 dark:bg-neutral-800">
                  {columns.slice(0, 20).map((col) => (
                    <th key={col.name} className="text-left py-2 px-3 text-[10px] font-bold text-white truncate max-w-[140px]" title={col.name}>
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Array.from({ length: Math.max(...columns.slice(0, 20).map(c => c.sampleValues?.length ?? 0), 0) }).map((_, rowIdx) => (
                  <tr key={rowIdx} className={`border-b border-neutral-100 dark:border-neutral-800 ${rowIdx % 2 === 0 ? "" : "bg-neutral-50/50 dark:bg-neutral-800/20"}`}>
                    {columns.slice(0, 20).map((col) => (
                      <td key={col.name} className="py-1.5 px-3 text-neutral-700 dark:text-neutral-300 truncate max-w-[140px]" title={col.sampleValues?.[rowIdx] ?? ""}>
                        {col.sampleValues?.[rowIdx] ?? ""}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-4 py-2 bg-neutral-50 dark:bg-neutral-800/50 border-t border-neutral-200 dark:border-neutral-700 text-[10px] text-neutral-400 dark:text-neutral-500 font-medium">
            Showing {Math.min(columns.length, 20)} of {columns.length} columns
          </div>
        </div>
      )}

      {/* Cast report */}
      {castReport && phase === "done" && (
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-neutral-200 dark:border-neutral-700">
            <h3 className="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Cast Report</h3>
          </div>
          <div className="px-6 py-4 grid grid-cols-2 gap-2">
            {Object.entries(castReport.fields).map(([fk, info]) => (
              <div
                key={fk}
                className="px-3 py-2 rounded-lg bg-neutral-50 dark:bg-neutral-800/50 text-xs border border-neutral-100 dark:border-neutral-800"
              >
                <span className="font-semibold text-neutral-700 dark:text-neutral-300">
                  {standardFields.find((f) => f.fieldKey === fk)?.displayName || fk}:
                </span>{" "}
                {info.mapped ? (
                  <span className={info.parseRate >= 95 ? "text-emerald-600 dark:text-emerald-400" : "text-amber-600 dark:text-amber-400"}>
                    {info.parseRate}% parsed ({info.nullRows} nulls)
                  </span>
                ) : (
                  <span className="text-neutral-400">Not mapped</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {error && (
        <div className="px-4 py-3 rounded-xl bg-primary-50 dark:bg-primary-900/20 border border-primary-100 dark:border-primary-900/30 text-sm text-primary">
          {error}
        </div>
      )}

      {/* Actions */}
      {phase === "review" && (
        <div className="flex justify-end">
          <button
            onClick={handleConfirm}
            disabled={loading}
            className="flex items-center gap-2 px-6 py-2.5 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 transition-colors shadow-md shadow-red-200/30 dark:shadow-red-900/20"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
            Confirm Mapping
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
}
