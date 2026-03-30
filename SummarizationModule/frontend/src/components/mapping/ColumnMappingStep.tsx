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
  numeric: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
  datetime: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300",
  string: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300",
};

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

  const getField = (fieldKey: string) =>
    standardFields.find((f) => f.fieldKey === fieldKey) ||
    aiMappings?.find((m) => m.fieldKey === fieldKey);

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
          <p className="text-sm text-neutral-500 mb-6 max-w-md mx-auto">
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

  return (
    <div className="max-w-4xl mx-auto">
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
        <div className="px-8 pt-8 pb-4">
          <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Column Mapping</h2>
          <p className="text-xs text-neutral-500 mt-1">
            Review and adjust the AI-suggested mappings below. Each field shows the expected data type.
          </p>
        </div>

        <div className="px-8 pb-6">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-neutral-200 dark:border-neutral-700">
                  <th className="text-left py-3 px-2 text-xs font-semibold text-neutral-500 uppercase tracking-wider">Field</th>
                  <th className="text-left py-3 px-2 text-xs font-semibold text-neutral-500 uppercase tracking-wider">Type</th>
                  <th className="text-left py-3 px-2 text-xs font-semibold text-neutral-500 uppercase tracking-wider">Mapped Column</th>
                  <th className="text-left py-3 px-2 text-xs font-semibold text-neutral-500 uppercase tracking-wider">Confidence</th>
                  <th className="text-left py-3 px-2 text-xs font-semibold text-neutral-500 uppercase tracking-wider">Status</th>
                </tr>
              </thead>
              <tbody>
                {aiMappings?.map((m) => {
                  const selectedCol = userMapping[m.fieldKey];
                  const field = standardFields.find((f) => f.fieldKey === m.fieldKey);
                  const expectedType = field?.expectedType || m.expectedType;

                  return (
                    <tr key={m.fieldKey} className="border-b border-neutral-100 dark:border-neutral-800">
                      <td className="py-3 px-2">
                        <div className="font-medium text-neutral-900 dark:text-neutral-100">
                          {field?.displayName || m.fieldKey}
                        </div>
                        <div className="text-xs text-neutral-400 mt-0.5">{field?.description}</div>
                      </td>
                      <td className="py-3 px-2">
                        <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${TYPE_COLORS[expectedType] || TYPE_COLORS.string}`}>
                          {expectedType}
                        </span>
                      </td>
                      <td className="py-3 px-2 min-w-[200px]">
                        <select
                          value={selectedCol || ""}
                          onChange={(e) =>
                            setUserMapping((prev) => ({
                              ...prev,
                              [m.fieldKey]: e.target.value || null,
                            }))
                          }
                          disabled={phase === "confirming" || phase === "done"}
                          className="w-full px-3 py-1.5 rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
                        >
                          <option value="">— Not mapped —</option>
                          {(() => { const bm = strVal(m.bestMatch); return bm ? <option value={bm}>⭐ {bm} (AI pick)</option> : null; })()}
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
                      <td className="py-3 px-2">
                        {strVal(m.bestMatch) && (
                          <div className="flex items-center gap-1">
                            <div className="w-16 h-1.5 rounded-full bg-neutral-200 dark:bg-neutral-700 overflow-hidden">
                              <div
                                className="h-full rounded-full bg-primary"
                                style={{ width: `${(m.confidence || 0) * 100}%` }}
                              />
                            </div>
                            <span className="text-xs text-neutral-500">
                              {Math.round((m.confidence || 0) * 100)}%
                            </span>
                          </div>
                        )}
                      </td>
                      <td className="py-3 px-2">
                        {selectedCol ? (
                          <Check className="w-4 h-4 text-green-500" />
                        ) : (
                          <span className="text-xs text-primary font-medium">Unmapped</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {castReport && phase === "done" && (
          <div className="px-8 pb-6">
            <h3 className="text-sm font-semibold text-neutral-900 dark:text-neutral-100 mb-3">Cast Report</h3>
            <div className="grid grid-cols-2 gap-2">
              {Object.entries(castReport.fields).map(([fk, info]) => (
                <div
                  key={fk}
                  className="px-3 py-2 rounded-lg bg-neutral-50 dark:bg-neutral-800/50 text-xs"
                >
                  <span className="font-medium text-neutral-700 dark:text-neutral-300">
                    {standardFields.find((f) => f.fieldKey === fk)?.displayName || fk}:
                  </span>{" "}
                  {info.mapped ? (
                    <span className={info.parseRate >= 95 ? "text-green-600" : "text-amber-600"}>
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
          <div className="px-8 pb-4">
            <p className="text-sm text-primary bg-primary-50 dark:bg-primary-900/20 px-4 py-2 rounded-lg">
              {error}
            </p>
          </div>
        )}

        <div className="px-8 pb-8 flex justify-end gap-3 border-t border-neutral-100 dark:border-neutral-800 pt-4">
          {phase === "review" && (
            <button
              onClick={handleConfirm}
              disabled={loading}
              className="flex items-center gap-2 px-6 py-2.5 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 transition-colors"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Confirm Mapping
              <ArrowRight className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
