import { useState } from "react";
import { ChevronDown, ChevronRight, Sparkles } from "lucide-react";

interface Props {
  summary?: string;
  loading?: boolean;
}

export default function AISummary({ summary, loading }: Props) {
  const [expanded, setExpanded] = useState(true);

  return (
    <div className="bg-primary-50/50 dark:bg-primary-900/10 border border-primary-100 dark:border-primary-900/30 rounded-xl overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2 px-4 py-3 text-left hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-colors"
      >
        <Sparkles className="w-4 h-4 text-primary shrink-0" />
        <span className="text-xs font-semibold uppercase tracking-widest text-primary">
          AI Analysis
        </span>
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-primary ml-auto" />
        ) : (
          <ChevronRight className="w-4 h-4 text-primary ml-auto" />
        )}
      </button>
      {expanded && (
        <div className="px-4 pb-4">
          {loading ? (
            <div className="space-y-2 animate-pulse">
              <div className="h-3 bg-primary-100 dark:bg-primary-900/30 rounded w-full" />
              <div className="h-3 bg-primary-100 dark:bg-primary-900/30 rounded w-5/6" />
              <div className="h-3 bg-primary-100 dark:bg-primary-900/30 rounded w-4/6" />
              <div className="h-3 bg-primary-100 dark:bg-primary-900/30 rounded w-full" />
              <div className="h-3 bg-primary-100 dark:bg-primary-900/30 rounded w-3/6" />
            </div>
          ) : (
            <p className="text-sm text-neutral-700 dark:text-neutral-300 leading-relaxed whitespace-pre-line">
              {summary}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
