import ReactMarkdown from "react-markdown";
import { Sparkles } from "lucide-react";

interface Props {
  summary: string;
}

export default function QualitySummary({ summary }: Props) {
  return (
    <div className="bg-primary-50/50 dark:bg-primary-900/10 border border-primary-100 dark:border-primary-900/30 rounded-xl overflow-hidden">
      <div className="flex items-center gap-2 px-4 py-3 border-b border-primary-100 dark:border-primary-900/30">
        <Sparkles className="w-4 h-4 text-primary shrink-0" />
        <span className="text-xs font-semibold uppercase tracking-widest text-primary">
          AI Quality Assessment
        </span>
      </div>
      <div className="px-4 py-4">
        <div className="ai-summary-content text-sm text-neutral-700 dark:text-neutral-300 leading-relaxed">
          <ReactMarkdown>{summary}</ReactMarkdown>
        </div>
      </div>
    </div>
  );
}
