import { Loader2 } from "lucide-react";
import type { ExecSummaryRow } from "../../../types";

interface Props {
  rows: ExecSummaryRow[];
  loading: boolean;
}

function formatFillRate(row: ExecSummaryRow): string {
  return `${row.fillRate}% — ${row.validRows.toLocaleString()} / ${row.totalRows.toLocaleString()}`;
}

export default function ExecutiveSummary({ rows, loading }: Props) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-5 h-5 text-primary animate-spin" />
        <span className="ml-2 text-sm text-neutral-500">Loading summary…</span>
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <p className="text-sm text-neutral-400 py-6 text-center">
        No summary data available.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-neutral-200 dark:border-neutral-700">
            <th className="text-left py-3 px-4 font-semibold text-neutral-600 dark:text-neutral-400 w-1/4">
              Key Column
            </th>
            <th className="text-left py-3 px-4 font-semibold text-neutral-600 dark:text-neutral-400 w-1/5">
              Fill Rate
            </th>
            <th className="text-left py-3 px-4 font-semibold text-neutral-600 dark:text-neutral-400">
              Insight
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.key}
              className="border-b border-neutral-100 dark:border-neutral-800 last:border-b-0"
            >
              <td className="py-3 px-4">
                {row.mapped ? (
                  <span className="font-medium text-neutral-900 dark:text-neutral-100">
                    {row.label}
                  </span>
                ) : (
                  <span className="text-neutral-400 dark:text-neutral-500">
                    {row.label}{" "}
                    <span className="text-xs">(Not mapped)</span>
                  </span>
                )}
              </td>
              <td className="py-3 px-4 tabular-nums">
                {row.mapped ? (
                  <span className="text-neutral-700 dark:text-neutral-300">
                    {formatFillRate(row)}
                  </span>
                ) : (
                  <span className="text-neutral-400 dark:text-neutral-500">—</span>
                )}
              </td>
              <td className="py-3 px-4">
                {row.mapped && row.insight ? (
                  <span className="text-neutral-700 dark:text-neutral-300">
                    {row.insight}
                  </span>
                ) : row.mapped ? (
                  <span className="text-neutral-400 dark:text-neutral-500 text-xs">
                    No data
                  </span>
                ) : (
                  <span className="text-neutral-400 dark:text-neutral-500 text-xs">
                    Not mapped
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
