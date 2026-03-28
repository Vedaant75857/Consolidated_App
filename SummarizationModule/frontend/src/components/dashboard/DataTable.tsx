import { useState, useMemo } from "react";
import { ArrowUpDown } from "lucide-react";

interface Props {
  data: Record<string, any>[];
  maxRows?: number;
}

function formatValue(val: any): string {
  if (val === null || val === undefined) return "—";
  if (typeof val === "number") {
    if (Math.abs(val) >= 1000) return val.toLocaleString("en-US", { maximumFractionDigits: 0 });
    return val.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  return String(val);
}

export default function DataTable({ data, maxRows = 50 }: Props) {
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  const columns = useMemo(() => {
    if (!data.length) return [];
    return Object.keys(data[0]);
  }, [data]);

  const sorted = useMemo(() => {
    const rows = data.slice(0, maxRows);
    if (!sortCol) return rows;
    return [...rows].sort((a, b) => {
      const va = a[sortCol];
      const vb = b[sortCol];
      if (va === vb) return 0;
      if (va === null || va === undefined) return 1;
      if (vb === null || vb === undefined) return -1;
      const cmp = typeof va === "number" && typeof vb === "number" ? va - vb : String(va).localeCompare(String(vb));
      return sortAsc ? cmp : -cmp;
    });
  }, [data, sortCol, sortAsc, maxRows]);

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(true);
    }
  };

  if (!data.length) {
    return <p className="text-sm text-neutral-400 p-4">No data available</p>;
  }

  return (
    <div className="overflow-auto max-h-[400px] rounded-lg border border-neutral-200 dark:border-neutral-700">
      <table className="w-full text-xs">
        <thead className="sticky top-0 z-10">
          <tr className="bg-neutral-100 dark:bg-neutral-800">
            {columns.map((col) => (
              <th
                key={col}
                onClick={() => handleSort(col)}
                className="text-left px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400 cursor-pointer hover:text-primary select-none whitespace-nowrap"
              >
                <span className="flex items-center gap-1">
                  {col}
                  <ArrowUpDown className="w-3 h-3 opacity-40" />
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={i}
              className={`border-t border-neutral-100 dark:border-neutral-800 ${
                i % 2 === 0 ? "bg-white dark:bg-neutral-900" : "bg-neutral-50/50 dark:bg-neutral-800/50"
              } hover:bg-primary-50/30 dark:hover:bg-primary-900/10 transition-colors`}
            >
              {columns.map((col) => (
                <td key={col} className="px-3 py-2 text-neutral-700 dark:text-neutral-300 whitespace-nowrap">
                  {formatValue(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > maxRows && (
        <div className="px-3 py-2 text-xs text-neutral-400 bg-neutral-50 dark:bg-neutral-800 border-t border-neutral-200 dark:border-neutral-700">
          Showing {maxRows} of {data.length} rows
        </div>
      )}
    </div>
  );
}
