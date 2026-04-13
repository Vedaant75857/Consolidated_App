import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";

const COL_VIRTUALIZATION_THRESHOLD = 30;
const ESTIMATED_COL_WIDTH = 160;

interface VirtualPreviewTableProps {
  columns: string[];
  rows: Record<string, any>[];
  hasRecordId: boolean;
  showCheckbox: boolean;
  selectedRowIds: Set<string | number>;
  onToggleRow?: (rowId: string | number) => void;
}

/**
 * Preview table with horizontal column virtualization for wide datasets.
 * Falls back to a plain <table> when column count is below the threshold.
 */
export default function VirtualPreviewTable({
  columns,
  rows,
  hasRecordId,
  showCheckbox,
  selectedRowIds,
  onToggleRow,
}: VirtualPreviewTableProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const useVirtual = columns.length > COL_VIRTUALIZATION_THRESHOLD;

  const colVirtualizer = useVirtualizer({
    count: columns.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ESTIMATED_COL_WIDTH,
    horizontal: true,
    overscan: 5,
    enabled: useVirtual,
  });

  const fixedCellClass =
    "px-2 py-1.5 text-center border-r border-neutral-100 dark:border-neutral-800 shrink-0";
  const headerFixedClass =
    "px-2 py-2 text-center font-bold text-neutral-400 dark:text-neutral-500 whitespace-nowrap border-b border-r border-neutral-200 dark:border-neutral-700 shrink-0";

  if (!useVirtual) {
    return (
      <div ref={scrollRef} className="overflow-x-auto border border-neutral-200 dark:border-neutral-700 rounded-lg max-h-80">
        <table className="min-w-full text-xs">
          <thead className="bg-neutral-50 dark:bg-neutral-800 sticky top-0 z-10">
            <tr>
              {showCheckbox && <th className={`${headerFixedClass} w-8`} />}
              <th className={`${headerFixedClass} w-10`}>#</th>
              {columns.map((col) => (
                <th key={col} className="px-3 py-2 text-left font-bold text-neutral-500 dark:text-neutral-400 whitespace-nowrap border-b border-neutral-200 dark:border-neutral-700">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {rows.map((row, ri) => {
              const rowId = hasRecordId ? row["RECORD_ID"] : ri;
              const isSelected = selectedRowIds.has(rowId);
              return (
                <tr key={ri} className={`transition-colors ${isSelected ? "bg-red-50 dark:bg-red-950/30" : "hover:bg-red-50/30"}`}>
                  {showCheckbox && (
                    <td className={fixedCellClass}>
                      <input type="checkbox" checked={isSelected} onChange={() => onToggleRow?.(rowId)} className="w-3 h-3 text-red-600 rounded border-neutral-300 focus:ring-red-500" />
                    </td>
                  )}
                  <td className="px-2 py-1.5 text-center text-[10px] text-neutral-400 dark:text-neutral-500 font-mono border-r border-neutral-100 dark:border-neutral-800">
                    {hasRecordId ? row["RECORD_ID"] : ri}
                  </td>
                  {columns.map((col) => (
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
    );
  }

  /* ── Virtualized path (30+ columns) ── */

  const virtualCols = colVirtualizer.getVirtualItems();
  const totalWidth = colVirtualizer.getTotalSize();

  return (
    <div ref={scrollRef} className="overflow-x-auto overflow-y-auto border border-neutral-200 dark:border-neutral-700 rounded-lg max-h-80">
      <div style={{ width: totalWidth + (showCheckbox ? 80 : 40), minWidth: "100%" }} className="text-xs">
        {/* Header */}
        <div className="flex bg-neutral-50 dark:bg-neutral-800 sticky top-0 z-10 border-b border-neutral-200 dark:border-neutral-700">
          {showCheckbox && <div className={`${headerFixedClass} w-8 flex items-center justify-center`} />}
          <div className={`${headerFixedClass} w-10 flex items-center justify-center`}>#</div>
          <div className="relative" style={{ width: totalWidth, height: 32 }}>
            {virtualCols.map((vc) => (
              <div
                key={vc.key}
                className="absolute top-0 h-full flex items-center px-3 font-bold text-neutral-500 dark:text-neutral-400 whitespace-nowrap"
                style={{ left: vc.start, width: vc.size }}
              >
                <span className="truncate">{columns[vc.index]}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Rows */}
        {rows.map((row, ri) => {
          const rowId = hasRecordId ? row["RECORD_ID"] : ri;
          const isSelected = selectedRowIds.has(rowId);
          return (
            <div
              key={ri}
              className={`flex border-b border-neutral-100 dark:border-neutral-800 transition-colors ${isSelected ? "bg-red-50 dark:bg-red-950/30" : "hover:bg-red-50/30"}`}
            >
              {showCheckbox && (
                <div className={`${fixedCellClass} w-8 flex items-center justify-center`}>
                  <input type="checkbox" checked={isSelected} onChange={() => onToggleRow?.(rowId)} className="w-3 h-3 text-red-600 rounded border-neutral-300 focus:ring-red-500" />
                </div>
              )}
              <div className="px-2 py-1.5 text-center text-[10px] text-neutral-400 dark:text-neutral-500 font-mono border-r border-neutral-100 dark:border-neutral-800 w-10 shrink-0">
                {hasRecordId ? row["RECORD_ID"] : ri}
              </div>
              <div className="relative" style={{ width: totalWidth, height: 28 }}>
                {virtualCols.map((vc) => {
                  const col = columns[vc.index];
                  return (
                    <div
                      key={vc.key}
                      className="absolute top-0 h-full flex items-center px-3 text-neutral-700 dark:text-neutral-300"
                      style={{ left: vc.start, width: vc.size }}
                    >
                      <span className="truncate">
                        {row[col] != null ? String(row[col]) : <span className="text-neutral-300 dark:text-neutral-600 italic">null</span>}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
