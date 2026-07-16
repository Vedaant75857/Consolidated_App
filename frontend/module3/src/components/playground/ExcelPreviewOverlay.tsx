import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DragEvent as ReactDragEvent, KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { motion } from "framer-motion";
import {
  AlertCircle,
  ArrowDown,
  ArrowLeft,
  ArrowRight,
  ArrowUp,
  ArrowUpDown,
  Calculator,
  Check,
  Columns3,
  Filter,
  GripVertical,
  Loader2,
  MoreHorizontal,
  ChevronLeft,
  ChevronRight,
  Pencil,
  Plus,
  RefreshCw,
  RotateCcw,
  RotateCw,
  Search,
  Table2,
  Trash2,
  Type,
  X,
} from "lucide-react";
import type { ColumnInfo, FileInventoryItem, PreviewData } from "../../types";
import type {
  PreviewColumn,
  PreviewColumnDataTypeOverride,
  PreviewFilter,
  PreviewFilterOp,
  PreviewOperationParams,
  PreviewPivotValueField,
  PreviewRow,
  PreviewSort,
  PreviewTableState,
} from "../../types/excelPreview";
import {
  previewColumnValues,
  previewOperation,
  previewRedo,
  previewRefreshInventory,
  previewState,
  previewUndo,
} from "../../api/client";

const CHUNK_SIZE = 1000;
const ROW_ESTIMATE_SIZE = 38;
const COLUMN_ESTIMATE_SIZE = 180;
const STICKY_COLUMNS_WIDTH = 136;
const PREFETCH_ROWS = 160;
const CACHE_CHUNK_RADIUS = 3;
const MAX_CACHED_CHUNKS = CACHE_CHUNK_RADIUS * 2 + 3;
const FILTER_VALUE_LIMIT = 500;
const DATA_TYPES: PreviewColumnDataTypeOverride[] = ["TEXT", "INTEGER", "DOUBLE", "DATE", "BOOLEAN"];
const AGGREGATIONS: PreviewPivotValueField["aggregation"][] = [
  "sum",
  "count",
  "avg",
  "min",
  "max",
  "count_distinct",
];
const NUMBER_ONLY_AGGREGATIONS = new Set<PreviewPivotValueField["aggregation"]>(["sum", "avg"]);
const MAX_PIVOT_ROW_FIELDS = 5;
const MAX_PIVOT_VALUE_FIELDS = 5;

type PivotZoneId = "rows" | "columns" | "values";

interface ExcelPreviewOverlayProps {
  sessionId: string;
  inventory: FileInventoryItem[];
  onClose: () => void;
  onInventoryRefresh?: (data: {
    fileInventory: FileInventoryItem[];
    columns: ColumnInfo[];
    previews: Record<string, PreviewData>;
  }) => void;
  title?: string;
}

interface ToolbarButtonProps {
  icon: ReactNode;
  label: string;
  onClick: () => void;
  disabled?: boolean;
  active?: boolean;
  variant?: "default" | "danger";
}

interface ColumnMenuState {
  columnKey: string;
  anchor: HTMLElement;
}

interface FilterMenuState {
  column: PreviewColumn;
  anchor: HTMLElement;
}

interface EditingCell {
  rowId: string | number;
  columnKey: string;
  value: string;
}

function tableLabel(tableKey: string) {
  return tableKey.split(/[\\/]/).pop() || tableKey;
}

function formatValue(value: unknown) {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function typeBadgeLabel(dataType?: string | null) {
  const normalized = String(dataType || "TEXT").toUpperCase();
  if (normalized === "INTEGER" || normalized === "DOUBLE" || normalized === "DECIMAL" || normalized === "NUMERIC") return "Num";
  if (normalized === "DATE" || normalized === "DATETIME" || normalized === "TIMESTAMP") return "Date";
  if (normalized === "BOOLEAN" || normalized === "BOOL") return "Bool";
  return "Text";
}

function typeBadgeClass(dataType?: string | null) {
  const normalized = String(dataType || "TEXT").toUpperCase();
  if (normalized === "INTEGER" || normalized === "DOUBLE" || normalized === "DECIMAL" || normalized === "NUMERIC") {
    return "border-blue-200 bg-blue-50 text-blue-700 dark:border-blue-900 dark:bg-blue-950/40 dark:text-blue-300";
  }
  if (normalized === "DATE" || normalized === "DATETIME" || normalized === "TIMESTAMP") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300";
  }
  if (normalized === "BOOLEAN" || normalized === "BOOL") {
    return "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300";
  }
  return "border-neutral-200 bg-neutral-50 text-neutral-600 dark:border-neutral-800 dark:bg-neutral-950 dark:text-neutral-300";
}

function isNumericDataType(dataType?: string | null) {
  const normalized = String(dataType || "").toUpperCase();
  return ["INTEGER", "DOUBLE", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "NUMBER"].includes(normalized);
}

function defaultAggregationForColumn(column: PreviewColumn): PreviewPivotValueField["aggregation"] {
  return isNumericDataType(column.dataType) ? "sum" : "count";
}

function pivotFieldName(value: string) {
  return value.trim().toLowerCase().replace(/[\s-]+/g, "_");
}

function isPivotDataColumn(column: PreviewColumn) {
  const key = pivotFieldName(column.key);
  const displayName = pivotFieldName(column.displayName);
  return key !== "record_id" && key !== "__row_id" && displayName !== "record_id" && displayName !== "__row_id";
}

function aggregationLabel(aggregation: PreviewPivotValueField["aggregation"]) {
  if (aggregation === "count_distinct") return "count distinct";
  return aggregation;
}

function filterLabel(filter: PreviewFilter, columnName: string) {
  const op = filter.op === "not_in" ? "excludes" : "includes";
  const values = (filter.values || [])
    .filter((value) => value !== null && value !== undefined && value !== "")
    .map((value) => String(value));
  const blank = filter.includeBlanks ? "blank" : "";
  const labelValues = [...values.slice(0, 3), blank].filter(Boolean);
  const more = values.length > 3 ? ` +${values.length - 3}` : "";
  return `${columnName} ${op} ${labelValues.join(", ")}${more}`;
}

function excludeColumnFilter(filters: PreviewFilter[], columnKey: string) {
  return filters.filter((filter) => filter.columnKey !== columnKey);
}

function chunkOffsetForIndex(index: number) {
  return Math.max(0, Math.floor(index / CHUNK_SIZE) * CHUNK_SIZE);
}

function chunkOffsetsForRange(startIndex: number, endIndex: number, totalRows: number) {
  if (totalRows <= 0) return [0];
  const start = chunkOffsetForIndex(Math.max(0, startIndex));
  const end = chunkOffsetForIndex(Math.min(Math.max(0, totalRows - 1), endIndex));
  const offsets: number[] = [];
  for (let offset = start; offset <= end; offset += CHUNK_SIZE) {
    offsets.push(offset);
  }
  return offsets;
}

function pruneChunkCache(
  cache: Map<number, PreviewRow[]>,
  centerOffset: number,
  totalRows: number,
) {
  for (const offset of cache.keys()) {
    if (offset >= totalRows) cache.delete(offset);
  }
  while (cache.size > MAX_CACHED_CHUNKS) {
    let farthestOffset: number | null = null;
    let farthestDistance = -1;
    for (const offset of cache.keys()) {
      if (offset === centerOffset) continue;
      const distance = Math.abs(offset - centerOffset);
      if (distance > farthestDistance) {
        farthestDistance = distance;
        farthestOffset = offset;
      }
    }
    if (farthestOffset === null) break;
    cache.delete(farthestOffset);
  }
}

function anchoredPanelStyle(anchor?: HTMLElement | null, width = 320) {
  if (!anchor || typeof window === "undefined") {
    return { top: 128, right: 32, width };
  }
  const margin = 12;
  const rect = anchor.getBoundingClientRect();
  const left = Math.min(
    Math.max(margin, rect.left),
    Math.max(margin, window.innerWidth - width - margin),
  );
  const top = Math.min(
    Math.max(margin, rect.bottom + 8),
    Math.max(margin, window.innerHeight - margin - 80),
  );
  return { top, left, width };
}

function ToolbarButton({ icon, label, onClick, disabled, active, variant = "default" }: ToolbarButtonProps) {
  const danger = variant === "danger";
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={label}
      aria-label={label}
      className={`inline-flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition-colors ${
        danger
          ? "border-red-600 bg-red-600 text-white hover:bg-red-700"
          : active
            ? "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300"
            : "border-neutral-200 bg-white text-neutral-700 hover:text-red-600 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300 dark:hover:text-red-400"
      } ${disabled ? "cursor-not-allowed opacity-50" : ""}`}
    >
      {icon}
      <span className="hidden sm:inline">{label}</span>
    </button>
  );
}

export default function ExcelPreviewOverlay({
  sessionId,
  inventory,
  onClose,
  onInventoryRefresh,
  title = "Raw Data Preview",
}: ExcelPreviewOverlayProps) {
  const [activeTableKey, setActiveTableKey] = useState(() => inventory[0]?.table_key ?? "");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<PreviewFilter[]>([]);
  const [sort, setSort] = useState<PreviewSort[]>([]);
  const [state, setState] = useState<PreviewTableState | null>(null);
  const [chunkRows, setChunkRows] = useState<Map<number, PreviewRow[]>>(() => new Map());
  const [loading, setLoading] = useState(false);
  const [operationLoading, setOperationLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedRows, setSelectedRows] = useState<Set<string | number>>(() => new Set());
  const [editingCell, setEditingCell] = useState<EditingCell | null>(null);
  const [filterMenu, setFilterMenu] = useState<FilterMenuState | null>(null);
  const [columnMenu, setColumnMenu] = useState<ColumnMenuState | null>(null);
  const [showCalcDialog, setShowCalcDialog] = useState(false);
  const [showPivotDialog, setShowPivotDialog] = useState(false);
  const [sidebarExpanded, setSidebarExpanded] = useState(true);
  const requestIdRef = useRef(0);
  const operationRequestIdRef = useRef(0);
  const activeTableKeyRef = useRef(activeTableKey);
  const pendingTableKeyRef = useRef<string | null>(null);
  const cancelCellEditRef = useRef(false);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const gridScrollRef = useRef<HTMLDivElement>(null);
  const loadedChunksRef = useRef<Set<number>>(new Set());
  const loadingChunksRef = useRef<Set<number>>(new Set());
  const chunkControllersRef = useRef<Map<number, AbortController>>(new Map());

  const activeInventory = useMemo(
    () => inventory.find((item) => item.table_key === activeTableKey),
    [activeTableKey, inventory],
  );
  const columns = state?.columns ?? [];
  const totalRows = state?.totalRows ?? activeInventory?.rows ?? 0;
  const activeSort = sort[0];
  const cachedRows = useMemo(
    () => Array.from(chunkRows.values()).reduce((total, rows) => total + rows.length, 0),
    [chunkRows],
  );
  const rowForIndex = useCallback(
    (index: number) => {
      const chunkOffset = chunkOffsetForIndex(index);
      return chunkRows.get(chunkOffset)?.[index - chunkOffset] ?? null;
    },
    [chunkRows],
  );
  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => gridScrollRef.current,
    estimateSize: () => ROW_ESTIMATE_SIZE,
    overscan: 18,
  });
  const virtualRows = rowVirtualizer.getVirtualItems();
  const columnVirtualizer = useVirtualizer({
    horizontal: true,
    count: columns.length,
    getScrollElement: () => gridScrollRef.current,
    estimateSize: () => COLUMN_ESTIMATE_SIZE,
    overscan: 4,
  });
  const virtualColumns = columnVirtualizer.getVirtualItems();
  const virtualColumnItems = virtualColumns
    .map((virtualColumn) => ({
      virtualColumn,
      column: columns[virtualColumn.index],
    }))
    .filter((item): item is { virtualColumn: (typeof virtualColumns)[number]; column: PreviewColumn } =>
      Boolean(item.column),
    );
  const lastVirtualColumn = virtualColumns[virtualColumns.length - 1];
  const leftColumnPadding = virtualColumns[0]?.start ?? 0;
  const rightColumnPadding = Math.max(
    0,
    columnVirtualizer.getTotalSize() - (lastVirtualColumn?.end ?? 0),
  );
  const tableWidth = Math.max(STICKY_COLUMNS_WIDTH + columnVirtualizer.getTotalSize(), STICKY_COLUMNS_WIDTH);
  const firstVirtualRow = virtualRows[0];
  const lastVirtualRow = virtualRows[virtualRows.length - 1];
  const visibleStartRow = totalRows > 0 && firstVirtualRow ? firstVirtualRow.index + 1 : 0;
  const visibleEndRow = totalRows > 0 && lastVirtualRow ? lastVirtualRow.index + 1 : 0;
  const missingVisibleRows = virtualRows.filter((virtualRow) => !rowForIndex(virtualRow.index)).length;
  const visibleLoadedRows = useMemo(
    () =>
      virtualRows
        .map((virtualRow) => rowForIndex(virtualRow.index))
        .filter((row): row is PreviewRow => Boolean(row)),
    [rowForIndex, virtualRows],
  );
  const allVisibleRowsSelected =
    visibleLoadedRows.length > 0 && visibleLoadedRows.every((row) => selectedRows.has(row.__row_id));

  const abortChunkRequests = useCallback(() => {
    for (const controller of chunkControllersRef.current.values()) {
      controller.abort();
    }
    chunkControllersRef.current.clear();
  }, []);

  const loadChunk = useCallback(async (requestedOffset: number) => {
    if (!sessionId || !activeTableKey) return;
    const chunkOffset = chunkOffsetForIndex(requestedOffset);
    if (loadedChunksRef.current.has(chunkOffset) || loadingChunksRef.current.has(chunkOffset)) return;

    const requestGeneration = requestIdRef.current;
    const controller = new AbortController();
    chunkControllersRef.current.set(chunkOffset, controller);
    loadingChunksRef.current.add(chunkOffset);
    if (chunkOffset === 0) setLoading(true);
    setError(null);
    try {
      const result = await previewState({
        sessionId,
        tableKey: activeTableKey,
        offset: chunkOffset,
        limit: CHUNK_SIZE,
        search: search || undefined,
        filters,
        sort,
      }, { signal: controller.signal });
      if (requestIdRef.current !== requestGeneration) return;
      const responseOffset = chunkOffsetForIndex(result.offset ?? chunkOffset);
      loadedChunksRef.current.add(responseOffset);
      setState(result);
      setChunkRows((current) => {
        const next = new Map(current);
        next.set(responseOffset, result.rows);
        pruneChunkCache(next, responseOffset, result.totalRows);
        for (const offset of loadedChunksRef.current) {
          if (!next.has(offset)) loadedChunksRef.current.delete(offset);
        }
        return next;
      });
    } catch (err: any) {
      if (requestIdRef.current !== requestGeneration) return;
      if (controller.signal.aborted) return;
      loadedChunksRef.current.delete(chunkOffset);
      setError(err?.message || "Failed to load preview state");
    } finally {
      if (chunkControllersRef.current.get(chunkOffset) === controller) {
        chunkControllersRef.current.delete(chunkOffset);
      }
      if (requestIdRef.current === requestGeneration) {
        loadingChunksRef.current.delete(chunkOffset);
        if (loadingChunksRef.current.size === 0) setLoading(false);
      }
    }
  }, [activeTableKey, filters, search, sessionId, sort]);

  const resetLoadedRows = useCallback((clearSelection = true, scrollToTop = true) => {
    requestIdRef.current += 1;
    abortChunkRequests();
    loadedChunksRef.current.clear();
    loadingChunksRef.current.clear();
    setChunkRows(new Map());
    setState(null);
    setEditingCell(null);
    setLoading(false);
    if (clearSelection) setSelectedRows(new Set());
    if (scrollToTop) gridScrollRef.current?.scrollTo({ top: 0, left: 0 });
  }, [abortChunkRequests]);

  const refreshPreview = useCallback(() => {
    const firstVisibleIndex = virtualRows[0]?.index ?? 0;
    resetLoadedRows(false, false);
    window.setTimeout(() => {
      void loadChunk(chunkOffsetForIndex(firstVisibleIndex));
    }, 0);
  }, [loadChunk, resetLoadedRows, virtualRows]);

  useEffect(() => {
    activeTableKeyRef.current = activeTableKey;
  }, [activeTableKey]);

  useEffect(() => {
    const activeExists = inventory.some((item) => item.table_key === activeTableKey);
    if (!activeTableKey || activeExists) {
      if (activeExists && pendingTableKeyRef.current === activeTableKey) {
        pendingTableKeyRef.current = null;
      }
      return;
    }
    if (pendingTableKeyRef.current === activeTableKey) return;
    setActiveTableKey(inventory[0]?.table_key ?? "");
  }, [activeTableKey, inventory]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSearch(searchInput.trim());
    }, 300);
    return () => window.clearTimeout(timer);
  }, [searchInput]);

  useEffect(() => {
    resetLoadedRows(true);
    void loadChunk(0);
  }, [activeTableKey, filters, loadChunk, resetLoadedRows, search, sort]);

  useEffect(() => {
    if (!activeTableKey) return;
    if (totalRows === 0) {
      if (!state && !loadedChunksRef.current.has(0)) void loadChunk(0);
      return;
    }
    if (virtualRows.length === 0) return;
    const first = virtualRows[0];
    const last = virtualRows[virtualRows.length - 1];
    const start = Math.max(0, first.index - PREFETCH_ROWS);
    const end = Math.min(totalRows - 1, last.index + PREFETCH_ROWS);
    for (const chunkOffset of chunkOffsetsForRange(start, end, totalRows)) {
      void loadChunk(chunkOffset);
    }
  }, [activeTableKey, loadChunk, state, totalRows, virtualRows]);

  useEffect(() => {
    if (virtualRows.length === 0 || totalRows <= 0) return;
    const first = virtualRows[0];
    const last = virtualRows[virtualRows.length - 1];
    const keepStart = Math.max(0, first.index - CACHE_CHUNK_RADIUS * CHUNK_SIZE);
    const keepEnd = Math.min(totalRows - 1, last.index + CACHE_CHUNK_RADIUS * CHUNK_SIZE);
    const keepOffsets = new Set(chunkOffsetsForRange(keepStart, keepEnd, totalRows));
    setChunkRows((current) => {
      let changed = false;
      const next = new Map(current);
      for (const offset of current.keys()) {
        if (!keepOffsets.has(offset)) {
          next.delete(offset);
          loadedChunksRef.current.delete(offset);
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [totalRows, virtualRows]);

  useEffect(() => {
    searchInputRef.current?.focus();
    return () => closeButtonRef.current?.focus();
  }, []);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (filterMenu || columnMenu || showCalcDialog || showPivotDialog || editingCell) {
          setFilterMenu(null);
          setColumnMenu(null);
          setShowCalcDialog(false);
          setShowPivotDialog(false);
          setEditingCell(null);
        } else {
          onClose();
        }
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [columnMenu, editingCell, filterMenu, onClose, showCalcDialog, showPivotDialog]);

  const changeTable = (tableKey: string) => {
    if (operationLoading) return;
    setActiveTableKey(tableKey);
    setSearchInput("");
    setSearch("");
    setFilters([]);
    setSort([]);
    setSelectedRows(new Set());
    setEditingCell(null);
  };

  const toggleSort = (columnKey: string) => {
    setSort((current) => {
      const existing = current[0];
      if (existing?.columnKey !== columnKey) return [{ columnKey, dir: "asc" }];
      if (existing.dir === "asc") return [{ columnKey, dir: "desc" }];
      return [];
    });
  };

  const applyStateFromOperation = (result: PreviewTableState) => {
    requestIdRef.current += 1;
    abortChunkRequests();
    const responseOffset = chunkOffsetForIndex(result.offset ?? 0);
    loadedChunksRef.current = new Set([responseOffset]);
    loadingChunksRef.current.clear();
    setState(result);
    setChunkRows(new Map([[responseOffset, result.rows]]));
    setSelectedRows(new Set());
    gridScrollRef.current?.scrollTo({ top: 0, left: 0 });
  };

  const runOperation = async (
    op:
      | "cell_edit"
      | "rows_delete"
      | "column_rename"
      | "column_delete"
      | "column_reorder"
      | "column_change_type"
      | "calculated_column"
      | "pivot",
    params: PreviewOperationParams,
  ) => {
    if (!activeTableKey) return null;
    const operationTableKey = activeTableKey;
    const operationRequestId = operationRequestIdRef.current + 1;
    operationRequestIdRef.current = operationRequestId;
    setOperationLoading(true);
    setError(null);
    try {
      const result = await previewOperation({
        sessionId,
        tableKey: operationTableKey,
        op,
        params,
      });
      if (operationRequestIdRef.current !== operationRequestId) return null;
      if (!result.newTableKey && activeTableKeyRef.current !== operationTableKey) return null;
      if (result.newTableKey) {
        pendingTableKeyRef.current = result.newTableKey;
        await refreshInventory(false);
        if (operationRequestIdRef.current !== operationRequestId) return null;
        setActiveTableKey(result.newTableKey);
        applyStateFromOperation(result);
        return result;
      }
      if (op === "pivot" || op === "calculated_column" || op === "column_delete" || op === "column_rename") {
        await refreshInventory(false);
      }
      const shouldScrollToTop = op !== "cell_edit" && op !== "rows_delete";
      const firstVisibleIndex = shouldScrollToTop ? 0 : (virtualRows[0]?.index ?? 0);
      resetLoadedRows(true, shouldScrollToTop);
      window.setTimeout(() => {
        void loadChunk(chunkOffsetForIndex(firstVisibleIndex));
      }, 0);
      return result;
    } catch (err: any) {
      setError(err?.message || `Failed to run ${op}`);
      return null;
    } finally {
      setOperationLoading(false);
    }
  };

  const refreshInventory = async (showSpinner = true) => {
    if (!sessionId) return null;
    if (showSpinner) setOperationLoading(true);
    setError(null);
    try {
      const result = await previewRefreshInventory({ sessionId });
      if (result.ok) onInventoryRefresh?.(result);
      return result;
    } catch (err: any) {
      setError(err?.message || "Failed to refresh inventory");
      return null;
    } finally {
      if (showSpinner) setOperationLoading(false);
    }
  };

  const runHistory = async (kind: "undo" | "redo") => {
    if (!activeTableKey) return;
    setOperationLoading(true);
    setError(null);
    try {
      const result = kind === "undo"
        ? await previewUndo({
            sessionId,
            tableKey: activeTableKey,
          })
        : await previewRedo({
            sessionId,
            tableKey: activeTableKey,
          });
      await refreshInventory(false);
      if ((result as PreviewTableState & { dropped?: boolean }).dropped) {
        applyStateFromOperation(result);
      } else {
        resetLoadedRows(true, true);
        window.setTimeout(() => {
          void loadChunk(0);
        }, 0);
      }
    } catch (err: any) {
      setError(err?.message || `Failed to ${kind}`);
    } finally {
      setOperationLoading(false);
    }
  };

  const toggleVisibleSelection = () => {
    setSelectedRows((current) => {
      const next = new Set(current);
      if (allVisibleRowsSelected) {
        visibleLoadedRows.forEach((row) => next.delete(row.__row_id));
      } else {
        visibleLoadedRows.forEach((row) => next.add(row.__row_id));
      }
      return next;
    });
  };

  const toggleRow = (rowId: string | number) => {
    setSelectedRows((current) => {
      const next = new Set(current);
      if (next.has(rowId)) next.delete(rowId);
      else next.add(rowId);
      return next;
    });
  };

  const commitCellEdit = async () => {
    if (!editingCell) return;
    if (cancelCellEditRef.current) {
      cancelCellEditRef.current = false;
      setEditingCell(null);
      return;
    }
    const payload = {
      rowId: editingCell.rowId,
      columnKey: editingCell.columnKey,
      value: editingCell.value === "" ? null : editingCell.value,
    };
    setEditingCell(null);
    await runOperation("cell_edit", payload);
  };

  const clearFilters = () => {
    setFilters([]);
  };

  const removeFilter = (columnKey: string) => {
    setFilters((current) => current.filter((filter) => filter.columnKey !== columnKey));
  };

  const setColumnFilter = (filter: PreviewFilter | null) => {
    if (!filter) {
      if (filterMenu) {
        setFilters((current) => current.filter((item) => item.columnKey !== filterMenu.column.key));
      }
      setFilterMenu(null);
      return;
    }
    setFilters((current) => [...excludeColumnFilter(current, filter.columnKey), filter]);
    setFilterMenu(null);
  };

  const columnByKey = (columnKey: string) => columns.find((column) => column.key === columnKey);
  const moveColumn = (columnKey: string, direction: -1 | 1) => {
    const keys = columns.map((column) => column.key);
    const index = keys.indexOf(columnKey);
    const target = index + direction;
    if (index < 0 || target < 0 || target >= keys.length) return;
    const next = [...keys];
    [next[index], next[target]] = [next[target], next[index]];
    runOperation("column_reorder", { columnKeys: next });
    setColumnMenu(null);
  };

  const toggleSidebar = () => {
    setSidebarExpanded((expanded) => !expanded);
    window.requestAnimationFrame(() => columnVirtualizer.measure());
  };

  return (
    <motion.div
      className="fixed inset-0 z-[100] flex flex-col bg-white text-neutral-900 dark:bg-neutral-950 dark:text-neutral-100"
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: 12 }}
      transition={{ duration: 0.18 }}
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      <header className="flex h-16 shrink-0 items-center gap-4 border-b border-neutral-200 bg-white px-5 dark:border-neutral-800 dark:bg-neutral-950">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-red-50 text-red-600 dark:bg-red-950/40 dark:text-red-400">
          <Table2 className="h-4 w-4" />
        </div>
        <div className="min-w-0 flex-1">
          <h2 className="truncate text-sm font-semibold text-neutral-950 dark:text-white">{title}</h2>
          <p className="truncate text-xs text-neutral-500 dark:text-neutral-400">
            {inventory.length} table{inventory.length === 1 ? "" : "s"} available
            {state?.dirty ? " - playground has changes" : ""}
          </p>
        </div>
        <ToolbarButton
          icon={<RotateCcw className="h-4 w-4" />}
          label={`Undo${state?.undoDepth ? ` (${state.undoDepth})` : ""}`}
          onClick={() => runHistory("undo")}
          disabled={operationLoading || !state?.undoDepth}
        />
        <ToolbarButton
          icon={<RotateCw className="h-4 w-4" />}
          label={`Redo${state?.redoDepth ? ` (${state.redoDepth})` : ""}`}
          onClick={() => runHistory("redo")}
          disabled={operationLoading || !state?.redoDepth}
        />
        <button
          type="button"
          onClick={() => refreshInventory()}
          disabled={operationLoading}
          className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-neutral-200 bg-white text-neutral-600 transition-colors hover:text-red-600 disabled:cursor-not-allowed disabled:opacity-50 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300 dark:hover:text-red-400"
          title="Refresh inventory"
          aria-label="Refresh inventory"
        >
          <RefreshCw className={`h-4 w-4 ${operationLoading ? "animate-spin" : ""}`} />
        </button>
        <button
          type="button"
          onClick={onClose}
          ref={closeButtonRef}
          className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-neutral-200 bg-white text-neutral-600 transition-colors hover:text-red-600 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300 dark:hover:text-red-400"
          title="Close preview"
          aria-label="Close preview"
        >
          <X className="h-4 w-4" />
        </button>
      </header>

      <div className="flex min-h-0 flex-1 overflow-hidden">
        <aside
          id="preview-table-inventory"
          hidden={!sidebarExpanded}
          className="w-72 shrink-0 overflow-y-auto bg-neutral-50 p-3 dark:bg-neutral-900/70"
        >
          <div className="space-y-1" role="tablist" aria-label="Preview tables">
            {inventory.map((item) => {
              const active = item.table_key === activeTableKey;
              return (
                <button
                  key={item.table_key}
                  type="button"
                  role="tab"
                  aria-selected={active}
                  disabled={operationLoading}
                  onClick={() => changeTable(item.table_key)}
                  className={`w-full rounded-lg px-3 py-2 text-left transition-colors ${
                    active
                      ? "bg-red-50 text-red-700 ring-1 ring-red-200 dark:bg-red-950/30 dark:text-red-300 dark:ring-red-900"
                      : "text-neutral-700 hover:bg-white dark:text-neutral-300 dark:hover:bg-neutral-800"
                    } ${operationLoading ? "cursor-not-allowed opacity-60" : ""}`}
                >
                  <span className="block truncate text-xs font-semibold">{tableLabel(item.table_key)}</span>
                  <span className="mt-0.5 block text-[11px] text-neutral-500 dark:text-neutral-400">
                    {item.rows.toLocaleString()} rows x {item.cols.toLocaleString()} cols
                  </span>
                </button>
              );
            })}
          </div>
        </aside>

        <div className="relative z-20 flex w-10 shrink-0 items-center justify-center border-x border-neutral-200 bg-neutral-50 dark:border-neutral-800 dark:bg-neutral-900/70">
          <button
            type="button"
            onClick={toggleSidebar}
            aria-label={sidebarExpanded ? "Collapse table inventory" : "Expand table inventory"}
            aria-expanded={sidebarExpanded}
            aria-controls="preview-table-inventory"
            title={sidebarExpanded ? "Collapse table inventory" : "Expand table inventory"}
            className="inline-flex h-11 w-10 items-center justify-center rounded-r-lg text-neutral-600 transition-colors hover:bg-white hover:text-red-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500 focus-visible:ring-inset dark:text-neutral-300 dark:hover:bg-neutral-800 dark:hover:text-red-400"
          >
            {sidebarExpanded ? (
              <ChevronLeft className="h-5 w-5" aria-hidden="true" />
            ) : (
              <ChevronRight className="h-5 w-5" aria-hidden="true" />
            )}
          </button>
        </div>

        <section className="flex min-w-0 flex-1 flex-col">
          <div className="flex shrink-0 flex-wrap items-center gap-3 border-b border-neutral-200 bg-white px-4 py-3 dark:border-neutral-800 dark:bg-neutral-950">
            <div className="relative min-w-[240px] flex-1">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-400" />
              <input
                type="search"
                ref={searchInputRef}
                value={searchInput}
                onChange={(event) => setSearchInput(event.target.value)}
                placeholder="Search rows"
                className="h-9 w-full rounded-lg border border-neutral-200 bg-white pl-9 pr-3 text-sm outline-none transition-colors placeholder:text-neutral-400 focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-800 dark:bg-neutral-900 dark:focus:border-red-700 dark:focus:ring-red-950"
              />
            </div>
            <ToolbarButton
              icon={<Trash2 className="h-4 w-4" />}
              label={`Delete rows${selectedRows.size ? ` (${selectedRows.size})` : ""}`}
              onClick={() => runOperation("rows_delete", { rowIds: [...selectedRows] })}
              disabled={operationLoading || selectedRows.size === 0}
              variant="danger"
            />
            <ToolbarButton
              icon={<Calculator className="h-4 w-4" />}
              label="Calculated"
              onClick={() => setShowCalcDialog(true)}
              disabled={!state || operationLoading}
            />
            <ToolbarButton
              icon={<Columns3 className="h-4 w-4" />}
              label="Pivot"
              onClick={() => setShowPivotDialog(true)}
              disabled={!state || operationLoading}
            />
            <button
              type="button"
              onClick={refreshPreview}
              disabled={loading || !activeTableKey}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-neutral-200 bg-white text-neutral-600 transition-colors hover:text-red-600 disabled:cursor-not-allowed disabled:opacity-50 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300 dark:hover:text-red-400"
              title="Refresh preview"
              aria-label="Refresh preview"
            >
              <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            </button>
            <div className="min-w-[260px] text-right text-xs tabular-nums text-neutral-500 dark:text-neutral-400">
              Showing {visibleStartRow.toLocaleString()}-{visibleEndRow.toLocaleString()} of {totalRows.toLocaleString()} rows
              <span className="ml-2 text-neutral-400">
                {cachedRows.toLocaleString()} cached
                {missingVisibleRows > 0 ? " - loading visible rows" : ""}
              </span>
            </div>
          </div>

          {filters.length > 0 && (
            <div className="flex shrink-0 flex-wrap items-center gap-2 border-b border-neutral-200 bg-neutral-50 px-4 py-2 dark:border-neutral-800 dark:bg-neutral-900/70">
              {filters.map((filter) => {
                const columnName = columnByKey(filter.columnKey)?.displayName ?? filter.columnKey;
                return (
                  <span
                    key={filter.columnKey}
                    className="inline-flex max-w-[320px] items-center gap-2 rounded-full border border-red-200 bg-red-50 px-2.5 py-1 text-xs font-medium text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300"
                  >
                    <span className="truncate">{filterLabel(filter, columnName)}</span>
                    <button
                      type="button"
                      onClick={() => removeFilter(filter.columnKey)}
                      className="text-red-500 hover:text-red-700"
                      title={`Remove filter for ${columnName}`}
                      aria-label={`Remove filter for ${columnName}`}
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </span>
                );
              })}
              <button
                type="button"
                onClick={clearFilters}
                className="text-xs font-semibold text-neutral-500 hover:text-red-600 dark:text-neutral-400 dark:hover:text-red-300"
              >
                Clear all
              </button>
            </div>
          )}

          {error && (
            <div className="m-4 flex items-start gap-3 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900/60 dark:bg-red-950/30 dark:text-red-300">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <p className="font-semibold">Preview playground action failed.</p>
                <p className="mt-1 text-xs">{error}</p>
              </div>
            </div>
          )}

          <div ref={gridScrollRef} className="relative min-h-0 flex-1 overflow-auto">
            {(loading || operationLoading) && (
              <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/70 backdrop-blur-sm dark:bg-neutral-950/70">
                <div className="flex items-center gap-2 rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-600 shadow-sm dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300">
                  <Loader2 className="h-4 w-4 animate-spin text-red-600" />
                  {operationLoading ? "Running playground action" : "Loading preview"}
                </div>
              </div>
            )}

            {state && state.columns.length > 0 ? (
              <table
                className="min-w-full border-separate border-spacing-0 text-left text-xs"
                style={{ width: tableWidth }}
              >
                <thead className="sticky top-0 z-10 bg-neutral-100 dark:bg-neutral-900">
                  <tr>
                    <th className="sticky left-0 z-20 w-10 border-b border-r border-neutral-200 bg-neutral-100 px-2 py-2 dark:border-neutral-800 dark:bg-neutral-900">
                      <input
                        type="checkbox"
                        checked={allVisibleRowsSelected}
                        onChange={toggleVisibleSelection}
                        disabled={visibleLoadedRows.length === 0}
                        aria-label="Select all rendered rows"
                        className="h-3.5 w-3.5 rounded border-neutral-300 text-red-600 focus:ring-red-500"
                      />
                    </th>
                    <th className="sticky left-10 z-20 w-24 border-b border-r border-neutral-200 bg-neutral-100 px-3 py-2 font-semibold text-neutral-500 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-400">
                      Row
                    </th>
                    {leftColumnPadding > 0 && (
                      <th
                        aria-hidden="true"
                        style={{ width: leftColumnPadding, minWidth: leftColumnPadding }}
                        className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                      />
                    )}
                    {virtualColumnItems.map(({ column, virtualColumn }) => {
                      const isSorted = activeSort?.columnKey === column.key;
                      const hasFilter = filters.some((filter) => filter.columnKey === column.key);
                      return (
                        <th
                          key={column.key}
                          style={{ width: virtualColumn.size, minWidth: virtualColumn.size }}
                          aria-sort={isSorted ? (activeSort.dir === "asc" ? "ascending" : "descending") : "none"}
                          className="max-w-[280px] whitespace-nowrap border-b border-r border-neutral-200 px-2 py-2 font-semibold text-neutral-700 dark:border-neutral-800 dark:text-neutral-200"
                        >
                          <div className="flex h-9 max-w-full items-center gap-1.5">
                            <button
                              type="button"
                              onClick={() => toggleSort(column.key)}
                              className={`group inline-flex h-7 min-w-0 flex-1 items-center gap-1.5 rounded px-1.5 text-left outline-none transition hover:bg-neutral-200/70 focus:ring-2 focus:ring-red-200 dark:hover:bg-neutral-800 ${
                                isSorted ? "text-red-700 dark:text-red-300" : "text-neutral-700 dark:text-neutral-200"
                              }`}
                              title={`Sort by ${column.displayName}`}
                              aria-label={`Sort by ${column.displayName}; current state ${isSorted ? activeSort.dir : "none"}`}
                            >
                              {isSorted ? (
                                activeSort.dir === "asc" ? (
                                  <ArrowUp className="h-3.5 w-3.5 shrink-0 text-red-600" />
                                ) : (
                                  <ArrowDown className="h-3.5 w-3.5 shrink-0 text-red-600" />
                                )
                              ) : (
                                <ArrowUpDown className="h-3.5 w-3.5 shrink-0 text-neutral-400 group-hover:text-neutral-600" />
                              )}
                              <span className="min-w-0 flex-1 truncate">{column.displayName}</span>
                              <span
                                className={`inline-flex shrink-0 items-center rounded-full border px-1.5 py-0.5 text-[10px] font-semibold leading-none ${typeBadgeClass(column.dataType)}`}
                                title={`Column type: ${column.dataType || "TEXT"}`}
                                aria-label={`Column type ${typeBadgeLabel(column.dataType)}`}
                              >
                                {typeBadgeLabel(column.dataType)}
                              </span>
                              {isSorted && <span className="sr-only">Sorted {activeSort.dir}</span>}
                            </button>
                            <button
                              type="button"
                              onClick={(event) => setFilterMenu({ column, anchor: event.currentTarget })}
                              aria-pressed={hasFilter}
                              className={`inline-flex h-7 w-7 shrink-0 items-center justify-center rounded outline-none transition focus:ring-2 focus:ring-red-200 ${
                                hasFilter
                                  ? "bg-red-50 text-red-600 ring-1 ring-red-200 dark:bg-red-950/40 dark:text-red-300 dark:ring-red-900"
                                  : "text-neutral-400 hover:bg-neutral-200/70 hover:text-red-600 dark:hover:bg-neutral-800"
                              }`}
                              title={`Filter ${column.displayName}`}
                              aria-label={`Filter ${column.displayName}`}
                            >
                              <Filter className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              onClick={(event) => setColumnMenu({ columnKey: column.key, anchor: event.currentTarget })}
                              className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded text-neutral-400 outline-none transition hover:bg-neutral-200/70 hover:text-red-600 focus:ring-2 focus:ring-red-200 dark:hover:bg-neutral-800"
                              title={`Column actions for ${column.displayName}`}
                              aria-label={`Column actions for ${column.displayName}`}
                            >
                              <MoreHorizontal className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        </th>
                      );
                    })}
                    {rightColumnPadding > 0 && (
                      <th
                        aria-hidden="true"
                        style={{ width: rightColumnPadding, minWidth: rightColumnPadding }}
                        className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                      />
                    )}
                  </tr>
                </thead>
                <tbody>
                  {totalRows === 0 ? (
                    <tr>
                      <td
                        colSpan={Math.max(3, virtualColumnItems.length + 4)}
                        className="px-4 py-10 text-center text-sm text-neutral-500 dark:text-neutral-400"
                      >
                        {search || filters.length > 0 ? "No rows match the current search or filters." : "No preview rows available."}
                      </td>
                    </tr>
                  ) : (
                    <>
                      {virtualRows[0] && virtualRows[0].start > 0 && (
                        <tr aria-hidden="true">
                          <td
                            colSpan={Math.max(3, virtualColumnItems.length + 4)}
                            style={{ height: virtualRows[0].start }}
                            className="border-0 p-0"
                          />
                        </tr>
                      )}
                      {virtualRows.map((virtualRow) => {
                      const row = rowForIndex(virtualRow.index);
                      if (!row) {
                        return (
                          <tr
                            key={virtualRow.key}
                            style={{ height: virtualRow.size }}
                            className="odd:bg-white even:bg-neutral-50/60 dark:odd:bg-neutral-950 dark:even:bg-neutral-900/50"
                          >
                            <td className="sticky left-0 z-[1] w-10 border-b border-r border-neutral-200 bg-inherit px-2 py-2 text-center dark:border-neutral-800" />
                            <td className="sticky left-10 z-[1] w-24 border-b border-r border-neutral-200 bg-inherit px-3 py-2 font-mono text-[11px] text-neutral-400 dark:border-neutral-800">
                              {(virtualRow.index + 1).toLocaleString()}
                            </td>
                            {leftColumnPadding > 0 && (
                              <td
                                aria-hidden="true"
                                style={{ width: leftColumnPadding, minWidth: leftColumnPadding }}
                                className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                              />
                            )}
                            <td
                              colSpan={Math.max(1, virtualColumnItems.length)}
                              className="border-b border-r border-neutral-200 px-3 py-2 text-xs text-neutral-400 dark:border-neutral-800"
                            >
                              Loading row
                            </td>
                            {rightColumnPadding > 0 && (
                              <td
                                aria-hidden="true"
                                style={{ width: rightColumnPadding, minWidth: rightColumnPadding }}
                                className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                              />
                            )}
                          </tr>
                        );
                      }
                      const selected = selectedRows.has(row.__row_id);
                      return (
                        <tr
                          key={String(row.__row_id)}
                          style={{ height: virtualRow.size }}
                          className={`odd:bg-white even:bg-neutral-50/60 dark:odd:bg-neutral-950 dark:even:bg-neutral-900/50 ${
                            selected ? "outline outline-1 outline-red-200 dark:outline-red-900" : ""
                          }`}
                        >
                          <td className="sticky left-0 z-[1] w-10 border-b border-r border-neutral-200 bg-inherit px-2 py-2 text-center dark:border-neutral-800">
                            <input
                              type="checkbox"
                              checked={selected}
                              onChange={() => toggleRow(row.__row_id)}
                              aria-label={`Select row ${row.__row_id}`}
                              className="h-3.5 w-3.5 rounded border-neutral-300 text-red-600 focus:ring-red-500"
                            />
                          </td>
                          <td className="sticky left-10 z-[1] w-24 border-b border-r border-neutral-200 bg-inherit px-3 py-2 font-mono text-[11px] text-neutral-500 dark:border-neutral-800 dark:text-neutral-400">
                            {row.__row_id}
                          </td>
                          {leftColumnPadding > 0 && (
                            <td
                              aria-hidden="true"
                              style={{ width: leftColumnPadding, minWidth: leftColumnPadding }}
                              className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                            />
                          )}
                          {virtualColumnItems.map(({ column, virtualColumn }) => {
                            const isEditing =
                              editingCell?.rowId === row.__row_id && editingCell.columnKey === column.key;
                            return (
                              <td
                                key={`${row.__row_id}-${column.key}`}
                                style={{ width: virtualColumn.size, minWidth: virtualColumn.size }}
                                className="max-w-[280px] whitespace-nowrap border-b border-r border-neutral-200 px-3 py-1.5 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300"
                                title={formatValue(row.values[column.key])}
                                onDoubleClick={() =>
                                  setEditingCell({
                                    rowId: row.__row_id,
                                    columnKey: column.key,
                                    value: formatValue(row.values[column.key]),
                                  })
                                }
                              >
                                {isEditing ? (
                                  <input
                                    autoFocus
                                    value={editingCell.value}
                                    onChange={(event) =>
                                      setEditingCell((current) =>
                                        current ? { ...current, value: event.target.value } : current,
                                      )
                                    }
                                    onBlur={commitCellEdit}
                                    onKeyDown={(event) => {
                                      if (event.key === "Enter") {
                                        event.preventDefault();
                                        event.stopPropagation();
                                        commitCellEdit();
                                      }
                                      if (event.key === "Escape") {
                                        event.preventDefault();
                                        event.stopPropagation();
                                        cancelCellEditRef.current = true;
                                        setEditingCell(null);
                                      }
                                    }}
                                    className="h-7 w-full min-w-[160px] rounded border border-red-300 bg-white px-2 text-xs outline-none ring-2 ring-red-100 dark:border-red-800 dark:bg-neutral-900 dark:ring-red-950"
                                  />
                                ) : (
                                  <button
                                    type="button"
                                    onClick={() =>
                                      setEditingCell({
                                        rowId: row.__row_id,
                                        columnKey: column.key,
                                        value: formatValue(row.values[column.key]),
                                      })
                                    }
                                    className="block w-full truncate text-left"
                                    aria-label={`Edit ${column.displayName} for row ${row.__row_id}`}
                                  >
                                    {formatValue(row.values[column.key]) || (
                                      <span className="italic text-neutral-400">blank</span>
                                    )}
                                  </button>
                                )}
                              </td>
                            );
                          })}
                          {rightColumnPadding > 0 && (
                            <td
                              aria-hidden="true"
                              style={{ width: rightColumnPadding, minWidth: rightColumnPadding }}
                              className="border-b border-r border-neutral-200 p-0 dark:border-neutral-800"
                            />
                          )}
                        </tr>
                      );
                    })}
                      {virtualRows[0] && (
                        <tr aria-hidden="true">
                          <td
                            colSpan={Math.max(3, virtualColumnItems.length + 4)}
                            style={{
                              height: Math.max(
                                0,
                                rowVirtualizer.getTotalSize() -
                                  (virtualRows[virtualRows.length - 1]?.end ?? 0),
                              ),
                            }}
                            className="border-0 p-0"
                          />
                        </tr>
                      )}
                    </>
                  )}
                </tbody>
              </table>
            ) : (
              !loading &&
              !error && (
                <div className="flex h-full items-center justify-center p-8 text-sm text-neutral-500 dark:text-neutral-400">
                  No preview rows available.
                </div>
              )
            )}
          </div>
        </section>
      </div>

      {filterMenu && (
        <ColumnFilterPopover
          key={filterMenu.column.key}
          sessionId={sessionId}
          tableKey={activeTableKey}
          column={filterMenu.column}
          anchor={filterMenu.anchor}
          filters={filters}
          existingFilter={filters.find((filter) => filter.columnKey === filterMenu.column.key)}
          onClose={() => setFilterMenu(null)}
          onSetFilter={setColumnFilter}
        />
      )}

      {columnMenu && (
        <ColumnMenu
          column={columnByKey(columnMenu.columnKey)}
          anchor={columnMenu.anchor}
          onClose={() => setColumnMenu(null)}
          onRename={(displayName) => {
            runOperation("column_rename", { columnKey: columnMenu.columnKey, displayName });
            setColumnMenu(null);
          }}
          onDelete={() => {
            runOperation("column_delete", { columnKey: columnMenu.columnKey });
            setColumnMenu(null);
          }}
          onMoveLeft={() => moveColumn(columnMenu.columnKey, -1)}
          onMoveRight={() => moveColumn(columnMenu.columnKey, 1)}
          onChangeType={(newType) => {
            runOperation("column_change_type", { columnKey: columnMenu.columnKey, newType });
            setColumnMenu(null);
          }}
        />
      )}

      {showCalcDialog && (
        <CalculatedColumnDialog
          columns={columns}
          onClose={() => setShowCalcDialog(false)}
          onCreate={async (params) => {
            const result = await runOperation("calculated_column", params);
            if (result) setShowCalcDialog(false);
          }}
        />
      )}

      {showPivotDialog && (
        <PivotDialog
          columns={columns}
          error={error}
          onClose={() => setShowPivotDialog(false)}
          onCreate={async (params) => {
            const result = await runOperation("pivot", params);
            if (result) setShowPivotDialog(false);
          }}
        />
      )}
    </motion.div>
  );
}

function ColumnFilterPopover({
  sessionId,
  tableKey,
  column,
  anchor,
  filters,
  existingFilter,
  onClose,
  onSetFilter,
}: {
  sessionId: string;
  tableKey: string;
  column: PreviewColumn;
  anchor?: HTMLElement;
  filters: PreviewFilter[];
  existingFilter?: PreviewFilter;
  onClose: () => void;
  onSetFilter: (filter: PreviewFilter | null) => void;
}) {
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [values, setValues] = useState<string[]>([]);
  const [hasBlanks, setHasBlanks] = useState(false);
  const [totalDistinct, setTotalDistinct] = useState(0);
  const [selected, setSelected] = useState<Set<string>>(
    () => new Set((existingFilter?.values || []).map((value) => String(value))),
  );
  const [includeBlanks, setIncludeBlanks] = useState(existingFilter?.includeBlanks ?? false);
  const [op, setOp] = useState<Extract<PreviewFilterOp, "in" | "not_in">>(
    existingFilter?.op === "not_in" ? "not_in" : "in",
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [panelStyle, setPanelStyle] = useState(() => anchoredPanelStyle(anchor));
  const panelRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    searchRef.current?.focus();
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => setSearch(searchInput.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [searchInput]);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(event.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [onClose]);

  useEffect(() => {
    const syncPosition = () => setPanelStyle(anchoredPanelStyle(anchor));
    syncPosition();
    window.addEventListener("resize", syncPosition);
    window.addEventListener("scroll", syncPosition, true);
    return () => {
      window.removeEventListener("resize", syncPosition);
      window.removeEventListener("scroll", syncPosition, true);
    };
  }, [anchor]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    previewColumnValues({
      sessionId,
      tableKey,
      columnKey: column.key,
      search: search || undefined,
      filters: excludeColumnFilter(filters, column.key),
      limit: FILTER_VALUE_LIMIT,
    }, { signal: controller.signal })
      .then((result) => {
        if (cancelled) return;
        setValues(result.values || []);
        setHasBlanks(Boolean(result.hasBlanks));
        setTotalDistinct(result.totalDistinct || 0);
      })
      .catch((err: any) => {
        if (!cancelled && !controller.signal.aborted) setError(err?.message || "Failed to load filter values");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [column.key, filters, search, sessionId, tableKey]);

  const toggleValue = (value: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(value)) next.delete(value);
      else next.add(value);
      return next;
    });
  };

  const selectAllVisible = () => {
    setSelected((current) => {
      const next = new Set(current);
      values.forEach((value) => next.add(value));
      return next;
    });
    if (hasBlanks) setIncludeBlanks(true);
  };

  const clearSelection = () => {
    setSelected(new Set());
    setIncludeBlanks(false);
  };

  const apply = () => {
    if (selected.size === 0 && !includeBlanks) {
      onSetFilter(null);
      return;
    }
    onSetFilter({
      columnKey: column.key,
      op,
      values: [...selected],
      includeBlanks,
    });
  };
  const hasMoreDistinctValues = totalDistinct > values.length;

  return (
    <div className="fixed inset-0 z-[120] pointer-events-none">
      <div
        ref={panelRef}
        style={panelStyle}
        className="pointer-events-auto absolute rounded-lg border border-neutral-200 bg-white p-3 shadow-xl dark:border-neutral-800 dark:bg-neutral-900"
        role="dialog"
        aria-label={`Filter ${column.displayName}`}
      >
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <p className="truncate text-sm font-semibold text-neutral-900 dark:text-white">{column.displayName}</p>
            <span
              className={`mt-1 inline-flex items-center rounded-full border px-1.5 py-0.5 text-[10px] font-semibold leading-none ${typeBadgeClass(column.dataType)}`}
            >
              {typeBadgeLabel(column.dataType)}
            </span>
          </div>
          <button type="button" onClick={onClose} aria-label="Close filter" className="rounded p-1 text-neutral-400 hover:text-red-600">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2">
          <button
            type="button"
            onClick={() => setOp("in")}
            aria-pressed={op === "in"}
            className={`rounded-lg border px-2 py-1.5 text-xs font-semibold ${
              op === "in" ? "border-red-200 bg-red-50 text-red-700" : "border-neutral-200 text-neutral-600 dark:border-neutral-800 dark:text-neutral-300"
            }`}
          >
            Include
          </button>
          <button
            type="button"
            onClick={() => setOp("not_in")}
            aria-pressed={op === "not_in"}
            className={`rounded-lg border px-2 py-1.5 text-xs font-semibold ${
              op === "not_in" ? "border-red-200 bg-red-50 text-red-700" : "border-neutral-200 text-neutral-600 dark:border-neutral-800 dark:text-neutral-300"
            }`}
          >
            Exclude
          </button>
        </div>
        <div className="relative mt-3">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-neutral-400" />
          <input
            ref={searchRef}
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder="Search values"
            className="h-9 w-full rounded-lg border border-neutral-200 bg-white pl-8 pr-3 text-xs outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-800 dark:bg-neutral-950"
          />
        </div>
        <div className="mt-3 flex items-center justify-between gap-2 text-[11px]">
          <span className="text-neutral-500">
            {selected.size + (includeBlanks ? 1 : 0)} selected / {totalDistinct.toLocaleString()} distinct
          </span>
          <div className="flex items-center gap-2">
            <button type="button" onClick={selectAllVisible} className="font-semibold text-neutral-600 hover:text-red-600 dark:text-neutral-300">
              {hasMoreDistinctValues ? "Select loaded" : "Select All"}
            </button>
            <button type="button" onClick={clearSelection} className="font-semibold text-neutral-600 hover:text-red-600 dark:text-neutral-300">
              Clear
            </button>
          </div>
        </div>
        {hasMoreDistinctValues && (
          <p className="mt-2 rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-[11px] text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-200">
            Showing first {values.length.toLocaleString()} values. Select loaded only applies to loaded values.
          </p>
        )}
        <div className="mt-3 max-h-64 overflow-y-auto rounded-lg border border-neutral-200 dark:border-neutral-800">
          {loading && (
            <div className="flex items-center gap-2 px-3 py-6 text-xs text-neutral-500">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Loading values
            </div>
          )}
          {error && <div className="px-3 py-3 text-xs text-red-600">{error}</div>}
          {!loading && !error && (
            <>
              {hasBlanks && (
                <label className="flex cursor-pointer items-center gap-2 border-b border-neutral-100 px-3 py-2 text-xs dark:border-neutral-800">
                  <input
                    type="checkbox"
                    checked={includeBlanks}
                    onChange={(event) => setIncludeBlanks(event.target.checked)}
                    className="h-3.5 w-3.5 rounded border-neutral-300 text-red-600"
                  />
                  <span className="italic text-neutral-500">Blank values</span>
                </label>
              )}
              {values.map((value) => (
                <label
                  key={value}
                  className="flex cursor-pointer items-center gap-2 border-b border-neutral-100 px-3 py-2 text-xs last:border-b-0 dark:border-neutral-800"
                >
                  <input
                    type="checkbox"
                    checked={selected.has(value)}
                    onChange={() => toggleValue(value)}
                    className="h-3.5 w-3.5 rounded border-neutral-300 text-red-600"
                  />
                  <span className="truncate" title={value}>{value}</span>
                </label>
              ))}
              {values.length === 0 && !hasBlanks && (
                <div className="px-3 py-6 text-center text-xs text-neutral-500">No values found.</div>
              )}
            </>
          )}
        </div>
        <div className="mt-3 flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-lg border border-neutral-200 px-3 py-1.5 text-xs font-semibold dark:border-neutral-800">
            Cancel
          </button>
          <button type="button" onClick={apply} className="rounded-lg bg-red-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-red-700">
            OK
          </button>
        </div>
      </div>
    </div>
  );
}

function ColumnMenu({
  column,
  anchor,
  onClose,
  onRename,
  onDelete,
  onMoveLeft,
  onMoveRight,
  onChangeType,
}: {
  column?: PreviewColumn;
  anchor?: HTMLElement;
  onClose: () => void;
  onRename: (displayName: string) => void;
  onDelete: () => void;
  onMoveLeft: () => void;
  onMoveRight: () => void;
  onChangeType: (type: PreviewColumnDataTypeOverride) => void;
}) {
  const [name, setName] = useState(column?.displayName ?? "");
  const [type, setType] = useState<PreviewColumnDataTypeOverride>(
    DATA_TYPES.includes(column?.dataType as PreviewColumnDataTypeOverride)
      ? (column?.dataType as PreviewColumnDataTypeOverride)
      : "TEXT",
  );
  const [panelStyle, setPanelStyle] = useState(() => anchoredPanelStyle(anchor));
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(event.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [onClose]);

  useEffect(() => {
    const syncPosition = () => setPanelStyle(anchoredPanelStyle(anchor));
    syncPosition();
    window.addEventListener("resize", syncPosition);
    window.addEventListener("scroll", syncPosition, true);
    return () => {
      window.removeEventListener("resize", syncPosition);
      window.removeEventListener("scroll", syncPosition, true);
    };
  }, [anchor]);

  if (!column) return null;

  return (
    <div className="fixed inset-0 z-[120] pointer-events-none">
      <div
        ref={panelRef}
        role="dialog"
        aria-label={`Column actions for ${column.displayName}`}
        style={panelStyle}
        className="pointer-events-auto absolute rounded-lg border border-neutral-200 bg-white p-3 shadow-xl dark:border-neutral-800 dark:bg-neutral-900"
      >
        <div className="flex items-center justify-between">
          <p className="text-sm font-semibold text-neutral-900 dark:text-white">Column actions</p>
          <button type="button" onClick={onClose} aria-label="Close column actions" className="rounded p-1 text-neutral-400 hover:text-red-600">
            <X className="h-4 w-4" />
          </button>
        </div>
        <label className="mt-3 block text-xs font-semibold text-neutral-500">
          Display name
          <div className="mt-1 flex gap-2">
            <input
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="h-9 min-w-0 flex-1 rounded-lg border border-neutral-200 bg-white px-3 text-xs outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-800 dark:bg-neutral-950"
            />
            <button
              type="button"
              onClick={() => onRename(name.trim() || column.displayName)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-red-600 text-white"
              title="Rename column"
              aria-label="Rename column"
            >
              <Check className="h-4 w-4" />
            </button>
          </div>
        </label>
        <div className="mt-3 grid grid-cols-2 gap-2">
          <button type="button" onClick={onMoveLeft} className="inline-flex items-center justify-center gap-2 rounded-lg border border-neutral-200 px-3 py-2 text-xs font-semibold dark:border-neutral-800">
            <ArrowLeft className="h-3.5 w-3.5" />
            Move left
          </button>
          <button type="button" onClick={onMoveRight} className="inline-flex items-center justify-center gap-2 rounded-lg border border-neutral-200 px-3 py-2 text-xs font-semibold dark:border-neutral-800">
            Move right
            <ArrowRight className="h-3.5 w-3.5" />
          </button>
        </div>
        <label className="mt-3 block text-xs font-semibold text-neutral-500">
          Type override
          <div className="mt-1 flex gap-2">
            <select
              value={type}
              onChange={(event) => setType(event.target.value as PreviewColumnDataTypeOverride)}
              className="h-9 min-w-0 flex-1 rounded-lg border border-neutral-200 bg-white px-3 text-xs outline-none focus:border-red-400 dark:border-neutral-800 dark:bg-neutral-950"
            >
              {DATA_TYPES.map((dataType) => (
                <option key={dataType} value={dataType}>{dataType}</option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => onChangeType(type)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-red-600 text-white"
              title="Update type"
              aria-label="Update type"
            >
              <Type className="h-4 w-4" />
            </button>
          </div>
        </label>
        <button
          type="button"
          onClick={onDelete}
          className="mt-4 inline-flex w-full items-center justify-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs font-semibold text-red-700 hover:bg-red-100 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300"
        >
          <Trash2 className="h-4 w-4" />
          Delete column
        </button>
      </div>
    </div>
  );
}

function Modal({
  title,
  children,
  onClose,
}: {
  title: string;
  children: ReactNode;
  onClose: () => void;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  const firstFocusRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    firstFocusRef.current?.focus();
  }, []);

  const trapFocus = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "Tab" || !panelRef.current) return;
    const focusable = Array.from(
      panelRef.current.querySelectorAll<HTMLElement>(
        'button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ),
    ).filter((element) => !element.hasAttribute("disabled"));
    if (focusable.length === 0) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  return (
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-neutral-950/50 p-4" onMouseDown={onClose}>
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-lg border border-neutral-200 bg-white shadow-2xl dark:border-neutral-800 dark:bg-neutral-900"
        onMouseDown={(event) => event.stopPropagation()}
        onKeyDown={trapFocus}
      >
        <div className="flex items-center justify-between border-b border-neutral-200 px-4 py-3 dark:border-neutral-800">
          <h3 className="text-sm font-semibold text-neutral-950 dark:text-white">{title}</h3>
          <button
            ref={firstFocusRef}
            type="button"
            onClick={onClose}
            className="rounded p-1 text-neutral-400 hover:text-red-600"
            aria-label={`Close ${title}`}
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

function CalculatedColumnDialog({
  columns,
  onClose,
  onCreate,
}: {
  columns: PreviewColumn[];
  onClose: () => void;
  onCreate: (params: { name: string; expression: string; dataType: PreviewColumnDataTypeOverride }) => void;
}) {
  const [name, setName] = useState("");
  const [expression, setExpression] = useState("");
  const [dataType, setDataType] = useState<PreviewColumnDataTypeOverride>("TEXT");
  const canSubmit = name.trim().length > 0 && expression.trim().length > 0;

  return (
    <Modal title="Add calculated column" onClose={onClose}>
      <div className="grid gap-4 p-4 md:grid-cols-[1fr_220px]">
        <div className="space-y-3">
          <label className="block text-xs font-semibold text-neutral-500">
            New column name
            <input
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="mt-1 h-9 w-full rounded-lg border border-neutral-200 bg-white px-3 text-sm outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-800 dark:bg-neutral-950"
            />
          </label>
          <label className="block text-xs font-semibold text-neutral-500">
            Expression
            <textarea
              value={expression}
              onChange={(event) => setExpression(event.target.value)}
              rows={7}
              placeholder="Example: COALESCE([COL_1], '')"
              className="mt-1 w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 font-mono text-xs outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-800 dark:bg-neutral-950"
            />
          </label>
          <label className="block text-xs font-semibold text-neutral-500">
            Result type
            <select
              value={dataType}
              onChange={(event) => setDataType(event.target.value as PreviewColumnDataTypeOverride)}
              className="mt-1 h-9 w-full rounded-lg border border-neutral-200 bg-white px-3 text-sm outline-none dark:border-neutral-800 dark:bg-neutral-950"
            >
              {DATA_TYPES.map((type) => (
                <option key={type} value={type}>{type}</option>
              ))}
            </select>
          </label>
        </div>
        <div className="rounded-lg border border-neutral-200 p-3 dark:border-neutral-800">
          <p className="text-xs font-semibold text-neutral-500">Columns</p>
          <div className="mt-2 max-h-64 space-y-1 overflow-y-auto">
            {columns.map((column) => (
              <button
                type="button"
                key={column.key}
                onClick={() => setExpression((current) => `${current}${current ? " " : ""}[${column.key}]`)}
                className="flex w-full items-center justify-between gap-2 rounded px-2 py-1.5 text-left text-xs hover:bg-neutral-100 dark:hover:bg-neutral-800"
                title={`${column.displayName} (${column.key})`}
              >
                <span className="truncate">{column.displayName}</span>
                <span className="shrink-0 font-mono text-[10px] text-neutral-400">{column.key}</span>
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="flex justify-end gap-2 border-t border-neutral-200 px-4 py-3 dark:border-neutral-800">
        <button type="button" onClick={onClose} className="rounded-lg border border-neutral-200 px-3 py-2 text-xs font-semibold dark:border-neutral-800">
          Cancel
        </button>
        <button
          type="button"
          disabled={!canSubmit}
          onClick={() => onCreate({ name: name.trim(), expression: expression.trim(), dataType })}
          className="rounded-lg bg-red-600 px-3 py-2 text-xs font-semibold text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Add column
        </button>
      </div>
    </Modal>
  );
}

function PivotDialog({
  columns,
  error,
  onClose,
  onCreate,
}: {
  columns: PreviewColumn[];
  error: string | null;
  onClose: () => void;
  onCreate: (params: {
    rowFields: string[];
    columnFields: string[];
    valueFields: PreviewPivotValueField[];
  }) => Promise<void>;
}) {
  const [rowFields, setRowFields] = useState<string[]>([]);
  const [columnFields, setColumnFields] = useState<string[]>([]);
  const [valueFields, setValueFields] = useState<PreviewPivotValueField[]>([]);
  const [dragOverZone, setDragOverZone] = useState<PivotZoneId | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitAttempted, setSubmitAttempted] = useState(false);
  const submitInFlightRef = useRef(false);
  const pivotColumns = useMemo(() => columns.filter(isPivotDataColumn), [columns]);
  const columnMap = useMemo(() => new Map(pivotColumns.map((column) => [column.key, column])), [pivotColumns]);
  const validColumnKeys = useMemo(() => new Set(pivotColumns.map((column) => column.key)), [pivotColumns]);
  const validRowFields = rowFields.filter((columnKey) => validColumnKeys.has(columnKey));
  const validColumnFields = columnFields.filter((columnKey) => validColumnKeys.has(columnKey));
  const validValueFields = valueFields.filter((field) => validColumnKeys.has(field.columnKey));
  const canSubmit = validValueFields.length > 0 && (validRowFields.length > 0 || validColumnFields.length === 0);

  useEffect(() => {
    setRowFields((current) => current.filter((columnKey) => validColumnKeys.has(columnKey)));
    setColumnFields((current) => current.filter((columnKey) => validColumnKeys.has(columnKey)));
    setValueFields((current) => current.filter((field) => validColumnKeys.has(field.columnKey)));
  }, [validColumnKeys]);

  const submit = async () => {
    if (!canSubmit || submitting || submitInFlightRef.current) return;
    submitInFlightRef.current = true;
    setSubmitting(true);
    setSubmitAttempted(true);
    try {
      await onCreate({
        rowFields: validRowFields,
        columnFields: validColumnFields,
        valueFields: validValueFields,
      });
    } finally {
      submitInFlightRef.current = false;
      setSubmitting(false);
    }
  };

  const addToZone = (column: PreviewColumn, zone: PivotZoneId) => {
    if (!validColumnKeys.has(column.key)) return;
    if (zone === "rows") {
      setRowFields((current) =>
        current.includes(column.key) || current.length >= MAX_PIVOT_ROW_FIELDS
          ? current
          : [...current, column.key],
      );
      return;
    }
    if (zone === "columns") {
      setColumnFields((current) => current.includes(column.key) ? current : [...current, column.key]);
      return;
    }
    setValueFields((current) =>
      current.some((field) => field.columnKey === column.key) || current.length >= MAX_PIVOT_VALUE_FIELDS
        ? current
        : [...current, { columnKey: column.key, aggregation: defaultAggregationForColumn(column) }],
    );
  };

  const removeFromZone = (columnKey: string, zone: PivotZoneId) => {
    if (zone === "rows") setRowFields((current) => current.filter((key) => key !== columnKey));
    if (zone === "columns") setColumnFields((current) => current.filter((key) => key !== columnKey));
    if (zone === "values") setValueFields((current) => current.filter((field) => field.columnKey !== columnKey));
  };

  const updateValueAggregation = (
    column: PreviewColumn,
    aggregation: PreviewPivotValueField["aggregation"],
  ) => {
    const nextAggregation = !isNumericDataType(column.dataType) && NUMBER_ONLY_AGGREGATIONS.has(aggregation)
      ? "count"
      : aggregation;
    setValueFields((current) =>
      current.map((field) =>
        field.columnKey === column.key ? { ...field, aggregation: nextAggregation } : field,
      ),
    );
  };

  const handleDragStart = (event: ReactDragEvent<HTMLElement>, columnKey: string) => {
    event.dataTransfer.setData("text/plain", columnKey);
    event.dataTransfer.effectAllowed = "copy";
  };

  const handleDragOver = (event: ReactDragEvent<HTMLElement>, zone: PivotZoneId) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
    setDragOverZone(zone);
  };

  const handleDrop = (event: ReactDragEvent<HTMLElement>, zone: PivotZoneId) => {
    event.preventDefault();
    const columnKey = event.dataTransfer.getData("text/plain");
    const column = columnMap.get(columnKey);
    setDragOverZone(null);
    if (column) addToZone(column, zone);
  };

  return (
    <Modal title="Create Pivot Table (Drag & Drop)" onClose={onClose}>
      <div className="space-y-4 p-4">
        <p className="text-xs text-neutral-500">
          Drag fields from Available Fields to Row Fields, Column Fields, or Values areas.
        </p>
        <div className="grid grid-cols-2 gap-4">
          <div className="rounded-lg border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-700 dark:bg-neutral-900">
            <p className="mb-3 text-[10px] font-bold uppercase text-neutral-500">Available Fields</p>
            <div className="max-h-[300px] space-y-1 overflow-y-auto">
            {pivotColumns.map((column) => (
              <div
                key={column.key}
                draggable
                onDragStart={(event) => handleDragStart(event, column.key)}
                className="flex cursor-grab items-center gap-2 rounded border border-neutral-200 bg-white px-3 py-2 text-xs transition-shadow hover:shadow-sm active:cursor-grabbing dark:border-neutral-700 dark:bg-neutral-800"
              >
                  <GripVertical className="h-4 w-4 shrink-0 text-neutral-400" aria-hidden="true" />
                  <span className="min-w-0 flex-1 truncate font-medium text-neutral-800 dark:text-neutral-100" title={column.displayName}>
                    {column.displayName}
                  </span>
                  <span className={`shrink-0 rounded-full border px-1.5 py-0.5 text-[9px] font-semibold leading-none ${typeBadgeClass(column.dataType)}`}>
                    {typeBadgeLabel(column.dataType)}
                  </span>
              </div>
            ))}
            </div>
            <p className="mt-2 text-[10px] text-neutral-400">Drag fields to the areas on the right →</p>
          </div>

          <div className="space-y-3">
          <PivotZone
            title="Row Fields"
            zone="rows"
            tone="blue"
            columnMap={columnMap}
            selected={validRowFields}
            dragOverZone={dragOverZone}
            onDragOver={handleDragOver}
            onDragLeave={() => setDragOverZone(null)}
            onDrop={handleDrop}
            onRemove={removeFromZone}
          />
          <PivotZone
            title="Column Fields"
            zone="columns"
            tone="emerald"
            columnMap={columnMap}
            selected={validColumnFields}
            dragOverZone={dragOverZone}
            onDragOver={handleDragOver}
            onDragLeave={() => setDragOverZone(null)}
            onDrop={handleDrop}
            onRemove={removeFromZone}
          />
          <PivotZone
            title="Values"
            zone="values"
            tone="amber"
            columnMap={columnMap}
            selected={validValueFields.map((field) => field.columnKey)}
            valueFields={validValueFields}
            dragOverZone={dragOverZone}
            onDragOver={handleDragOver}
            onDragLeave={() => setDragOverZone(null)}
            onDrop={handleDrop}
            onRemove={removeFromZone}
            onAggregationChange={updateValueAggregation}
          />
        </div>
        </div>
      </div>
      <div className="flex justify-end gap-2 border-t border-neutral-200 px-4 py-3 dark:border-neutral-800">
        {submitAttempted && error && (
          <p role="alert" className="mr-auto self-center text-xs text-red-600 dark:text-red-400">{error}</p>
        )}
        <button type="button" onClick={onClose} className="rounded-lg border border-neutral-200 px-3 py-2 text-xs font-semibold dark:border-neutral-800">
          Cancel
        </button>
        <button
          type="button"
          disabled={!canSubmit || submitting}
          onClick={submit}
          className="rounded-lg bg-red-600 px-3 py-2 text-xs font-semibold text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {submitting ? "Creating..." : "Create pivot"}
        </button>
      </div>
    </Modal>
  );
}

function PivotZone({
  title,
  zone,
  tone,
  columnMap,
  selected,
  valueFields,
  dragOverZone,
  onDragOver,
  onDragLeave,
  onDrop,
  onRemove,
  onAggregationChange,
}: {
  title: string;
  zone: PivotZoneId;
  tone: "blue" | "emerald" | "amber";
  columnMap: Map<string, PreviewColumn>;
  selected: string[];
  valueFields?: PreviewPivotValueField[];
  dragOverZone: PivotZoneId | null;
  onDragOver: (event: ReactDragEvent<HTMLElement>, zone: PivotZoneId) => void;
  onDragLeave: () => void;
  onDrop: (event: ReactDragEvent<HTMLElement>, zone: PivotZoneId) => void;
  onRemove: (columnKey: string, zone: PivotZoneId) => void;
  onAggregationChange?: (column: PreviewColumn, aggregation: PreviewPivotValueField["aggregation"]) => void;
}) {
  const toneClass = {
    blue: "bg-blue-100 text-blue-700 dark:bg-blue-950/50 dark:text-blue-300",
    emerald: "bg-green-100 text-green-700 dark:bg-green-950/50 dark:text-green-300",
    amber: "bg-amber-100 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300",
  }[tone];
  const titleClass = {
    blue: "text-blue-600",
    emerald: "text-green-600",
    amber: "text-amber-600",
  }[tone];
  const emptyCopy = zone === "rows"
    ? "Drop fields here for rows"
    : zone === "columns"
      ? "Drop fields here for columns"
      : "Drop fields here for values";

  return (
    <div
      className={`min-h-[100px] rounded-lg border-2 border-dashed p-3 transition-all duration-200 ${
        dragOverZone === zone
          ? "border-red-400 bg-red-50 dark:bg-red-950/20"
          : "border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
      }`}
      onDragOver={(event) => onDragOver(event, zone)}
      onDragLeave={onDragLeave}
      onDrop={(event) => onDrop(event, zone)}
    >
      <div className="mb-2 flex items-center justify-between gap-2">
        <p className={`text-[10px] font-bold uppercase ${titleClass}`}>{title}</p>
        {selected.length > 0 && <span className="text-[10px] text-neutral-400">{selected.length} field(s)</span>}
      </div>
      <div className={zone === "values" ? "flex flex-col gap-2" : "flex flex-wrap gap-1"}>
        {selected.map((columnKey) => {
          const column = columnMap.get(columnKey);
          const valueField = valueFields?.find((field) => field.columnKey === columnKey);
          if (!column) return null;
          const isNumeric = isNumericDataType(column.dataType);
          return (
            <div key={columnKey} className={`flex items-center gap-2 rounded px-2 py-1 text-[10px] ${toneClass}`}>
              <div className="flex min-w-0 items-center gap-2">
                <span className="min-w-0 flex-1 truncate font-medium" title={column.displayName}>{column.displayName}</span>
                <button
                  type="button"
                  onClick={() => onRemove(columnKey, zone)}
                  className="hover:opacity-75"
                  aria-label={`Remove ${column.displayName} from ${title}`}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
              {zone === "values" && valueField && onAggregationChange && (
                <select
                  value={valueField.aggregation}
                  onChange={(event) =>
                    onAggregationChange(column, event.target.value as PreviewPivotValueField["aggregation"])
                  }
                  className="ml-auto rounded border border-amber-200 bg-white px-1 py-0.5 text-[10px] text-neutral-700 outline-none focus:border-red-400 dark:border-amber-900 dark:bg-neutral-800 dark:text-neutral-200"
                  aria-label={`Aggregation for ${column.displayName}`}
                >
                  {AGGREGATIONS.map((aggregation) => (
                    <option
                      key={aggregation}
                      value={aggregation}
                      disabled={!isNumeric && NUMBER_ONLY_AGGREGATIONS.has(aggregation)}
                    >
                      {aggregationLabel(aggregation)}
                      {!isNumeric && NUMBER_ONLY_AGGREGATIONS.has(aggregation) ? " (number only)" : ""}
                    </option>
                  ))}
                </select>
              )}
            </div>
          );
        })}
        {selected.length === 0 && (
          <p className="py-4 text-center text-[10px] text-neutral-400">
            {emptyCopy}
          </p>
        )}
      </div>
    </div>
  );
}
