import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { X, ArrowLeft, Table2, Search, ArrowUpDown, Calculator, RotateCcw, RotateCw, Download, Check, ChevronDown, Grid3x3, Plus, Trash2, Edit3, MoreHorizontal, Type, MousePointer2 } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import DataEditor, { CompactSelection, GridCellKind, type GridColumn, type GridSelection, type Item } from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import type {
  PreviewFilter,
  PreviewSort,
  ApplyTarget,
  PivotConfig,
  CalcColumnConfig,
  CellEditParams,
  ColumnDataType,
  PreviewOperationRequest,
} from "../../types/excelPreview";
import {
  previewOperation,
  previewState,
  previewColumnValues,
  previewApply,
  previewUndo,
  previewRedo,
  previewRefreshInventory,
} from "./services/stitchingApi";
import {
  isReservedProvenanceColumn,
  pruneReservedColumnTargets,
  sanitizePreviewDto,
  sanitizePreviewMap,
} from "../../utils/reservedColumns";
import {
  PREVIEW_PAGE_SIZE,
  preserveResolvedRowIds,
  previewPageOffset,
  previewQueryKey,
  readPreviewRow,
  storePreviewPage,
} from "../../utils/previewPageStore.js";

// --- Types ---

interface InventoryItem {
  table_key: string;
  rows: number;
  cols: number;
  label?: string;
  [key: string]: any;
}

interface ExcelPreviewOverlayProps {
  previews: Record<string, { columns: string[]; rows: any[]; columnTypes?: Record<string, ColumnDataType>; totalRows?: number }>;
  inventory: InventoryItem[];
  onClose: () => void;
  title?: string;
  sessionId: string;
  onApplyToPipeline?: (result: { resetStep: number; statePatch: Record<string, unknown> }) => void;
  guardedApply?: (resetStep: number, action: () => Promise<void>) => void;
  maxStepReached?: number;
  groupSchema?: any[];
  mergeHistory?: any[];
}

interface ToolbarButtonProps {
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
  active?: boolean;
  disabled?: boolean;
  shortcut?: string;
  variant?: "default" | "danger";
}

// --- Constants ---

const PAGE_SIZE = PREVIEW_PAGE_SIZE;
const ROW_ID_COL = "__row_id";

/** Apply preview API payload to local grid state. */
function applyPreviewGridState(
  result: {
    rows?: Record<string, unknown>[];
    columns?: string[];
    columnTypes?: Record<string, ColumnDataType>;
    totalRows?: number;
    offset?: number;
  },
  apply: {
    setRows: React.Dispatch<React.SetStateAction<Record<string, unknown>[]>>;
    setColumns: React.Dispatch<React.SetStateAction<string[]>>;
    setColumnOrder: React.Dispatch<React.SetStateAction<string[]>>;
    setColumnTypes: React.Dispatch<React.SetStateAction<Record<string, ColumnDataType>>>;
    setTotalRows: React.Dispatch<React.SetStateAction<number>>;
    setRowOffset: React.Dispatch<React.SetStateAction<number>>;
  },
  offsetFallback = 0,
) {
  const safe = sanitizePreviewDto(result);
  if (Array.isArray(safe.rows)) {
    apply.setRows(safe.rows);
  }
  if (Array.isArray(safe.columns)) {
    const cols = safe.columns.filter((c) => c !== ROW_ID_COL);
    apply.setColumns(cols);
    apply.setColumnOrder(cols);
  }
  if (safe.columnTypes) {
    apply.setColumnTypes(safe.columnTypes);
  }
  if (result.totalRows !== undefined) {
    apply.setTotalRows(result.totalRows);
  }
  apply.setRowOffset(result.offset ?? offsetFallback);
}

/** Custom header icons for sort direction indicators in glide-data-grid. */
const SORT_HEADER_ICONS = {
  sortAsc: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M8 3L12 9H4L8 3Z" fill="${fgColor}"/>
    </svg>`,
  sortDesc: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M8 13L4 7H12L8 13Z" fill="${fgColor}"/>
    </svg>`,
};

/** Column data-type icons shown in headers when column is not sorted. */
const TYPE_HEADER_ICONS = {
  typeText: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
      <text x="2" y="12" font-size="10" font-weight="600" fill="${fgColor}">Aa</text>
    </svg>`,
  typeNumber: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
      <text x="3" y="12" font-size="11" font-weight="700" fill="${fgColor}">#</text>
    </svg>`,
  typeDate: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="3" width="12" height="11" rx="1" stroke="${fgColor}" stroke-width="1.2" fill="none"/>
      <path d="M2 6H14" stroke="${fgColor}" stroke-width="1"/>
      <path d="M5 1V4M11 1V4" stroke="${fgColor}" stroke-width="1.2" stroke-linecap="round"/>
    </svg>`,
  typeBool: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M3 8L6.5 11.5L13 4.5" stroke="${fgColor}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>`,
};

const HEADER_ICONS = { ...SORT_HEADER_ICONS, ...TYPE_HEADER_ICONS };

/** Funnel icon shown on column headers with an active filter. */
const FILTER_HEADER_ICONS = {
  filterActive: ({ fgColor }: { fgColor: string; bgColor: string }) =>
    `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2 3H14L9 9.5V13L7 14V9.5L2 3Z" fill="${fgColor}" stroke="${fgColor}" stroke-width="0.5"/>
    </svg>`,
};

const ALL_HEADER_ICONS = { ...HEADER_ICONS, ...FILTER_HEADER_ICONS };

const EMPTY_GRID_SELECTION: GridSelection = {
  columns: CompactSelection.empty(),
  rows: CompactSelection.empty(),
};

function typeIconKey(dataType: ColumnDataType | undefined): string {
  switch (dataType) {
    case "INTEGER":
    case "DOUBLE":
      return "typeNumber";
    case "DATE":
      return "typeDate";
    case "BOOLEAN":
      return "typeBool";
    default:
      return "typeText";
  }
}

function isNumericColumnType(dataType: ColumnDataType | undefined): boolean {
  return dataType === "INTEGER" || dataType === "DOUBLE";
}

// --- Helper Components ---

function ToolbarButton({ icon, label, onClick, active, disabled, shortcut, variant }: ToolbarButtonProps) {
  const danger = variant === "danger";
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={`
        inline-flex h-9 items-center justify-center gap-2 rounded-lg px-3 text-xs font-semibold transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500
        ${danger
          ? "bg-red-600 text-white hover:bg-red-700 border border-red-600"
          : active
            ? "bg-red-100 text-red-700 dark:bg-red-950/50 dark:text-red-400"
            : "bg-white dark:bg-neutral-800 text-neutral-700 dark:text-neutral-300 hover:bg-neutral-50 dark:hover:bg-neutral-700"
        }
        ${disabled ? "opacity-50 cursor-not-allowed" : ""}
        ${danger ? "" : "border border-neutral-200 dark:border-neutral-700"}
      `}
      title={shortcut ? `${label} (${shortcut})` : label}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

function shortLabel(tableKey: string): string {
  if (tableKey.startsWith("playground::pivot_")) {
    return `Pivot — ${tableKey.split("playground::pivot_")[1]}`;
  }
  const parts = tableKey.split("::");
  const file = parts[0].split(/[/\\]/).pop() || parts[0];
  const sheet = parts[1] || "";
  if (sheet) return `${file} — ${sheet}`;
  return file;
}

function tabLabel(tableKey: string, inventory: InventoryItem[]): string {
  const meta = inventory.find((inv) => inv.table_key === tableKey);
  if (meta?.label) return meta.label;
  return shortLabel(tableKey);
}

// --- Main Component ---

export default function ExcelPreviewOverlay({
  previews,
  inventory,
  onClose,
  title = "Data Preview",
  sessionId,
  onApplyToPipeline,
  guardedApply,
  maxStepReached = 1,
  groupSchema = [],
  mergeHistory = [],
}: ExcelPreviewOverlayProps) {
  // --- State ---
  const [localPreviews, setLocalPreviews] = useState(() => sanitizePreviewMap(previews));
  const [localInventory, setLocalInventory] = useState(inventory);
  /** Tab list prefers server inventory; falls back to preview keys when inventory is empty. */
  const tableKeys = useMemo(() => {
    const fromInventory = localInventory.map((inv) => inv.table_key);
    if (fromInventory.length > 0) return fromInventory;
    return Object.keys(localPreviews);
  }, [localInventory, localPreviews]);
  const [activeKey, setActiveKey] = useState<string>(tableKeys[0] || "");

  // Grid data state
  const [columns, setColumns] = useState<string[]>([]);
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [columnTypes, setColumnTypes] = useState<Record<string, ColumnDataType>>({});
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [rowOffset, setRowOffset] = useState(0);

  // Operations state
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [filters, setFilters] = useState<PreviewFilter[]>([]);
  const [sort, setSort] = useState<PreviewSort[]>([]);
  const [loading, setLoading] = useState(false);
  const [backgroundLoading, setBackgroundLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

  // Dialog states
  const [filterPopover, setFilterPopover] = useState<{
    column: string;
    x: number;
    y: number;
  } | null>(null);
  const [showPivotDialog, setShowPivotDialog] = useState(false);
  const [showCalcDialog, setShowCalcDialog] = useState(false);
  const [showApplyDialog, setShowApplyDialog] = useState(false);
  const [showColumnMenu, setShowColumnMenu] = useState<{ column: string; x: number; y: number } | null>(null);
  const [showTypePicker, setShowTypePicker] = useState<{ column: string; x: number; y: number } | null>(null);

  // Row selection state (grid indices + stable __row_id captured at selection time)
  const [gridSelection, setGridSelection] = useState<GridSelection>(EMPTY_GRID_SELECTION);
  const [selectedRowIds, setSelectedRowIds] = useState<Map<number, number>>(new Map());
  const [rowSelectionMode, setRowSelectionMode] = useState(false);

  const selectedRowCount = rowSelectionMode ? gridSelection.rows.length : 0;

  // Undo/Redo stacks
  const [undoStack, setUndoStack] = useState<Array<{ op: string; params: any }>>([]);
  const [redoStack, setRedoStack] = useState<Array<{ op: string; params: any }>>([]);

  // Refs
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pageCacheRef = useRef<Map<number, { rows: Record<string, unknown>[] }>>(new Map());
  const inFlightRef = useRef<Map<string, { controller: AbortController; promise: Promise<any>; offset: number; prefetch: boolean }>>(new Map());
  const requestGenerationRef = useRef(0);
  const latestViewRequestRef = useRef(0);
  const focusOffsetRef = useRef(0);
  const foregroundRequestKeyRef = useRef<string | null>(null);
  const selectionReadRef = useRef<{ id: number; controller: AbortController } | null>(null);
  const selectionRequestRef = useRef(0);
  const queryKeyRef = useRef("");
  const snapshotQueryKeyRef = useRef("");
  const [, setCacheVersion] = useState(0);
  const activeMeta = useMemo(() => {
    return activeKey ? localInventory.find((inv) => inv.table_key === activeKey) : undefined;
  }, [activeKey, localInventory]);
  const currentQueryKey = useMemo(() => previewQueryKey({
    tableKey: activeKey,
    search: debouncedSearch,
    filters,
    sort,
  }), [activeKey, debouncedSearch, filters, sort]);

  // --- Effects ---

  useEffect(() => {
    setLocalPreviews(sanitizePreviewMap(previews));
    setLocalInventory(inventory);
  }, [previews, inventory]);

  // Debounce search
  useEffect(() => {
    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    searchTimeoutRef.current = setTimeout(() => {
      setDebouncedSearch(search);
    }, 300);
    return () => {
      if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    };
  }, [search]);

  // Ensure activeKey is set when tabs first become available
  useEffect(() => {
    if (tableKeys.length > 0 && !activeKey) {
      setActiveKey(tableKeys[0]);
    }
  }, [tableKeys, activeKey]);

  // Reset state when table changes
  useEffect(() => {
    if (tableKeys.length > 0 && !tableKeys.includes(activeKey)) {
      setActiveKey(tableKeys[0]);
    }
  }, [tableKeys, activeKey]);

  // Reset operational state when active table changes; bootstrap from snapshot if available
  useEffect(() => {
    if (!activeKey) return;
    selectionRequestRef.current += 1;
    selectionReadRef.current?.controller.abort();
    selectionReadRef.current = null;
    const snapshot = sanitizePreviewDto(localPreviews[activeKey] || {});
    if (snapshot?.columns?.length) {
      const cols = snapshot.columns.filter((c: string) => c !== ROW_ID_COL);
      setColumns(cols);
      setColumnOrder(cols);
      setRows(snapshot.rows || []);
      setTotalRows(snapshot.totalRows ?? snapshot.rows?.length ?? 0);
      pageCacheRef.current = new Map([[0, { rows: snapshot.rows || [] }]]);
      snapshotQueryKeyRef.current = previewQueryKey({ tableKey: activeKey });
      setCacheVersion((version) => version + 1);
    } else {
      setColumns([]);
      setColumnOrder([]);
      setRows([]);
      setTotalRows(0);
      pageCacheRef.current = new Map();
      snapshotQueryKeyRef.current = "";
    }
    setRowOffset(0);
    focusOffsetRef.current = 0;
    foregroundRequestKeyRef.current = null;
    setSearch("");
    setDebouncedSearch("");
    setFilters([]);
    setSort([]);
    setHasUnsavedChanges(false);
    setUndoStack([]);
    setRedoStack([]);
    setError(null);
      setColumnTypes(snapshot.columnTypes || {});
    setGridSelection(EMPTY_GRID_SELECTION);
    setSelectedRowIds(new Map());
    setRowSelectionMode(false);
    setFilterPopover(null);
  }, [activeKey, localPreviews]);

  useEffect(() => () => {
    requestGenerationRef.current += 1;
    selectionRequestRef.current += 1;
    selectionReadRef.current?.controller.abort();
    selectionReadRef.current = null;
    for (const request of inFlightRef.current.values()) request.controller.abort();
    inFlightRef.current.clear();
    focusOffsetRef.current = 0;
    foregroundRequestKeyRef.current = null;
  }, []);

  // Prune dependent state whenever a defensive boundary removes a reserved column.
  useEffect(() => {
    setFilters((prev) => pruneReservedColumnTargets(prev).filter((item) => columns.includes(item.column)));
    setSort((prev) => pruneReservedColumnTargets(prev).filter((item) => columns.includes(item.column)));
    if (filterPopover && !columns.includes(filterPopover.column)) setFilterPopover(null);
    if (showColumnMenu && !columns.includes(showColumnMenu.column)) setShowColumnMenu(null);
    if (showTypePicker && !columns.includes(showTypePicker.column)) setShowTypePicker(null);
  }, [columns, filterPopover, showColumnMenu, showTypePicker]);

  // Clear row selection when view changes (sort/filter/search)
  useEffect(() => {
    setGridSelection(EMPTY_GRID_SELECTION);
    setSelectedRowIds(new Map());
  }, [debouncedSearch, filters, sort]);

  // Fetch data when operations change
  useEffect(() => {
    if (!activeKey) return;
    requestGenerationRef.current += 1;
    selectionRequestRef.current += 1;
    selectionReadRef.current?.controller.abort();
    selectionReadRef.current = null;
    queryKeyRef.current = currentQueryKey;
    for (const request of inFlightRef.current.values()) request.controller.abort();
    inFlightRef.current.clear();
    focusOffsetRef.current = 0;
    foregroundRequestKeyRef.current = null;
    const mayRetainInitialSnapshot = snapshotQueryKeyRef.current === currentQueryKey;
    snapshotQueryKeyRef.current = "";
    if (!mayRetainInitialSnapshot) {
      pageCacheRef.current = new Map();
      setRows([]);
      setCacheVersion((version) => version + 1);
    }
    void fetchPreviewData(0, { force: true });
  }, [currentQueryKey]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.metaKey) {
        if (e.key === "z" && !e.shiftKey) {
          e.preventDefault();
          handleUndo();
        } else if ((e.key === "y") || (e.key === "z" && e.shiftKey)) {
          e.preventDefault();
          handleRedo();
        }
      } else if (e.key === "Escape" && !e.defaultPrevented) {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [undoStack, redoStack, activeKey]);

  // --- Data Fetching ---

  const fetchPreviewData = useCallback(async (
    requestedOffset = 0,
    options: { prefetch?: boolean; force?: boolean } = {},
  ): Promise<any> => {
    if (!activeKey || !sessionId) return null;
    const offset = previewPageOffset(requestedOffset, PAGE_SIZE);
    const viewRequest = options.prefetch ? latestViewRequestRef.current : ++latestViewRequestRef.current;
    const generation = requestGenerationRef.current;
    const requestKey = `${currentQueryKey}:${offset}`;

    if (options.prefetch) {
      if (Math.abs(offset - focusOffsetRef.current) > PAGE_SIZE) return null;
    } else {
      focusOffsetRef.current = offset;
      for (const [key, request] of inFlightRef.current) {
        const staleForeground = key === foregroundRequestKeyRef.current && key !== requestKey;
        const outsideWindow = request.prefetch && Math.abs(request.offset - offset) > PAGE_SIZE;
        if (staleForeground || outsideWindow) {
          request.controller.abort();
          inFlightRef.current.delete(key);
        }
      }
      foregroundRequestKeyRef.current = requestKey;
    }

    const cached = pageCacheRef.current.get(offset);
    if (cached && !options.force) {
      if (!options.prefetch && viewRequest === latestViewRequestRef.current) {
        setRows(cached.rows);
        setRowOffset(offset);
        foregroundRequestKeyRef.current = null;
        const previous = offset - PAGE_SIZE;
        const next = offset + PAGE_SIZE;
        if (previous >= 0) void fetchPreviewData(previous, { prefetch: true });
        if (next < totalRows) void fetchPreviewData(next, { prefetch: true });
      }
      return cached;
    }

    const duplicate = inFlightRef.current.get(requestKey);
    if (duplicate) {
      if (options.prefetch) return duplicate.promise;
      if (pageCacheRef.current.size > 0) setBackgroundLoading(true);
      else setLoading(true);
      setError(null);
      return duplicate.promise.then((result) => {
        if (
          generation !== requestGenerationRef.current
          || queryKeyRef.current !== currentQueryKey
          || viewRequest !== latestViewRequestRef.current
        ) return result;
        const page = pageCacheRef.current.get(offset);
        if (page) {
          setRows(page.rows);
          setRowOffset(offset);
          foregroundRequestKeyRef.current = null;
          const resultTotal = result?.totalRows ?? totalRows;
          const previous = offset - PAGE_SIZE;
          const next = offset + PAGE_SIZE;
          if (previous >= 0) void fetchPreviewData(previous, { prefetch: true });
          if (next < resultTotal) void fetchPreviewData(next, { prefetch: true });
        }
        return result;
      }).finally(() => {
        if (generation === requestGenerationRef.current && viewRequest === latestViewRequestRef.current) {
          setLoading(false);
          setBackgroundLoading(false);
        }
      });
    }

    const controller = new AbortController();
    const hasUsableRows = pageCacheRef.current.size > 0;
    if (!options.prefetch) {
      if (hasUsableRows) setBackgroundLoading(true);
      else setLoading(true);
      setError(null);
    }

    const promise = previewState(sessionId, activeKey, {
      offset,
      limit: PAGE_SIZE,
      search: debouncedSearch || undefined,
      filters: filters.length > 0 ? filters : undefined,
      sort: sort.length > 0 ? sort : undefined,
    }, controller.signal).then((result) => {
      if (generation !== requestGenerationRef.current || queryKeyRef.current !== currentQueryKey) return null;
      if (options.prefetch && Math.abs(offset - focusOffsetRef.current) > PAGE_SIZE) return null;
      const safe = sanitizePreviewDto(result);
      const page = { rows: safe.rows || [] };
      pageCacheRef.current = storePreviewPage(pageCacheRef.current, offset, page, focusOffsetRef.current);
      setCacheVersion((version) => version + 1);
      const cols = safe.columns?.filter((column: string) => column !== ROW_ID_COL) || [];
      if (cols.length > 0) {
        setColumns(cols);
        setColumnOrder(cols);
      }
      setColumnTypes((safe.columnTypes as Record<string, ColumnDataType>) || {});
      setTotalRows(result.totalRows || 0);
      if (!options.prefetch && viewRequest === latestViewRequestRef.current) {
        setRows(page.rows);
        setRowOffset(offset);
        foregroundRequestKeyRef.current = null;
        const previous = offset - PAGE_SIZE;
        const next = offset + PAGE_SIZE;
        if (previous >= 0) void fetchPreviewData(previous, { prefetch: true });
        if (next < (result.totalRows || 0)) void fetchPreviewData(next, { prefetch: true });
      }
      return result;
    }).catch((e: any) => {
      if (e?.name === "AbortError") return null;
      if (!options.prefetch && generation === requestGenerationRef.current && viewRequest === latestViewRequestRef.current) {
        setError(e.message);
        if (pageCacheRef.current.size === 0 && !localPreviews[activeKey]?.columns?.length) {
          setColumns([]);
          setColumnOrder([]);
          setColumnTypes({});
          setTotalRows(0);
        }
      }
      return null;
    }).finally(() => {
      const current = inFlightRef.current.get(requestKey);
      if (current?.promise === promise) inFlightRef.current.delete(requestKey);
      if (foregroundRequestKeyRef.current === requestKey && current?.promise === promise) {
        foregroundRequestKeyRef.current = null;
      }
      if (!options.prefetch && generation === requestGenerationRef.current && viewRequest === latestViewRequestRef.current) {
        setLoading(false);
        setBackgroundLoading(false);
      }
    });

    inFlightRef.current.set(requestKey, { controller, promise, offset, prefetch: !!options.prefetch });
    return promise;
  }, [sessionId, activeKey, currentQueryKey, debouncedSearch, filters, sort, localPreviews, rowOffset, totalRows]);

  // --- Operations ---

  const invalidateInFlightReads = useCallback(() => {
    requestGenerationRef.current += 1;
    latestViewRequestRef.current += 1;
    selectionRequestRef.current += 1;
    selectionReadRef.current?.controller.abort();
    selectionReadRef.current = null;
    for (const request of inFlightRef.current.values()) request.controller.abort();
    inFlightRef.current.clear();
    foregroundRequestKeyRef.current = null;
  }, []);

  const currentView = useCallback(() => ({
    offset: rowOffset,
    limit: PAGE_SIZE,
    search: debouncedSearch || undefined,
    filters: filters.length > 0 ? filters : undefined,
    sort: sort.length > 0 ? sort : undefined,
  }), [rowOffset, debouncedSearch, filters, sort]);

  const consumeMutationView = useCallback(async (result: any) => {
    pageCacheRef.current = new Map();
    setCacheVersion((version) => version + 1);
    if (result?.rows || result?.columns) {
      const safe = sanitizePreviewDto(result);
      const offset = result.offset ?? rowOffset;
      const page = { rows: safe.rows || [] };
      focusOffsetRef.current = offset;
      pageCacheRef.current = storePreviewPage(new Map(), offset, page, offset);
      setRows(page.rows);
      applyPreviewGridState(
        result,
        { setRows, setColumns, setColumnOrder, setColumnTypes, setTotalRows, setRowOffset },
        offset,
      );
      setCacheVersion((version) => version + 1);
      const resultTotal = result.totalRows ?? page.rows.length;
      const previous = offset - PAGE_SIZE;
      const next = offset + PAGE_SIZE;
      if (previous >= 0) void fetchPreviewData(previous, { prefetch: true });
      if (next < resultTotal) void fetchPreviewData(next, { prefetch: true });
      return;
    }
    await fetchPreviewData(rowOffset, { force: true });
  }, [fetchPreviewData, rowOffset]);

  const executeOperation = async (op: PreviewOperationRequest["op"], params: Record<string, unknown>, addToUndo = true) => {
    if (!activeKey || !sessionId) return;
    for (const key of ["column", "oldName", "newName", "name", "field"]) {
      if (isReservedProvenanceColumn(params[key])) {
        setError("Reserved provenance columns cannot be used in preview operations.");
        return;
      }
    }
    if (Array.isArray(params.order)) {
      params = { ...params, order: params.order.filter((column) => !isReservedProvenanceColumn(column)) };
    }
    setLoading(true);
    setError(null);
    try {
      invalidateInFlightReads();
      const result = await previewOperation(sessionId, activeKey, op, params, currentView());
      setHasUnsavedChanges(true);

      if (addToUndo) {
        setUndoStack((prev) => [...prev, { op, params }]);
        setRedoStack([]);
      }

      await consumeMutationView(result);
      return result;
    } catch (e: any) {
      setError(e.message);
      throw e;
    } finally {
      setLoading(false);
    }
  };

  // --- Cell Editing ---

  const handleCellEdit = useCallback(async (rowIndex: number, column: string, value: string) => {
    const pageOffset = previewPageOffset(rowIndex, PAGE_SIZE);
    const pageIndex = rowIndex - pageOffset;
    const row = readPreviewRow(pageCacheRef.current, rowIndex, PAGE_SIZE);
    const rowId = row?.[ROW_ID_COL];
    if (rowId == null) {
      setError("Cannot edit: Row ID not found");
      return;
    }

    const oldValue = row[column];

    // Optimistic update
    const currentPage = pageCacheRef.current.get(pageOffset)?.rows || [];
    const newRows = [...currentPage];
    newRows[pageIndex] = { ...row, [column]: value };
    pageCacheRef.current = storePreviewPage(pageCacheRef.current, pageOffset, { rows: newRows }, rowOffset);
    setCacheVersion((version) => version + 1);
    if (pageOffset === rowOffset) setRows(newRows);

    try {
      await executeOperation("cell_edit", { rowId, column, value }, true);
    } catch {
      const rollbackRows = [...currentPage];
      rollbackRows[pageIndex] = { ...row, [column]: oldValue };
      pageCacheRef.current = storePreviewPage(pageCacheRef.current, pageOffset, { rows: rollbackRows }, rowOffset);
      setCacheVersion((version) => version + 1);
      if (pageOffset === rowOffset) setRows(rollbackRows);
    }
  }, [rows, rowOffset, executeOperation]);

  // --- Column Operations ---

  const handleColumnSort = useCallback((column: string) => {
    const existing = sort.find((s) => s.column === column);
    let newSort: PreviewSort[];
    if (!existing) {
      newSort = [...sort, { column, dir: "asc" }];
    } else if (existing.dir === "asc") {
      newSort = sort.map((s) => s.column === column ? { ...s, dir: "desc" } : s);
    } else {
      newSort = sort.filter((s) => s.column !== column);
    }
    setSort(newSort);
  }, [sort]);

  const handleColumnRename = async (oldName: string, newName: string) => {
    if (!newName || newName === oldName) return;
    await executeOperation("column_rename", { oldName, newName });
    setColumnOrder((prev) => prev.map((c) => c === oldName ? newName : c));
  };

  const handleColumnDelete = async (column: string) => {
    if (!confirm(`Delete column "${column}"? This cannot be undone.`)) return;
    await executeOperation("column_delete", { column });
    setColumnOrder((prev) => prev.filter((c) => c !== column));
  };

  const handleColumnReorder = async (newOrder: string[]) => {
    setColumnOrder(newOrder);
    await executeOperation("column_reorder", { order: newOrder });
  };

  const handleColumnChangeType = async (column: string, newType: ColumnDataType) => {
    await executeOperation("column_change_type", { column, newType });
    setColumnTypes((prev) => ({ ...prev, [column]: newType }));
    setShowColumnMenu(null);
    setShowTypePicker(null);
  };

  // --- Row Operations ---

  const handleGridSelectionChange = useCallback(
    async (sel: GridSelection) => {
      if (!rowSelectionMode) return;
      setGridSelection(sel);
      const requestId = ++selectionRequestRef.current;
      selectionReadRef.current?.controller.abort();
      const controller = new AbortController();
      selectionReadRef.current = { id: requestId, controller };
      const generation = requestGenerationRef.current;
      const selectedIndices = [...sel.rows];
      const { resolved, unresolved } = preserveResolvedRowIds(
        selectedRowIds,
        selectedIndices,
        (rowIndex: number) => readPreviewRow(pageCacheRef.current, rowIndex, PAGE_SIZE)?.[ROW_ID_COL] as number | undefined,
      );
      setSelectedRowIds(new Map(resolved));

      const missingByPage = new Map<number, number[]>();
      for (const rowIndex of unresolved) {
        const page = previewPageOffset(rowIndex, PAGE_SIZE);
        const indices = missingByPage.get(page) || [];
        indices.push(rowIndex);
        missingByPage.set(page, indices);
      }

      try {
        // Selection reads are sequential and page-deduped, so they remain bounded
        // without weakening the viewport's strict previous/current/next window.
        for (const [page, indices] of missingByPage) {
          const result = await previewState(sessionId, activeKey, {
            offset: page,
            limit: PAGE_SIZE,
            search: debouncedSearch || undefined,
            filters: filters.length > 0 ? filters : undefined,
            sort: sort.length > 0 ? sort : undefined,
          }, controller.signal);
          if (
            requestId !== selectionRequestRef.current
            || generation !== requestGenerationRef.current
            || controller.signal.aborted
          ) return;
          const safe = sanitizePreviewDto(result);
          for (const rowIndex of indices) {
            const rowId = safe.rows?.[rowIndex - page]?.[ROW_ID_COL];
            if (rowId != null) resolved.set(rowIndex, rowId as number);
          }
          setSelectedRowIds(new Map(resolved));
        }
        if (resolved.size !== selectedIndices.length) {
          setError(`Could not resolve ${selectedIndices.length - resolved.size} selected row ID(s). Reselect those rows before deleting.`);
        }
      } catch (e: any) {
        if (e?.name !== "AbortError" && requestId === selectionRequestRef.current) {
          setError("Could not resolve all selected row IDs. Check the connection and reselect before deleting.");
        }
      } finally {
        if (selectionReadRef.current?.id === requestId) selectionReadRef.current = null;
      }
    },
    [rowSelectionMode, sessionId, activeKey, selectedRowIds, debouncedSearch, filters, sort],
  );

  const handleDeleteSelectedRows = async () => {
    if (selectedRowCount === 0) return;
    if (!confirm(`Delete ${selectedRowCount} selected row(s)? This cannot be undone.`)) return;

    const rowIds = Array.from(selectedRowIds.values());
    if (selectedRowIds.size !== selectedRowCount || rowIds.length !== selectedRowCount) {
      setError(`Cannot delete: resolved ${rowIds.length} of ${selectedRowCount} selected row IDs. Reselect unresolved rows and try again.`);
      return;
    }

    setLoading(true);
    try {
      invalidateInFlightReads();
      const result = await previewOperation(sessionId, activeKey, "rows_delete", { rowIds }, currentView());
      setGridSelection(EMPTY_GRID_SELECTION);
      setSelectedRowIds(new Map());
      setHasUnsavedChanges(true);
      setUndoStack((prev) => [...prev, { op: "rows_delete", params: { rowIds } }]);
      setRedoStack([]);
      await consumeMutationView(result);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const clearRowSelection = () => {
    selectionRequestRef.current += 1;
    selectionReadRef.current?.controller.abort();
    selectionReadRef.current = null;
    setGridSelection(EMPTY_GRID_SELECTION);
    setSelectedRowIds(new Map());
    setRowSelectionMode(false);
  };

  // --- Undo/Redo ---

  const handleUndo = async () => {
    if (undoStack.length === 0 || !activeKey) return;
    const lastOp = undoStack[undoStack.length - 1];
    setLoading(true);
    try {
      invalidateInFlightReads();
      const result = await previewUndo(sessionId, activeKey, currentView());
      setUndoStack((prev) => prev.slice(0, -1));
      setRedoStack((prev) => [...prev, lastOp]);
      await consumeMutationView(result);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRedo = async () => {
    if (redoStack.length === 0 || !activeKey) return;
    const nextOp = redoStack[redoStack.length - 1];
    setLoading(true);
    try {
      invalidateInFlightReads();
      const result = await previewRedo(sessionId, activeKey, currentView());
      setRedoStack((prev) => prev.slice(0, -1));
      setUndoStack((prev) => [...prev, nextOp]);
      await consumeMutationView(result);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  // --- Calculated Column ---

  const handleAddCalcColumn = async (config: CalcColumnConfig) => {
    await executeOperation("calculated_column", config as unknown as Record<string, unknown>);
    setShowCalcDialog(false);
  };

  // --- Filter ---

  const handleApplyColumnFilter = (column: string, filter: PreviewFilter | null) => {
    if (isReservedProvenanceColumn(column)) {
      setFilterPopover(null);
      return;
    }
    setFilters((prev) => {
      const rest = prev.filter((f) => f.column !== column);
      if (!filter) return rest;
      return [...rest, filter];
    });
    setFilterPopover(null);
  };

  const handleRemoveFilter = (column: string) => {
    setFilters((prev) => prev.filter((f) => f.column !== column));
  };

  const handleClearFilters = () => {
    setFilters([]);
    setSearch("");
  };

  const getFilterForColumn = (column: string): PreviewFilter | undefined =>
    filters.find((f) => f.column === column);

  // --- Pivot ---

  const handlePivot = async (config: PivotConfig) => {
    if (!activeKey || !sessionId) return;
    const safeConfig: PivotConfig = {
      rowFields: config.rowFields.filter((field) => !isReservedProvenanceColumn(field)),
      columnFields: config.columnFields.filter((field) => !isReservedProvenanceColumn(field)),
      valueFields: config.valueFields.filter((item) => !isReservedProvenanceColumn(item.field)),
    };
    setLoading(true);
    setError(null);
    try {
      invalidateInFlightReads();
      const result = await previewOperation(sessionId, activeKey, "pivot", safeConfig as unknown as Record<string, unknown>, currentView());
      setShowPivotDialog(false);

      if (result.newTableKey) {
        const refresh = await previewRefreshInventory(sessionId);
        if (refresh.previews) setLocalPreviews(sanitizePreviewMap(refresh.previews));
        if (refresh.inventory) setLocalInventory(refresh.inventory);
        const newKey = result.newTableKey as string;
        setFilters([]);
        setSort([]);
        setSearch("");
        setDebouncedSearch("");
        setActiveKey(newKey);
        setHasUnsavedChanges(true);
        return;
      }

      await consumeMutationView(result);
      setHasUnsavedChanges(true);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  // --- Apply to Pipeline ---

  const handleApply = async (target: ApplyTarget) => {
    if (!activeKey) return;
    const resetStep = target.kind === "raw" ? 2 : target.kind === "group" ? 5 : 6;

    const doApply = async () => {
      setLoading(true);
      try {
        invalidateInFlightReads();
        const result = await previewApply(sessionId, activeKey, {
          kind: target.kind,
          id: target.id,
          mode: target.mode,
        });
        setHasUnsavedChanges(false);
        onApplyToPipeline?.(result);
        setShowApplyDialog(false);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };

    if (guardedApply && maxStepReached > resetStep) {
      guardedApply(resetStep, doApply);
    } else {
      await doApply();
    }
  };

  // --- Grid Configuration ---

  const gridColumns: GridColumn[] = useMemo(() => {
    return columnOrder.map((c) => {
      const sortEntry = sort.find((s) => s.column === c);
      const hasFilter = filters.some((f) => f.column === c);
      let icon: string | undefined;
      if (hasFilter) {
        icon = "filterActive";
      } else if (sortEntry) {
        icon = sortEntry.dir === "asc" ? "sortAsc" : "sortDesc";
      } else {
        icon = typeIconKey(columnTypes[c]);
      }
      return {
        title: c,
        id: c,
        width: 140,
        hasMenu: true,
        icon,
      };
    });
  }, [columnOrder, sort, columnTypes, filters]);

  const getCellContent = useCallback(
    ([col, row]: Item) => {
      const colName = columnOrder[col];
      const dataRow = readPreviewRow(pageCacheRef.current, row, PAGE_SIZE);
      const val = dataRow?.[colName];
      return {
        kind: GridCellKind.Text,
        data: val != null ? String(val) : "",
        displayData: val != null ? String(val) : "",
        allowOverlay: true,
        readonly: false,
      };
    },
    [columnOrder],
  );

  const onCellEdited = useCallback(
    ([col, row]: Item, newValue: { data?: string }) => {
      const colName = columnOrder[col];
      handleCellEdit(row, colName, newValue.data ?? "");
    },
    [columnOrder, handleCellEdit],
  );

  const onHeaderClicked = useCallback(
    (colIndex: number, e?: { shiftKey?: boolean; ctrlKey?: boolean; metaKey?: boolean }) => {
      const colName = columnOrder[colIndex];
      const sortEntry = sort.find((s) => s.column === colName);
      // Shift+click opens type picker; normal click cycles sort
      if (e?.shiftKey && !sortEntry) {
        const gridEl = document.querySelector("[data-testid='preview-grid']");
        const rect = gridEl?.getBoundingClientRect();
        const x = rect ? rect.left + 80 + colIndex * 140 : 200;
        const y = rect ? rect.top + 40 : 120;
        setShowTypePicker({ column: colName, x, y });
        return;
      }
      handleColumnSort(colName);
    },
    [columnOrder, handleColumnSort, sort],
  );

  const onHeaderContextMenu = useCallback(
    (colIndex: number, e: any) => {
      e.preventDefault();
      const colName = columnOrder[colIndex];
      setShowColumnMenu({ column: colName, x: e.clientX, y: e.clientY });
    },
    [columnOrder],
  );

  const onHeaderMenuClick = useCallback(
    (colIndex: number, bounds: { x: number; y: number; height: number }) => {
      const colName = columnOrder[colIndex];
      setFilterPopover({
        column: colName,
        x: bounds.x,
        y: bounds.y + bounds.height,
      });
    },
    [columnOrder],
  );

  // --- Render Helpers ---

  const getApplyOptions = (): Array<{ kind: ApplyTarget["kind"]; id: string; label: string; allowAdd?: boolean }> => {
    const opts: Array<{ kind: ApplyTarget["kind"]; id: string; label: string; allowAdd?: boolean }> = [];
    for (const inv of localInventory) {
      if (inv.table_key) {
        opts.push({ kind: "raw", id: inv.table_key, label: `Raw: ${shortLabel(inv.table_key)}`, allowAdd: true });
      }
    }
    for (const g of groupSchema) {
      if (g.group_id) {
        opts.push({ kind: "group", id: g.group_id, label: `Group: ${g.group_name || g.group_id}`, allowAdd: true });
      }
    }
    return opts;
  };

  // --- Render ---

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        className="fixed inset-0 z-[9999] flex flex-col bg-white dark:bg-neutral-950"
      >
        {/* Identity header and responsive action row */}
        <div className="flex shrink-0 flex-col border-b border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-950">
          <header className="flex h-16 min-w-0 items-center gap-3 px-4">
            <button
              type="button"
              onClick={onClose}
              className="flex items-center gap-1.5 text-sm font-bold text-neutral-500 hover:text-red-600 dark:hover:text-red-400 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back
            </button>
            <div className="h-5 w-px bg-neutral-200 dark:bg-neutral-700" />
            <div className="flex min-w-0 items-center gap-2">
              <Table2 className="w-4 h-4 text-red-500" />
              <h2 className="truncate text-sm font-bold text-neutral-900 dark:text-white">{title}</h2>
            </div>
            {activeMeta && (
              <div className="flex items-center gap-2 ml-2">
                <span className="text-[10px] font-bold text-neutral-500 bg-neutral-100 dark:bg-neutral-800 px-2 py-0.5 rounded-full">
                  {(error ? activeMeta.rows : totalRows).toLocaleString()} rows
                </span>
                <span className="text-[10px] font-bold text-neutral-500 bg-neutral-100 dark:bg-neutral-800 px-2 py-0.5 rounded-full">
                  {error ? activeMeta.cols : columns.length} cols
                </span>
              </div>
            )}
            {hasUnsavedChanges && (
              <span className="text-[10px] font-bold text-amber-600 bg-amber-50 dark:bg-amber-950/30 px-2 py-0.5 rounded-full">
                Unsaved changes
              </span>
            )}
            <button type="button" onClick={onClose} className="ml-auto inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-neutral-200 text-neutral-500 hover:text-red-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500 dark:border-neutral-700" aria-label="Close preview" title="Close preview">
              <X className="h-4 w-4" aria-hidden="true" />
            </button>
          </header>

          <div className="flex flex-wrap items-center gap-x-3 gap-y-2 border-t border-neutral-200 bg-neutral-50/70 px-4 py-3 dark:border-neutral-800 dark:bg-neutral-900/60">
            {/* Search */}
            <div className="relative min-w-0 flex-[1_1_16rem]">
              <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-neutral-400" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search..."
                className="h-9 w-full min-w-0 rounded-lg border border-neutral-200 bg-white pl-8 pr-3 text-xs text-neutral-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500 dark:border-neutral-700 dark:bg-neutral-800 dark:text-white"
              />
            </div>

            <div className="h-5 w-px bg-neutral-200 dark:bg-neutral-700 mx-1" />

            {/* Toolbar Buttons */}
            <ToolbarButton
              icon={<ArrowUpDown className="w-3.5 h-3.5" />}
              label={sort.length > 0 ? `Clear Sort (${sort.length})` : "Sort"}
              onClick={() => sort.length > 0 && setSort([])}
              active={sort.length > 0}
            />
            <ToolbarButton
              icon={<Calculator className="w-3.5 h-3.5" />}
              label="Calc Column"
              onClick={() => setShowCalcDialog(true)}
            />
            <ToolbarButton
              icon={<Grid3x3 className="w-3.5 h-3.5" />}
              label="Pivot"
              onClick={() => setShowPivotDialog(true)}
            />

            <div className="h-5 w-px bg-neutral-200 dark:bg-neutral-700 mx-1" />

            {/* Row Selection Controls */}
            {rowSelectionMode && selectedRowCount > 0 ? (
              <ToolbarButton
                icon={<Trash2 className="w-3.5 h-3.5" />}
                label={`Delete ${selectedRowCount} Row${selectedRowCount === 1 ? "" : "s"}`}
                onClick={handleDeleteSelectedRows}
                variant="danger"
              />
            ) : (
              <ToolbarButton
                icon={<MousePointer2 className="w-3.5 h-3.5" />}
                label={rowSelectionMode ? "Exit Select" : "Select Rows"}
                onClick={() => {
                  if (rowSelectionMode) {
                    clearRowSelection();
                  } else {
                    setRowSelectionMode(true);
                  }
                }}
                active={rowSelectionMode}
              />
            )}

            <div className="h-5 w-px bg-neutral-200 dark:bg-neutral-700 mx-1" />

            <ToolbarButton
              icon={<RotateCcw className="w-3.5 h-3.5" />}
              label="Undo"
              onClick={handleUndo}
              disabled={undoStack.length === 0}
              shortcut="Ctrl+Z"
            />
            <ToolbarButton
              icon={<RotateCw className="w-3.5 h-3.5" />}
              label="Redo"
              onClick={handleRedo}
              disabled={redoStack.length === 0}
              shortcut="Ctrl+Y"
            />

            <div className="h-5 w-px bg-neutral-200 dark:border-neutral-700 mx-1" />

            <ToolbarButton
              icon={<Check className="w-3.5 h-3.5" />}
              label="Apply"
              onClick={() => setShowApplyDialog(true)}
              active={hasUnsavedChanges}
            />
            <ToolbarButton
              icon={<Download className="w-3.5 h-3.5" />}
              label="Export"
              onClick={() => {}}
            />

          </div>
        </div>

        {/* Tabs */}
        {tableKeys.length > 0 && (
          <div className="shrink-0 border-b border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-900">
            <div className="flex overflow-x-auto scrollbar-thin px-2 py-1 gap-0.5">
              {tableKeys.map((key) => {
                const isActive = key === activeKey;
                return (
                  <button
                    key={key}
                    type="button"
                    onClick={() => setActiveKey(key)}
                    className={`
                      flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-lg whitespace-nowrap transition-colors
                      ${isActive
                        ? "bg-white dark:bg-neutral-950 text-red-600 dark:text-red-400 border border-neutral-200 dark:border-neutral-700 shadow-sm"
                        : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400 border border-transparent hover:bg-neutral-200 dark:hover:bg-neutral-700 hover:text-neutral-700 dark:hover:text-neutral-300"
                      }
                    `}
                  >
                    <Table2 className="w-3 h-3" />
                    {tabLabel(key, localInventory)}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Active Filters Display */}
        {(filters.length > 0 || search) && (
          <div className="shrink-0 px-4 py-2 border-b border-neutral-200 dark:border-neutral-700 bg-neutral-50/50 dark:bg-neutral-900/50 flex items-center gap-2 flex-wrap">
            <span className="text-[10px] font-semibold text-neutral-500">Active:</span>
            {search && (
              <span className="inline-flex items-center gap-1 px-2 py-0.5 text-[10px] bg-violet-100 text-violet-700 dark:bg-violet-950/50 dark:text-violet-400 rounded-full">
                Search: "{search}"
                <button onClick={() => setSearch("")} className="hover:text-violet-900">
                  <X className="w-3 h-3" />
                </button>
              </span>
            )}
            {filters.map((f) => (
              <span key={f.column} className="inline-flex items-center gap-1 px-2 py-0.5 text-[10px] bg-blue-100 text-blue-700 dark:bg-blue-950/50 dark:text-blue-400 rounded-full">
                {formatFilterChip(f)}
                <button onClick={() => handleRemoveFilter(f.column)} className="hover:text-blue-900">
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
            {(filters.length > 0 || search) && (
              <button
                onClick={handleClearFilters}
                className="text-[10px] text-neutral-500 hover:text-red-600 underline"
              >
                Clear all
              </button>
            )}
          </div>
        )}

        {/* Error Banner */}
        {error && (
          <div className="shrink-0 px-4 py-2 text-xs text-red-600 bg-red-50 dark:bg-red-950/30 border-b border-red-200 dark:border-red-800">
            {error}
          </div>
        )}

        {/* Grid Area */}
        <div className="flex-1 min-h-0 p-4 relative bg-neutral-50 dark:bg-neutral-900">
          {loading && pageCacheRef.current.size === 0 && (
            <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/60 dark:bg-neutral-950/60">
              <div className="flex items-center gap-2 text-sm text-neutral-500">
                <div className="w-5 h-5 border-2 border-neutral-300 border-t-red-500 rounded-full animate-spin" />
                Loading...
              </div>
            </div>
          )}

          {activeKey && columns.length > 0 ? (
            <div
              className="h-full w-full rounded-lg overflow-hidden border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-950"
              data-testid="preview-grid"
            >
              <DataEditor
                columns={gridColumns}
                rows={totalRows}
                headerIcons={ALL_HEADER_ICONS}
                getCellContent={getCellContent}
                onCellEdited={onCellEdited}
                onHeaderClicked={(colIndex, event) => onHeaderClicked(colIndex, event)}
                onHeaderMenuClick={onHeaderMenuClick}
                onHeaderContextMenu={onHeaderContextMenu}
                onVisibleRegionChanged={(region) => {
                  const page = Math.floor(region.y / PAGE_SIZE) * PAGE_SIZE;
                  if (page !== rowOffset && page < totalRows) {
                    void fetchPreviewData(page);
                  }
                }}
                rowMarkers={rowSelectionMode ? "checkbox" : "number"}
                rowMarkerStartIndex={rowOffset}
                rowSelect="multi"
                columnSelect="none"
                gridSelection={rowSelectionMode ? gridSelection : EMPTY_GRID_SELECTION}
                onGridSelectionChange={(sel) => {
                  if (rowSelectionMode) void handleGridSelectionChange(sel);
                }}
                smoothScrollX
                smoothScrollY
                height="100%"
                width="100%"
                getCellsForSelection={true}
                theme={{
                  accentColor: "#dc2626",
                  accentFg: "#ffffff",
                  accentLight: "rgba(220,38,38,0.1)",
                  textDark: "#171717",
                  textMedium: "#525252",
                  textLight: "#a3a3a3",
                  textBubble: "#ffffff",
                  bgIconHeader: "#f5f5f5",
                  fgIconHeader: "#737373",
                  textHeader: "#525252",
                  textHeaderSelected: "#ffffff",
                  bgCell: "#ffffff",
                  bgCellMedium: "#fafafa",
                  bgHeader: "#f5f5f5",
                  bgHeaderHasFocus: "#fef2f2",
                  bgHeaderHovered: "#f5f5f5",
                  bgBubble: "#ffffff",
                  bgBubbleSelected: "#fef2f2",
                  bgSearchResult: "#fef9c3",
                  borderColor: "#e5e5e5",
                  drilldownBorder: "#e5e5e5",
                  linkColor: "#dc2626",
                  headerFontStyle: "600 11px",
                  baseFontStyle: "12px",
                  fontFamily: "ui-sans-serif, system-ui, sans-serif",
                }}
              />
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-sm text-neutral-400">
              {tableKeys.length === 0
                ? "No tables loaded"
                : error
                  ? null
                  : "No data in this table"}
            </div>
          )}
        </div>

        {/* Status Bar */}
        <div className="shrink-0 px-4 py-2 border-t border-neutral-200 dark:border-neutral-700 bg-neutral-50 dark:bg-neutral-900 text-[10px] text-neutral-500 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span>
              Showing rows {rowOffset + 1}–{Math.min(rowOffset + rows.length, totalRows)} of {totalRows.toLocaleString()}
            </span>
            {sort.length > 0 && (
              <span className="text-neutral-400 flex items-center gap-1">
                Sorted by: {sort.map((s) => `${s.column} ${s.dir.toUpperCase()}`).join(", ")}
                <button
                  type="button"
                  onClick={() => setSort([])}
                  className="text-neutral-500 hover:text-red-600 underline ml-1"
                >
                  Clear sort
                </button>
              </span>
            )}
            <span className="text-neutral-400 hidden sm:inline">
              Click header arrow to filter · Shift+click header to change type
            </span>
          </div>
          <div className="flex items-center gap-2">
            {backgroundLoading && (
              <span className="text-neutral-400" role="status">Loading rows in background...</span>
            )}
            <span className={undoStack.length > 0 ? "text-amber-600" : ""}>
              {undoStack.length} changes to undo
            </span>
          </div>
        </div>

        {/* Dialogs */}
        {filterPopover && activeKey && sessionId && (
          <ColumnFilterPopover
            column={filterPopover.column}
            x={filterPopover.x}
            y={filterPopover.y}
            sessionId={sessionId}
            tableKey={activeKey}
            filters={filters.filter((f) => f.column !== filterPopover.column)}
            existingFilter={getFilterForColumn(filterPopover.column)}
            onClose={() => setFilterPopover(null)}
            onApply={(filter) => handleApplyColumnFilter(filterPopover.column, filter)}
          />
        )}

        {showCalcDialog && (
          <CalcColumnDialog
            columns={columns}
            onClose={() => setShowCalcDialog(false)}
            onApply={handleAddCalcColumn}
          />
        )}

        {showPivotDialog && (
          <PivotDialog
            columns={columns}
            columnTypes={columnTypes}
            sampleRows={rows}
            onClose={() => setShowPivotDialog(false)}
            onApply={handlePivot}
          />
        )}

        {showApplyDialog && (
          <ApplyDialog
            options={getApplyOptions()}
            onClose={() => setShowApplyDialog(false)}
            onApply={handleApply}
          />
        )}

        {/* Column type picker (Shift+click header) */}
        {showTypePicker && (
          <ColumnTypePicker
            column={showTypePicker.column}
            currentType={columnTypes[showTypePicker.column] || "TEXT"}
            x={showTypePicker.x}
            y={showTypePicker.y}
            onClose={() => setShowTypePicker(null)}
            onChangeType={handleColumnChangeType}
          />
        )}

        {/* Column Context Menu */}
        {showColumnMenu && (
          <ColumnContextMenu
            column={showColumnMenu.column}
            x={showColumnMenu.x}
            y={showColumnMenu.y}
            currentType={columnTypes[showColumnMenu.column] || "TEXT"}
            canDelete={columns.length > 1}
            onClose={() => setShowColumnMenu(null)}
            onDelete={handleColumnDelete}
            onChangeType={handleColumnChangeType}
          />
        )}
      </motion.div>
    </AnimatePresence>
  );
}

// --- Dialog Components ---

function formatFilterChip(f: PreviewFilter): string {
  if (f.op === "in") {
    const parts: string[] = [];
    if (f.values?.length) parts.push(`${f.values.length} value${f.values.length === 1 ? "" : "s"}`);
    if (f.includeBlanks) parts.push("(Blanks)");
    return `${f.column}: ${parts.join(", ") || "filtered"}`;
  }
  return `${f.column} ${f.op} ${f.value || ""}`;
}

function ColumnFilterPopover({
  column,
  x,
  y,
  sessionId,
  tableKey,
  filters,
  existingFilter,
  onClose,
  onApply,
}: {
  column: string;
  x: number;
  y: number;
  sessionId: string;
  tableKey: string;
  filters: PreviewFilter[];
  existingFilter?: PreviewFilter;
  onClose: () => void;
  onApply: (filter: PreviewFilter | null) => void;
}) {
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [values, setValues] = useState<string[]>([]);
  const [hasBlanks, setHasBlanks] = useState(false);
  const [totalDistinct, setTotalDistinct] = useState(0);
  const [selectedValues, setSelectedValues] = useState<Set<string>>(new Set());
  const [blanksChecked, setBlanksChecked] = useState(true);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [initialized, setInitialized] = useState(false);
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const popoverRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    searchTimeoutRef.current = setTimeout(() => setDebouncedSearch(search), 250);
    return () => {
      if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    };
  }, [search]);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setLoadError(null);
    previewColumnValues(sessionId, tableKey, column, {
      search: debouncedSearch || undefined,
      filters: filters.length > 0 ? filters : undefined,
      limit: 500,
    }, controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        setValues(result.values);
        setHasBlanks(result.hasBlanks);
        setTotalDistinct(result.totalDistinct);
        setLoadError(null);
      })
      .catch((err) => {
        if (err?.name !== "AbortError" && !controller.signal.aborted) {
          setValues([]);
          setHasBlanks(false);
          setTotalDistinct(0);
          setLoadError(err?.message || "Failed to load filter values");
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [sessionId, tableKey, column, debouncedSearch, filters]);

  // Initialize checkbox state once when values first load (no search)
  useEffect(() => {
    if (initialized || loading || debouncedSearch || loadError) return;
    if (existingFilter?.op === "in") {
      setSelectedValues(new Set(existingFilter.values || []));
      setBlanksChecked(existingFilter.includeBlanks ?? false);
    } else {
      setSelectedValues(new Set(values));
      setBlanksChecked(hasBlanks);
    }
    setInitialized(true);
  }, [initialized, loading, debouncedSearch, loadError, values, hasBlanks, existingFilter]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  const toggleValue = (val: string) => {
    setSelectedValues((prev) => {
      const next = new Set(prev);
      if (next.has(val)) next.delete(val);
      else next.add(val);
      return next;
    });
  };

  const selectAll = () => {
    if (loading || loadError) return;
    setSelectedValues(new Set(values));
    if (hasBlanks) setBlanksChecked(true);
  };

  const clearAll = () => {
    setSelectedValues(new Set());
    setBlanksChecked(false);
  };

  const handleOk = () => {
    if (loading || loadError) return;
    const noneSelected = selectedValues.size === 0 && !blanksChecked;
    if (noneSelected) {
      onApply({
        column,
        op: "in",
        values: [],
        includeBlanks: false,
      });
      return;
    }
    const allSelected =
      !debouncedSearch &&
      totalDistinct <= values.length &&
      values.every((v) => selectedValues.has(v)) &&
      selectedValues.size === totalDistinct &&
      (!hasBlanks || blanksChecked);
    if (allSelected) {
      onApply(null);
      return;
    }
    onApply({
      column,
      op: "in",
      values: Array.from(selectedValues),
      includeBlanks: blanksChecked,
    });
  };

  const style: React.CSSProperties = {
    position: "fixed",
    left: Math.min(x, window.innerWidth - 280),
    top: Math.min(y + 4, window.innerHeight - 400),
    zIndex: 10000,
  };

  return (
    <div
      ref={popoverRef}
      style={style}
      className="w-64 max-h-96 flex flex-col bg-white dark:bg-neutral-900 border border-neutral-200 dark:border-neutral-700 rounded-lg shadow-xl"
    >
      <div className="px-3 py-2 border-b border-neutral-200 dark:border-neutral-700">
        <p className="text-xs font-bold text-neutral-800 dark:text-neutral-200 truncate">{column}</p>
        <div className="relative mt-2">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-neutral-400" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search values..."
            className="w-full pl-7 pr-2 py-1.5 text-xs rounded border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800"
            autoFocus
          />
        </div>
      </div>
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-neutral-100 dark:border-neutral-800">
        <button
          type="button"
          onClick={selectAll}
          disabled={loading || !!loadError}
          className="text-[10px] text-blue-600 hover:underline disabled:text-neutral-300 disabled:no-underline disabled:cursor-not-allowed"
        >
          Select All
        </button>
        <button type="button" onClick={clearAll} className="text-[10px] text-neutral-500 hover:underline">Clear</button>
      </div>
      <div className="flex-1 overflow-y-auto px-2 py-1 min-h-[120px] max-h-52">
        {loading ? (
          <p className="text-xs text-neutral-400 text-center py-4">Loading...</p>
        ) : loadError ? (
          <p className="text-xs text-red-600 dark:text-red-400 text-center py-4 px-2 break-words">
            Could not load values: {loadError}
          </p>
        ) : (
          <>
            {hasBlanks && !debouncedSearch && (
              <label className="flex items-center gap-2 px-1 py-1 text-xs hover:bg-neutral-50 dark:hover:bg-neutral-800 rounded cursor-pointer">
                <input
                  type="checkbox"
                  checked={blanksChecked}
                  onChange={(e) => setBlanksChecked(e.target.checked)}
                  className="rounded"
                />
                <span className="text-neutral-500 italic">(Blanks)</span>
              </label>
            )}
            {values.map((val) => (
              <label
                key={val}
                className="flex items-center gap-2 px-1 py-1 text-xs hover:bg-neutral-50 dark:hover:bg-neutral-800 rounded cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={selectedValues.has(val)}
                  onChange={() => toggleValue(val)}
                  className="rounded"
                />
                <span className="truncate" title={val}>{val}</span>
              </label>
            ))}
            {values.length === 0 && !hasBlanks && (
              <p className="text-xs text-neutral-400 text-center py-4">No values found</p>
            )}
          </>
        )}
      </div>
      <div className="flex justify-end gap-2 px-3 py-2 border-t border-neutral-200 dark:border-neutral-700">
        <button type="button" onClick={onClose} className="px-3 py-1 text-xs text-neutral-600 hover:bg-neutral-100 rounded">Cancel</button>
        <button
          type="button"
          onClick={handleOk}
          disabled={loading || !!loadError}
          className="px-3 py-1 text-xs text-white bg-red-600 hover:bg-red-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
        >
          OK
        </button>
      </div>
    </div>
  );
}

function CalcColumnDialog({ columns, onClose, onApply }: { columns: string[]; onClose: () => void; onApply: (c: CalcColumnConfig) => Promise<void> }) {
  const [name, setName] = useState("");
  const [expression, setExpression] = useState("");
  const [dataType, setDataType] = useState<CalcColumnConfig["dataType"]>("TEXT");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const canSubmit = name.trim().length > 0 && expression.trim().length > 0 && !submitting;

  const submit = async () => {
    if (!canSubmit) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      await onApply({ name: name.trim(), expression: expression.trim(), dataType });
    } catch (error: any) {
      setSubmitError(error?.message || "Could not add the calculated column.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog title="Add Calculated Column" onClose={onClose} wide>
      <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_220px]">
        <div className="space-y-3">
          <label className="block text-xs font-semibold text-neutral-600 dark:text-neutral-400">
            New column name
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="New Column"
              className="mt-1 h-9 w-full rounded-lg border border-neutral-200 bg-white px-3 text-sm outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-700 dark:bg-neutral-950"
          />
          </label>
          <label className="block text-xs font-semibold text-neutral-600 dark:text-neutral-400">
            Expression
            <textarea
            value={expression}
              onChange={(event) => setExpression(event.target.value)}
              rows={7}
            placeholder="e.g., UPPER([Name]) or [Price] * [Quantity]"
              className="mt-1 w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 font-mono text-xs outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100 dark:border-neutral-700 dark:bg-neutral-950"
            />
          </label>
          <label className="block text-xs font-semibold text-neutral-600 dark:text-neutral-400">
            Result type
          <select
            value={dataType}
              onChange={(e) => setDataType(e.target.value as CalcColumnConfig["dataType"])}
              className="mt-1 h-9 w-full rounded-lg border border-neutral-200 bg-white px-3 text-sm outline-none focus:border-red-400 dark:border-neutral-700 dark:bg-neutral-950"
          >
            <option value="TEXT">Text</option>
            <option value="INTEGER">Integer</option>
            <option value="DOUBLE">Decimal</option>
            <option value="DATE">Date</option>
            <option value="BOOLEAN">Boolean</option>
          </select>
          </label>
          {submitError && <p role="alert" className="text-xs text-red-600 dark:text-red-400">{submitError}</p>}
        </div>
        <div className="rounded-lg border border-neutral-200 p-3 dark:border-neutral-700">
          <p className="text-xs font-semibold text-neutral-500">Columns</p>
          <div className="mt-2 max-h-64 space-y-1 overflow-y-auto">
            {columns.map((column) => (
              <button
                type="button"
                key={column}
                onClick={() => setExpression((current) => `${current}${current ? " " : ""}[${column}]`)}
                className="w-full truncate rounded px-2 py-1.5 text-left text-xs hover:bg-neutral-100 dark:hover:bg-neutral-800"
                title={`Insert [${column}]`}
              >
                {column}
              </button>
            ))}
          </div>
        </div>
      </div>
        <div className="-mx-4 -mb-4 mt-4 flex justify-end gap-2 border-t border-neutral-200 px-4 py-3 dark:border-neutral-700">
          <button type="button" onClick={onClose} disabled={submitting} className="rounded-lg border border-neutral-200 px-3 py-2 text-xs font-semibold dark:border-neutral-700">Cancel</button>
          <button
            type="button"
            onClick={submit}
            disabled={!canSubmit}
            className="rounded-lg bg-red-600 px-3 py-2 text-xs font-semibold text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {submitting ? "Adding..." : "Add Column"}
          </button>
        </div>
    </Dialog>
  );
}

function looksNumericColumn(column: string, sampleRows: Record<string, unknown>[]): boolean {
  const samples = sampleRows
    .slice(0, 20)
    .map((r) => r[column])
    .filter((v) => v != null && v !== "");
  if (samples.length === 0) return false;
  return samples.every((v) => !Number.isNaN(Number(v)));
}

function PivotDialog({
  columns,
  columnTypes,
  sampleRows,
  onClose,
  onApply,
}: {
  columns: string[];
  columnTypes: Record<string, ColumnDataType>;
  sampleRows: Record<string, unknown>[];
  onClose: () => void;
  onApply: (p: PivotConfig) => void;
}) {
  const [rowFields, setRowFields] = useState<string[]>([]);
  const [columnFields, setColumnFields] = useState<string[]>([]);
  const [valueFields, setValueFields] = useState<Array<{ field: string; aggregation: PivotConfig["valueFields"][0]["aggregation"] }>>([]);
  const [dragOverZone, setDragOverZone] = useState<string | null>(null);

  const fieldIsNumeric = (field: string) => {
    if (isNumericColumnType(columnTypes[field])) return true;
    return looksNumericColumn(field, sampleRows);
  };

  const fieldTypeLabel = (field: string) => {
    const t = columnTypes[field];
    if (t === "INTEGER" || t === "DOUBLE") return "Number";
    if (t === "DATE") return "Date";
    if (t === "BOOLEAN") return "Bool";
    return "Text";
  };

  const aggregations: PivotConfig["valueFields"][0]["aggregation"][] = ["sum", "count", "avg", "min", "max", "count_distinct"];

  const handleDragStart = (e: React.DragEvent, field: string) => {
    e.dataTransfer.setData("text/plain", field);
    e.dataTransfer.effectAllowed = "copy";
  };

  const handleDragOver = (e: React.DragEvent, zone: string) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
    setDragOverZone(zone);
  };

  const handleDragLeave = () => {
    setDragOverZone(null);
  };

  const handleDrop = (e: React.DragEvent, targetZone: string) => {
    e.preventDefault();
    const field = e.dataTransfer.getData("text/plain");
    if (!field || !columns.includes(field)) return;

    setDragOverZone(null);

    switch (targetZone) {
      case "rows":
        if (!rowFields.includes(field)) {
          setRowFields([...rowFields, field]);
        }
        break;
      case "columns":
        if (!columnFields.includes(field)) {
          setColumnFields([...columnFields, field]);
        }
        break;
      case "values":
        if (!valueFields.some((vf) => vf.field === field)) {
          const defaultAgg = fieldIsNumeric(field) ? "sum" : "count";
          setValueFields([...valueFields, { field, aggregation: defaultAgg }]);
        }
        break;
    }
  };

  const removeField = (field: string, fromZone: string) => {
    switch (fromZone) {
      case "rows":
        setRowFields(rowFields.filter((f) => f !== field));
        break;
      case "columns":
        setColumnFields(columnFields.filter((f) => f !== field));
        break;
      case "values":
        setValueFields(valueFields.filter((vf) => vf.field !== field));
        break;
    }
  };

  const updateValueAggregation = (field: string, aggregation: PivotConfig["valueFields"][0]["aggregation"]) => {
    setValueFields(valueFields.map((vf) => vf.field === field ? { ...vf, aggregation } : vf));
  };

  const getDropZoneClass = (zone: string) => {
    const baseClass = "border-2 border-dashed rounded-lg p-3 min-h-[100px] transition-all duration-200 ";
    const activeClass = dragOverZone === zone
      ? "border-red-500 bg-red-50 dark:bg-red-950/20 "
      : "border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-900 ";
    return baseClass + activeClass;
  };

  return (
    <Dialog title="Create Pivot Table (Drag & Drop)" onClose={onClose}>
      <div className="space-y-4 max-h-[70vh] overflow-auto">
        <p className="text-xs text-neutral-500">
          Drag fields from Available Fields to Row Fields, Column Fields, or Values areas.
        </p>

        <div className="grid grid-cols-2 gap-4">
          {/* Available Fields - Draggable Source */}
          <div className="border border-neutral-200 dark:border-neutral-700 rounded-lg p-3 bg-neutral-50 dark:bg-neutral-900">
            <p className="text-[10px] font-bold text-neutral-500 uppercase mb-3">Available Fields</p>
            <div className="space-y-1 max-h-[300px] overflow-y-auto">
              {columns.map((c) => {
                const isNumeric = fieldIsNumeric(c);
                const typeLabel = fieldTypeLabel(c);
                return (
                <div
                  key={c}
                  draggable
                  onDragStart={(e) => handleDragStart(e, c)}
                  className="text-xs py-2 px-3 bg-white dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 rounded cursor-grab active:cursor-grabbing hover:shadow-sm transition-shadow flex items-center gap-2"
                >
                  <span className="text-neutral-400">⋮⋮</span>
                  <span className="flex-1 font-medium">{c}</span>
                  <span className={`text-[9px] px-1.5 py-0.5 rounded-full ${isNumeric ? "bg-blue-100 text-blue-700" : "bg-neutral-100 text-neutral-500"}`}>
                    {typeLabel}
                  </span>
                </div>
              );
              })}
            </div>
            <p className="text-[10px] text-neutral-400 mt-2">Drag fields to the areas on the right →</p>
          </div>

          {/* Pivot Areas - Drop Zones */}
          <div className="space-y-3">
            {/* Row Fields Drop Zone */}
            <div
              className={getDropZoneClass("rows")}
              onDragOver={(e) => handleDragOver(e, "rows")}
              onDragLeave={handleDragLeave}
              onDrop={(e) => handleDrop(e, "rows")}
            >
              <div className="flex items-center justify-between mb-2">
                <p className="text-[10px] font-bold text-blue-600 uppercase">Row Fields</p>
                {rowFields.length > 0 && (
                  <span className="text-[10px] text-neutral-400">{rowFields.length} field(s)</span>
                )}
              </div>
              <div className="flex flex-wrap gap-1">
                {rowFields.map((f) => (
                  <span key={f} className="text-[10px] px-2 py-1 bg-blue-100 text-blue-700 rounded-full flex items-center gap-1">
                    {f}
                    <button onClick={() => removeField(f, "rows")} className="hover:text-blue-900">
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
              </div>
              {rowFields.length === 0 && (
                <p className="text-[10px] text-neutral-400 text-center py-4">Drop fields here for rows</p>
              )}
            </div>

            {/* Column Fields Drop Zone */}
            <div
              className={getDropZoneClass("columns")}
              onDragOver={(e) => handleDragOver(e, "columns")}
              onDragLeave={handleDragLeave}
              onDrop={(e) => handleDrop(e, "columns")}
            >
              <div className="flex items-center justify-between mb-2">
                <p className="text-[10px] font-bold text-green-600 uppercase">Column Fields</p>
                {columnFields.length > 0 && (
                  <span className="text-[10px] text-neutral-400">{columnFields.length} field(s)</span>
                )}
              </div>
              <div className="flex flex-wrap gap-1">
                {columnFields.map((f) => (
                  <span key={f} className="text-[10px] px-2 py-1 bg-green-100 text-green-700 rounded-full flex items-center gap-1">
                    {f}
                    <button onClick={() => removeField(f, "columns")} className="hover:text-green-900">
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
              </div>
              {columnFields.length === 0 && (
                <p className="text-[10px] text-neutral-400 text-center py-4">Drop fields here for columns</p>
              )}
            </div>

            {/* Values Drop Zone */}
            <div
              className={getDropZoneClass("values")}
              onDragOver={(e) => handleDragOver(e, "values")}
              onDragLeave={handleDragLeave}
              onDrop={(e) => handleDrop(e, "values")}
            >
              <div className="flex items-center justify-between mb-2">
                <p className="text-[10px] font-bold text-amber-600 uppercase">Values</p>
                {valueFields.length > 0 && (
                  <span className="text-[10px] text-neutral-400">{valueFields.length} value(s)</span>
                )}
              </div>
              <div className="flex flex-col gap-2">
                {valueFields.map((vf) => {
                  const isNumeric = fieldIsNumeric(vf.field);
                  const numericAggs = new Set(["sum", "avg"]);
                  return (
                  <div key={vf.field} className="flex items-center gap-2 text-[10px] px-2 py-1 bg-amber-100 text-amber-700 rounded">
                    <span className="font-medium">{vf.field}</span>
                    <select
                      value={vf.aggregation}
                      onChange={(e) => updateValueAggregation(vf.field, e.target.value as PivotConfig["valueFields"][0]["aggregation"])}
                      className="text-[10px] bg-white dark:bg-neutral-800 border border-amber-200 rounded px-1 py-0.5"
                    >
                      {aggregations.map((agg) => (
                        <option
                          key={agg}
                          value={agg}
                          disabled={!isNumeric && numericAggs.has(agg)}
                        >
                          {agg}{!isNumeric && numericAggs.has(agg) ? " (number only)" : ""}
                        </option>
                      ))}
                    </select>
                    <button onClick={() => removeField(vf.field, "values")} className="hover:text-amber-900 ml-auto">
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                );
                })}
              </div>
              {valueFields.length === 0 && (
                <p className="text-[10px] text-neutral-400 text-center py-4">Drop fields here for values</p>
              )}
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-2 pt-2">
          <button onClick={onClose} className="px-4 py-2 text-xs font-semibold text-neutral-600 hover:bg-neutral-100 rounded-lg">Cancel</button>
          <button
            onClick={() => {
              onApply({ rowFields, columnFields, valueFields });
              onClose();
            }}
            disabled={rowFields.length === 0 || valueFields.length === 0}
            className="px-4 py-2 text-xs font-semibold text-white bg-red-600 hover:bg-red-700 rounded-lg disabled:opacity-50"
          >
            Create Pivot
          </button>
        </div>
      </div>
    </Dialog>
  );
}

function ApplyDialog({ options, onClose, onApply }: { options: Array<{ kind: ApplyTarget["kind"]; id: string; label: string; allowAdd?: boolean }>; onClose: () => void; onApply: (t: ApplyTarget) => void }) {
  const [selected, setSelected] = useState<{ kind: ApplyTarget["kind"]; id: string } | null>(null);
  const [mode, setMode] = useState<"replace" | "add">("replace");

  const selectedOption = options.find((o) => o.kind === selected?.kind && o.id === selected?.id);

  return (
    <Dialog title="Apply Changes to Pipeline" onClose={onClose}>
      <div className="space-y-4">
        <p className="text-xs text-neutral-600 dark:text-neutral-400">
          Apply your changes back to the data pipeline. This will replace or add to the selected target.
        </p>
        <div className="border border-neutral-200 dark:border-neutral-700 rounded-lg overflow-hidden">
          <div className="max-h-[200px] overflow-auto">
            {options.map((opt) => (
              <button
                key={`${opt.kind}-${opt.id}`}
                onClick={() => setSelected({ kind: opt.kind, id: opt.id })}
                className={`
                  w-full text-left px-4 py-2 text-xs hover:bg-neutral-50 dark:hover:bg-neutral-800 flex items-center justify-between
                  ${selected?.kind === opt.kind && selected?.id === opt.id ? "bg-red-50 dark:bg-red-950/30 text-red-700 dark:text-red-400" : ""}
                `}
              >
                <span>{opt.label}</span>
                <span className="text-[10px] text-neutral-400 uppercase">{opt.kind}</span>
              </button>
            ))}
          </div>
        </div>

        {selectedOption?.allowAdd && (
          <div className="flex gap-4">
            <label className="flex items-center gap-2">
              <input
                type="radio"
                value="replace"
                checked={mode === "replace"}
                onChange={() => setMode("replace")}
              />
              <span className="text-sm">Replace existing</span>
            </label>
            <label className="flex items-center gap-2">
              <input
                type="radio"
                value="add"
                checked={mode === "add"}
                onChange={() => setMode("add")}
              />
              <span className="text-sm">Add as new</span>
            </label>
          </div>
        )}

        <div className="flex justify-end gap-2 pt-2">
          <button onClick={onClose} className="px-4 py-2 text-xs font-semibold text-neutral-600 hover:bg-neutral-100 rounded-lg">Cancel</button>
          <button
            onClick={() => selected && onApply({ ...selected, mode })}
            disabled={!selected}
            className="px-4 py-2 text-xs font-semibold text-white bg-red-600 hover:bg-red-700 rounded-lg disabled:opacity-50"
          >
            Apply Changes
          </button>
        </div>
      </div>
    </Dialog>
  );
}

function Dialog({ title, children, onClose, wide = false }: { title: string; children: React.ReactNode; onClose: () => void; wide?: boolean }) {
  const panelRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const restoreFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    restoreFocusRef.current = document.activeElement as HTMLElement | null;
    closeRef.current?.focus();
    return () => restoreFocusRef.current?.focus();
  }, []);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      onClose();
      return;
    }
    if (event.key !== "Tab" || !panelRef.current) return;
    const focusable = Array.from(panelRef.current.querySelectorAll(
      'button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])',
    )) as HTMLElement[];
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
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-black/50 p-4" onMouseDown={onClose}>
      <motion.div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        onMouseDown={(event) => event.stopPropagation()}
        onKeyDown={handleKeyDown}
        className={`max-h-[90vh] overflow-y-auto bg-white dark:bg-neutral-900 rounded-xl shadow-xl border border-neutral-200 dark:border-neutral-700 w-full ${wide ? "max-w-3xl" : "max-w-lg"}`}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-neutral-200 dark:border-neutral-700">
          <h3 className="text-sm font-bold">{title}</h3>
          <button ref={closeRef} type="button" onClick={onClose} aria-label={`Close ${title}`} className="p-1 hover:bg-neutral-100 dark:hover:bg-neutral-800 rounded focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="p-4">{children}</div>
      </motion.div>
    </div>
  );
}

// --- Column Type Picker ---

function ColumnTypePicker({
  column,
  currentType,
  x,
  y,
  onClose,
  onChangeType,
}: {
  column: string;
  currentType: ColumnDataType;
  x: number;
  y: number;
  onClose: () => void;
  onChangeType: (column: string, newType: ColumnDataType) => void;
}) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  const dataTypes: { value: ColumnDataType; label: string; icon: string }[] = [
    { value: "TEXT", label: "Text", icon: "Aa" },
    { value: "INTEGER", label: "Integer", icon: "123" },
    { value: "DOUBLE", label: "Decimal", icon: "#" },
    { value: "DATE", label: "Date", icon: "📅" },
    { value: "BOOLEAN", label: "Boolean", icon: "✓" },
  ];

  return (
    <div
      ref={menuRef}
      className="fixed z-[100] bg-white dark:bg-neutral-900 rounded-lg shadow-xl border border-neutral-200 dark:border-neutral-700 py-1 min-w-[180px]"
      style={{ left: x, y: y }}
    >
      <div className="px-3 py-2 border-b border-neutral-200 dark:border-neutral-700">
        <p className="text-xs font-semibold text-neutral-500 uppercase">Type: {column}</p>
      </div>
      <div className="py-1">
        {dataTypes.map((dt) => (
          <button
            key={dt.value}
            onClick={() => onChangeType(column, dt.value)}
            className={`w-full text-left px-3 py-1.5 text-xs flex items-center gap-2 ${
              currentType === dt.value
                ? "bg-red-50 dark:bg-red-950/30 text-red-600 font-semibold"
                : "hover:bg-neutral-100 dark:hover:bg-neutral-800"
            }`}
          >
            <span className="text-[10px] w-6 text-center text-neutral-400">{dt.icon}</span>
            <span>{dt.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

// --- Column Context Menu Component ---

function ColumnContextMenu({
  column,
  x,
  y,
  currentType,
  canDelete,
  onClose,
  onDelete,
  onChangeType,
}: {
  column: string;
  x: number;
  y: number;
  currentType: ColumnDataType;
  canDelete: boolean;
  onClose: () => void;
  onDelete: (column: string) => void;
  onChangeType: (column: string, newType: ColumnDataType) => void;
}) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  const dataTypes: { value: ColumnDataType; label: string; icon: string }[] = [
    { value: "TEXT", label: "Text", icon: "T" },
    { value: "INTEGER", label: "Integer", icon: "123" },
    { value: "DOUBLE", label: "Decimal", icon: "1.23" },
    { value: "DATE", label: "Date", icon: "📅" },
    { value: "BOOLEAN", label: "Boolean", icon: "✓" },
  ];

  return (
    <div
      ref={menuRef}
      className="fixed z-[100] bg-white dark:bg-neutral-900 rounded-lg shadow-xl border border-neutral-200 dark:border-neutral-700 py-1 min-w-[180px]"
      style={{ left: x, top: y }}
    >
      <div className="px-3 py-2 border-b border-neutral-200 dark:border-neutral-700">
        <p className="text-xs font-semibold text-neutral-500 uppercase">Column: {column}</p>
      </div>

      <div className="py-1">
        <p className="px-3 py-1 text-[10px] font-semibold text-neutral-400 uppercase">Change Data Type</p>
        {dataTypes.map((dt) => (
          <button
            key={dt.value}
            onClick={() => onChangeType(column, dt.value)}
            className={`w-full text-left px-3 py-1.5 text-xs flex items-center gap-2 ${
              currentType === dt.value
                ? "bg-red-50 dark:bg-red-950/30 text-red-600 font-semibold"
                : "hover:bg-neutral-100 dark:hover:bg-neutral-800"
            }`}
          >
            <span className="text-[10px] w-6 text-center text-neutral-400">{dt.icon}</span>
            <span>{dt.label}</span>
          </button>
        ))}
      </div>

      <div className="border-t border-neutral-200 dark:border-neutral-700 py-1">
        <button
          onClick={() => {
            if (!canDelete) return;
            onDelete(column);
            onClose();
          }}
          disabled={!canDelete}
          title={canDelete ? undefined : "Cannot delete the last column"}
          className={`w-full text-left px-3 py-1.5 text-xs flex items-center gap-2 ${
            canDelete
              ? "hover:bg-red-50 dark:hover:bg-red-950/30 text-red-600"
              : "text-neutral-400 cursor-not-allowed"
          }`}
        >
          <Trash2 className="w-3.5 h-3.5" />
          <span>Delete Column</span>
        </button>
      </div>
    </div>
  );
}
