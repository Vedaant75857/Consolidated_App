/** Excel-like Preview Overlay types - replaces playground types */

export type PreviewFilterOp =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "startswith"
  | "endswith"
  | "is_null"
  | "is_not_null"
  | "in"
  | "not_in";

export interface PreviewFilter {
  column: string;
  op: PreviewFilterOp;
  value?: string;
  values?: string[];
  includeBlanks?: boolean;
}

export interface PreviewSort {
  column: string;
  dir: "asc" | "desc";
}

export interface PreviewTableState {
  tableKey: string;
  columns: string[];
  columnOrder: string[];
  columnTypes?: Record<string, ColumnDataType>;
  rows: Record<string, unknown>[];
  totalRows: number;
  filters: PreviewFilter[];
  sort: PreviewSort[];
  search: string;
  offset: number;
  limit: number;
}

export interface PreviewOperationRequest {
  sessionId: string;
  tableKey: string;
  op:
    | "cell_edit"
    | "column_rename"
    | "column_delete"
    | "column_reorder"
    | "column_change_type"
    | "rows_delete"
    | "calculated_column"
    | "filter"
    | "sort"
    | "pivot"
    | "undo"
    | "redo";
  params: Record<string, unknown>;
}

/** Data type options for column type change */
export type ColumnDataType = "TEXT" | "INTEGER" | "DOUBLE" | "DATE" | "BOOLEAN";

export interface PreviewOperationResult {
  columns: string[];
  columnTypes?: Record<string, ColumnDataType>;
  rows: Record<string, unknown>[];
  totalRows: number;
  applied: boolean;
  message?: string;
}

export type ApplyTargetKind = "raw" | "group" | "merge_version" | "merge_group";

export interface ApplyTarget {
  kind: ApplyTargetKind;
  id: string;
  mode?: "replace" | "add";
}

export interface ApplyResult {
  ok: boolean;
  resetStep: number;
  statePatch: Record<string, unknown>;
  message: string;
}

/** Pivot configuration */
export interface PivotConfig {
  rowFields: string[];
  columnFields: string[];
  valueFields: Array<{
    field: string;
    aggregation: "sum" | "count" | "avg" | "min" | "max" | "count_distinct";
  }>;
}

/** Calculated column configuration */
export interface CalcColumnConfig {
  name: string;
  expression: string;
  dataType?: "TEXT" | "INTEGER" | "DOUBLE" | "DATE" | "BOOLEAN";
}

/** Cell edit parameters */
export interface CellEditParams {
  rowId: number;
  column: string;
  value: string | number | null;
}
