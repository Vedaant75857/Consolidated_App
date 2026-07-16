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

/** Successful response from POST /api/preview/state. */
export interface PreviewStateResponse {
  columns: string[];
  columnTypes: Record<string, ColumnDataType>;
  rows: Record<string, unknown>[];
  totalRows: number;
  offset: number;
  limit: number;
}

/** Backwards-compatible name for the server preview-state DTO. */
export type PreviewTableState = PreviewStateResponse;

export interface PreviewView {
  offset?: number;
  limit?: number;
  search?: string;
  filters?: PreviewFilter[];
  sort?: PreviewSort[];
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
  view?: PreviewView;
}

/** Data type options for column type change */
export type ColumnDataType = "TEXT" | "INTEGER" | "DOUBLE" | "DATE" | "BOOLEAN";

export interface PreviewOperationResult {
  ok: boolean;
  columns?: string[];
  columnTypes?: Record<string, ColumnDataType>;
  rows?: Record<string, unknown>[];
  totalRows?: number;
  offset?: number;
  limit?: number;
  success?: boolean;
  message?: string;
  newTableKey?: string;
  tableName?: string;
}

export interface PreviewColumnValuesResponse {
  values: string[];
  hasBlanks: boolean;
  totalDistinct: number;
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
