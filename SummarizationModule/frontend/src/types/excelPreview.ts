import type { ColumnInfo, FileInventoryItem, PreviewData } from ".";

export type PreviewColumnDataType = "TEXT" | "INTEGER" | "DOUBLE" | "DATE" | "BOOLEAN" | string;

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
  columnKey: string;
  op: PreviewFilterOp;
  value?: string | number | boolean | null;
  values?: Array<string | number | boolean | null>;
  includeBlanks?: boolean;
}

export interface PreviewSort {
  columnKey: string;
  dir: "asc" | "desc";
}

export interface PreviewColumn {
  key: string;
  displayName: string;
  dataType: PreviewColumnDataType;
}

export interface PreviewRow {
  __row_id: string | number;
  values: Record<string, unknown>;
}

export interface PreviewStateRequest {
  sessionId: string;
  tableKey: string;
  offset?: number;
  limit?: number;
  search?: string;
  filters?: PreviewFilter[];
  sort?: PreviewSort[];
}

export interface PreviewTableState {
  tableKey: string;
  columns: PreviewColumn[];
  rows: PreviewRow[];
  totalRows: number;
  offset: number;
  limit: number;
  tableVersion: number;
  undoDepth: number;
  redoDepth: number;
  dirty: boolean;
  sourceTableKey?: string | null;
  playgroundTableName?: string | null;
  dropped?: boolean;
}

export interface PreviewColumnValuesRequest {
  sessionId: string;
  tableKey: string;
  columnKey: string;
  search?: string;
  filters?: PreviewFilter[];
  limit?: number;
}

export interface PreviewColumnValuesResult {
  columnKey: string;
  values: string[];
  hasBlanks: boolean;
  totalDistinct: number;
}

export type PreviewOperationType =
  | "cell_edit"
  | "column_rename"
  | "column_delete"
  | "column_reorder"
  | "column_change_type"
  | "rows_delete"
  | "calculated_column"
  | "pivot";

export type PreviewColumnDataTypeOverride = "TEXT" | "INTEGER" | "DOUBLE" | "DATE" | "BOOLEAN";

export type PreviewAggregation =
  | "sum"
  | "count"
  | "avg"
  | "min"
  | "max"
  | "count_distinct";

export interface PreviewCellEditParams {
  rowId: string | number;
  columnKey: string;
  value: string | number | boolean | null;
}

export interface PreviewRowsDeleteParams {
  rowIds: Array<string | number>;
}

export interface PreviewColumnRenameParams {
  columnKey: string;
  displayName: string;
}

export interface PreviewColumnDeleteParams {
  columnKey: string;
}

export interface PreviewColumnReorderParams {
  columnKeys: string[];
}

export interface PreviewColumnChangeTypeParams {
  columnKey: string;
  newType: PreviewColumnDataTypeOverride;
}

export interface PreviewCalculatedColumnParams {
  name: string;
  expression: string;
  dataType: PreviewColumnDataTypeOverride;
}

export interface PreviewPivotValueField {
  columnKey: string;
  aggregation: PreviewAggregation;
}

export interface PreviewPivotParams {
  rowFields: string[];
  columnFields: string[];
  valueFields: PreviewPivotValueField[];
}

export type PreviewOperationParams =
  | PreviewCellEditParams
  | PreviewRowsDeleteParams
  | PreviewColumnRenameParams
  | PreviewColumnDeleteParams
  | PreviewColumnReorderParams
  | PreviewColumnChangeTypeParams
  | PreviewCalculatedColumnParams
  | PreviewPivotParams;

export interface PreviewOperationRequest {
  sessionId: string;
  tableKey: string;
  op: PreviewOperationType;
  params: PreviewOperationParams;
}

export interface PreviewOperationResult extends PreviewTableState {
  ok: boolean;
  message?: string;
  newTableKey?: string;
}

export interface PreviewHistoryRequest {
  sessionId: string;
  tableKey: string;
}

export interface PreviewRefreshInventoryRequest {
  sessionId: string;
}

export interface PreviewRefreshInventoryResult {
  ok: boolean;
  fileInventory: FileInventoryItem[];
  columns: ColumnInfo[];
  previews: Record<string, PreviewData>;
}
