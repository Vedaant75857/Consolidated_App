/** Playground workbook types. */

export type PlaygroundSourceKind = "raw" | "group" | "merge" | "playground";

export interface PlaygroundSourceRef {
  kind: PlaygroundSourceKind;
  id: string;
}

export interface PlaygroundSource {
  kind: PlaygroundSourceKind;
  id: string;
  name: string;
  rows: number;
  cols: number;
}

export interface PlaygroundSheetMeta {
  sheetId: string;
  name: string;
  sqlName?: string;
  sourceRef: PlaygroundSourceRef;
  createdAt: string;
  updatedAt: string;
  rowCount: number;
  columns: string[];
}

export interface PlaygroundFilter {
  column: string;
  op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "contains" | "startswith" | "endswith" | "is_null" | "is_not_null";
  value?: string;
}

export interface PlaygroundSort {
  column: string;
  dir: "asc" | "desc";
}

export interface PlaygroundPreviewResult {
  columns: string[];
  rows: Record<string, unknown>[];
  totalRows: number;
  sheetMeta: PlaygroundSheetMeta;
}

export type PlaygroundApplyTargetKind = "raw" | "group" | "merge_version" | "merge_group";

export interface PlaygroundApplyTarget {
  kind: PlaygroundApplyTargetKind;
  id: string;
  mode?: "replace" | "add";
}

export interface PlaygroundApplyResult {
  ok: boolean;
  resetStep: number;
  statePatch: Record<string, unknown>;
  message: string;
}
