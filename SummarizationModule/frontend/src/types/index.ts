export interface ColumnInfo {
  name: string;
  detectedType: "numeric" | "datetime" | "string";
  parseSuccessRate: number;
  distinctCount: number;
  sampleValues: string[];
}

export interface FileInventoryItem {
  tableName: string;
  rowCount: number;
  columnCount: number;
}

export interface StandardField {
  fieldKey: string;
  displayName: string;
  expectedType: "numeric" | "datetime" | "string";
  description: string;
}

export interface AIMapping {
  fieldKey: string;
  expectedType: string;
  bestMatch: string | null;
  bestMatchDetectedType?: string;
  confidence: number;
  alternatives: string[];
  reasoning: string;
}

export interface FieldCastReport {
  mapped: boolean;
  sourceColumn: string | null;
  validRows: number;
  nullRows: number;
  parseRate: number;
  sampleFailures: string[];
}

export interface CastReport {
  total_rows: number;
  fields: Record<string, FieldCastReport>;
}

export interface ViewDefinition {
  viewId: string;
  title: string;
  description: string;
  requiredFields: string[];
  chartType: string;
  available: boolean;
}

export interface MekkoSegment {
  label: string;
  value: number;
  share: number;
}

export interface MekkoColumn {
  label: string;
  totalSpend: number;
  width: number;
  segments: MekkoSegment[];
}

export interface MekkoData {
  columns: MekkoColumn[];
  grandTotal: number;
}

export interface TreeNode {
  name: string;
  level: string;
  totalSpend: number;
  percentOfParent: number;
  percentOfTotal: number;
  children: TreeNode[];
}

export interface ViewResult {
  viewId: string;
  title: string;
  chartType: string;
  tableData: any;
  chartData: any;
  aiSummary?: string;
  excludedRows?: number;
  totalSuppliers?: number;
  suppliersInGroup?: number;
  threshold?: number;
  treeData?: TreeNode[];
  availableLevels?: string[];
  error?: string;
}

export interface ViewConfig {
  topN: number;
  paretoThreshold: number;
}

export type AppStep = 1 | 2 | 3 | 4;
