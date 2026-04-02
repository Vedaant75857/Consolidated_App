export interface ColumnInfo {
  name: string;
  sampleValues: string[];
}

export interface FileInventoryItem {
  table_key: string;
  rows: number;
  cols: number;
}

export interface UploadWarning {
  file: string;
  message: string;
}

export interface PreviewData {
  columns: string[];
  rows: any[];
}

export interface StandardField {
  fieldKey: string;
  displayName: string;
  expectedType: "numeric" | "datetime" | "string";
  description: string;
  aliases?: string[];
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
  metrics?: Record<string, string>;
  excludedRows?: number;
  totalSuppliers?: number;
  suppliersInGroup?: number;
  threshold?: number;
  treeData?: TreeNode[];
  availableLevels?: string[];
  error?: string;
}

export interface ViewConfig {
  topN?: number;
  paretoThreshold?: number;
}

export interface NextStep {
  action: string;
  owner: string;
  timeline: string;
}

export interface EmailContext {
  recipient_name: string;
  client_name: string;
  sender_name: string;
  sender_role: string;
  scope_note: string;
  next_steps: NextStep[];
}

export interface ProcurementViewAvailability {
  viewId: string;
  title: string;
  requiredFields: string[];
  available: boolean;
  missingFields: string[];
}

export interface ExecSummaryRow {
  key: string;
  label: string;
  mapped: boolean;
  fillRate: number;
  validRows: number;
  totalRows: number;
  insight: string | null;
}

export type AppStep = 1 | 2 | 3 | 4 | 5 | 6 | 7;
