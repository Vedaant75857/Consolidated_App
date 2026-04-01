export interface QualityIntersections {
  AC: number;
  AD: number;
  BC: number;
  BD: number;
}

export interface DescriptionMetrics {
  completionRate: number;
  codedAlphanumericPct: number;
  multiWordPct: number;
  singleWordPct: number;
  longPct: number;
  shortPct: number;
  intersections: QualityIntersections;
}

export interface QualityMetrics {
  description: DescriptionMetrics;
  supplierFillRate: number;
  totalRows: number;
  hasDescription: boolean;
  hasSupplier: boolean;
}

export type QualityPhase =
  | "idle"
  | "computing"
  | "summarizing"
  | "done"
  | "error";
