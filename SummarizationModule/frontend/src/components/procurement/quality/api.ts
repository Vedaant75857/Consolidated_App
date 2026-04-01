import type { QualityMetrics } from "./types";

const BASE = "/api";

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || `Request failed: ${res.status}`);
  }
  return res.json();
}

export async function runQualityAnalysis(sessionId: string) {
  return post<{ metrics: QualityMetrics }>("/quality-analysis", { sessionId });
}

export async function getQualityAnalysisSummary(
  sessionId: string,
  metrics: QualityMetrics,
  apiKey: string,
) {
  return post<{ summary: string }>("/quality-analysis-summary", {
    sessionId,
    metrics,
    apiKey,
  });
}
