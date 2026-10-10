import { isTerminal } from "./consoleData.js";

// --- ordering ---

export function orderedWorkloads(workloads) {
  return [...workloads].sort((a, b) => {
    const aTerminal = isTerminal(a.status);
    const bTerminal = isTerminal(b.status);
    if (aTerminal !== bTerminal) return aTerminal ? 1 : -1;
    return compareTimestamps(b.created_at, a.created_at);
  });
}

function compareTimestamps(a, b) {
  const da = new Date(a);
  const db = new Date(b);
  const ta = Number.isNaN(da.getTime()) ? 0 : da.getTime();
  const tb = Number.isNaN(db.getTime()) ? 0 : db.getTime();
  return ta - tb;
}

// --- filter derivation ---

export function workloadPhase(workload) {
  const s = String(workload?.status || "").toLowerCase();
  return s || "unknown";
}

export function workloadGpuLabel(workload) {
  const selected = workload?.placement?.receipt?.selected;
  if (!selected?.provider) return "not recorded";
  return selected.gpu_kind || "CPU";
}

export function workloadCluster(workload) {
  return workload?.placement?.granted_cluster_id
    || workload?.cluster_id
    || "";
}

export function workloadTemplate(workload) {
  const t = workload?.template;
  return (t && typeof t === "object" && t.id) ? t.id : "";
}

export function workloadSelectedProvider(workload) {
  return workload?.placement?.receipt?.selected?.provider || "";
}

export function workloadSelectedRegion(workload) {
  return workload?.placement?.receipt?.selected?.region || "";
}

// --- filter options from records ---

export function filterOptions(workloads) {
  const phases = new Set();
  const clusters = new Set();
  const gpus = new Set();
  const templates = new Set();
  for (const w of workloads) {
    phases.add(workloadPhase(w));
    const cluster = workloadCluster(w);
    if (cluster) clusters.add(cluster);
    gpus.add(workloadGpuLabel(w));
    const tmpl = workloadTemplate(w);
    if (tmpl) templates.add(tmpl);
  }
  return {
    phases: [...phases].sort(),
    clusters: [...clusters].sort(),
    gpus: [...gpus].sort(),
    templates: [...templates].sort(),
  };
}

// --- date filter ---

const DAY_MS = 86_400_000;

export const DATE_CHOICES = [
  { key: "24h", label: "Last 24 hours", days: 1 },
  { key: "7d", label: "Last 7 days", days: 7 },
  { key: "30d", label: "Last 30 days", days: 30 },
  { key: "all", label: "All time", days: null },
];

function dateFilterCutoff(dateKey, now) {
  const choice = DATE_CHOICES.find((c) => c.key === dateKey);
  if (!choice || choice.days === null) return null;
  return new Date(now.getTime() - choice.days * DAY_MS);
}

// --- combined filter ---

export function filterWorkloads(workloads, filters, now) {
  const { phase, cluster, gpu, template, date } = filters;
  const cutoff = dateFilterCutoff(date, now);
  return workloads.filter((w) => {
    if (phase && workloadPhase(w) !== phase) return false;
    if (cluster && workloadCluster(w) !== cluster) return false;
    if (gpu && workloadGpuLabel(w) !== gpu) return false;
    if (template && workloadTemplate(w) !== template) return false;
    if (cutoff) {
      const created = new Date(w.created_at);
      if (Number.isNaN(created.getTime()) || created < cutoff) return false;
    }
    return true;
  });
}

export function hasActiveFilters(filters) {
  return !!(filters.phase || filters.cluster || filters.gpu || filters.template || (filters.date && filters.date !== "all"));
}

export const EMPTY_FILTERS = { phase: "", cluster: "", gpu: "", template: "", date: "all" };

// --- attention ---

export function workloadNeedsAttention(workload) {
  const cleanup = workload?.cleanup;
  if (!cleanup || typeof cleanup !== "object" || Array.isArray(cleanup)) return false;
  return cleanup.state === "manual_attention";
}

// --- display helpers ---

export function elapsedLabel(workload, now) {
  const start = workload.started_at || workload.created_at;
  if (!start) return "not recorded";
  const from = new Date(start);
  if (Number.isNaN(from.getTime())) return "not recorded";
  if (isTerminal(workload.status) && !workload.finished_at) return "not recorded";
  const end = isTerminal(workload.status) ? new Date(workload.finished_at) : now;
  if (!(end instanceof Date) || Number.isNaN(end.getTime())) return "not recorded";
  const seconds = Math.max(0, Math.round((end - from) / 1000));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

export function gpuDisplay(workload) {
  const selected = workload?.placement?.receipt?.selected;
  if (!selected?.provider) return "not recorded";
  if (!selected.gpu_kind) return "CPU";
  return `${selected.gpu_kind} × ${selected.gpu_count || 1}`;
}
