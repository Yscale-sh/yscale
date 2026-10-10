const TERMINAL = new Set(["succeeded", "failed", "cancelled", "canceled", "complete", "completed", "settled"]);
const ATTENTION = new Set(["manual_attention", "needs_attention"]);
const CLEANUP_ACTIVE = new Set(["deleting", "retrying", "manual_attention", "deleted", "absent", "settled"]);

export const GPU_FAILURE_STATES = Object.freeze([
  { code: "no_connected_cluster", title: "No connected eligible cluster", detail: "Connect a cluster or change the tenant cluster policy before launching.", action: "Open clusters" },
  { code: "connector_offline", title: "Cluster connector offline", detail: "The cluster is registered, but its connector is not currently reachable. Work stays unlaunched.", action: "Check connector" },
  { code: "no_eligible_placement", title: "No eligible GPU placement", detail: "No provider and region satisfy the selected GPU, cluster, and policy constraints.", action: "Edit compute" },
  { code: "price_above_cap", title: "GPU price above cap", detail: "The trusted quote exceeds the maximum hourly GPU price. No paid resource was created.", action: "Edit budget" },
  { code: "placement_changed", title: "Placement changed", detail: "The reviewed placement decision expired or changed before launch. Review the new decision before confirming.", action: "Review again" },
  { code: "node_join_failed", title: "Node failed to join", detail: "Capacity was created, but the node did not join the selected Kubernetes cluster.", action: "View timeline" },
  { code: "gpu_unhealthy", title: "GPU unhealthy", detail: "The node joined, but the requested GPU resource never became healthy.", action: "View node checks" },
  { code: "pod_unschedulable", title: "Pod unschedulable", detail: "Capacity is ready, but Kubernetes cannot place the workload pod with its current constraints.", action: "View scheduling" },
  { code: "provider_delete_retrying", title: "Cleanup retrying", detail: "The workload is finished, but provider deletion is not yet confirmed. Cost and cleanup remain open.", action: "View cleanup" },
  { code: "manual_attention", title: "Manual attention required", detail: "Automatic teardown exhausted its safe retries. Yscale has not declared the provider resource absent.", action: "Open incident" },
]);

const FAILURE_BY_CODE = new Map(GPU_FAILURE_STATES.map((state) => [state.code, state]));

function clean(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizedState(value) {
  return clean(value).toLowerCase().replaceAll("-", "_");
}

export function workloadNeedsAttention(workload) {
  const values = [
    workload?.status,
    workload?.phase,
    workload?.lifecycle?.phase,
    workload?.lifecycle?.cleanup_state,
    workload?.cleanup?.state,
    workload?.provider_delete?.state,
    workload?.failure?.code,
    workload?.error?.code,
  ];
  return values.some((value) => {
    const state = normalizedState(value);
    return ATTENTION.has(state) || FAILURE_BY_CODE.has(state);
  });
}

export function workloadFailureState(workload) {
  const values = [workload?.failure?.code, workload?.error?.code, workload?.lifecycle?.phase, workload?.phase, workload?.status];
  const code = values.map(normalizedState).find((value) => FAILURE_BY_CODE.has(value));
  if (code) return FAILURE_BY_CODE.get(code);
  if (workloadNeedsAttention(workload)) return FAILURE_BY_CODE.get("manual_attention");
  const cleanup = normalizedState(workload?.lifecycle?.cleanup_state || workload?.cleanup?.state || workload?.provider_delete?.state);
  return cleanup === "retrying" ? FAILURE_BY_CODE.get("provider_delete_retrying") : null;
}

export function workloadPhase(workload) {
  return clean(workload?.lifecycle?.phase || workload?.phase || workload?.status) || "unknown";
}

export function workloadCluster(workload) {
  return clean(workload?.placement?.granted_cluster_id || workload?.cluster_id) || "not reported";
}

export function workloadGPU(workload) {
  const selected = selectedPlacement(workload);
  const unknown = { label: "not reported", kind: "not reported", count: null };
  if (!selected) return unknown;
  const kind = clean(selected.gpu_kind) || "not reported";
  if ((selected.gpu_kind === undefined || selected.gpu_kind === "") && (selected.gpu_count === undefined || selected.gpu_count === 0)) {
    return { label: "CPU only", kind: "cpu", count: 0 };
  }
  const count = Number.isSafeInteger(selected.gpu_count) && selected.gpu_count > 0 ? selected.gpu_count : null;
  if (!count || kind === "not reported") return unknown;
  return { label: `${kind} × ${count}`, kind, count };
}

// Requested compute and legacy flat fields are not evidence of what central
// selected. List and detail responses share this persisted receipt contract.
function selectedPlacement(workload) {
  const selected = workload?.placement?.receipt?.selected;
  return selected && typeof selected === "object" && !Array.isArray(selected) && clean(selected.provider) ? selected : null;
}

export function workloadPlacement(workload) {
  const selected = selectedPlacement(workload);
  return {
    provider: clean(selected?.provider) || "not reported",
    region: clean(selected?.region) || "not reported",
    sku: clean(selected?.sku) || "not reported",
  };
}

function elapsedBetween(start, end) {
  const from = Date.parse(start || "");
  const to = typeof end === "number" ? end : Date.parse(end || "");
  if (!Number.isFinite(from) || !Number.isFinite(to) || to < from) return "not reported";
  const seconds = Math.floor((to - from) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

export function workloadElapsed(workload, now = Date.now()) {
  const terminal = workloadIsTerminal(workload);
  return elapsedBetween(workload?.started_at || workload?.created_at, terminal ? workload?.finished_at : now);
}

export function workloadIsTerminal(workload) {
  const phaseIsTerminal = [workload?.lifecycle?.phase, workload?.phase, workload?.status]
    .some((value) => TERMINAL.has(normalizedState(value)));
  const finishedAt = Date.parse(workload?.finished_at || "");
  const cleanup = normalizedState(workload?.lifecycle?.cleanup_state || workload?.cleanup?.state || workload?.provider_delete?.state);
  return phaseIsTerminal || Number.isFinite(finishedAt) || CLEANUP_ACTIVE.has(cleanup);
}

export function sortWorkloadsActiveFirst(workloads) {
  return [...workloads].sort((left, right) => {
    const rank = (workload) => workloadNeedsAttention(workload) ? 0 : workloadIsTerminal(workload) ? 2 : 1;
    const difference = rank(left) - rank(right);
    if (difference) return difference;
    const newest = Date.parse(right?.created_at || "") - Date.parse(left?.created_at || "");
    if (Number.isFinite(newest) && newest) return newest;
    return clean(left?.id).localeCompare(clean(right?.id));
  });
}

export function filterGPUWorkloads(workloads, filters = {}) {
  const phase = normalizedState(filters.phase);
  const cluster = clean(filters.cluster);
  const gpu = clean(filters.gpu).toLowerCase();
  const template = clean(filters.template);
  const query = clean(filters.query).toLowerCase();
  const date = clean(filters.date);
  const now = Number.isFinite(filters.now) ? filters.now : Date.now();
  const dateWindow = { "24h": 24 * 60 * 60 * 1000, "7d": 7 * 24 * 60 * 60 * 1000, "30d": 30 * 24 * 60 * 60 * 1000 }[date];
  return sortWorkloadsActiveFirst(workloads).filter((workload) => {
    const receipt = workload?.template?.id || "";
    const haystack = [workload?.id, workload?.spec?.metadata?.name, workload?.spec?.spec?.image].map(clean).join(" ").toLowerCase();
    return (!phase || normalizedState(workloadPhase(workload)) === phase)
      && (!cluster || workloadCluster(workload) === cluster)
      && (!gpu || workloadGPU(workload).kind.toLowerCase() === gpu)
      && (!template || receipt === template)
      && (!dateWindow || (Number.isFinite(Date.parse(workload?.created_at || "")) && now - Date.parse(workload.created_at) >= 0 && now - Date.parse(workload.created_at) <= dateWindow))
      && (!query || haystack.includes(query));
  });
}
