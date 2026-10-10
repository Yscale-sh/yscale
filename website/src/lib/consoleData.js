// Everything the console derives from records it can already read: the workload
// list and the tenant summary on /v1/account. No endpoint behind any of it, so
// every caller is expected to label the result as record-derived rather than
// authoritative — a console that cannot read a ledger must not print one.

import { formatDate, formatUSD, formatBytes } from "./format.js";
import { DEFAULT_WORKLOAD_NAMESPACE, templateForWorkload } from "./workloadTemplates.js";
import { deriveCleanupPresentation, isTerminal } from "./workloadLifecycle.js";

export { isTerminal } from "./workloadLifecycle.js";
const SUCCESS = new Set(["succeeded", "complete", "completed"]);

export function isSucceeded(status) {
  return SUCCESS.has(String(status || "").toLowerCase());
}

// Number() reads null, "", [] and true as numbers, and every one of those would
// land in a total as a value central never sent. Only something already numeric
// — or a non-empty string of one — counts as reported.
function reportedNumber(value) {
  if (typeof value !== "number" && !(typeof value === "string" && value.trim())) return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

function reportedCount(value) {
  const number = reportedNumber(value);
  return Number.isSafeInteger(number) ? number : null;
}

// The optional cost block central freezes after provider absence is confirmed.
// It estimates what one run's capacity cost
// at the rate it was quoted — not provider invoicing, not tenant billing, not
// prepaid-ledger spend, and never an all-time total. Legacy records, runs still
// going, attempts that failed before capacity existed, and anything central
// never observed carry no block at all, and a caller that turns that absence
// into 0.00 has invented a number the platform never reported.
export function costObservation(workload) {
  const cost = workload?.cost;
  if (!cost || typeof cost !== "object") return null;
  const usd = reportedNumber(cost.usd);
  if (usd === null) return null;
  return { ...cost, usd };
}

// Central reports accrued spend while the provider resource remains live, even
// after compute finishes. The console must never derive it from browser
// time × quoted rate — a page open across a paused clock, a rate change, or a
// suspended run would print a number the platform never sent. Frozen cost or
// canonical provider-absence proof takes precedence over a stale live number.
export function liveSpendObservation(workload) {
  if (!workload || costObservation(workload) || deriveCleanupPresentation(workload)?.providerAbsentConfirmed) return null;
  const usd = workload.spent_usd;
  if (typeof usd !== "number" || !Number.isFinite(usd) || usd < 0) return null;
  return { usd };
}

// Telemetry is a pair — gpu_util_percent AND last_heartbeat_at — so the reader
// can tell what was observed and when. A record with one half of the pair, a
// utilization outside 0..100, or a timestamp Date cannot parse fails closed:
// the caller sees `{ present: false, malformed: true }` and can choose to hide
// the chip rather than print a percentage the platform never proved. A record
// with neither half returns `{ present: false }` — a legitimate "no telemetry
// yet" state, distinct from bad data.
export function liveTelemetryObservation(workload) {
  const rawPercent = workload?.gpu_util_percent;
  const rawHeartbeat = workload?.last_heartbeat_at;
  const hasPercent = rawPercent !== undefined && rawPercent !== null;
  const hasHeartbeat = rawHeartbeat !== undefined && rawHeartbeat !== null && rawHeartbeat !== "";
  if (!hasPercent && !hasHeartbeat) return { present: false, malformed: false };
  if (!hasPercent || !hasHeartbeat) return { present: false, malformed: true };
  if (typeof rawPercent !== "number" || !Number.isFinite(rawPercent) || rawPercent < 0 || rawPercent > 100) {
    return { present: false, malformed: true };
  }
  const timestamp = typeof rawHeartbeat === "string" ? rawHeartbeat.trim() : "";
  const rfc3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
  if (!rfc3339.test(timestamp)) return { present: false, malformed: true };
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) return { present: false, malformed: true };
  return { present: true, malformed: false, percent: rawPercent, heartbeatAt: timestamp };
}

// A row without a number has three different reasons the reader must be able
// to tell apart: a live run central is reporting spend for, a live run central
// has not attached a burst to yet, and a terminal run without an observation.
// A bare dash would collapse all of them into the same non-answer.
export function costCellLabel(workload) {
  const cost = costObservation(workload);
  if (cost) return formatUSD(cost.usd);
  const live = liveSpendObservation(workload);
  if (live) return `${formatUSD(live.usd)}${isTerminal(workload?.status) ? " · accruing" : ""}`;
  if (!isTerminal(workload?.status) && !deriveCleanupPresentation(workload)?.providerAbsentConfirmed) return "running";
  return "not recorded";
}

// A terminal compute phase does not stop cleanup or cost updates. Keep polling
// admitted bursts until provider absence and the frozen estimate are both
// recorded, including manual-attention and post-delete receipt repair.
export function shouldPollWorkload(workload) {
  if (!workload) return false;
  if (!isTerminal(workload.status)) return true;
  if (!workload.burst_id) return false;
  return !deriveCleanupPresentation(workload)?.providerAbsentConfirmed || !costObservation(workload);
}

// Detail responses are complete snapshots. Omitted live fields must clear the
// older list values, not revive them through object-spread fallback.
export function mergeWorkloadDetail(fromList, detail) {
  if (!detail) return fromList;
  if (!fromList) return detail;
  const result = { ...fromList, ...detail, spec: fromList.spec ?? detail.spec };
  for (const field of ["spent_usd", "gpu_util_percent", "last_heartbeat_at", "cost", "cleanup"]) {
    delete result[field];
    if (Object.hasOwn(detail, field)) result[field] = detail[field];
  }
  return result;
}

// How central says it arrived at the number. Spelling out the known bases
// keeps the detail readable; a basis this console has never
// seen is still shown, de-underscored, because an unlabelled method beats a
// number with no stated method at all.
const COST_BASIS_TEXT = {
  hourly_rate_x_runtime_to_reap_claim: "quoted hourly rate × measured runtime, frozen when teardown was claimed",
  hourly_rate_x_runtime_to_provider_delete: "quoted hourly rate × measured runtime, frozen at confirmed provider deletion",
};

export function costBasisText(basis) {
  const key = typeof basis === "string" ? basis.trim() : "";
  if (!key) return "not reported";
  return COST_BASIS_TEXT[key] || key.replaceAll("_", " ");
}

function formatRuntime(seconds) {
  const total = reportedNumber(seconds);
  if (total === null) return "not reported";
  const whole = Math.round(total);
  const hours = Math.floor(whole / 3600);
  const minutes = Math.floor((whole % 3600) / 60);
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${whole % 60}s`;
  return `${whole}s`;
}

// Enough provenance for a reader to explain the number to someone else: what
// rate it was quoted at, how long central measured, when the estimate stopped
// moving, which backend and burst it belongs to, and how it was computed.
export function costDetailRows(workload) {
  const cost = costObservation(workload);
  if (!cost) return [{ label: "Est. cost", value: costCellLabel(workload) }];
  const hourly = reportedNumber(cost.hourly_usd);
  return [
    { label: "Est. cost", value: formatUSD(cost.usd) },
    { label: "Quoted rate", value: hourly === null ? "not reported" : `${formatUSD(hourly)} / hour` },
    { label: "Measured runtime", value: formatRuntime(cost.runtime_seconds) },
    { label: "Frozen", value: cost.frozen_at ? formatDate(cost.frozen_at) : "not reported" },
    { label: "Backend", value: cost.backend || "not reported" },
    { label: "Burst receipt", value: cost.burst_id || "not reported" },
    { label: "Basis", value: costBasisText(cost.basis) },
  ];
}

// A coarse "how long since central last heard from the burst" label for the
// freshness column, keyed off the same heartbeat central sends. Takes `now`
// so tests can pin the clock without shimming Date.
export function freshnessLabel(heartbeatAt, now = new Date()) {
  if (!heartbeatAt) return "not reported";
  const then = new Date(heartbeatAt);
  const clock = now instanceof Date ? now : new Date(now);
  if (Number.isNaN(then.getTime()) || Number.isNaN(clock.getTime())) return "not reported";
  const seconds = Math.max(0, Math.round((clock.getTime() - then.getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m ago`;
}

// The rows the live-spend panel prints while provider cost remains live. Every row is
// labelled from what central reported, so an absent number, a partial pair,
// and an out-of-range one all read as three separate honest states rather
// than the same "—".
export function liveDetailRows(workload, now = new Date()) {
  const live = liveSpendObservation(workload);
  const telemetry = liveTelemetryObservation(workload);
  let utilization;
  let freshness;
  if (telemetry.present) {
    const rounded = Math.round(telemetry.percent);
    utilization = telemetry.percent === 0 ? "0% (idle)" : `${rounded}%`;
    freshness = freshnessLabel(telemetry.heartbeatAt, now);
  } else if (telemetry.malformed) {
    utilization = "not reported · signal malformed";
    freshness = "not reported · signal malformed";
  } else {
    utilization = "not reported";
    freshness = "not reported";
  }
  return [
    { label: "Accrued spend", value: live ? formatUSD(live.usd) : "not reported" },
    { label: "GPU utilization", value: utilization },
    { label: "Telemetry updated", value: freshness },
  ];
}

export function summarizeRuns(workloads) {
  const totals = { active: 0, succeeded: 0, failed: 0, estimatedUSD: 0, estimateRecords: 0, total: workloads.length };
  for (const workload of workloads) {
    if (!isTerminal(workload.status)) totals.active += 1;
    else if (isSucceeded(workload.status)) totals.succeeded += 1;
    else totals.failed += 1;
    const cost = costObservation(workload);
    if (cost) {
      totals.estimatedUSD += cost.usd;
      totals.estimateRecords += 1;
    }
  }
  return totals;
}

function newest(a, b) {
  if (!a) return b;
  if (!b) return a;
  return new Date(b) > new Date(a) ? b : a;
}

// burstTargets groups the tenant's records by the placement they actually
// asked for. It is an observation of submissions, not an inventory read: a
// backend that has never been requested cannot appear, and one that has been
// requested does not prove capacity is live there right now.
export function burstTargets(workloads) {
  const byKey = new Map();
  for (const workload of workloads) {
    const spec = workload.spec?.spec || {};
    const backend = spec.backend || "auto";
    const region = spec.region || "";
    const key = `${backend}|${region}`;
    const target = byKey.get(key) || {
      key,
      backend,
      region,
      runs: 0,
      active: 0,
      gpu: false,
      bursts: new Set(),
      lastAt: null,
    };
    target.runs += 1;
    if (!isTerminal(workload.status)) target.active += 1;
    if (spec.gpu) target.gpu = true;
    if (workload.burst_id) target.bursts.add(workload.burst_id);
    target.lastAt = newest(target.lastAt, workload.created_at);
    byKey.set(key, target);
  }
  return [...byKey.values()]
    .map((target) => ({ ...target, bursts: target.bursts.size }))
    .sort((a, b) => b.runs - a.runs || a.key.localeCompare(b.key));
}

export function lastActivityAt(workloads) {
  return workloads.reduce((latest, workload) => newest(latest, workload.created_at), null);
}

// The origin central stamps when the Connector saw a Pending pod carrying both
// burst keys and submitted capacity for it. Which controller created that pod —
// KEDA, HPA, Argo, a hand-applied Deployment — is not observable at the seam,
// so the field says how the submission arrived and nothing about who wrote it.
export const PENDING_POD_ORIGIN = "pending-pod";

// Exported so the panel can state the bound it is rendering under instead of
// printing a count the reader would take for a total.
export const CONTROLLER_TRIGGERED_LIMIT = 6;

function requestedMillis(workload) {
  const at = new Date(workload?.created_at).getTime();
  return Number.isNaN(at) ? Number.NEGATIVE_INFINITY : at;
}

// Only an exact pending-pod origin counts. nodeOnly, a cluster-credential
// submitter, the manifest text, and a name that reads like an autoscaler are
// all shapes a human can produce by hand, and a record with no origin at all is
// one from a central that never reported the field — neither is evidence that a
// controller triggered anything. The result is bounded because it renders as a
// list, not a ledger: a caller asking for an unusable bound gets the default
// rather than the tenant's whole history.
export function controllerTriggeredWorkloads(workloads, limit = CONTROLLER_TRIGGERED_LIMIT) {
  const records = Array.isArray(workloads) ? workloads : [];
  const bound = Number.isSafeInteger(limit) && limit >= 0 ? limit : CONTROLLER_TRIGGERED_LIMIT;
  return records
    .filter((workload) => workload?.submission_origin === PENDING_POD_ORIGIN)
    .sort((a, b) => requestedMillis(b) - requestedMillis(a))
    .slice(0, bound);
}

export function usageByTemplate(workloads) {
  const byId = new Map();
  for (const workload of workloads) {
    const template = templateForWorkload(workload);
    const row = byId.get(template.id) || { id: template.id, title: template.title, runs: 0, active: 0, estimatedUSD: 0, estimateRecords: 0 };
    row.runs += 1;
    if (!isTerminal(workload.status)) row.active += 1;
    const cost = costObservation(workload);
    if (cost) {
      row.estimatedUSD += cost.usd;
      row.estimateRecords += 1;
    }
    byId.set(template.id, row);
  }
  return [...byId.values()].sort((a, b) => b.runs - a.runs || a.id.localeCompare(b.id));
}

// usageByMonth buckets on the UTC month a workload was requested. A record with
// an unusable created_at is counted under "unknown" rather than dropped, so the
// rows always add up to the record count the page reports.
export function usageByMonth(workloads) {
  const byKey = new Map();
  for (const workload of workloads) {
    const at = new Date(workload.created_at);
    const key = Number.isNaN(at.getTime())
      ? "unknown"
      : `${at.getUTCFullYear()}-${String(at.getUTCMonth() + 1).padStart(2, "0")}`;
    const row = byKey.get(key) || { key, runs: 0, succeeded: 0, failed: 0, estimatedUSD: 0, estimateRecords: 0 };
    row.runs += 1;
    if (isTerminal(workload.status)) {
      if (isSucceeded(workload.status)) row.succeeded += 1;
      else row.failed += 1;
    }
    const cost = costObservation(workload);
    if (cost) {
      row.estimatedUSD += cost.usd;
      row.estimateRecords += 1;
    }
    byKey.set(key, row);
  }
  return [...byKey.values()].sort((a, b) => b.key.localeCompare(a.key));
}

// What central records on every accepted submission. Rendered from the records
// themselves so the page shows this tenant's rules rather than a claim about
// them; a record that predates provenance simply has no authorization block.
export function provenanceSummary(workloads) {
  const rules = new Map();
  const submitters = new Set();
  let recorded = 0;
  for (const workload of workloads) {
    const decision = workload.authorization;
    if (workload.submitted_by?.account_id) submitters.add(workload.submitted_by.account_id);
    if (!decision) continue;
    recorded += 1;
    const key = `${decision.rule || "unnamed rule"}@${decision.rule_version || "unversioned"}`;
    const row = rules.get(key) || { key, rule: decision.rule || "unnamed rule", version: decision.rule_version || "", runs: 0 };
    row.runs += 1;
    rules.set(key, row);
  }
  return {
    total: workloads.length,
    recorded,
    submitters: submitters.size,
    rules: [...rules.values()].sort((a, b) => b.runs - a.runs || a.key.localeCompare(b.key)),
  };
}

// The namespaces central says this tenant may launch into, in central's own
// order — the first entry is the one an unqualified submission is decided
// against, so the order is load-bearing rather than presentational. A summary
// from a central that predates the field, or a fixture written before it, reads
// as the single namespace every tenant has always had; the launch form has to
// offer something, and offering nothing would strand the reader.
export function workloadNamespaces(tenant) {
  const listed = Array.isArray(tenant?.workload_namespaces) ? tenant.workload_namespaces : [];
  const allowed = [];
  for (const entry of listed) {
    const name = typeof entry === "string" ? entry.trim() : "";
    if (name && !allowed.includes(name)) allowed.push(name);
  }
  return allowed.length ? allowed : [DEFAULT_WORKLOAD_NAMESPACE];
}

// The limit keys /v1/account is known to report. A tenant summary that carries
// a key this console has never seen is still shown — unlabelled but present
// beats silently dropped — and a 0 is central's "no ceiling", not "nothing".
const LIMIT_LABELS = {
  max_concurrent_bursts: { label: "Concurrent bursts", unit: "count" },
  max_hourly_usd: { label: "Hourly spend ceiling", unit: "usd" },
  max_monthly_spend_usd: { label: "Monthly spend ceiling", unit: "usd" },
};

export function limitRows(limits) {
  if (!limits || typeof limits !== "object") return [];
  return Object.entries(limits)
    .filter(([, value]) => typeof value === "number" && Number.isFinite(value))
    .map(([key, value]) => {
      const known = LIMIT_LABELS[key];
      return {
        key,
        label: known?.label || key.replaceAll("_", " "),
        unit: known?.unit || "count",
        value,
        unlimited: value === 0,
      };
    })
    .sort((a, b) => a.key.localeCompare(b.key));
}

function outcomeObservation(workload) {
  const outcome = workload?.outcome;
  if (!outcome || typeof outcome !== "object") return null;
  return outcome;
}

export function workloadReceiptDetails(workload) {
  const spec = workload?.spec?.spec?.data || null;
  const outcome = outcomeObservation(workload);
  const terminal = isTerminal(workload?.status);

  const hasRequestSpec = spec !== null && typeof spec === "object";
  const requestedInputCount = hasRequestSpec ? reportedCount(spec.pull_before_run_inputs) : null;
  const requestedOutputCount = hasRequestSpec ? reportedCount(spec.push_after_run_outputs) : null;
  const requestedInputs = requestedInputCount !== null
    ? `${requestedInputCount} object input${requestedInputCount === 1 ? "" : "s"}`
    : "None requested";
  const inputLimit = hasRequestSpec && reportedNumber(spec.input_size_hint_gb) !== null
    ? `${spec.input_size_hint_gb} GiB`
    : "not set";
  const requestedOutputs = requestedOutputCount !== null
    ? `${requestedOutputCount} object output${requestedOutputCount === 1 ? "" : "s"}`
    : "None requested";
  const uploadLimit = hasRequestSpec && reportedNumber(spec.output_max_upload_gb) !== null
    ? `${spec.output_max_upload_gb} GiB`
    : "not set";

  const compute = outcome?.compute;
  const hasComputeOutcome = compute != null && typeof compute === "object";
  const computeResultRaw = compute?.result ? String(compute.result).toLowerCase() : null;
  const computeReason = compute?.reason ? String(compute.reason).trim() : null;

  let computeStatusText = "no outcome recorded";
  let computeBadgeTone = "neutral";

  if (computeResultRaw === "succeeded" || computeResultRaw === "completed" || computeResultRaw === "complete") {
    computeStatusText = "succeeded";
    computeBadgeTone = "ok";
  } else if (computeResultRaw === "failed" || computeResultRaw === "cancelled" || computeResultRaw === "canceled") {
    computeStatusText = computeResultRaw;
    computeBadgeTone = "danger";
  } else if (computeResultRaw) {
    computeStatusText = computeResultRaw.replaceAll("_", " ");
    computeBadgeTone = "info";
  } else if (hasComputeOutcome) {
    computeStatusText = "observed";
    computeBadgeTone = "info";
  } else if (!terminal) {
    computeStatusText = "in flight";
    computeBadgeTone = "info";
  } else {
    computeStatusText = "no outcome recorded";
    computeBadgeTone = "neutral";
  }

  const computeLabel = computeReason ? `${computeStatusText} · ${computeReason}` : computeStatusText;

  const artifacts = outcome?.artifacts;
  const artifactResultRaw = artifacts?.result ? String(artifacts.result).toLowerCase() : null;
  const artifactReason = artifacts?.reason ? String(artifacts.reason).trim() : null;
  const hasArtifactOutcome = artifacts != null && typeof artifacts === "object";

  let artifactStatusText = "no outcome recorded";
  let artifactBadgeTone = "neutral";

  if (artifactResultRaw === "succeeded" || artifactResultRaw === "completed" || artifactResultRaw === "complete") {
    artifactStatusText = "succeeded";
    artifactBadgeTone = "ok";
  } else if (artifactResultRaw === "failed") {
    artifactStatusText = "failed";
    artifactBadgeTone = "danger";
  } else if (artifactResultRaw === "skipped" || artifactResultRaw === "none") {
    artifactStatusText = "skipped";
    artifactBadgeTone = "neutral";
  } else if (artifactResultRaw) {
    artifactStatusText = artifactResultRaw.replaceAll("_", " ");
    artifactBadgeTone = "info";
  } else if (hasArtifactOutcome) {
    artifactStatusText = "observed";
    artifactBadgeTone = "info";
  } else if (!terminal) {
    artifactStatusText = "not observed yet";
    artifactBadgeTone = "info";
  } else if (!hasRequestSpec || (reportedNumber(spec.push_after_run_outputs) ?? 0) === 0) {
    artifactStatusText = "not requested";
    artifactBadgeTone = "neutral";
  } else {
    artifactStatusText = "no outcome recorded";
    artifactBadgeTone = "neutral";
  }

  const artifactLabel = artifactReason ? `${artifactStatusText} · ${artifactReason}` : artifactStatusText;

  const objectsUploaded = hasArtifactOutcome ? reportedCount(artifacts.objects_uploaded) : null;
  const bytesUploaded = hasArtifactOutcome ? reportedCount(artifacts.bytes_uploaded) : null;

  return {
    hasRequestSpec,
    hasOutcome: outcome !== null,
    hasComputeOutcome,
    hasArtifactOutcome,
    requestedInputs,
    inputLimit,
    requestedOutputs,
    uploadLimit,
    computeResultRaw,
    computeReason,
    computeStatusText,
    computeBadgeTone,
    computeLabel,
    artifactResultRaw,
    artifactReason,
    artifactStatusText,
    artifactBadgeTone,
    artifactLabel,
    objectsUploaded,
    bytesUploaded,
  };
}
