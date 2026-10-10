// Pure derivation of workload lifecycle timeline steps and cleanup
// presentation from Central's authoritative response shape. JSX renders
// the output; it never reimplements the state machine.

import { formatDate, statusLabel } from "./format.js";

const TERMINAL = new Set(["succeeded", "failed", "cancelled", "canceled", "complete", "completed"]);

export function isTerminal(status) {
  return TERMINAL.has(String(status || "").toLowerCase());
}

export function validTimestamp(value) {
  if (!value || typeof value !== "string") return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : value;
}

function normalizeCleanup(workload) {
  const raw = workload?.cleanup;
  return raw != null && typeof raw === "object" && !Array.isArray(raw) ? raw : null;
}

function cleanupRawStep(cleanup) {
  if (!cleanup) {
    return { label: "Teardown evidence not recorded", time: null, done: false, special: "unresolved" };
  }

  const state = typeof cleanup.state === "string" ? cleanup.state.toLowerCase() : "";

  switch (state) {
    case "queued":
      return { label: "Teardown queued", time: null, done: false };
    case "deleting":
      return { label: "Teardown in progress", time: null, done: false };
    case "retrying":
      return { label: "Teardown retrying", time: validTimestamp(cleanup.updated_at), done: false };
    case "manual_attention":
      return { label: "Manual teardown required", time: validTimestamp(cleanup.updated_at), done: false, special: "attention" };
    case "terminated": {
      const deletedAt = validTimestamp(cleanup.deleted_at);
      if (deletedAt) return { label: "Provider capacity removed", time: deletedAt, done: true };
      return { label: "Provider removal unconfirmed", time: null, done: false, special: "unresolved" };
    }
    default:
      return { label: "Unknown cleanup state", time: null, done: false, special: "unresolved" };
  }
}

// Returns an array of { label, time, status } where status is one of:
// "done" | "current" | "pending" | "attention" | "unresolved"
export function deriveTimelineSteps(workload) {
  if (!workload) return [];

  const status = String(workload.status || "unknown").toLowerCase();
  const terminal = isTerminal(status);
  const cleanup = normalizeCleanup(workload);

  const providerCreated = validTimestamp(cleanup?.provider_created_at);
  const hasPlacementEvidence = !!workload.burst_id || !!workload.started_at;

  const raw = [
    { label: "Requested", time: validTimestamp(workload.created_at), done: true },
    { label: "Capacity placed", time: providerCreated || null, done: !!providerCreated || hasPlacementEvidence },
    { label: "Running", time: validTimestamp(workload.started_at), done: !!workload.started_at },
    { label: terminal ? statusLabel(status) : "Completion", time: validTimestamp(workload.finished_at), done: terminal },
  ];

  if (terminal || cleanup) {
    raw.push(cleanupRawStep(cleanup));
  }

  let foundCurrent = false;
  return raw.map((step) => {
    if (step.done) return { label: step.label, time: step.time, status: "done" };
    if (step.special) { foundCurrent = true; return { label: step.label, time: step.time, status: step.special }; }
    if (!foundCurrent) { foundCurrent = true; return { label: step.label, time: step.time, status: "current" }; }
    return { label: step.label, time: step.time, status: "pending" };
  });
}

// Returns the structured data the cleanup panel renders, or null when
// the panel should not appear.
export function deriveCleanupPresentation(workload) {
  if (!workload) return null;

  const terminal = isTerminal(workload.status);
  const cleanup = normalizeCleanup(workload);

  if (!terminal && !cleanup) return null;

  if (!cleanup) {
    return {
      available: false,
      stateLabel: "not recorded",
      reason: null,
      requestedAt: null,
      providerAbsentConfirmed: false,
      providerAbsentText: "unknown",
      durableReap: false,
      hasProviderDetail: false,
      provider: null,
      region: null,
      sku: null,
      tone: "warn",
      message: "Central did not record cleanup evidence for this workload. Teardown state is unknown.",
    };
  }

  const state = typeof cleanup.state === "string" ? cleanup.state.toLowerCase() : "";
  const reason = cleanup.reason || "not reported";
  const requestedAt = validTimestamp(cleanup.requested_at);
  const deletedAt = validTimestamp(cleanup.deleted_at);
  const durableReap = cleanup.durable_reap_receipt === true;

  const provider = cleanup.provider || null;
  const region = cleanup.region || null;
  const sku = cleanup.sku || null;
  const hasProviderDetail = !!(provider || region || sku);

  let stateLabel, providerAbsentConfirmed, providerAbsentText, tone, message;

  switch (state) {
    case "queued":
      stateLabel = "queued";
      providerAbsentConfirmed = false;
      providerAbsentText = "not yet";
      tone = "plain";
      message = null;
      break;
    case "deleting":
      stateLabel = "deleting";
      providerAbsentConfirmed = false;
      providerAbsentText = "not yet";
      tone = "plain";
      message = null;
      break;
    case "retrying":
      stateLabel = "retrying";
      providerAbsentConfirmed = false;
      providerAbsentText = "not yet";
      tone = "warn";
      message = "Central is actively retrying teardown with the provider.";
      break;
    case "manual_attention":
      stateLabel = "manual attention";
      providerAbsentConfirmed = false;
      providerAbsentText = "unproven";
      tone = "danger";
      message = "Automatic teardown retries have stopped. Review the provider account to confirm whether capacity has been removed, and contact your provider if cloud costs continue.";
      break;
    case "terminated":
      stateLabel = "terminated";
      providerAbsentConfirmed = !!deletedAt;
      providerAbsentText = deletedAt ? null : "timestamp not recorded";
      tone = deletedAt ? "plain" : "warn";
      message = deletedAt ? null : "Cleanup terminated but the provider-absence timestamp was not recorded.";
      break;
    default:
      stateLabel = state || "unknown";
      providerAbsentConfirmed = false;
      providerAbsentText = "unknown";
      tone = "warn";
      message = "Unrecognized cleanup state. Teardown status is uncertain.";
      break;
  }

  return {
    available: true,
    state,
    stateLabel,
    reason,
    requestedAt,
    providerAbsentConfirmed,
    providerAbsentTime: deletedAt,
    providerAbsentText,
    durableReap,
    hasProviderDetail,
    provider,
    region,
    sku,
    tone,
    message,
  };
}

// The detail panel and timeline must consume the same deletion proof. Neither
// a terminal compute phase nor a legacy confirmed_at field proves absence.
export function cleanupDetailRows(workload) {
  const cleanup = deriveCleanupPresentation(workload);
  const needsHuman = cleanup?.state === "manual_attention"
    ? "yes · automatic teardown stopped retrying"
    : cleanup?.state === "retrying"
      ? "no · automatic cleanup is retrying"
      : cleanup?.providerAbsentConfirmed ? "no" : "not reported";
  return [
    { label: "Cleanup state", value: cleanup?.stateLabel || "not reported" },
    { label: "Provider resource", value: cleanup?.providerAbsentConfirmed ? `confirmed absent ${formatDate(cleanup.providerAbsentTime)}` : "absence not declared" },
    { label: "Teardown reason", value: typeof cleanup?.reason === "string" ? cleanup.reason : "not reported" },
    { label: "Durable cleanup receipt", value: cleanup?.durableReap ? "recorded" : "not recorded" },
    { label: "Needs a human", value: needsHuman },
  ];
}
