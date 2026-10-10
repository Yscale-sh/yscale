import { ApiError } from "./accountApi.js";
import {
  isDevPreviewToken,
  previewCancelWorkload,
  previewCreateWorkload,
  previewPlacementPreview,
  previewWorkloadLogs,
  previewRetryWorkload,
  previewWorkload,
  previewWorkloads,
} from "./devPreview.js";
import { IDEMPOTENCY_HEADER } from "./workloadIdempotency.js";

// The header server/proxy.go validates and forwards to central. It carries the
// cluster the run should land on; nothing else about the target crosses.
export const CLUSTER_HEADER = "X-Cluster-ID";

// The catalog entry this submission was composed from. It is provenance rather
// than spec — central stores it on the record — and it travels as a pair, so a
// run can never be recorded against an id with no version behind it.
export const TEMPLATE_ID_HEADER = "X-Template-ID";
export const TEMPLATE_VERSION_HEADER = "X-Template-Version";

// The signed placement token from a successful preview. Carried on create so
// central can bind the launch to the previewed placement decision.
export const PLACEMENT_TOKEN_HEADER = "X-Yscale-Placement-Token";

const MESSAGES = {
  0: "Could not reach the workload service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "Your tenant role does not allow that workload action.",
  404: "That workload does not exist in this tenant.",
  409: "This submission is already being handled. Wait a moment, then retry the same request.",
  413: "The workload YAML is too large.",
  415: "The workload request was not valid YAML content.",
  429: "Too many requests. Try again in a moment.",
  502: "The workload service did not answer.",
  503: "The workload service is unavailable in this environment.",
  504: "The cluster connector did not return logs before the request timed out.",
};

// Central answers `error` with a stable code and `message` with the sentence a
// human should act on — the in_progress retry and the idempotency conflicts put
// everything useful in `message`. Bounded and whitespace-collapsed so an
// upstream cannot paste an essay into the submit form.
const MAX_MESSAGE = 240;
const BILLING_KEYS = new Set(["hold_id", "state", "reserved_micro_usd", "captured_micro_usd", "currency", "quote_id", "pricing_version"]);
const BILLING_STATES = new Set(["pending", "captured", "released", "expired"]);

export function normalizeWorkloadBilling(value) {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).length !== BILLING_KEYS.size || Object.keys(value).some((key) => !BILLING_KEYS.has(key))) {
    throw new ApiError(502, "The workload service returned an invalid billing receipt.");
  }
  const clean = (item) => typeof item === "string" && item === item.trim() && item.length > 0 && item.length <= 128 ? item : "";
  if (!Number.isSafeInteger(value.hold_id) || value.hold_id <= 0 || !BILLING_STATES.has(value.state) || !Number.isSafeInteger(value.reserved_micro_usd) || value.reserved_micro_usd < 0
      || !Number.isSafeInteger(value.captured_micro_usd) || value.captured_micro_usd < 0 || value.currency !== "USD" || !clean(value.quote_id) || !Number.isSafeInteger(value.pricing_version) || value.pricing_version <= 0) {
    throw new ApiError(502, "The workload service returned an invalid billing receipt.");
  }
  return { holdId: value.hold_id, state: value.state, reservedMicroUsd: value.reserved_micro_usd, capturedMicroUsd: value.captured_micro_usd, currency: "USD", quoteId: value.quote_id, pricingVersion: value.pricing_version };
}

function withBilling(workload) {
  if (!workload || typeof workload !== "object" || Array.isArray(workload)) throw new ApiError(502, "The workload service returned an invalid record.");
  const billing = normalizeWorkloadBilling(workload.billing);
  const result = { ...workload };
  if (billing) result.billing = billing;
  return result;
}

function safeMessage(value) {
  if (typeof value !== "string") return "";
  const text = value.replace(/\s+/g, " ").trim();
  return text.length > MAX_MESSAGE ? `${text.slice(0, MAX_MESSAGE - 1)}…` : text;
}

async function call(path, { token, method = "GET", body, signal, idempotencyKey, clusterId, template, placementToken, includeErrorPayload = false } = {}) {
  const headers = { Authorization: `Bearer ${token}`, Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "text/yaml";
  // The caller owns the key for the whole logical submission; this layer only
  // carries it, so a retried fetch never invents a second paid run.
  if (idempotencyKey) headers[IDEMPOTENCY_HEADER] = idempotencyKey;
  // The target cluster is routing, not workload spec: it rides as a header the
  // seam validates and forwards, and never appears in the YAML central stores.
  if (clusterId) headers[CLUSTER_HEADER] = clusterId;
  // Both or neither: the seam refuses half a receipt, so sending one alone would
  // only turn a launch into a 400 the reader cannot act on.
  if (template?.id && template?.version) {
    headers[TEMPLATE_ID_HEADER] = template.id;
    headers[TEMPLATE_VERSION_HEADER] = String(template.version);
  }
  if (placementToken) headers[PLACEMENT_TOKEN_HEADER] = placementToken;
  let response;
  try {
    response = await fetch(path, {
      method,
      headers,
      body,
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  if (response.status === 204) return null;
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = safeMessage(payload?.error);
    const message = safeMessage(payload?.message);
    throw new ApiError(response.status, message || detail || MESSAGES[response.status] || `Request failed (HTTP ${response.status}).`, {
      code: safeMessage(payload?.code) || detail,
      payload: includeErrorPayload ? payload : null,
    });
  }
  return payload;
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/workloads`;
}

export async function previewPlacement({ token, tenantId, yaml, clusterId, signal }) {
  if (isDevPreviewToken(token)) return Promise.resolve(previewPlacementPreview(tenantId, yaml, clusterId));
  const path = `/api/tenants/${encodeURIComponent(tenantId)}/placement-preview`;
  try {
    return await call(path, { token, method: "POST", body: yaml, clusterId, signal, includeErrorPayload: true });
  } catch (error) {
    if (error?.payload?.status === "rejected") {
      return {
        ...error.payload,
        code: safeMessage(error.payload.code),
        message: safeMessage(error.payload.message) || error.message,
      };
    }
    throw error;
  }
}

export function fetchWorkloads({ token, tenantId, signal }) {
  const result = isDevPreviewToken(token) ? Promise.resolve(previewWorkloads(tenantId)) : call(base(tenantId), { token, signal });
  return result.then((payload) => {
    if (!payload || typeof payload !== "object" || !Array.isArray(payload.workloads)) throw new ApiError(502, "The workload service returned an invalid list.");
    return { ...payload, workloads: payload.workloads.map(withBilling) };
  });
}

// idempotencyKey belongs to the submission, not to this call: the caller keeps
// it stable across retries of the same YAML and target. The dev preview never
// crosses the network, so it has nothing to make idempotent.
export function createWorkload({ token, tenantId, yaml, clusterId, template, placementToken, idempotencyKey, signal }) {
  if (isDevPreviewToken(token)) {
    const accepted = previewCreateWorkload(tenantId, yaml, clusterId, template, placementToken);
    return accepted
      ? Promise.resolve(accepted)
      : Promise.reject(new ApiError(403, MESSAGES[403]));
  }
  return call(base(tenantId), { token, method: "POST", body: yaml, clusterId, template, placementToken, idempotencyKey, signal });
}

export function fetchWorkload({ token, tenantId, workloadId, signal }) {
  if (isDevPreviewToken(token)) {
    const workload = previewWorkload(tenantId, workloadId);
    return workload
      ? Promise.resolve(withBilling(workload))
      : Promise.reject(new ApiError(404, MESSAGES[404]));
  }
  return call(`${base(tenantId)}/${encodeURIComponent(workloadId)}`, { token, signal }).then(withBilling);
}

export function fetchWorkloadLogs({ token, tenantId, workloadId, tail = 200, signal }) {
  if (isDevPreviewToken(token)) {
    const snapshot = previewWorkloadLogs(tenantId, workloadId, tail);
    return snapshot
      ? Promise.resolve(snapshot)
      : Promise.reject(new ApiError(404, MESSAGES[404]));
  }
  const boundedTail = Number.isInteger(tail) && tail >= 1 && tail <= 1000 ? tail : 200;
  return call(`${base(tenantId)}/${encodeURIComponent(workloadId)}/logs?tail=${boundedTail}`, { token, signal });
}

export function cancelWorkload({ token, tenantId, workloadId, signal }) {
  if (isDevPreviewToken(token)) {
    return previewCancelWorkload(tenantId, workloadId)
      ? Promise.resolve(null)
      : Promise.reject(new ApiError(404, MESSAGES[404]));
  }
  return call(`${base(tenantId)}/${encodeURIComponent(workloadId)}`, { token, method: "DELETE", signal });
}

export function retryWorkload({ token, tenantId, workloadId, idempotencyKey, signal }) {
  if (isDevPreviewToken(token)) {
    const accepted = previewRetryWorkload(tenantId, workloadId);
    return accepted
      ? Promise.resolve(accepted)
      : Promise.reject(new ApiError(404, MESSAGES[404]));
  }
  return call(`${base(tenantId)}/${encodeURIComponent(workloadId)}/retry`, { token, method: "POST", idempotencyKey, signal });
}
