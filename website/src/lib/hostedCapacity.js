import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewHostedCapacity,
  previewRequestHostedCapacity,
} from "./devPreview.js";

const STATUSES = new Set(["not_requested", "requested", "assigned"]);
const ROLES = new Set(["owner", "admin", "member", "viewer"]);
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

const MESSAGES = {
  0: "Could not reach the hosted capacity service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "Only a tenant owner or admin can request shared capacity.",
  404: "That tenant does not exist for this account.",
  429: "Too many requests. Try again in a moment.",
  502: "The hosted capacity service did not answer.",
  503: "The hosted capacity service is unavailable in this environment.",
};

function cleanText(value) {
  return typeof value === "string" ? value.trim() : "";
}

export function normalizeHostedCapacity(payload, expectedTenantId = "") {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new ApiError(502, "The hosted capacity service answered with an invalid status.");
  }
  const tenantId = cleanText(payload.tenant_id);
  const role = cleanText(payload.role);
  const status = cleanText(payload.status);
  const hasRequestedAt = Object.hasOwn(payload, "requested_at");
  const requestedAt = hasRequestedAt ? cleanText(payload.requested_at) : "";
  if (!tenantId || (expectedTenantId && tenantId !== expectedTenantId) || !ROLES.has(role) || !STATUSES.has(status)) {
    throw new ApiError(502, "The hosted capacity service answered with an invalid status.");
  }
  if (hasRequestedAt && (typeof payload.requested_at !== "string" || !requestedAt || !RFC3339.test(requestedAt) || Number.isNaN(new Date(requestedAt).getTime()))) {
    throw new ApiError(502, "The hosted capacity service answered with an invalid requested time.");
  }
  if ((status === "requested") !== hasRequestedAt) {
    throw new ApiError(502, "The hosted capacity service answered with an inconsistent requested time.");
  }
  return { tenantId, role, status, requestedAt };
}

async function request(path, { token, method = "GET", signal } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const code = cleanText(payload?.error);
    const message = cleanText(payload?.message);
    throw new ApiError(response.status, message || MESSAGES[response.status] || code || `Request failed (HTTP ${response.status}).`, { code });
  }
  return payload;
}

function pathFor(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/hosted-capacity`;
}

export function fetchHostedCapacity({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) {
    if (signal?.aborted) return Promise.reject(signal.reason || new DOMException("Aborted", "AbortError"));
    return Promise.resolve(previewHostedCapacity(tenantId)).then((payload) => normalizeHostedCapacity(payload, tenantId));
  }
  return request(pathFor(tenantId), { token, signal }).then((payload) => normalizeHostedCapacity(payload, tenantId));
}

export function requestHostedCapacity({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) {
    if (signal?.aborted) return Promise.reject(signal.reason || new DOMException("Aborted", "AbortError"));
    return Promise.resolve(previewRequestHostedCapacity(tenantId)).then((payload) => normalizeHostedCapacity(payload, tenantId));
  }
  return request(pathFor(tenantId), { token, method: "POST", signal }).then((payload) => normalizeHostedCapacity(payload, tenantId));
}

// Pure state selection keeps copy and controls testable without coupling tests
// to React rendering details.
export function hostedCapacityCardState({ loadState, capacity, canManage, posting, fleetObserved, hosted, hostedReason }) {
  if (loadState === "loading") return { kind: "loading", label: "checking status" };
  if (loadState === "error") return { kind: "error", label: "status unavailable", canRetry: true };
  if (!STATUSES.has(capacity?.status)) return { kind: "error", label: "status unavailable", canRetry: true };
  if (capacity?.status === "requested") {
    return { kind: "requested", label: "request pending", requestedAt: capacity.requestedAt };
  }
  if (capacity?.status === "assigned") {
    if (!fleetObserved) return { kind: "assigned_waiting", label: "assigned" };
    if (!hosted) {
      return { kind: "assigned_unavailable", label: "assigned", reason: "The assigned hosted target is not present in the current cluster observation." };
    }
    if (hostedReason) return { kind: "assigned_unavailable", label: "assigned", reason: hostedReason };
    return { kind: "assigned_launchable", label: "assigned" };
  }
  return canManage
    ? { kind: "not_requested", label: "not requested", canRequest: true, posting: !!posting }
    : { kind: "not_requested_restricted", label: "not requested" };
}
