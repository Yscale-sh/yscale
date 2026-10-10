import { ApiError } from "./apiError.js";
import { normalizeClusterObservation, normalizeClusterRow } from "./clusters.js";
import { normalizeClusterPolicy } from "./clusterPolicy.js";
import {
  isDevPreviewToken,
  previewClusters,
  previewDeleteCluster,
  previewRegisterCluster,
  previewRotateClusterCredential,
} from "./devPreview.js";

// Same-origin client for the tenant's durable cluster registry
// (/v1/tenants/{tenant_id}/clusters behind server/proxy.go). The read is what
// the launch form targets a workload with, so a failure here is a failure the
// form has to show — never an empty list that reads like "no clusters". The
// writes hand back a connector credential exactly once; nothing in this module
// stores, logs, or re-reads it.

const MESSAGES = {
  0: "Could not reach the cluster service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot do that to the cluster registry.",
  404: "That tenant does not exist for this account.",
  409: "That conflicts with the registry's current state.",
  413: "The cluster registration is too large.",
  415: "The cluster registration must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The cluster service did not answer.",
  503: "The cluster service is unavailable in this environment.",
};

const READ_MESSAGES = {
  ...MESSAGES,
  403: "This account is not allowed to read this tenant's clusters.",
};

// Central answers `error` with a stable code and `message` with the sentence a
// human should act on. Bounded and whitespace-collapsed so an upstream cannot
// paste an essay into the page.
const MAX_MESSAGE = 240;

function safeMessage(value) {
  if (typeof value !== "string") return "";
  const message = value.replace(/\s+/g, " ").trim();
  return message.length > MAX_MESSAGE ? `${message.slice(0, MAX_MESSAGE - 1)}…` : message;
}

async function request(path, { token, method = "GET", body, signal, messages = MESSAGES } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, messages[0]);
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = safeMessage(payload?.error);
    const message = safeMessage(payload?.message);
    const code = /^[a-z0-9_]+$/.test(detail) ? detail : "";
    throw new ApiError(response.status, message || (code ? "" : detail) || messages[response.status] || `Request failed (HTTP ${response.status}).`, {
      code: code || detail,
    });
  }
  return payload;
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/clusters`;
}

function clusterPath(tenantId, clusterId) {
  return `${base(tenantId)}/${encodeURIComponent(clusterId)}`;
}

// The one-time credential answer register and rotate share. The token and the
// helm command are the whole point of the call, so an answer missing either is
// refused rather than revealed half-empty.
export function normalizeClusterCredential(payload) {
  const cluster = normalizeClusterRow(payload?.cluster);
  const connectorToken = typeof payload?.connector_token === "string" ? payload.connector_token.trim() : "";
  const helmCommand = typeof payload?.helm_install === "string" ? payload.helm_install.trim() : "";
  if (!cluster || !connectorToken || !helmCommand) {
    throw new ApiError(502, "The cluster service answered without a usable connector credential.");
  }
  return {
    tenantId: typeof payload?.tenant_id === "string" ? payload.tenant_id.trim() : "",
    cluster,
    connectorToken,
    helmCommand,
    role: typeof payload?.role === "string" ? payload.role.trim() : "",
  };
}

export function fetchClusters({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return previewClusters(tenantId);
  return request(base(tenantId), { token, signal, messages: READ_MESSAGES }).then((payload) => {
    // A 200 with an unreadable body is not an empty fleet. A payload that
    // carries no `clusters` array at all is a contract this console does not
    // recognise, and pretending it means zero clusters would silently strand
    // every launch form.
    if (!Array.isArray(payload?.clusters)) {
      throw new ApiError(502, "The cluster service answered without a cluster list.");
    }
    const observation = normalizeClusterObservation(payload);
    if (payload.policy !== undefined) {
      observation.policy = normalizeClusterPolicy(payload).policy;
    }
    return observation;
  });
}

// POST /clusters mints the durable row and its connector credential. The
// display name is the only required field; a custom id is opt-in.
export function registerCluster({ token, tenantId, name, clusterId = "", signal }) {
  if (isDevPreviewToken(token)) {
    return previewRegisterCluster(tenantId, { name, clusterId }).then(normalizeClusterCredential);
  }
  const body = { name, ...(clusterId ? { cluster_id: clusterId } : {}) };
  return request(base(tenantId), { token, method: "POST", body, signal }).then(normalizeClusterCredential);
}

// POST /clusters/{cluster}/credential revokes the current credential and mints
// a new one — the answer carries a helm upgrade command instead of an install.
export function rotateClusterCredential({ token, tenantId, clusterId, signal }) {
  if (isDevPreviewToken(token)) {
    return previewRotateClusterCredential(tenantId, clusterId).then(normalizeClusterCredential);
  }
  return request(`${clusterPath(tenantId, clusterId)}/credential`, { token, method: "POST", signal })
    .then(normalizeClusterCredential);
}

export function deleteCluster({ token, tenantId, clusterId, signal }) {
  if (isDevPreviewToken(token)) {
    return previewDeleteCluster(tenantId, clusterId).then(() => ({ deleted: true, clusterId }));
  }
  return request(clusterPath(tenantId, clusterId), { token, method: "DELETE", signal }).then((payload) => {
    if (payload?.deleted !== true) {
      throw new ApiError(502, "The cluster service answered without confirming the delete.");
    }
    return { deleted: true, clusterId };
  });
}
