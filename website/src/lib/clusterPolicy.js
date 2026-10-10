import { ApiError } from "./apiError.js";
import { isClusterId } from "./clusters.js";
import {
  isDevPreviewToken,
  previewClusterPolicy,
  previewPutClusterPolicy,
} from "./devPreview.js";

export const CLUSTER_POLICY_AUTOS = Object.freeze(["require_pin", "ordered"]);

const MESSAGES = {
  0: "Could not reach the cluster policy service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot change the cluster policy.",
  404: "That tenant does not exist for this account.",
  413: "The cluster policy is too large.",
  415: "The cluster policy must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The cluster policy service did not answer.",
  503: "The cluster policy service is unavailable in this environment.",
};

function list(value, field) {
  if (!Array.isArray(value)) throw new ApiError(502, `The cluster policy answered with an invalid ${field} list.`);
  const out = [];
  const seen = new Set();
  for (const item of value) {
    if (!isClusterId(item)) throw new ApiError(502, `The cluster policy answered with an invalid ${field} cluster id.`);
    if (seen.has(item)) throw new ApiError(502, `The cluster policy answered with a duplicate ${field} cluster id.`);
    seen.add(item);
    out.push(item);
  }
  return out;
}

export function normalizeClusterPolicy(payload) {
  if (!payload || typeof payload !== "object" || !payload.policy || typeof payload.policy !== "object") {
    throw new ApiError(502, "The cluster policy service answered without a policy.");
  }
  const tenantId = typeof payload.tenant_id === "string" ? payload.tenant_id.trim() : "";
  const auto = payload.policy.auto;
  if (!tenantId || !CLUSTER_POLICY_AUTOS.includes(auto)) {
    throw new ApiError(502, "The cluster policy service answered with an invalid policy.");
  }
  const allow = list(payload.policy.allow, "allow");
  const deny = list(payload.policy.deny, "deny");
  const denied = new Set(deny);
  if (allow.some((id) => denied.has(id))) {
    throw new ApiError(502, "The cluster policy service answered with overlapping allow and deny lists.");
  }
  return {
    tenantId,
    role: typeof payload.role === "string" ? payload.role.trim() : "",
    changed: payload.changed === true,
    policy: {
      allow,
      deny,
      auto,
    },
  };
}

export function normalizeClusterPolicyDraft({ allow = [], deny = [], auto = "require_pin" } = {}) {
  if (!CLUSTER_POLICY_AUTOS.includes(auto)) {
    throw new Error("Choose a valid automatic placement mode.");
  }
  const parse = (value, name) => {
    const values = Array.isArray(value) ? value : String(value || "").split(/[\s,]+/);
    const out = [];
    const seen = new Set();
    for (const raw of values) {
      const id = String(raw || "").trim();
      if (!id) continue;
      if (!isClusterId(id)) throw new Error(`${name} contains an invalid cluster id: ${id}`);
      if (seen.has(id)) throw new Error(`${name} contains a duplicate cluster id: ${id}`);
      seen.add(id);
      out.push(id);
    }
    return out;
  };
  const cleanAllow = parse(allow, "Allow");
  const cleanDeny = parse(deny, "Deny");
  const denied = new Set(cleanDeny);
  const overlap = cleanAllow.find((id) => denied.has(id));
  if (overlap) throw new Error(`A cluster cannot be both allowed and denied: ${overlap}`);
  return { allow: cleanAllow, deny: cleanDeny, auto };
}

async function call(path, { token, method = "GET", body, signal } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = typeof payload?.error === "string" ? payload.error : "";
    throw new ApiError(response.status, MESSAGES[response.status] || detail || `Request failed (HTTP ${response.status}).`, {
      code: detail,
    });
  }
  return normalizeClusterPolicy(payload);
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/cluster-policy`;
}

export function fetchClusterPolicy({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return previewClusterPolicy(tenantId).then(normalizeClusterPolicy);
  return call(base(tenantId), { token, signal });
}

export function putClusterPolicy({ token, tenantId, policy, signal }) {
  const body = normalizeClusterPolicyDraft(policy);
  if (isDevPreviewToken(token)) return previewPutClusterPolicy(tenantId, body).then(normalizeClusterPolicy);
  return call(base(tenantId), { token, method: "PUT", body, signal });
}
