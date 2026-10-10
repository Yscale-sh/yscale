import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewAccount,
  previewAddMember,
  previewCreateTenant,
  previewMembers,
  previewRemoveMember,
  previewUpdateMemberRole,
  previewUsage,
} from "./devPreview.js";

// Same-origin client for the account seam (server/proxy.go). The bearer is
// attached per call from the in-memory session. Same-origin cookies must reach
// Cloudflare Access on the dev preview; the Go seam rebuilds every upstream
// request and never forwards those browser cookies to the account service.

export { ApiError } from "./apiError.js";

const MESSAGES = {
  0: "Could not reach the account service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This account is not allowed to see that.",
  404: "That record does not exist.",
  429: "Too many requests. Try again in a moment.",
  502: "The account service did not answer.",
  503: "The account service is not configured for this environment.",
};

async function call(path, { token, method = "GET", body, signal } = {}) {
  const headers = { Authorization: `Bearer ${token}`, Accept: "application/json" };
  let requestBody;
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    requestBody = JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(path, {
      method,
      headers,
      credentials: "same-origin",
      body: requestBody,
      signal,
    });
  } catch (err) {
    if (err?.name === "AbortError") throw err;
    throw new ApiError(0, MESSAGES[0]);
  }

  if (res.status === 204) return null;
  const payload = await res.json().catch(() => null);
  if (!res.ok) {
    const detail = typeof payload?.error === "string" ? payload.error : "";
    throw new ApiError(res.status, MESSAGES[res.status] || detail || `Request failed (HTTP ${res.status}).`, {
      code: detail,
    });
  }
  return payload;
}

export function fetchAccount({ token, signal }) {
  if (isDevPreviewToken(token)) return Promise.resolve(previewAccount(token));
  return call("/api/account", { token, signal });
}

export function createTenant({ token, name, signal }) {
  if (isDevPreviewToken(token)) return previewCreateTenant(name, token);
  return call("/api/account/tenants", {
    token,
    method: "POST",
    body: { name },
    signal,
  });
}

export function fetchMembers({ token, tenantId, after, limit = 100, signal }) {
  if (isDevPreviewToken(token)) return Promise.resolve(previewMembers(tenantId));
  const query = new URLSearchParams({ limit: String(limit) });
  if (after) query.set("after", after);
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/members?${query}`, { token, signal });
}

export function addMember({ token, tenantId, email, role, signal }) {
  if (isDevPreviewToken(token)) return previewAddMember(tenantId, email, role);
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/members`, {
    token,
    method: "POST",
    body: { email, role },
    signal,
  });
}

export function updateMemberRole({ token, tenantId, accountId, role, signal }) {
  if (isDevPreviewToken(token)) return previewUpdateMemberRole(tenantId, accountId, role);
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(accountId)}`, {
    token,
    method: "PATCH",
    body: { role },
    signal,
  });
}

export function removeMember({ token, tenantId, accountId, signal }) {
  if (isDevPreviewToken(token)) {
    return previewRemoveMember(tenantId, accountId)
      ? Promise.resolve(null)
      : Promise.reject(new ApiError(404, MESSAGES[404]));
  }
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(accountId)}`, {
    token,
    method: "DELETE",
    signal,
  });
}

export function fetchUsage({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return Promise.resolve(previewUsage(tenantId));
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/usage`, { token, signal });
}
