import { ApiError } from "./apiError.js";
import { isDevPreviewToken, previewAudit } from "./devPreview.js";

// Same-origin client for the tenant governance journal
// (GET /v1/tenants/{tenant_id}/audit behind server/proxy.go). The journal is a
// management surface: central answers 403 for a member or a viewer, and this
// module passes that through rather than dressing it up as an empty page.

const MESSAGES = {
  0: "Could not reach the audit journal from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "Reading the audit journal requires the owner or admin role in this tenant.",
  404: "That tenant does not exist for this account.",
  429: "Too many requests. Try again in a moment.",
  502: "The audit service did not answer.",
  503: "The audit journal is unavailable in this environment.",
};

// Central's own ceiling (state.MaxAuditLimit); asking for more is a 400 there
// and a 400 at the website seam before that.
export const MAX_AUDIT_LIMIT = 200;

export function fetchAudit({ token, tenantId, after, limit = 50, signal }) {
  if (isDevPreviewToken(token)) return previewAudit(tenantId, { after, limit });
  const query = new URLSearchParams({ limit: String(limit) });
  if (after) query.set("after", after);
  return call(`/api/tenants/${encodeURIComponent(tenantId)}/audit?${query}`, { token, signal });
}

async function call(path, { token, signal } = {}) {
  let response;
  try {
    response = await fetch(path, {
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
    const detail = typeof payload?.error === "string" ? payload.error : "";
    throw new ApiError(response.status, MESSAGES[response.status] || detail || `Request failed (HTTP ${response.status}).`, {
      code: detail,
    });
  }
  // next_after is present only when older rows exist behind this page, so an
  // absent cursor means the journal ends here — never "start again at the top".
  return {
    events: Array.isArray(payload?.events) ? payload.events : [],
    next_after: typeof payload?.next_after === "string" ? payload.next_after : "",
  };
}
