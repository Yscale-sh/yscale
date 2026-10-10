import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewDeleteRuntimeBinding,
  previewPutRuntimeBinding,
  previewRuntimeBindings,
} from "./devPreview.js";

const ROLES = new Set(["owner", "admin", "member", "viewer"]);
const KEY_RE = /^[A-Z_][A-Z0-9_]{0,62}$/;
const REVISION_RE = /^(?:0|[1-9][0-9]{0,18})$/;
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const SAFE_SYNC_STATES = new Set(["pending", "synced", "error"]);
const BINDING_KEYS = new Set(["id", "key", "name", "revision", "sync_state", "created_at", "updated_at"]);
const LIST_KEYS = new Set(["tenant_id", "role", "bindings"]);

const MESSAGES = {
  0: "Could not reach the runtime binding service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "Only a tenant owner or admin can change runtime bindings.",
  404: "That runtime binding no longer exists.",
  409: "That runtime binding changed. Refresh and try again.",
  413: "The runtime binding request is too large.",
  415: "The runtime binding request must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The runtime binding service returned an unsafe or invalid response.",
  503: "Runtime bindings are unavailable in this environment.",
};

function exactKeys(value, allowed, required = allowed) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return keys.every((key) => allowed.has(key)) && [...required].every((key) => Object.hasOwn(value, key));
}

function containsSecretLikeField(value) {
  if (Array.isArray(value)) return value.some(containsSecretLikeField);
  if (!value || typeof value !== "object") return false;
  return Object.entries(value).some(([key, child]) => {
    const lowered = key.toLowerCase();
    return ["value", "cipher", "nonce", "hash", "secret", "credential", "token"].some((part) => lowered.includes(part)) || containsSecretLikeField(child);
  });
}

function cleanString(value, max) {
  return typeof value === "string" && value === value.trim() && value.length > 0 && new TextEncoder().encode(value).length <= max ? value : "";
}

function normalizeBinding(raw) {
  if (!exactKeys(raw, BINDING_KEYS)) throw new ApiError(502, MESSAGES[502]);
  const id = cleanString(raw.id, 128);
  const key = cleanString(raw.key, 63);
  const name = cleanString(raw.name, 100);
  const revision = cleanString(raw.revision, 128);
  const syncState = cleanString(raw.sync_state, 32);
  const createdAt = cleanString(raw.created_at, 64);
  const updatedAt = cleanString(raw.updated_at, 64);
  if (!id || !KEY_RE.test(key) || !name || !REVISION_RE.test(revision) || !SAFE_SYNC_STATES.has(syncState) ||
      !RFC3339.test(createdAt) || !RFC3339.test(updatedAt) || Number.isNaN(Date.parse(createdAt)) || Number.isNaN(Date.parse(updatedAt))) {
    throw new ApiError(502, MESSAGES[502]);
  }
  return { id, key, name, revision, syncState, createdAt, updatedAt };
}

export function normalizeRuntimeBindings(payload, expectedTenantId = "") {
  if (containsSecretLikeField(payload) || !exactKeys(payload, LIST_KEYS) || !Array.isArray(payload.bindings) || payload.bindings.length > 32) {
    throw new ApiError(502, MESSAGES[502]);
  }
  const tenantId = cleanString(payload.tenant_id, 128);
  const role = cleanString(payload.role, 16);
  if (!tenantId || (expectedTenantId && tenantId !== expectedTenantId) || !ROLES.has(role)) throw new ApiError(502, MESSAGES[502]);
  const bindings = payload.bindings.map(normalizeBinding);
  if (new Set(bindings.map((binding) => binding.key)).size !== bindings.length) throw new ApiError(502, MESSAGES[502]);
  return { tenantId, role, bindings };
}

export function normalizeRuntimeBinding(payload, expectedTenantId = "") {
  if (containsSecretLikeField(payload) || !exactKeys(payload, new Set(["tenant_id", "role", "binding"]))) throw new ApiError(502, MESSAGES[502]);
  const list = normalizeRuntimeBindings({ tenant_id: payload.tenant_id, role: payload.role, bindings: [payload.binding] }, expectedTenantId);
  return { tenantId: list.tenantId, role: list.role, binding: list.bindings[0] };
}

export function normalizeDeletedRuntimeBinding(payload, expectedKey = "") {
  if (containsSecretLikeField(payload) || !exactKeys(payload, new Set(["key", "deleted"])) || payload.deleted !== true || !KEY_RE.test(payload.key) || (expectedKey && payload.key !== expectedKey)) {
    throw new ApiError(502, MESSAGES[502]);
  }
  return { key: payload.key, deleted: true };
}

function safeErrorText(value) {
  if (typeof value !== "string") return "";
  const text = value.replace(/\s+/g, " ").trim();
  return text.length <= 240 ? text : `${text.slice(0, 239)}…`;
}

async function request(path, { token, method = "GET", body, signal } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json", ...(body ? { "Content-Type": "application/json" } : {}) },
      body: body ? JSON.stringify(body) : undefined,
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  if (containsSecretLikeField(payload)) throw new ApiError(502, MESSAGES[502]);
  if (!response.ok) {
    const code = safeErrorText(payload?.error);
    const message = safeErrorText(payload?.message);
    throw new ApiError(response.status, message || MESSAGES[response.status] || code || `Request failed (HTTP ${response.status}).`, { code });
  }
  return payload;
}

function collectionPath(tenantId) { return `/api/tenants/${encodeURIComponent(tenantId)}/runtime-bindings`; }
function itemPath(tenantId, key) { return `${collectionPath(tenantId)}/${encodeURIComponent(key)}`; }

export function fetchRuntimeBindings({ token, tenantId, signal }) {
  const result = isDevPreviewToken(token) ? previewRuntimeBindings(tenantId) : request(collectionPath(tenantId), { token, signal });
  return Promise.resolve(result).then((payload) => normalizeRuntimeBindings(payload, tenantId));
}

export function putRuntimeBinding({ token, tenantId, key, name, value, signal }) {
  const nameBytes = typeof name === "string" ? new TextEncoder().encode(name).length : 0;
  const valueBytes = typeof value === "string" ? new TextEncoder().encode(value).length : 0;
  if (!KEY_RE.test(key) || name !== name?.trim() || nameBytes < 1 || nameBytes > 100 || valueBytes < 1 || valueBytes > 8192 || value.includes("\0")) {
    return Promise.reject(new ApiError(400, "Use a valid key, name, and write-only value."));
  }
  const body = { name, value };
  const result = isDevPreviewToken(token) ? previewPutRuntimeBinding(tenantId, key, body) : request(itemPath(tenantId, key), { token, method: "PUT", body, signal });
  return Promise.resolve(result).then((payload) => normalizeRuntimeBinding(payload, tenantId));
}

export function deleteRuntimeBinding({ token, tenantId, key, signal }) {
  if (!KEY_RE.test(key)) return Promise.reject(new ApiError(400, "Use a valid runtime binding key."));
  const result = isDevPreviewToken(token) ? previewDeleteRuntimeBinding(tenantId, key) : request(itemPath(tenantId, key), { token, method: "DELETE", signal });
  return Promise.resolve(result).then((payload) => normalizeDeletedRuntimeBinding(payload, key));
}

export { KEY_RE as RUNTIME_BINDING_KEY_RE };
