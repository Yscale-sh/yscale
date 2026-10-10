import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewDeleteLinodeCloudAccount,
  previewLinodeCloudAccount,
  previewPutLinodeCloudAccount,
} from "./devPreview.js";

const ROLES = new Set(["owner", "admin", "member", "viewer"]);
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const ENVELOPE_KEYS = new Set(["tenant_id", "role", "account", "changed"]);
const ACCOUNT_KEYS = new Set(["id", "provider", "provider_account_id", "region", "cpu_image_ready", "gpu_image_ready", "updated_at"]);

const MESSAGES = {
  0: "Could not reach the cloud account service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "Only a tenant owner or admin can change this cloud account.",
  404: "That tenant does not exist for this account.",
  409: "Disconnect is blocked while this account has a live burst or lease.",
  413: "The cloud account request is too large.",
  415: "The cloud account request must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The cloud account service returned an unsafe or invalid response.",
  503: "The cloud account service is unavailable in this environment.",
};

function cleanString(value, max = 512) {
  if (typeof value !== "string" || value !== value.trim() || !value || value.length > max) return "";
  return value;
}

function exactKeys(value, allowed, required = allowed) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return keys.every((key) => allowed.has(key)) && [...required].every((key) => Object.hasOwn(value, key));
}

function containsSecretField(value) {
  if (Array.isArray(value)) return value.some(containsSecretField);
  if (!value || typeof value !== "object") return false;
  return Object.entries(value).some(([key, child]) => {
    const lowered = key.toLowerCase();
    return lowered.includes("token") || lowered.includes("cipher") || lowered.includes("secret") || lowered.includes("credential") || containsSecretField(child);
  });
}

export function normalizeLinodeCloudAccount(payload, expectedTenantId = "") {
  if (containsSecretField(payload) || !exactKeys(payload, ENVELOPE_KEYS, new Set(["tenant_id", "role", "account"]))) {
    throw new ApiError(502, MESSAGES[502]);
  }
  const tenantId = cleanString(payload.tenant_id, 128);
  const role = cleanString(payload.role, 16);
  if (!tenantId || (expectedTenantId && tenantId !== expectedTenantId) || !ROLES.has(role)) {
    throw new ApiError(502, MESSAGES[502]);
  }
  if (Object.hasOwn(payload, "changed") && typeof payload.changed !== "boolean") {
    throw new ApiError(502, MESSAGES[502]);
  }
  if (payload.account === null) {
    return { tenantId, role, account: null, changed: payload.changed === true };
  }
  if (!exactKeys(payload.account, ACCOUNT_KEYS) || payload.account.provider !== "linode") {
    throw new ApiError(502, MESSAGES[502]);
  }
  const id = cleanString(payload.account.id);
  const providerAccountId = cleanString(payload.account.provider_account_id);
  const region = cleanString(payload.account.region, 128);
  const updatedAt = cleanString(payload.account.updated_at, 64);
  if (!id || !providerAccountId || !region || !updatedAt || !RFC3339.test(updatedAt) || Number.isNaN(new Date(updatedAt).getTime()) ||
      typeof payload.account.cpu_image_ready !== "boolean" || typeof payload.account.gpu_image_ready !== "boolean") {
    throw new ApiError(502, MESSAGES[502]);
  }
  return {
    tenantId,
    role,
    changed: payload.changed === true,
    account: {
      id,
      provider: "linode",
      providerAccountId,
      region,
      cpuImageReady: payload.account.cpu_image_ready,
      gpuImageReady: payload.account.gpu_image_ready,
      updatedAt,
    },
  };
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
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  if (containsSecretField(payload)) throw new ApiError(502, MESSAGES[502]);
  if (!response.ok) {
    const code = safeErrorText(payload?.error);
    const message = safeErrorText(payload?.message);
    throw new ApiError(response.status, message || MESSAGES[response.status] || code || `Request failed (HTTP ${response.status}).`, { code });
  }
  return payload;
}

function pathFor(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/cloud-accounts/linode`;
}

export function fetchLinodeCloudAccount({ token, tenantId, signal }) {
  const result = isDevPreviewToken(token)
    ? previewLinodeCloudAccount(tenantId)
    : request(pathFor(tenantId), { token, signal });
  return Promise.resolve(result).then((payload) => normalizeLinodeCloudAccount(payload, tenantId));
}

export function putLinodeCloudAccount({ token, tenantId, providerToken, region, cpuImage = "", gpuImage = "", signal }) {
  const body = { token: providerToken, region, ...(cpuImage ? { cpu_image: cpuImage } : {}), ...(gpuImage ? { gpu_image: gpuImage } : {}) };
  const result = isDevPreviewToken(token)
    ? previewPutLinodeCloudAccount(tenantId, body)
    : request(pathFor(tenantId), { token, method: "PUT", body, signal });
  return Promise.resolve(result).then((payload) => normalizeLinodeCloudAccount(payload, tenantId));
}

export function deleteLinodeCloudAccount({ token, tenantId, signal }) {
  const result = isDevPreviewToken(token)
    ? previewDeleteLinodeCloudAccount(tenantId)
    : request(pathFor(tenantId), { token, method: "DELETE", signal });
  return Promise.resolve(result).then((payload) => normalizeLinodeCloudAccount(payload, tenantId));
}
