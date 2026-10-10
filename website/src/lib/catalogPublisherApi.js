import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewCatalogPublishers,
  previewCreateCatalogPublisher,
  previewDeleteCatalogPublisher,
  previewRotateCatalogPublisher,
} from "./devPreview.js";

// A catalog publisher is a write credential for automation, not a user-facing
// catalog entry. The safe read shape deliberately excludes the credential;
// create and rotate reveal it once through a separate normalizer.
const PUBLISHER_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const MAX_NAME = 100;
const MAX_TOKEN = 8192;
const MAX_PUBLISHERS = 100;
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

const MESSAGES = {
  0: "Could not reach the catalog publisher service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot manage catalog publishers.",
  404: "That tenant or publisher no longer exists.",
  409: "That publisher name is already in use.",
  413: "The catalog publisher request is too large.",
  415: "The catalog publisher request must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The catalog publisher service did not answer safely.",
  503: "The catalog publisher service is unavailable in this environment.",
};

function text(value) {
  return typeof value === "string" ? value.trim() : "";
}

function hasControl(value) {
  for (const char of value) {
    const code = char.codePointAt(0);
    if (code < 0x20 || (code >= 0x7f && code <= 0x9f)) return true;
  }
  return false;
}

function utf8Bytes(value) {
  return new TextEncoder().encode(value).length;
}

function exactKeys(value, allowed) {
  return value && typeof value === "object" && !Array.isArray(value) &&
    Object.keys(value).every((key) => allowed.includes(key));
}

function invalid(detail) {
  return new ApiError(502, `The catalog publisher service answered with ${detail}.`);
}

function timestamp(value) {
  const stamp = text(value);
  if (!stamp || stamp.length > 64 || !RFC3339.test(stamp) || !Number.isFinite(Date.parse(stamp))) return "";
  return stamp;
}

function normalizePublisher(value) {
  if (!exactKeys(value, ["id", "name", "created_at", "updated_at"])) throw invalid("an unsafe publisher record");
  const id = text(value.id);
  const name = text(value.name);
  const createdAt = timestamp(value.created_at);
  const updatedAt = timestamp(value.updated_at);
  if (!PUBLISHER_ID.test(id) || !name || utf8Bytes(name) > MAX_NAME || hasControl(name) || !createdAt || !updatedAt) {
    throw invalid("an invalid publisher record");
  }
  return { id, name, createdAt, updatedAt };
}

export function normalizeCatalogPublishers(payload) {
  if (!exactKeys(payload, ["publishers"]) || !Array.isArray(payload.publishers)) throw invalid("no publisher list");
  if (payload.publishers.length > MAX_PUBLISHERS) throw invalid(`more than ${MAX_PUBLISHERS} publishers`);
  const publishers = payload.publishers.map(normalizePublisher);
  if (new Set(publishers.map((publisher) => publisher.id)).size !== publishers.length) throw invalid("duplicate publisher ids");
  return publishers;
}

// Keep this allowlist literal in step with central. In particular, never trust
// an upstream command that could interpolate the one-time token into the DOM.
export const CATALOG_PUBLISHER_COMMAND = "yscale catalog apply --server https://central.example.com --tenant TENANT_ID -f catalog.yaml";

export function normalizeCatalogPublisherCredential(payload) {
  if (!exactKeys(payload, ["publisher", "token", "command"])) throw invalid("an unsafe credential envelope");
  const publisher = normalizePublisher(payload.publisher);
  const token = typeof payload.token === "string" ? payload.token.trim() : "";
  if (!token || token.length > MAX_TOKEN || [...token].some((char) => char.codePointAt(0) < 0x21 || char.codePointAt(0) > 0x7e)) throw invalid("no usable one-time token");
  if (payload.command !== undefined && payload.command !== CATALOG_PUBLISHER_COMMAND) throw invalid("an unrecognized command");
  return { publisher, token, command: payload.command || "" };
}

function safeMessage(value) {
  if (typeof value !== "string") return "";
  const message = value.replace(/\s+/g, " ").trim();
  return message.length > 240 ? `${message.slice(0, 239)}…` : message;
}

async function request(path, { token, method = "GET", body, signal } = {}) {
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
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = response.status === 204 ? null : await response.json().catch(() => null);
  if (!response.ok) {
    const detail = safeMessage(payload?.error);
    const message = safeMessage(payload?.message);
    const code = /^[a-z0-9_]+$/.test(detail) ? detail : "";
    throw new ApiError(response.status, message || (code ? "" : detail) || MESSAGES[response.status] || `Request failed (HTTP ${response.status}).`, { code: code || detail });
  }
  return payload;
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/catalog-publishers`;
}

export function catalogPublisherName(value) {
  const name = text(value);
  return name && utf8Bytes(name) <= MAX_NAME && !hasControl(name) ? name : "";
}

export function fetchCatalogPublishers({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return previewCatalogPublishers(tenantId).then(normalizeCatalogPublishers);
  return request(base(tenantId), { token, signal }).then(normalizeCatalogPublishers);
}

export function createCatalogPublisher({ token, tenantId, name, signal }) {
  const normalizedName = catalogPublisherName(name);
  if (!normalizedName) return Promise.reject(new ApiError(400, `Use a publisher name of 1–${MAX_NAME} UTF-8 bytes on one line.`));
  if (isDevPreviewToken(token)) return previewCreateCatalogPublisher(tenantId, { name: normalizedName }).then(normalizeCatalogPublisherCredential);
  return request(base(tenantId), { token, method: "POST", body: { name: normalizedName }, signal }).then(normalizeCatalogPublisherCredential);
}

export function rotateCatalogPublisherCredential({ token, tenantId, publisherId, signal }) {
  if (isDevPreviewToken(token)) return previewRotateCatalogPublisher(tenantId, publisherId).then(normalizeCatalogPublisherCredential);
  return request(`${base(tenantId)}/${encodeURIComponent(publisherId)}/credential`, { token, method: "POST", signal })
    .then(normalizeCatalogPublisherCredential);
}

export function deleteCatalogPublisher({ token, tenantId, publisherId, signal }) {
  if (isDevPreviewToken(token)) return previewDeleteCatalogPublisher(tenantId, publisherId).then(() => true);
  return request(`${base(tenantId)}/${encodeURIComponent(publisherId)}`, { token, method: "DELETE", signal }).then(() => true);
}
