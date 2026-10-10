import { ApiError } from "./apiError.js";
import { isDevPreviewToken, previewBilling } from "./devPreview.js";

const BILLING_KEYS = new Set(["tenant_id", "currency", "balance_micro_usd", "held_micro_usd", "spendable_micro_usd", "debt_micro_usd", "frozen", "updated_at", "open_holds"]);
const HOLD_KEYS = new Set(["id", "workload_id", "amount_micro_usd", "expires_at", "created_at"]);
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const MESSAGES = {
  0: "Could not reach the billing service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot read billing.",
  404: "Billing is not available for this tenant.",
  429: "Too many billing requests. Try again in a moment.",
  502: "The billing service returned an invalid response.",
  503: "Billing is unavailable in this environment.",
};

function exact(value, keys, required = keys) {
  return !!value && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).every((key) => keys.has(key)) && [...required].every((key) => Object.hasOwn(value, key));
}
function text(value, max = 128) { return typeof value === "string" && value === value.trim() && value.length > 0 && new TextEncoder().encode(value).length <= max ? value : ""; }
function micros(value) { return Number.isSafeInteger(value) && value >= 0 ? value : null; }
function time(value) { return text(value, 64) && RFC3339.test(value) && !Number.isNaN(Date.parse(value)) ? value : ""; }
function invalid() { throw new ApiError(502, MESSAGES[502]); }

export function normalizeBilling(payload, expectedTenantId = "") {
  const required = new Set([...BILLING_KEYS].filter((key) => key !== "updated_at"));
  if (!exact(payload, BILLING_KEYS, required) || payload.currency !== "USD" || typeof payload.frozen !== "boolean" || !Array.isArray(payload.open_holds) || payload.open_holds.length > 100) invalid();
  const tenantId = text(payload.tenant_id);
  if (!tenantId || (expectedTenantId && tenantId !== expectedTenantId)) invalid();
  const values = [payload.balance_micro_usd, payload.held_micro_usd, payload.spendable_micro_usd, payload.debt_micro_usd].map(micros);
  if (values.some((value) => value === null)) invalid();
  const seen = new Set();
  const openHolds = payload.open_holds.map((row) => {
    if (!exact(row, HOLD_KEYS)) invalid();
    const id = Number.isSafeInteger(row.id) && row.id > 0 ? row.id : null; const workloadId = text(row.workload_id); const amountMicroUsd = micros(row.amount_micro_usd);
    const expiresAt = time(row.expires_at); const createdAt = time(row.created_at);
    if (id === null || !workloadId || amountMicroUsd === null || !expiresAt || !createdAt || seen.has(id)) invalid();
    seen.add(id);
    return { id, workloadId, amountMicroUsd, expiresAt, createdAt };
  });
  return {
    tenantId, currency: "USD", balanceMicroUsd: values[0], heldMicroUsd: values[1], spendableMicroUsd: values[2], debtMicroUsd: values[3], frozen: payload.frozen,
    ...(Object.hasOwn(payload, "updated_at") ? { updatedAt: time(payload.updated_at) || invalid() } : {}), openHolds,
  };
}

export function formatMicroUSD(value) {
  if (!Number.isSafeInteger(value)) return "not reported";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 6 }).format(value / 1_000_000);
}

function safeMessage(value) { const result = typeof value === "string" ? value.replace(/\s+/g, " ").trim() : ""; return result.length <= 240 ? result : `${result.slice(0, 239)}…`; }
async function request(path, { token, signal }) {
  let response;
  try { response = await fetch(path, { method: "GET", headers: { Authorization: `Bearer ${token}`, Accept: "application/json" }, credentials: "same-origin", signal }); }
  catch (error) { if (error?.name === "AbortError") throw error; throw new ApiError(0, MESSAGES[0]); }
  const payload = await response.json().catch(() => null);
  if (!response.ok) throw new ApiError(response.status, safeMessage(payload?.message) || MESSAGES[response.status] || safeMessage(payload?.error) || `Request failed (HTTP ${response.status}).`);
  return payload;
}

export function fetchBilling({ token, tenantId, signal }) {
  const result = isDevPreviewToken(token) ? previewBilling(tenantId) : request(`/api/tenants/${encodeURIComponent(tenantId)}/billing`, { token, signal });
  return Promise.resolve(result).then((payload) => normalizeBilling(payload, tenantId));
}
