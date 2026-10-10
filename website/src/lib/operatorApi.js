import { ApiError } from "./apiError.js";
import { isDevPreviewToken, previewGrantServiceCredit, previewHostedClusters, previewHostedRequests, previewOperatorTenants } from "./devPreview.js";

const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const QUEUE_KEYS = new Set(["requests"]);
const QUEUE_ROW_KEYS = new Set(["tenant_id", "name", "plan", "requested_at"]);
const INVENTORY_KEYS = new Set(["clusters"]);
const INVENTORY_ROW_KEYS = new Set(["tenant_id", "name", "plan", "cluster"]);
const ASSIGNMENT_KEYS = new Set(["tenant_id", "cluster", "connector_token", "helm_release", "connector_rbac_set", "helm_command"]);
const CLUSTER_KEYS = new Set(["cluster_id", "name", "source", "state", "hosted_namespace", "registered_at"]);
const DELETE_KEYS = new Set(["tenant_id", "cluster_id", "hosted_namespace", "deleted"]);
const TENANT_PAGE_KEYS = new Set(["tenants", "next_after"]);
const TENANT_KEYS = new Set(["tenant_id", "tenant_name", "plan", "running_bursts", "hourly_usd", "projected_daily_usd", "limits"]);
const TENANT_LIMIT_KEYS = new Set(["max_concurrent_bursts", "max_hourly_usd"]);
const TENANT_UPDATE_KEYS = new Set(["tenant", "changed"]);
const SERVICE_CREDIT_KEYS = new Set(["tenant_id", "amount_micro_usd", "currency", "idempotency_key", "granted"]);
const SERVICE_CREDIT_KEY_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/;
const MESSAGES = {
  0: "Could not reach the operator service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This account does not have platform operator access.",
  404: "Operator capacity management is not available in this environment.",
  409: "This request was already assigned or its assignment conflicts with current platform state. Refresh the queue before trying again.",
  429: "Too many operator requests. Try again in a moment.",
  502: "The operator service did not answer.",
  503: "The operator service is unavailable in this environment.",
};

function cleanText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function validTime(value) {
  return typeof value === "string" && RFC3339.test(value) && !Number.isNaN(new Date(value).getTime());
}

function invalid(message) {
  throw new ApiError(502, message);
}

function hasOnlyKeys(value, allowed) {
  return Object.keys(value).every((key) => allowed.has(key));
}

function safeNonNegative(value, integer = false) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER
    && (!integer || Number.isSafeInteger(value));
}

function normalizeOperatorTenant(row) {
  if (!row || typeof row !== "object" || Array.isArray(row) || Object.keys(row).length !== TENANT_KEYS.size || !hasOnlyKeys(row, TENANT_KEYS)
      || !row.limits || typeof row.limits !== "object" || Array.isArray(row.limits)
      || Object.keys(row.limits).length !== TENANT_LIMIT_KEYS.size || !hasOnlyKeys(row.limits, TENANT_LIMIT_KEYS)) {
    return invalid("The operator service answered with an invalid tenant guardrail row.");
  }
  const tenantId = cleanText(row.tenant_id);
  const tenantName = cleanText(row.tenant_name);
  const plan = cleanText(row.plan);
  if (!tenantId || typeof row.tenant_name !== "string" || !plan || tenantId !== row.tenant_id
      || (tenantName && tenantName !== row.tenant_name) || plan !== row.plan
      || !safeNonNegative(row.running_bursts, true) || !safeNonNegative(row.hourly_usd)
      || !safeNonNegative(row.projected_daily_usd) || !safeNonNegative(row.limits.max_concurrent_bursts, true)
      || !safeNonNegative(row.limits.max_hourly_usd)) {
    return invalid("The operator service answered with an invalid tenant guardrail row.");
  }
  return {
    tenantId,
    tenantName: tenantName || tenantId,
    plan,
    runningBursts: row.running_bursts,
    hourlyUsd: row.hourly_usd,
    projectedDailyUsd: row.projected_daily_usd,
    limits: {
      maxConcurrentBursts: row.limits.max_concurrent_bursts,
      maxHourlyUsd: row.limits.max_hourly_usd,
    },
  };
}

export function normalizeOperatorTenantPage(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !hasOnlyKeys(payload, TENANT_PAGE_KEYS)
      || !Object.hasOwn(payload, "tenants") || !Array.isArray(payload.tenants)) {
    return invalid("The operator service answered with an invalid tenant inventory.");
  }
  const tenants = payload.tenants.map(normalizeOperatorTenant);
  for (let index = 1; index < tenants.length; index += 1) {
    if (tenants[index - 1].tenantId.localeCompare(tenants[index].tenantId) >= 0) {
      return invalid("The operator service answered with an unsorted tenant inventory.");
    }
  }
  const result = { tenants };
  if (Object.hasOwn(payload, "next_after")) {
    const nextAfter = cleanText(payload.next_after);
    if (!nextAfter || nextAfter !== payload.next_after) return invalid("The operator service answered with an invalid tenant cursor.");
    result.nextAfter = nextAfter;
  }
  return result;
}

export function normalizeOperatorTenantUpdate(payload, expectedTenantId = "") {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || Object.keys(payload).length !== TENANT_UPDATE_KEYS.size
      || !hasOnlyKeys(payload, TENANT_UPDATE_KEYS) || typeof payload.changed !== "boolean") {
    return invalid("The operator service answered with an invalid tenant guardrail update.");
  }
  const tenant = normalizeOperatorTenant(payload.tenant);
  if (expectedTenantId && tenant.tenantId !== expectedTenantId) return invalid("The operator service answered with the wrong tenant guardrail.");
  return { tenant, changed: payload.changed };
}

export function tenantLimitDraft(limits = {}) {
  return {
    maxConcurrentBursts: String(limits.maxConcurrentBursts ?? ""),
    maxHourlyUsd: String(limits.maxHourlyUsd ?? ""),
  };
}

export function tenantLimitValues(draft) {
  const values = {
    maxConcurrentBursts: Number(draft?.maxConcurrentBursts),
    maxHourlyUsd: Number(draft?.maxHourlyUsd),
  };
  const errors = {};
  for (const [key, label] of [["maxConcurrentBursts", "Concurrent ceiling"], ["maxHourlyUsd", "Hourly ceiling"]]) {
    const raw = draft?.[key];
    if (typeof raw !== "string" || !raw.trim()) errors[key] = `${label} is required.`;
    else if (!safeNonNegative(values[key])) errors[key] = `${label} must be a finite non-negative number.`;
  }
  if (!errors.maxConcurrentBursts && !Number.isSafeInteger(values.maxConcurrentBursts)) {
    errors.maxConcurrentBursts = "Concurrent ceiling must be a non-negative whole number.";
  }
  return { values, errors };
}

export function usdToMicroUSD(value) {
  if (typeof value !== "string" || !/^(?:0|[1-9]\d{0,9})(?:\.\d{1,6})?$/.test(value)) return { value: null, error: "Enter a positive USD amount with at most six decimal places." };
  const [whole, fraction = ""] = value.split(".");
  const micro = Number(whole) * 1_000_000 + Number(fraction.padEnd(6, "0"));
  if (!Number.isSafeInteger(micro) || micro <= 0) return { value: null, error: "Service credit must be greater than zero and within the supported range." };
  return { value: micro, error: "" };
}

export function createServiceCreditKey() {
  const id = globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `svc-credit-${id}`;
}

export function normalizeServiceCreditGrant(payload, expectedTenantId = "", expectedAmount = null, expectedKey = "") {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || Object.keys(payload).length !== SERVICE_CREDIT_KEYS.size || !hasOnlyKeys(payload, SERVICE_CREDIT_KEYS)
      || payload.currency !== "USD" || payload.granted !== true || !safeNonNegative(payload.amount_micro_usd, true)) invalid("The operator service answered with an invalid service-credit receipt.");
  const tenantId = cleanText(payload.tenant_id); const key = cleanText(payload.idempotency_key);
  if (!tenantId || !key || (expectedTenantId && tenantId !== expectedTenantId) || (expectedKey && key !== expectedKey) || (expectedAmount !== null && payload.amount_micro_usd !== expectedAmount)) invalid("The operator service answered with the wrong service-credit receipt.");
  return { tenantId, amountMicroUsd: payload.amount_micro_usd, currency: "USD", idempotencyKey: key, granted: true };
}

export function appendOperatorTenantPage(current, page) {
  const tenants = [...current, ...page.tenants];
  for (let index = 1; index < tenants.length; index += 1) {
    if (tenants[index - 1].tenantId.localeCompare(tenants[index].tenantId) >= 0) {
      return invalid("The operator service returned an overlapping tenant page.");
    }
  }
  return tenants;
}

export function replaceOperatorTenant(tenants, replacement) {
  let found = false;
  const next = tenants.map((tenant) => {
    if (tenant.tenantId !== replacement.tenantId) return tenant;
    found = true;
    return replacement;
  });
  if (!found) return invalid("The operator service updated a tenant outside the loaded inventory.");
  return next;
}

export function normalizeHostedRequestQueue(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !hasOnlyKeys(payload, QUEUE_KEYS) || !Array.isArray(payload.requests)) {
    return invalid("The operator service answered with an invalid hosted-capacity queue.");
  }
  const seen = new Set();
  const requests = payload.requests.map((row) => {
    if (!row || typeof row !== "object" || Array.isArray(row) || !hasOnlyKeys(row, QUEUE_ROW_KEYS)) return invalid("The operator service answered with an invalid queue row.");
    const tenantId = cleanText(row.tenant_id);
    const tenantName = cleanText(row.name);
    const plan = cleanText(row.plan);
    const requestedAt = cleanText(row.requested_at);
    if (!tenantId || (Object.hasOwn(row, "name") && typeof row.name !== "string") || !plan || !validTime(requestedAt) || seen.has(tenantId)) {
      return invalid("The operator service answered with an invalid queue row.");
    }
    seen.add(tenantId);
    return { tenantId, tenantName: tenantName || tenantId, plan, requestedAt };
  });
  requests.sort((left, right) => Date.parse(left.requestedAt) - Date.parse(right.requestedAt) || left.tenantId.localeCompare(right.tenantId));
  return { requests };
}

export function normalizeHostedInventory(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !hasOnlyKeys(payload, INVENTORY_KEYS) || !Array.isArray(payload.clusters)) {
    return invalid("The operator service answered with an invalid hosted-cluster inventory.");
  }
  const seen = new Set();
  const clusters = payload.clusters.map((row) => {
    if (!row || typeof row !== "object" || Array.isArray(row) || !hasOnlyKeys(row, INVENTORY_ROW_KEYS)
        || !row.cluster || typeof row.cluster !== "object" || Array.isArray(row.cluster) || !hasOnlyKeys(row.cluster, CLUSTER_KEYS)) {
      return invalid("The operator service answered with an invalid inventory row.");
    }
    const tenantId = cleanText(row.tenant_id);
    const tenantName = cleanText(row.name);
    const plan = cleanText(row.plan);
    const clusterId = cleanText(row.cluster.cluster_id);
    const name = cleanText(row.cluster.name);
    const source = cleanText(row.cluster.source);
    const state = cleanText(row.cluster.state);
    const hostedNamespace = cleanText(row.cluster.hosted_namespace);
    const registeredAt = cleanText(row.cluster.registered_at);
    const key = `${tenantId}\u0000${clusterId}`;
    if (!tenantId || (Object.hasOwn(row, "name") && typeof row.name !== "string") || !plan || !clusterId
        || (Object.hasOwn(row.cluster, "name") && typeof row.cluster.name !== "string") || source !== "hosted" || !state
        || !hostedNamespace || !validTime(registeredAt) || seen.has(key)) {
      return invalid("The operator service answered with an invalid inventory row.");
    }
    seen.add(key);
    return {
      tenantId,
      tenantName: tenantName || tenantId,
      plan,
      cluster: { clusterId, name: name || clusterId, source, state, hostedNamespace, registeredAt },
    };
  });
  clusters.sort((left, right) => left.tenantId.localeCompare(right.tenantId) || left.cluster.clusterId.localeCompare(right.cluster.clusterId));
  return { clusters };
}

export function assignmentDraft(overrides = {}) {
  return {
    advanced: !!overrides.advanced,
    clusterId: cleanText(overrides.clusterId),
    name: cleanText(overrides.name),
    namespace: cleanText(overrides.namespace),
  };
}

export function assignmentBody(draft) {
  const normalized = assignmentDraft(draft);
  const body = {};
  if (normalized.advanced && normalized.clusterId) body.cluster_id = normalized.clusterId;
  if (normalized.advanced && normalized.name) body.name = normalized.name;
  if (normalized.advanced && normalized.namespace) body.namespace = normalized.namespace;
  return body;
}

export function normalizeHostedAssignment(payload, expectedTenantId = "") {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !hasOnlyKeys(payload, ASSIGNMENT_KEYS) || !payload.cluster || typeof payload.cluster !== "object" || Array.isArray(payload.cluster) || !hasOnlyKeys(payload.cluster, CLUSTER_KEYS)) {
    return invalid("The operator service answered with an invalid hosted-cluster assignment.");
  }
  const tenantId = cleanText(payload.tenant_id);
  const connectorToken = cleanText(payload.connector_token);
  const helmRelease = cleanText(payload.helm_release);
  const connectorRbacSet = cleanText(payload.connector_rbac_set);
  const helmCommand = cleanText(payload.helm_command);
  const clusterId = cleanText(payload.cluster.cluster_id);
  const name = cleanText(payload.cluster.name);
  const source = cleanText(payload.cluster.source);
  const state = cleanText(payload.cluster.state);
  const hostedNamespace = cleanText(payload.cluster.hosted_namespace);
  const registeredAt = cleanText(payload.cluster.registered_at);
  if (!tenantId || (expectedTenantId && tenantId !== expectedTenantId) || !clusterId || source !== "hosted" || !state || !hostedNamespace || !validTime(registeredAt)
      || !connectorToken || !helmRelease || !connectorRbacSet || !helmCommand) {
    return invalid("The operator service answered with an invalid hosted-cluster assignment.");
  }
  return {
    tenantId,
    cluster: { clusterId, name: name || clusterId, source, state, hostedNamespace, registeredAt },
    connectorToken,
    helmRelease,
    connectorRbacSet,
    helmCommand,
  };
}

export function normalizeHostedDelete(payload, expectedTenantId = "", expectedClusterId = "") {
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !hasOnlyKeys(payload, DELETE_KEYS)) {
    return invalid("The operator service answered with an invalid hosted-cluster removal.");
  }
  const tenantId = cleanText(payload.tenant_id);
  const clusterId = cleanText(payload.cluster_id);
  const hostedNamespace = cleanText(payload.hosted_namespace);
  if (!tenantId || !clusterId || payload.deleted !== true
      || (Object.hasOwn(payload, "hosted_namespace") && typeof payload.hosted_namespace !== "string")
      || (expectedTenantId && tenantId !== expectedTenantId) || (expectedClusterId && clusterId !== expectedClusterId)) {
    return invalid("The operator service answered with an invalid hosted-cluster removal.");
  }
  return { tenantId, clusterId, hostedNamespace, deleted: true };
}

export function hostedUninstallCommand(clusterId) {
  const id = cleanText(clusterId);
  return id ? `helm uninstall yscale-agent-${id} --namespace yscale-system` : "";
}

export function operatorCapability(error = null) {
  if (!error) return { available: true, expired: false, warning: "" };
  if (error instanceof ApiError && error.status === 401) return { available: false, expired: true, warning: "" };
  if (error instanceof ApiError && (error.status === 403 || error.status === 404)) return { available: false, expired: false, warning: "" };
  return { available: false, expired: false, warning: error.message || MESSAGES[0] };
}

export function operatorMutationAccess(error) {
  return {
    expired: error instanceof ApiError && error.status === 401,
    unavailable: error instanceof ApiError && error.status === 403,
  };
}

async function request(path, { token, method = "GET", body, signal } = {}) {
  const headers = { Authorization: `Bearer ${token}`, Accept: "application/json" };
  let requestBody;
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    requestBody = JSON.stringify(body);
  }
  let response;
  try {
    response = await fetch(path, { method, headers, credentials: "same-origin", body: requestBody, signal });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const code = cleanText(payload?.error);
    const detail = cleanText(payload?.message);
    throw new ApiError(response.status, MESSAGES[response.status] || detail || code || `Request failed (HTTP ${response.status}).`, { code });
  }
  return payload;
}

export function fetchHostedRequests({ token, signal }) {
  if (isDevPreviewToken(token)) return previewHostedRequests().then(normalizeHostedRequestQueue);
  return request("/api/operator/hosted-capacity/requests", { token, signal }).then(normalizeHostedRequestQueue);
}

export function fetchHostedClusters({ token, signal }) {
  if (isDevPreviewToken(token)) return previewHostedClusters().then(normalizeHostedInventory);
  return request("/api/operator/hosted-clusters", { token, signal }).then(normalizeHostedInventory);
}

export function fetchOperatorTenants({ token, after = "", limit = 50, signal }) {
  if (isDevPreviewToken(token)) return previewOperatorTenants().then(normalizeOperatorTenantPage);
  const query = new URLSearchParams();
  if (after) query.set("after", after);
  query.set("limit", String(limit));
  return request(`/api/operator/tenants?${query}`, { token, signal }).then(normalizeOperatorTenantPage);
}

export function grantServiceCredit({ token, tenantId, amountMicroUsd, idempotencyKey, signal }) {
  if (!cleanText(tenantId) || tenantId !== tenantId.trim() || !safeNonNegative(amountMicroUsd, true) || amountMicroUsd <= 0 || !SERVICE_CREDIT_KEY_RE.test(idempotencyKey || "")) {
    return Promise.reject(new ApiError(400, "Invalid service-credit grant."));
  }
  const result = isDevPreviewToken(token)
    ? previewGrantServiceCredit(tenantId, amountMicroUsd, idempotencyKey)
    : request(`/api/operator/tenants/${encodeURIComponent(tenantId)}/billing/service-credits`, { token, method: "POST", body: { amount_micro_usd: amountMicroUsd, idempotency_key: idempotencyKey }, signal });
  return Promise.resolve(result).then((payload) => normalizeServiceCreditGrant(payload, tenantId, amountMicroUsd, idempotencyKey));
}

export function updateOperatorTenantLimits({ token, tenantId, limits, signal }) {
  return request(`/api/operator/tenants/${encodeURIComponent(tenantId)}/limits`, {
    token,
    method: "PATCH",
    body: {
      max_concurrent_bursts: limits.maxConcurrentBursts,
      max_hourly_usd: limits.maxHourlyUsd,
    },
    signal,
  }).then((payload) => normalizeOperatorTenantUpdate(payload, tenantId));
}

export function assignHostedCluster({ token, tenantId, draft, signal }) {
  return request(`/api/operator/tenants/${encodeURIComponent(tenantId)}/hosted-clusters`, {
    token,
    method: "POST",
    body: assignmentBody(draft),
    signal,
  }).then((payload) => normalizeHostedAssignment(payload, tenantId));
}

export function rotateHostedClusterCredential({ token, tenantId, clusterId, signal }) {
  return request(`/api/operator/tenants/${encodeURIComponent(tenantId)}/hosted-clusters/${encodeURIComponent(clusterId)}/credential`, {
    token,
    method: "POST",
    signal,
  }).then((payload) => {
    const assignment = normalizeHostedAssignment(payload, tenantId);
    if (assignment.cluster.clusterId !== clusterId) return invalid("The operator service answered with the wrong hosted cluster.");
    return assignment;
  });
}

export function revokeHostedCluster({ token, tenantId, clusterId, signal }) {
  return request(`/api/operator/tenants/${encodeURIComponent(tenantId)}/hosted-clusters/${encodeURIComponent(clusterId)}`, {
    token,
    method: "DELETE",
    signal,
  }).then((payload) => normalizeHostedDelete(payload, tenantId, clusterId));
}
