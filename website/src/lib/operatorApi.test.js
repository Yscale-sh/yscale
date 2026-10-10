import assert from "node:assert/strict";
import test from "node:test";

import { ApiError } from "./apiError.js";
import {
  assignHostedCluster,
  appendOperatorTenantPage,
  assignmentBody,
  assignmentDraft,
  fetchHostedRequests,
  fetchHostedClusters,
  fetchOperatorTenants,
  createServiceCreditKey,
  grantServiceCredit,
  hostedUninstallCommand,
  normalizeHostedAssignment,
  normalizeHostedDelete,
  normalizeHostedInventory,
  normalizeHostedRequestQueue,
  normalizeOperatorTenantPage,
  normalizeOperatorTenantUpdate,
  operatorCapability,
  operatorMutationAccess,
  revokeHostedCluster,
  rotateHostedClusterCredential,
  replaceOperatorTenant,
  tenantLimitDraft,
  tenantLimitValues,
  updateOperatorTenantLimits,
  usdToMicroUSD,
} from "./operatorApi.js";

test("service credit converts USD exactly and posts only the bounded grant body", async () => {
  assert.deepEqual(usdToMicroUSD("12.345678"), { value: 12345678, error: "" });
  assert.equal(usdToMicroUSD("0").value, null);
  assert.equal(usdToMicroUSD("1.0000001").value, null);
  assert.match(createServiceCreditKey(), /^svc-credit-/);
  const stub = stubFetch(async () => jsonResponse(200, { tenant_id: "tenant-a", amount_micro_usd: 1250000, currency: "USD", idempotency_key: "svc-credit-test", granted: true }));
  try {
    const result = await grantServiceCredit({ token: "token", tenantId: "tenant-a", amountMicroUsd: 1250000, idempotencyKey: "svc-credit-test" });
    assert.equal(result.amountMicroUsd, 1250000);
    assert.equal(stub.calls[0].url, "/api/operator/tenants/tenant-a/billing/service-credits");
    assert.deepEqual(JSON.parse(stub.calls[0].init.body), { amount_micro_usd: 1250000, idempotency_key: "svc-credit-test" });
    assert.equal(stub.calls[0].init.credentials, "same-origin");
  } finally { stub.restore(); }
});

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

function stubFetch(handler) {
  const calls = [];
  const previous = globalThis.fetch;
  globalThis.fetch = async (url, init) => { calls.push({ url, init }); return handler(url, init); };
  return { calls, restore: () => { globalThis.fetch = previous; } };
}

const QUEUE = {
  requests: [
    { tenant_id: "tenant_new", name: "New Lab", plan: "scale", requested_at: "2026-08-16T12:00:00Z" },
    { tenant_id: "tenant_old", plan: "starter", requested_at: "2026-08-15T12:00:00Z" },
  ],
};

const ASSIGNMENT = {
  tenant_id: "tenant_old",
  cluster: {
    cluster_id: "hosted-old",
    name: "Old Lab Hosted",
    source: "hosted",
    state: "never_connected",
    hosted_namespace: "ten-old",
    registered_at: "2026-08-16T12:30:00Z",
  },
  connector_token: "ysc_once",
  helm_release: "yscale-agent",
  connector_rbac_set: "hosted-tenant",
  helm_command: "helm upgrade --install yscale-agent ...",
};

const INVENTORY = {
  clusters: [
    { tenant_id: "tenant-z", plan: "starter", cluster: { ...ASSIGNMENT.cluster, cluster_id: "hosted-z", name: "Hosted Z" } },
    { tenant_id: "tenant-a", name: "Acme", plan: "enterprise", cluster: { ...ASSIGNMENT.cluster, cluster_id: "hosted-b", name: "Hosted B" } },
    { tenant_id: "tenant-a", name: "Acme", plan: "enterprise", cluster: { ...ASSIGNMENT.cluster, cluster_id: "hosted-a", name: "Hosted A" } },
  ],
};

const TENANT_ROW = {
  tenant_id: "tenant-a",
  tenant_name: "Acme Research",
  plan: "enterprise",
  running_bursts: 3,
  hourly_usd: 12.5,
  projected_daily_usd: 148.25,
  limits: { max_concurrent_bursts: 8, max_hourly_usd: 25 },
};

test("tenant inventory normalization is exact, ordered, unique, finite, and preserves its cursor", () => {
  const page = normalizeOperatorTenantPage({ tenants: [TENANT_ROW, { ...TENANT_ROW, tenant_id: "tenant-z", tenant_name: "Zulu Lab" }], next_after: "tenant-z" });
  assert.deepEqual(page, {
    tenants: [
      { tenantId: "tenant-a", tenantName: "Acme Research", plan: "enterprise", runningBursts: 3, hourlyUsd: 12.5, projectedDailyUsd: 148.25, limits: { maxConcurrentBursts: 8, maxHourlyUsd: 25 } },
      { tenantId: "tenant-z", tenantName: "Zulu Lab", plan: "enterprise", runningBursts: 3, hourlyUsd: 12.5, projectedDailyUsd: 148.25, limits: { maxConcurrentBursts: 8, maxHourlyUsd: 25 } },
    ],
    nextAfter: "tenant-z",
  });
  assert.deepEqual(normalizeOperatorTenantPage({ tenants: [] }), { tenants: [] });
  assert.equal(normalizeOperatorTenantPage({ tenants: [{ ...TENANT_ROW, tenant_name: "" }] }).tenants[0].tenantName, "tenant-a");
  for (const bad of [
    null,
    {},
    { tenants: [], next_after: "" },
    { tenants: [TENANT_ROW], extra: true },
    { tenants: [{ ...TENANT_ROW, tenant_name: " Acme " }] },
    { tenants: [{ ...TENANT_ROW, running_bursts: 1.5 }] },
    { tenants: [{ ...TENANT_ROW, hourly_usd: Infinity }] },
    { tenants: [{ ...TENANT_ROW, limits: { ...TENANT_ROW.limits, max_concurrent_bursts: 1.5 } }] },
    { tenants: [{ ...TENANT_ROW, limits: { ...TENANT_ROW.limits, max_hourly_usd: -1 } }] },
    { tenants: [{ ...TENANT_ROW, limits: { ...TENANT_ROW.limits, spare: 1 } }] },
    { tenants: [{ ...TENANT_ROW, tenant_id: "tenant-z" }, TENANT_ROW] },
    { tenants: [TENANT_ROW, TENANT_ROW] },
  ]) assert.throws(() => normalizeOperatorTenantPage(bad), (error) => error instanceof ApiError && error.status === 502);
});

test("tenant page helpers reject overlap and replace only the acknowledged tenant", () => {
  const first = normalizeOperatorTenantPage({ tenants: [TENANT_ROW] }).tenants;
  const second = normalizeOperatorTenantPage({ tenants: [{ ...TENANT_ROW, tenant_id: "tenant-z", tenant_name: "Zulu" }] });
  assert.deepEqual(appendOperatorTenantPage(first, second).map((row) => row.tenantId), ["tenant-a", "tenant-z"]);
  assert.throws(() => appendOperatorTenantPage(first, { tenants: first }), (error) => error.status === 502);
  const replacement = { ...first[0], limits: { maxConcurrentBursts: 0, maxHourlyUsd: 10 } };
  assert.deepEqual(replaceOperatorTenant(first, replacement), [replacement]);
  assert.throws(() => replaceOperatorTenant(first, { ...replacement, tenantId: "other" }), (error) => error.status === 502);
});

test("tenant limit drafts require both finite non-negative values and accept zero as unlimited", () => {
  assert.deepEqual(tenantLimitDraft({ maxConcurrentBursts: 0, maxHourlyUsd: 12.5 }), { maxConcurrentBursts: "0", maxHourlyUsd: "12.5" });
  assert.deepEqual(tenantLimitValues({ maxConcurrentBursts: "0", maxHourlyUsd: " 12.5 " }), { values: { maxConcurrentBursts: 0, maxHourlyUsd: 12.5 }, errors: {} });
  for (const draft of [
    { maxConcurrentBursts: "", maxHourlyUsd: "2" },
    { maxConcurrentBursts: "-1", maxHourlyUsd: "2" },
    { maxConcurrentBursts: "1.5", maxHourlyUsd: "2" },
    { maxConcurrentBursts: "2", maxHourlyUsd: "NaN" },
    { maxConcurrentBursts: "2", maxHourlyUsd: "Infinity" },
  ]) assert.notDeepEqual(tenantLimitValues(draft).errors, {});
});

test("tenant inventory GET and guardrail PATCH use canonical same-origin transport", async () => {
  const update = { tenant: { ...TENANT_ROW, tenant_id: "tenant/a", limits: { max_concurrent_bursts: 0, max_hourly_usd: 30 } }, changed: true };
  const responses = [jsonResponse(200, { tenants: [TENANT_ROW], next_after: "tenant-a" }), jsonResponse(200, update)];
  const stub = stubFetch(() => responses.shift());
  const controller = new AbortController();
  try {
    await fetchOperatorTenants({ token: "operator-token", after: "tenant 0", limit: 25, signal: controller.signal });
    const saved = await updateOperatorTenantLimits({ token: "operator-token", tenantId: "tenant/a", limits: { maxConcurrentBursts: 0, maxHourlyUsd: 30 }, signal: controller.signal });
    assert.equal(stub.calls[0].url, "/api/operator/tenants?after=tenant+0&limit=25");
    assert.equal(stub.calls[0].init.method, "GET");
    assert.equal(stub.calls[0].init.body, undefined);
    assert.equal(stub.calls[1].url, "/api/operator/tenants/tenant%2Fa/limits");
    assert.equal(stub.calls[1].init.method, "PATCH");
    assert.equal(stub.calls[1].init.body, '{"max_concurrent_bursts":0,"max_hourly_usd":30}');
    assert.equal(stub.calls[1].init.headers["Content-Type"], "application/json");
    assert.equal(saved.changed, true);
    assert.equal(saved.tenant.limits.maxConcurrentBursts, 0);
  } finally { stub.restore(); }
});

test("tenant update normalization rejects wrong tenants, extra keys, and non-boolean change flags", () => {
  const good = { tenant: TENANT_ROW, changed: false };
  assert.equal(normalizeOperatorTenantUpdate(good, "tenant-a").changed, false);
  for (const bad of [
    { ...good, changed: "false" },
    { ...good, extra: true },
    { ...good, tenant: { ...TENANT_ROW, tenant_id: "other" } },
  ]) assert.throws(() => normalizeOperatorTenantUpdate(bad, "tenant-a"), (error) => error.status === 502);
});

test("queue normalization is strict and always oldest first", () => {
  assert.deepEqual(normalizeHostedRequestQueue(QUEUE).requests.map((row) => row.tenantId), ["tenant_old", "tenant_new"]);
  assert.equal(normalizeHostedRequestQueue(QUEUE).requests[0].tenantName, "tenant_old");
  for (const bad of [null, {}, { requests: {}, extra: true }, { requests: [{ ...QUEUE.requests[0], tenant_name: "wrong key" }] }, { requests: [{ ...QUEUE.requests[0], tenant_id: "" }] }, { requests: [{ ...QUEUE.requests[0], name: null }] }, { requests: [{ ...QUEUE.requests[0], requested_at: "yesterday" }] }, { requests: [QUEUE.requests[0], QUEUE.requests[0]] }]) {
    assert.throws(() => normalizeHostedRequestQueue(bad), (error) => error instanceof ApiError && error.status === 502);
  }
});

test("capability derives only from the operator probe outcome", () => {
  assert.deepEqual(operatorCapability(), { available: true, expired: false, warning: "" });
  for (const status of [403, 404]) assert.deepEqual(operatorCapability(new ApiError(status, "hidden")), { available: false, expired: false, warning: "" });
  assert.equal(operatorCapability(new ApiError(401, "expired")).expired, true);
  for (const status of [0, 429, 502, 503]) {
    const decision = operatorCapability(new ApiError(status, `failure ${status}`));
    assert.equal(decision.available, false);
    assert.equal(decision.expired, false);
    assert.match(decision.warning, /failure/);
  }
});

test("operator mutations preserve their row and form on tenant-not-found", () => {
  assert.deepEqual(operatorMutationAccess(new ApiError(401, "expired")), { expired: true, unavailable: false });
  assert.deepEqual(operatorMutationAccess(new ApiError(403, "hidden")), { expired: false, unavailable: true });
  assert.deepEqual(operatorMutationAccess(new ApiError(404, "tenant not found")), { expired: false, unavailable: false });
  assert.deepEqual(operatorMutationAccess(new ApiError(503, "transient")), { expired: false, unavailable: false });
});

test("the operator probe is a bearer GET with AbortSignal support", async () => {
  const stub = stubFetch(() => jsonResponse(200, QUEUE));
  const controller = new AbortController();
  try {
    await fetchHostedRequests({ token: "operator-token", signal: controller.signal });
    assert.deepEqual(stub.calls.map(({ url }) => url), ["/api/operator/hosted-capacity/requests"]);
    assert.equal(stub.calls[0].init.method, "GET");
    assert.equal(stub.calls[0].init.headers.Authorization, "Bearer operator-token");
    assert.equal(stub.calls[0].init.signal, controller.signal);
  } finally { stub.restore(); }
});

test("mapped operator errors preserve status and actionable copy", async () => {
  for (const [status, pattern] of [[401, /Sign in again/], [403, /operator access/], [404, /not available/], [409, /already assigned/], [429, /Too many/], [502, /did not answer/], [503, /unavailable/]]) {
    const stub = stubFetch(() => jsonResponse(status, { error: "upstream-code", message: "unsafe detail" }));
    try {
      await assert.rejects(fetchHostedRequests({ token: "t" }), (error) => error.status === status && error.code === "upstream-code" && pattern.test(error.message));
    } finally { stub.restore(); }
  }
  const stub = stubFetch(() => { throw new TypeError("offline"); });
  try { await assert.rejects(fetchHostedRequests({ token: "t" }), (error) => error.status === 0 && error.isOffline); } finally { stub.restore(); }
});

test("assignment defaults are server generated and advanced values are opt in", () => {
  assert.deepEqual(assignmentDraft(), { advanced: false, clusterId: "", name: "", namespace: "" });
  assert.deepEqual(assignmentBody({ advanced: false, clusterId: "ignored", name: "ignored", namespace: "ignored" }), {});
  assert.deepEqual(assignmentBody({ advanced: true, clusterId: " hosted-1 ", name: " Hosted One ", namespace: " ten-one " }), {
    cluster_id: "hosted-1", name: "Hosted One", namespace: "ten-one",
  });
});

test("assignment posts the bounded draft and normalizes the one-time response", async () => {
  const stub = stubFetch(() => jsonResponse(201, ASSIGNMENT));
  try {
    const issued = await assignHostedCluster({ token: "operator-token", tenantId: "tenant_old", draft: { advanced: false } });
    assert.equal(issued.cluster.hostedNamespace, "ten-old");
    assert.equal(issued.connectorToken, "ysc_once");
    assert.equal(stub.calls[0].url, "/api/operator/tenants/tenant_old/hosted-clusters");
    assert.equal(stub.calls[0].init.method, "POST");
    assert.equal(stub.calls[0].init.body, "{}");
  } finally { stub.restore(); }
});

test("assignment response refuses tenant drift, non-hosted clusters, and partial reveals", () => {
  for (const bad of [
    { ...ASSIGNMENT, tenant_id: "other" },
    { ...ASSIGNMENT, cluster: { ...ASSIGNMENT.cluster, source: "tenant" } },
    { ...ASSIGNMENT, cluster: { ...ASSIGNMENT.cluster, hosted_namespace: "" } },
    { ...ASSIGNMENT, connector_token: "" },
    { ...ASSIGNMENT, helm_command: "" },
    { ...ASSIGNMENT, credential_copy: "must not be accepted" },
  ]) assert.throws(() => normalizeHostedAssignment(bad, "tenant_old"), (error) => error.status === 502);
});

test("inventory normalization is exact, hosted-only, and deterministically sorted", () => {
  const inventory = normalizeHostedInventory(INVENTORY);
  assert.deepEqual(inventory.clusters.map((row) => `${row.tenantId}/${row.cluster.clusterId}`), ["tenant-a/hosted-a", "tenant-a/hosted-b", "tenant-z/hosted-z"]);
  assert.equal(inventory.clusters[2].tenantName, "tenant-z");
  for (const bad of [
    null,
    {},
    { clusters: "all" },
    { clusters: [{ ...INVENTORY.clusters[0], extra: true }] },
    { clusters: [{ ...INVENTORY.clusters[0], cluster: { ...INVENTORY.clusters[0].cluster, source: "tenant" } }] },
    { clusters: [{ ...INVENTORY.clusters[0], cluster: { ...INVENTORY.clusters[0].cluster, hosted_namespace: "" } }] },
    { clusters: [INVENTORY.clusters[0], INVENTORY.clusters[0]] },
  ]) assert.throws(() => normalizeHostedInventory(bad), (error) => error instanceof ApiError && error.status === 502);
});

test("inventory fetch is an authenticated same-origin GET", async () => {
  const stub = stubFetch(() => jsonResponse(200, INVENTORY));
  const controller = new AbortController();
  try {
    const inventory = await fetchHostedClusters({ token: "operator-token", signal: controller.signal });
    assert.equal(inventory.clusters.length, 3);
    assert.equal(stub.calls[0].url, "/api/operator/hosted-clusters");
    assert.equal(stub.calls[0].init.method, "GET");
    assert.equal(stub.calls[0].init.headers.Authorization, "Bearer operator-token");
    assert.equal(stub.calls[0].init.signal, controller.signal);
    assert.equal(stub.calls[0].init.body, undefined);
  } finally { stub.restore(); }
});

test("rotate posts no body or content type and reuses the strict one-time response", async () => {
  const stub = stubFetch(() => jsonResponse(200, ASSIGNMENT));
  try {
    const issued = await rotateHostedClusterCredential({ token: "operator-token", tenantId: "tenant_old", clusterId: "hosted-old" });
    assert.equal(issued.connectorToken, "ysc_once");
    assert.equal(stub.calls[0].url, "/api/operator/tenants/tenant_old/hosted-clusters/hosted-old/credential");
    assert.equal(stub.calls[0].init.method, "POST");
    assert.equal(stub.calls[0].init.body, undefined);
    assert.equal(stub.calls[0].init.headers["Content-Type"], undefined);
  } finally { stub.restore(); }
});

test("rotate refuses a credential for a different cluster", async () => {
  const stub = stubFetch(() => jsonResponse(200, { ...ASSIGNMENT, cluster: { ...ASSIGNMENT.cluster, cluster_id: "hosted-other" } }));
  try {
    await assert.rejects(rotateHostedClusterCredential({ token: "t", tenantId: "tenant_old", clusterId: "hosted-old" }), (error) => error.status === 502);
  } finally { stub.restore(); }
});

test("revoke deletes without a body and requires an exact confirmation", async () => {
  const confirmation = { tenant_id: "tenant_old", cluster_id: "hosted-old", hosted_namespace: "ten-old", deleted: true };
  const stub = stubFetch(() => jsonResponse(200, confirmation));
  try {
    assert.deepEqual(await revokeHostedCluster({ token: "operator-token", tenantId: "tenant_old", clusterId: "hosted-old" }), {
      tenantId: "tenant_old", clusterId: "hosted-old", hostedNamespace: "ten-old", deleted: true,
    });
    assert.equal(stub.calls[0].url, "/api/operator/tenants/tenant_old/hosted-clusters/hosted-old");
    assert.equal(stub.calls[0].init.method, "DELETE");
    assert.equal(stub.calls[0].init.body, undefined);
  } finally { stub.restore(); }
  for (const bad of [{ ...confirmation, deleted: false }, { ...confirmation, extra: true }, { ...confirmation, cluster_id: "other" }]) {
    assert.throws(() => normalizeHostedDelete(bad, "tenant_old", "hosted-old"), (error) => error.status === 502);
  }
});

test("the non-secret follow-up uses the release naming contract", () => {
  assert.equal(hostedUninstallCommand("hosted-old"), "helm uninstall yscale-agent-hosted-old --namespace yscale-system");
  assert.equal(hostedUninstallCommand(""), "");
});

test("operator API code has no browser storage seam", async () => {
  const source = await import("node:fs/promises").then((fs) => fs.readFile(new URL("./operatorApi.js", import.meta.url), "utf8"));
  assert.doesNotMatch(source, /localStorage|sessionStorage|indexedDB/);
});
