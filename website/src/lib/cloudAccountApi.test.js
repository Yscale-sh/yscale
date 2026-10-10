import assert from "node:assert/strict";
import test from "node:test";

import { ApiError } from "./apiError.js";
import { deleteLinodeCloudAccount, fetchLinodeCloudAccount, normalizeLinodeCloudAccount, putLinodeCloudAccount } from "./cloudAccountApi.js";
import { previewDeleteLinodeCloudAccount, previewLinodeCloudAccount, previewPutLinodeCloudAccount, resetDevPreview } from "./devPreview.js";

const SAFE = {
  tenant_id: "cust_42", role: "owner", changed: true,
  account: { id: "ca_1", provider: "linode", provider_account_id: "acct_1", region: "us-east", cpu_image_ready: true, gpu_image_ready: false, updated_at: "2026-08-16T12:00:00Z" },
};

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

function stubFetch(handler) {
  const previous = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, init }); return handler(url, init); };
  return { calls, restore: () => { globalThis.fetch = previous; } };
}

test("strict normalization accepts only the safe Linode envelope", () => {
  const value = normalizeLinodeCloudAccount(SAFE, "cust_42");
  assert.equal(value.account.providerAccountId, "acct_1");
  assert.equal(value.account.gpuImageReady, false);
  assert.equal(normalizeLinodeCloudAccount({ tenant_id: "cust_42", role: "viewer", account: null }, "cust_42").account, null);
  for (const payload of [
    { ...SAFE, token: "leak" },
    { ...SAFE, account: { ...SAFE.account, ciphertext: "sealed" } },
    { ...SAFE, tenant_id: "other" },
    { ...SAFE, account: { ...SAFE.account, provider: "aws" } },
    { ...SAFE, changed: "yes" },
    { ...SAFE, account: { ...SAFE.account, updated_at: "today" } },
  ]) assert.throws(() => normalizeLinodeCloudAccount(payload, "cust_42"), ApiError);
});

test("GET PUT and DELETE use the exact same-origin lifecycle route", async () => {
  const stub = stubFetch(() => jsonResponse(200, { ...SAFE, tenant_id: "cust 42" }));
  try {
    await fetchLinodeCloudAccount({ token: "bearer", tenantId: "cust 42" });
    await putLinodeCloudAccount({ token: "bearer", tenantId: "cust 42", providerToken: "write-only", region: "us-east", cpuImage: "linode/ubuntu24.04" });
    await deleteLinodeCloudAccount({ token: "bearer", tenantId: "cust 42" });
    assert.deepEqual(stub.calls.map((call) => [call.url, call.init.method]), [
      ["/api/tenants/cust%2042/cloud-accounts/linode", "GET"],
      ["/api/tenants/cust%2042/cloud-accounts/linode", "PUT"],
      ["/api/tenants/cust%2042/cloud-accounts/linode", "DELETE"],
    ]);
    assert.deepEqual(JSON.parse(stub.calls[1].init.body), { token: "write-only", region: "us-east", cpu_image: "linode/ubuntu24.04" });
    assert.equal(stub.calls[2].init.body, undefined);
    for (const call of stub.calls) {
      assert.equal(call.init.credentials, "same-origin");
      assert.equal(call.init.headers.Authorization, "Bearer bearer");
    }
  } finally { stub.restore(); }
});

test("secret-bearing success and error payloads are refused", async () => {
  for (const [status, body] of [[200, { ...SAFE, token: "leak" }], [409, { error: "busy", credential: "leak" }]]) {
    const stub = stubFetch(() => jsonResponse(status, body));
    try {
      await assert.rejects(fetchLinodeCloudAccount({ token: "bearer", tenantId: "cust_42" }), (error) => error.status === 502);
    } finally { stub.restore(); }
  }
});

test("a safe disconnect conflict remains actionable", async () => {
  const stub = stubFetch(() => jsonResponse(409, { error: "cloud_account_in_use", message: "A live burst still holds this account." }));
  try {
    await assert.rejects(deleteLinodeCloudAccount({ token: "bearer", tenantId: "cust_42" }), (error) => error.status === 409 && /live burst/.test(error.message));
  } finally { stub.restore(); }
});

test("the preview walks disconnect, connect, rotate, blocked disconnect, and successful disconnect without retaining a token", async () => {
  resetDevPreview();
  assert.equal((await previewLinodeCloudAccount("tenant-acme-research")).account, null);
  const connected = await previewPutLinodeCloudAccount("tenant-acme-research", { token: "dummy-value", region: "us-east" });
  assert.equal(connected.account.provider_account_id, "linode-preview-owner");
  assert.equal(JSON.stringify(connected).includes("dummy-value"), false);
  const rotated = await previewPutLinodeCloudAccount("tenant-acme-research", { token: "another-dummy", region: "us-southeast", gpu_image: "private/yscale-gpu" });
  assert.equal(rotated.account.provider_account_id, connected.account.provider_account_id);
  assert.equal(rotated.account.gpu_image_ready, true);
  await assert.rejects(previewDeleteLinodeCloudAccount("tenant-acme-research"), (error) => error.status === 409);
  assert.notEqual((await previewLinodeCloudAccount("tenant-acme-research")).account, null);
  assert.equal((await previewDeleteLinodeCloudAccount("tenant-acme-research")).account, null);
  await assert.rejects(previewPutLinodeCloudAccount("tenant-platform-lab", { token: "dummy", region: "us-east" }), (error) => error.status === 403);
});
