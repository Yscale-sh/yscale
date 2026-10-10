import test from "node:test";
import assert from "node:assert/strict";

import { addMember, createTenant, fetchUsage, updateMemberRole } from "./accountApi.js";

function stubFetch(handler) {
  const calls = [];
  const previous = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({ url, init });
    return handler(url, init);
  };
  return {
    calls,
    restore() { globalThis.fetch = previous; },
  };
}

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

test("add member sends a bounded same-origin JSON request", async () => {
  const summary = { account_id: "acct_9", email: "person@example.com", role: "member" };
  const stub = stubFetch(() => jsonResponse(201, summary));
  try {
    const got = await addMember({ token: "bearer-value", tenantId: "cust 42", email: "person@example.com", role: "member" });
    assert.deepEqual(got, summary);
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust%2042/members");
    assert.equal(init.method, "POST");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.headers["Content-Type"], "application/json");
    assert.equal(init.body, JSON.stringify({ email: "person@example.com", role: "member" }));
  } finally {
    stub.restore();
  }
});

test("create tenant sends only the workspace name to the account seam", async () => {
  const envelope = { account_id: "acct_9", tenants: [{ customer_id: "tenant_1", role: "owner" }] };
  const stub = stubFetch(() => jsonResponse(201, envelope));
  try {
    const got = await createTenant({ token: "bearer-value", name: "First Workspace" });
    assert.deepEqual(got, envelope);
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/account/tenants");
    assert.equal(init.method, "POST");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.headers["Content-Type"], "application/json");
    assert.equal(init.body, JSON.stringify({ name: "First Workspace" }));
  } finally {
    stub.restore();
  }
});

test("update member role sends only the role JSON body", async () => {
  const summary = { account_id: "acct_9", email: "person@example.com", role: "admin" };
  const stub = stubFetch(() => jsonResponse(200, summary));
  try {
    const got = await updateMemberRole({ token: "bearer-value", tenantId: "cust_42", accountId: "acct 9", role: "admin" });
    assert.deepEqual(got, summary);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/members/acct%209");
    assert.equal(init.method, "PATCH");
    assert.equal(init.headers["Content-Type"], "application/json");
    assert.equal(init.body, JSON.stringify({ role: "admin" }));
  } finally {
    stub.restore();
  }
});

test("fetch usage reads the live same-origin endpoint without touching workload history", async () => {
  const usage = { tenant_id: "cust_42", observed_at: "2026-08-12T20:30:00Z", running_bursts: 1 };
  const stub = stubFetch(() => jsonResponse(200, usage));
  try {
    assert.deepEqual(await fetchUsage({ token: "bearer-value", tenantId: "cust_42" }), usage);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/usage");
    assert.equal(init.method, "GET");
    assert.equal(init.headers.Accept, "application/json");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.body, undefined);
  } finally {
    stub.restore();
  }
});
