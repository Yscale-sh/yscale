import assert from "node:assert/strict";
import test from "node:test";

import { ApiError } from "./apiError.js";
import {
  fetchClusterPolicy,
  normalizeClusterPolicy,
  normalizeClusterPolicyDraft,
  putClusterPolicy,
} from "./clusterPolicy.js";

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

const POLICY = {
  tenant_id: "cust_42",
  role: "owner",
  changed: true,
  policy: { auto: "ordered", allow: ["b", "a", "future-1"], deny: ["blocked"] },
};

test("normalizes an ordered policy without sorting the allow list", () => {
  const normalized = normalizeClusterPolicy(POLICY);
  assert.equal(normalized.tenantId, "cust_42");
  assert.equal(normalized.policy.auto, "ordered");
  assert.equal(normalized.changed, true);
  assert.deepEqual(normalized.policy.allow, ["b", "a", "future-1"]);
  assert.deepEqual(normalized.policy.deny, ["blocked"]);
});

test("malformed policy responses are rejected", () => {
  const bad = [
    null,
    {},
    { tenant_id: "cust_42", policy: { auto: "ordered", allow: [], deny: ["bad/id"] } },
    { tenant_id: "cust_42", policy: { auto: "anything", allow: [], deny: [] } },
    { tenant_id: "cust_42", policy: { auto: "ordered", allow: ["a", "a"], deny: [] } },
    { tenant_id: "cust_42", policy: { auto: "ordered", allow: ["a"], deny: ["a"] } },
    { tenant_id: "cust_42", policy: { auto: "ordered", allow: "a", deny: [] } },
  ];
  for (const payload of bad) {
    assert.throws(() => normalizeClusterPolicy(payload), ApiError);
  }
});

test("draft validation accepts disconnected ids but refuses shape, duplicates, and overlap", () => {
  assert.deepEqual(
    normalizeClusterPolicyDraft({ auto: "ordered", allow: "future-a\nfuture-b", deny: "blocked-1" }),
    { auto: "ordered", allow: ["future-a", "future-b"], deny: ["blocked-1"] },
  );
  assert.throws(() => normalizeClusterPolicyDraft({ allow: "bad/id" }), /invalid cluster id/);
  assert.throws(() => normalizeClusterPolicyDraft({ allow: "a\na" }), /duplicate/);
  assert.throws(() => normalizeClusterPolicyDraft({ allow: "a", deny: "a" }), /both allowed and denied/);
});

test("GET and PUT use the same-origin policy seam", async () => {
  const stub = stubFetch((url, init) => jsonResponse(200, POLICY));
  try {
    const read = await fetchClusterPolicy({ token: "bearer-value", tenantId: "cust 42" });
    assert.equal(read.policy.auto, "ordered");
    assert.equal(stub.calls[0].url, "/api/tenants/cust%2042/cluster-policy");
    assert.equal(stub.calls[0].init.credentials, "same-origin");
    assert.equal(stub.calls[0].init.headers.Authorization, "Bearer bearer-value");

    await putClusterPolicy({ token: "bearer-value", tenantId: "cust 42", policy: { auto: "require_pin", allow: ["a"], deny: [] } });
    assert.equal(stub.calls[1].init.method, "PUT");
    assert.equal(stub.calls[1].init.headers["Content-Type"], "application/json");
    assert.deepEqual(JSON.parse(stub.calls[1].init.body), { auto: "require_pin", allow: ["a"], deny: [] });
  } finally {
    stub.restore();
  }
});

test("policy service refusals keep their status", async () => {
  const stub = stubFetch(() => jsonResponse(403, { error: "owner_required" }));
  try {
    await assert.rejects(
      fetchClusterPolicy({ token: "t", tenantId: "cust_42" }),
      (error) => error.status === 403 && error.code === "owner_required",
    );
  } finally {
    stub.restore();
  }
});
