import assert from "node:assert/strict";
import test from "node:test";

import { ApiError } from "./apiError.js";
import { deleteCluster, fetchClusters, registerCluster, rotateClusterCredential } from "./clusterApi.js";

// The client is exercised against a stub fetch: these tests are about the
// request it builds and the answers it turns into, not about the network.
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

test("a cluster read is a same-origin GET carrying only the bearer", async () => {
  const stub = stubFetch(() => jsonResponse(200, {
    tenant_id: "cust 42",
    observed_at: "2026-08-12T20:31:04Z",
    partial: true,
    policy: { auto: "ordered", allow: ["edge-1"], deny: [] },
    clusters: [{ cluster_id: "edge-1", connections: 1 }],
  }));
  try {
    const observation = await fetchClusters({ token: "bearer-value", tenantId: "cust 42" });
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust%2042/clusters");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.method, "GET");
    assert.equal(observation.livePartial, true);
    assert.equal(observation.policy.auto, "ordered");
    assert.deepEqual(observation.clusters.map((cluster) => cluster.id), ["edge-1"]);
  } finally {
    stub.restore();
  }
});

// The launch form fails closed on an error, so an error has to stay an error.
// A 200 that carries no cluster list is not "this tenant has no clusters".
test("an unreadable answer is refused rather than read as an empty fleet", async () => {
  for (const body of [null, {}, { clusters: "all of them" }]) {
    const stub = stubFetch(() => jsonResponse(200, body));
    try {
      await assert.rejects(
        fetchClusters({ token: "t", tenantId: "cust_42" }),
        (error) => error instanceof ApiError && error.status === 502,
      );
    } finally {
      stub.restore();
    }
  }
});

test("a malformed additive policy on the cluster answer is refused", async () => {
  const stub = stubFetch(() => jsonResponse(200, {
    tenant_id: "cust_42",
    clusters: [],
    policy: { auto: "ordered", allow: ["bad/id"], deny: [] },
  }));
  try {
    await assert.rejects(
      fetchClusters({ token: "t", tenantId: "cust_42" }),
      (error) => error instanceof ApiError && error.status === 502,
    );
  } finally {
    stub.restore();
  }
});

test("an upstream refusal keeps its status and says what to do", async () => {
  const cases = [
    [401, /Sign in again/],
    [403, /not allowed to read/],
    [404, /does not exist/],
    [503, /unavailable in this environment/],
  ];
  for (const [status, message] of cases) {
    const stub = stubFetch(() => jsonResponse(status, { error: "denied" }));
    try {
      await assert.rejects(
        fetchClusters({ token: "t", tenantId: "cust_42" }),
        (error) => error.status === status && message.test(error.message) && error.code === "denied",
      );
    } finally {
      stub.restore();
    }
  }
});

test("a browser that cannot reach the seam says so instead of reporting no clusters", async () => {
  const stub = stubFetch(() => { throw new TypeError("Failed to fetch"); });
  try {
    await assert.rejects(
      fetchClusters({ token: "t", tenantId: "cust_42" }),
      (error) => error instanceof ApiError && error.status === 0 && error.isOffline,
    );
  } finally {
    stub.restore();
  }
});

test("an aborted read is not turned into a failure the form has to show", async () => {
  const stub = stubFetch(() => {
    const error = new Error("aborted");
    error.name = "AbortError";
    throw error;
  });
  try {
    await assert.rejects(fetchClusters({ token: "t", tenantId: "cust_42" }), (error) => error.name === "AbortError");
  } finally {
    stub.restore();
  }
});

const CREDENTIAL_ANSWER = {
  tenant_id: "cust_42",
  cluster: { cluster_id: "prod-east", name: "Prod East", state: "never_connected", registered_at: "2026-08-13T10:00:00Z" },
  connector_token: "ysc_one_time_value",
  helm_install: "helm install yscale-agent ...",
  role: "owner",
};

test("registering a cluster is a JSON POST that returns the one-time credential", async () => {
  const stub = stubFetch(() => jsonResponse(201, CREDENTIAL_ANSWER));
  try {
    const issued = await registerCluster({ token: "bearer-value", tenantId: "cust_42", name: "Prod East", clusterId: "prod-east" });
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/clusters");
    assert.equal(init.method, "POST");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.headers["Content-Type"], "application/json");
    assert.deepEqual(JSON.parse(init.body), { name: "Prod East", cluster_id: "prod-east" });
    assert.equal(issued.cluster.id, "prod-east");
    assert.equal(issued.cluster.name, "Prod East");
    assert.equal(issued.connectorToken, "ysc_one_time_value");
    assert.match(issued.helmCommand, /^helm install/);
    assert.equal(issued.role, "owner");
  } finally {
    stub.restore();
  }
});

// The primary flow asks for a name only; the body must not carry an empty
// cluster_id central would have to guess about.
test("a register without a custom id sends only the name", async () => {
  const stub = stubFetch(() => jsonResponse(201, CREDENTIAL_ANSWER));
  try {
    await registerCluster({ token: "t", tenantId: "cust_42", name: "Prod East" });
    assert.deepEqual(JSON.parse(stub.calls[0].init.body), { name: "Prod East" });
  } finally {
    stub.restore();
  }
});

test("rotating posts to the credential route with no body", async () => {
  const stub = stubFetch(() => jsonResponse(200, {
    ...CREDENTIAL_ANSWER,
    helm_install: "helm upgrade yscale-agent ...",
  }));
  try {
    const issued = await rotateClusterCredential({ token: "bearer-value", tenantId: "cust 42", clusterId: "prod east" });
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust%2042/clusters/prod%20east/credential");
    assert.equal(init.method, "POST");
    assert.equal(init.body, undefined);
    assert.equal(init.headers["Content-Type"], undefined);
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.match(issued.helmCommand, /^helm upgrade/);
  } finally {
    stub.restore();
  }
});

// A credential answer missing its token or command is refused whole: revealing
// half a one-time handoff would strand the reader with no way back to the rest.
test("a credential answer without a usable token or command is refused", async () => {
  for (const body of [
    { ...CREDENTIAL_ANSWER, connector_token: "" },
    { ...CREDENTIAL_ANSWER, connector_token: undefined },
    { ...CREDENTIAL_ANSWER, helm_install: "  " },
    { ...CREDENTIAL_ANSWER, cluster: { cluster_id: "bad id" } },
    {},
  ]) {
    const stub = stubFetch(() => jsonResponse(201, body));
    try {
      await assert.rejects(
        registerCluster({ token: "t", tenantId: "cust_42", name: "Prod" }),
        (error) => error instanceof ApiError && error.status === 502 && /connector credential/.test(error.message),
      );
    } finally {
      stub.restore();
    }
  }
});

test("deleting a cluster is a DELETE that must be confirmed by the answer", async () => {
  const stub = stubFetch(() => jsonResponse(200, { deleted: true, tenant_id: "cust_42", cluster_id: "prod-east" }));
  try {
    const result = await deleteCluster({ token: "bearer-value", tenantId: "cust_42", clusterId: "prod-east" });
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/clusters/prod-east");
    assert.equal(init.method, "DELETE");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.deepEqual(result, { deleted: true, clusterId: "prod-east" });
  } finally {
    stub.restore();
  }

  const unconfirmed = stubFetch(() => jsonResponse(200, { ok: true }));
  try {
    await assert.rejects(
      deleteCluster({ token: "t", tenantId: "cust_42", clusterId: "prod-east" }),
      (error) => error instanceof ApiError && error.status === 502 && /confirming the delete/.test(error.message),
    );
  } finally {
    unconfirmed.restore();
  }
});

// Central's own sentence wins on a refusal — a policy conflict has to reach the
// reader as what to do, not as a generic failure — and the stable code rides
// along for the page to branch on.
test("a write refusal keeps central's message, code, and status", async () => {
  const stub = stubFetch(() => jsonResponse(409, {
    error: "policy_conflict",
    message: "The placement policy names this cluster. Remove it from the policy first.",
  }));
  try {
    await assert.rejects(
      deleteCluster({ token: "t", tenantId: "cust_42", clusterId: "prod-east" }),
      (error) => error instanceof ApiError
        && error.status === 409
        && error.code === "policy_conflict"
        && /Remove it from the policy first/.test(error.message),
    );
  } finally {
    stub.restore();
  }

  // Current central writes its actionable sentence directly in `error`.
  // Do not replace a policy conflict with the generic 409 fallback.
  const currentCentral = stubFetch(() => jsonResponse(409, {
    error: "cluster is still named by the tenant cluster policy; remove it from allow/deny first",
  }));
  try {
    await assert.rejects(
      deleteCluster({ token: "t", tenantId: "cust_42", clusterId: "prod-east" }),
      (error) => error instanceof ApiError
        && error.status === 409
        && /remove it from allow\/deny first/.test(error.message),
    );
  } finally {
    currentCentral.restore();
  }

  const bare = stubFetch(() => jsonResponse(403, {}));
  try {
    await assert.rejects(
      registerCluster({ token: "t", tenantId: "cust_42", name: "Prod" }),
      (error) => error.status === 403 && /cannot do that to the cluster registry/.test(error.message),
    );
  } finally {
    bare.restore();
  }
});

test("write aborts and offline failures keep their own shape", async () => {
  const aborted = stubFetch(() => {
    const error = new Error("aborted");
    error.name = "AbortError";
    throw error;
  });
  try {
    await assert.rejects(
      rotateClusterCredential({ token: "t", tenantId: "cust_42", clusterId: "c1" }),
      (error) => error.name === "AbortError",
    );
  } finally {
    aborted.restore();
  }

  const offline = stubFetch(() => { throw new TypeError("Failed to fetch"); });
  try {
    await assert.rejects(
      registerCluster({ token: "t", tenantId: "cust_42", name: "Prod" }),
      (error) => error instanceof ApiError && error.status === 0 && error.isOffline,
    );
  } finally {
    offline.restore();
  }
});
