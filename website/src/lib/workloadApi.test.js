import test from "node:test";
import assert from "node:assert/strict";
import { ApiError } from "./apiError.js";
import {
  CLUSTER_HEADER,
  PLACEMENT_TOKEN_HEADER,
  TEMPLATE_ID_HEADER,
  TEMPLATE_VERSION_HEADER,
  createWorkload,
  fetchWorkloadLogs,
  fetchWorkloads,
  previewPlacement,
  normalizeWorkloadBilling,
  retryWorkload,
} from "./workloadApi.js";

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

const YAML = "kind: Workload\nmetadata:\n  name: \"trainer\"\n";
const KEY = "wl_9f2c1a7b4e6d80315c2a9b7e4f1d6038"; // gitleaks:allow -- deterministic test fixture

test("workload billing accepts the backend-real numeric receipt contract", () => {
  const safe = { hold_id: 41, state: "pending", reserved_micro_usd: 2500000, captured_micro_usd: 0, currency: "USD", quote_id: "quote_1", pricing_version: 3 };
  assert.deepEqual(normalizeWorkloadBilling(safe), { holdId: 41, state: "pending", reservedMicroUsd: 2500000, capturedMicroUsd: 0, currency: "USD", quoteId: "quote_1", pricingVersion: 3 });
  assert.throws(() => normalizeWorkloadBilling({ ...safe, provider: "leak" }), ApiError);
  assert.throws(() => normalizeWorkloadBilling({ ...safe, reserved_micro_usd: 1.5 }), ApiError);
  assert.throws(() => normalizeWorkloadBilling({ ...safe, hold_id: "41" }), ApiError);
  assert.throws(() => normalizeWorkloadBilling({ ...safe, state: "held" }), ApiError);
  assert.throws(() => normalizeWorkloadBilling({ ...safe, pricing_version: "3" }), ApiError);
});

test("a create carries the caller's key verbatim with the YAML", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    const accepted = await createWorkload({ token: "bearer-value", tenantId: "cust 42", yaml: YAML, idempotencyKey: KEY });
    assert.deepEqual(accepted, { id: "wl_1" });
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust%2042/workloads");
    assert.equal(init.method, "POST");
    assert.equal(init.body, YAML);
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers["Content-Type"], "text/yaml");
    assert.equal(init.headers["Idempotency-Key"], KEY);
  } finally {
    stub.restore();
  }
});

// The target cluster is routing, not spec: it rides as a header the seam
// validates, and the document the reader approved stays exactly as reviewed.
test("a create sends the chosen cluster as X-Cluster-ID and not in the YAML", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, clusterId: "edge-fra-1", idempotencyKey: KEY });
    const { init } = stub.calls[0];
    assert.equal(init.headers[CLUSTER_HEADER], "edge-fra-1");
    assert.equal(init.body, YAML);
    assert.equal(init.body.includes("edge-fra-1"), false);
  } finally {
    stub.restore();
  }
});

test("automatic placement omits X-Cluster-ID", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, clusterId: "", idempotencyKey: KEY });
    assert.equal(stub.calls[0].init.headers[CLUSTER_HEADER], undefined);
  } finally {
    stub.restore();
  }
});

// The catalog entry a submission was composed from is provenance, not spec: it
// rides as a header pair and the reviewed document is unchanged by it.
test("a create sends the template selection as a header pair", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    await createWorkload({
      token: "t",
      tenantId: "cust_42",
      yaml: YAML,
      template: { id: "pytorch-training", version: 4 },
      idempotencyKey: KEY,
    });
    const { init } = stub.calls[0];
    assert.equal(init.headers[TEMPLATE_ID_HEADER], "pytorch-training");
    assert.equal(init.headers[TEMPLATE_VERSION_HEADER], "4");
    assert.equal(init.body, YAML);
    assert.equal(init.body.includes("pytorch-training"), false);
  } finally {
    stub.restore();
  }
});

// The seam refuses half a receipt, so half a selection never leaves the browser.
test("a create omits the pair unless both halves are present", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    for (const template of [undefined, null, {}, { id: "container-job" }, { version: 2 }]) {
      await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, template, idempotencyKey: KEY });
    }
    for (const { init } of stub.calls) {
      assert.equal(init.headers[TEMPLATE_ID_HEADER], undefined);
      assert.equal(init.headers[TEMPLATE_VERSION_HEADER], undefined);
    }
  } finally {
    stub.restore();
  }
});

test("a read never carries a target cluster", async () => {
  const stub = stubFetch(() => jsonResponse(200, { workloads: [] }));
  try {
    await fetchWorkloads({ token: "t", tenantId: "cust_42" });
    assert.equal(stub.calls[0].init.headers[CLUSTER_HEADER], undefined);
  } finally {
    stub.restore();
  }
});

test("a log snapshot carries only a bounded tail in the URL", async () => {
  const payload = { observed_at: "2026-08-14T00:00:00Z", streams: [], truncated: false };
  const stub = stubFetch(() => jsonResponse(200, payload));
  try {
    assert.deepEqual(await fetchWorkloadLogs({ token: "t", tenantId: "cust 42", workloadId: "wl/9", tail: 40 }), payload);
    assert.equal(stub.calls[0].url, "/api/tenants/cust%2042/workloads/wl%2F9/logs?tail=40");
    assert.equal(stub.calls[0].init.method, "GET");
    assert.equal(stub.calls[0].init.body, undefined);
    assert.equal(stub.calls[0].init.headers[CLUSTER_HEADER], undefined);
    assert.equal(stub.calls[0].init.headers["Content-Type"], undefined);
    await fetchWorkloadLogs({ token: "t", tenantId: "cust", workloadId: "wl", tail: 5000 });
    assert.equal(stub.calls[1].url, "/api/tenants/cust/workloads/wl/logs?tail=200");
  } finally {
    stub.restore();
  }
});

// The key belongs to the submission, so retrying the same call must not mint a
// second one behind the caller's back.
test("retrying the same submission sends the same key", async () => {
  const stub = stubFetch(() => jsonResponse(503, { error: "unavailable" }));
  try {
    for (let i = 0; i < 3; i += 1) {
      await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }).catch(() => {});
    }
    const sent = stub.calls.map(({ init }) => init.headers["Idempotency-Key"]);
    assert.deepEqual(sent, [KEY, KEY, KEY]);
  } finally {
    stub.restore();
  }
});

test("reads and cancels do not carry an idempotency key", async () => {
  const stub = stubFetch(() => jsonResponse(200, { workloads: [] }));
  try {
    await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY });
    const { fetchWorkloads } = await import("./workloadApi.js");
    await fetchWorkloads({ token: "t", tenantId: "cust_42" });
    assert.equal(stub.calls[1].init.headers["Idempotency-Key"], undefined);
  } finally {
    stub.restore();
  }
});

test("retry starts a new run with an empty body and no cluster header", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_retry" }));
  try {
    const accepted = await retryWorkload({ token: "t", tenantId: "cust_42", workloadId: "wl_source", idempotencyKey: KEY });
    assert.deepEqual(accepted, { id: "wl_retry" });
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/workloads/wl_source/retry");
    assert.equal(init.method, "POST");
    assert.equal(init.body, undefined);
    assert.equal(init.headers["Idempotency-Key"], KEY);
    assert.equal(init.headers[CLUSTER_HEADER], undefined);
    assert.equal(init.headers[TEMPLATE_ID_HEADER], undefined);
    assert.equal(init.headers["Content-Type"], undefined);
  } finally {
    stub.restore();
  }
});

// central says why in `message` — the in_progress retry and the idempotency
// conflicts put nothing useful in `error` alone.
test("an error prefers central's message over its code", async () => {
  const stub = stubFetch(() => jsonResponse(409, {
    error: "in_progress",
    message: "That submission is already being processed. Retry the same request in a few seconds.",
  }));
  try {
    await assert.rejects(
      createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }),
      (error) => {
        assert.ok(error instanceof ApiError);
        assert.equal(error.status, 409);
        assert.equal(error.code, "in_progress");
        assert.match(error.message, /Retry the same request/);
        return true;
      },
    );
  } finally {
    stub.restore();
  }
});

test("a conflict with nothing to say still tells the human what to do", async () => {
  const stub = stubFetch(() => jsonResponse(409, {}));
  try {
    await assert.rejects(
      createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }),
      (error) => error.status === 409 && /retry the same request/i.test(error.message),
    );
  } finally {
    stub.restore();
  }
});

test("an upstream essay is bounded before it reaches the form", async () => {
  const stub = stubFetch(() => jsonResponse(400, { message: `${"x".repeat(900)}\n\nand more` }));
  try {
    await assert.rejects(
      createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }),
      (error) => error.message.length <= 240 && !error.message.includes("\n"),
    );
  } finally {
    stub.restore();
  }
});

test("an upstream error fallback is bounded before it reaches the form", async () => {
  const stub = stubFetch(() => jsonResponse(400, { error: `invalid: ${"detail ".repeat(100)}` }));
  try {
    await assert.rejects(
      createWorkload({ token: "bearer-value", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }),
      (error) => error.status === 400 && error.message.length <= 240 && error.code.length <= 240,
    );
  } finally {
    stub.restore();
  }
});

test("a create carries the placement token as X-Yscale-Placement-Token", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    await createWorkload({
      token: "t",
      tenantId: "cust_42",
      yaml: YAML,
      placementToken: "tok_signed_abc",
      idempotencyKey: KEY,
    });
    assert.equal(stub.calls[0].init.headers[PLACEMENT_TOKEN_HEADER], "tok_signed_abc");
  } finally {
    stub.restore();
  }
});

test("a create without a placement token omits the header", async () => {
  const stub = stubFetch(() => jsonResponse(202, { id: "wl_1" }));
  try {
    await createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY });
    assert.equal(stub.calls[0].init.headers[PLACEMENT_TOKEN_HEADER], undefined);
  } finally {
    stub.restore();
  }
});

test("previewPlacement sends YAML and optional cluster to the preview endpoint", async () => {
  const stub = stubFetch(() => jsonResponse(200, { status: "ok", placement: { granted_cluster_id: "lab" }, launch_token: "tok" }));
  try {
    const result = await previewPlacement({ token: "t", tenantId: "cust_42", yaml: YAML, clusterId: "lab-1" });
    assert.equal(result.status, "ok");
    assert.equal(result.launch_token, "tok");
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust_42/placement-preview");
    assert.equal(init.method, "POST");
    assert.equal(init.body, YAML);
    assert.equal(init.headers["Content-Type"], "text/yaml");
    assert.equal(init.headers[CLUSTER_HEADER], "lab-1");
  } finally {
    stub.restore();
  }
});

test("previewPlacement without cluster omits X-Cluster-ID", async () => {
  const stub = stubFetch(() => jsonResponse(200, { status: "ok", placement: {}, launch_token: "tok" }));
  try {
    await previewPlacement({ token: "t", tenantId: "cust_42", yaml: YAML, clusterId: "" });
    assert.equal(stub.calls[0].init.headers[CLUSTER_HEADER], undefined);
  } finally {
    stub.restore();
  }
});

test("previewPlacement preserves Central's 409 refusal receipt", async () => {
  const refusal = {
    status: "rejected",
    code: "placement_unavailable",
    message: "No candidate satisfies the current constraints.",
    placement: {
      version: 1,
      candidates: [{ provider: "linode", reason: "price_cap_exceeded" }],
      selected: {},
    },
  };
  const stub = stubFetch(() => jsonResponse(409, refusal));
  try {
    const result = await previewPlacement({ token: "t", tenantId: "cust_42", yaml: YAML });
    assert.equal(result.status, "rejected");
    assert.equal(result.code, "placement_unavailable");
    assert.equal(result.placement.candidates[0].reason, "price_cap_exceeded");
  } finally {
    stub.restore();
  }
});

test("a create refusal preserves payload.code for re-review detection", async () => {
  const stub = stubFetch(() => jsonResponse(409, {
    error: "conflict",
    code: "placement_changed",
    message: "The placement changed since preview.",
  }));
  try {
    await assert.rejects(
      createWorkload({ token: "t", tenantId: "cust_42", yaml: YAML, idempotencyKey: KEY }),
      (error) => {
        assert.equal(error.code, "placement_changed");
        assert.match(error.message, /placement changed/);
        return true;
      },
    );
  } finally {
    stub.restore();
  }
});
