import assert from "node:assert/strict";
import test from "node:test";

import { ApiError } from "./apiError.js";
import {
  fetchHostedCapacity,
  hostedCapacityCardState,
  normalizeHostedCapacity,
  requestHostedCapacity,
} from "./hostedCapacity.js";

function stubFetch(handler) {
  const calls = [];
  const previous = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({ url, init });
    return handler(url, init);
  };
  return { calls, restore: () => { globalThis.fetch = previous; } };
}

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

const REQUESTED = {
  tenant_id: "tenant 42",
  role: "owner",
  status: "requested",
  requested_at: "2026-08-15T10:20:30Z",
};

test("hosted capacity GET is same-origin, bearer-authenticated, and abortable", async () => {
  const controller = new AbortController();
  const stub = stubFetch(() => jsonResponse(200, { tenant_id: "tenant 42", role: "owner", status: "not_requested" }));
  try {
    const status = await fetchHostedCapacity({ token: "token-value", tenantId: "tenant 42", signal: controller.signal });
    assert.deepEqual(status, { tenantId: "tenant 42", role: "owner", status: "not_requested", requestedAt: "" });
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/tenant%2042/hosted-capacity");
    assert.equal(init.method, "GET");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer token-value");
    assert.equal(init.signal, controller.signal);
    assert.equal(init.body, undefined);
  } finally {
    stub.restore();
  }
});

test("hosted capacity POST sends no body and accepts first and duplicate response statuses", async () => {
  for (const httpStatus of [202, 200]) {
    const stub = stubFetch(() => jsonResponse(httpStatus, REQUESTED));
    try {
      const status = await requestHostedCapacity({ token: "token-value", tenantId: "tenant 42" });
      assert.equal(status.status, "requested");
      assert.equal(status.requestedAt, REQUESTED.requested_at);
      assert.equal(stub.calls[0].init.method, "POST");
      assert.equal(stub.calls[0].init.body, undefined);
      assert.equal(stub.calls[0].init.headers["Content-Type"], undefined);
    } finally {
      stub.restore();
    }
  }
});

test("malformed hosted answers are refused rather than becoming not requested", () => {
  const malformed = [
    null,
    {},
    { tenant_id: "tenant 42", role: "owner", status: "unknown" },
    { tenant_id: "other", role: "owner", status: "not_requested" },
    { tenant_id: "tenant 42", role: "operator", status: "not_requested" },
    { tenant_id: "tenant 42", role: "owner", status: "requested" },
    { tenant_id: "tenant 42", role: "owner", status: "assigned", requested_at: REQUESTED.requested_at },
    { tenant_id: "tenant 42", role: "owner", status: "not_requested", requested_at: REQUESTED.requested_at },
    { ...REQUESTED, requested_at: "not-a-time" },
    { ...REQUESTED, requested_at: null },
    { ...REQUESTED, requested_at: 1234 },
  ];
  for (const payload of malformed) {
    assert.throws(
      () => normalizeHostedCapacity(payload, "tenant 42"),
      (error) => error instanceof ApiError && error.status === 502,
    );
  }
});

test("hosted capacity errors keep HTTP status and aborts stay aborts", async () => {
  const forbidden = stubFetch(() => jsonResponse(403, { error: "role_forbidden" }));
  try {
    await assert.rejects(
      requestHostedCapacity({ token: "t", tenantId: "tenant-42" }),
      (error) => error instanceof ApiError && error.status === 403 && error.code === "role_forbidden",
    );
  } finally {
    forbidden.restore();
  }

  const aborted = stubFetch(() => {
    const error = new Error("aborted");
    error.name = "AbortError";
    throw error;
  });
  try {
    await assert.rejects(fetchHostedCapacity({ token: "t", tenantId: "tenant-42" }), (error) => error.name === "AbortError");
  } finally {
    aborted.restore();
  }
});

test("hosted capacity UI state exposes only actions supported by each state", () => {
  const base = { loadState: "ready", canManage: true, posting: false, fleetObserved: true, hosted: null, hostedReason: "" };
  assert.deepEqual(hostedCapacityCardState({ ...base, loadState: "error" }), { kind: "error", label: "status unavailable", canRetry: true });
  assert.deepEqual(hostedCapacityCardState(base), { kind: "error", label: "status unavailable", canRetry: true });
  assert.equal(hostedCapacityCardState({ ...base, capacity: { status: "not_requested" } }).canRequest, true);
  assert.equal(hostedCapacityCardState({ ...base, canManage: false, capacity: { status: "not_requested" } }).kind, "not_requested_restricted");
  assert.deepEqual(
    hostedCapacityCardState({ ...base, capacity: { status: "requested", requestedAt: REQUESTED.requested_at } }),
    { kind: "requested", label: "request pending", requestedAt: REQUESTED.requested_at },
  );
  assert.equal(hostedCapacityCardState({ ...base, capacity: { status: "assigned" }, hosted: { id: "hosted-1" } }).kind, "assigned_launchable");
  assert.deepEqual(
    hostedCapacityCardState({ ...base, capacity: { status: "assigned" }, hosted: { id: "hosted-1" }, hostedReason: "Disconnected." }),
    { kind: "assigned_unavailable", label: "assigned", reason: "Disconnected." },
  );
});
