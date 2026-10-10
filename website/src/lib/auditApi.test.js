import test from "node:test";
import assert from "node:assert/strict";
import { ApiError } from "./apiError.js";
import { MAX_AUDIT_LIMIT, fetchAudit } from "./auditApi.js";

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

test("the journal read is same-origin, bearer-authenticated, and page-bounded", async () => {
  const stub = stubFetch(() => jsonResponse(200, { events: [{ id: "aud_1" }], next_after: "aud_1" }));
  try {
    const page = await fetchAudit({ token: "bearer-value", tenantId: "cust 42", after: "aud_0", limit: MAX_AUDIT_LIMIT });
    assert.deepEqual(page, { events: [{ id: "aud_1" }], next_after: "aud_1" });
    assert.equal(stub.calls.length, 1);
    const { url, init } = stub.calls[0];
    assert.equal(url, "/api/tenants/cust%2042/audit?limit=200&after=aud_0");
    assert.equal(init.credentials, "same-origin");
    assert.equal(init.headers.Authorization, "Bearer bearer-value");
    assert.equal(init.method, undefined, "the journal read must be a GET");
  } finally {
    stub.restore();
  }
});

test("an absent cursor ends the journal rather than restarting it", async () => {
  const stub = stubFetch(() => jsonResponse(200, { events: [] }));
  try {
    const page = await fetchAudit({ token: "t", tenantId: "cust_42" });
    assert.deepEqual(page, { events: [], next_after: "" });
    assert.equal(stub.calls[0].url, "/api/tenants/cust_42/audit?limit=50");
  } finally {
    stub.restore();
  }
});

// Every refusal the journal can answer arrives as an ApiError carrying the
// status, so the page can tell "sign in again" from "your role may not" from
// "the service is down" instead of rendering one empty journal for all three.
test("a refusal keeps its status so the console can tell role from outage", async (t) => {
  const cases = [
    { status: 401, flag: "isExpired" },
    { status: 403, flag: "isForbidden" },
    { status: 404, flag: null },
    { status: 429, flag: null },
    { status: 502, flag: "isUnavailable" },
    { status: 503, flag: "isUnavailable" },
  ];
  for (const { status, flag } of cases) {
    await t.test(`HTTP ${status}`, async () => {
      const stub = stubFetch(() => jsonResponse(status, { error: `upstream said ${status}` }));
      try {
        await assert.rejects(fetchAudit({ token: "t", tenantId: "cust_42" }), (error) => {
          assert.ok(error instanceof ApiError);
          assert.equal(error.status, status);
          // The console's own wording is shown; the upstream detail is kept
          // beside it rather than in place of it.
          assert.ok(error.message);
          assert.equal(error.code, `upstream said ${status}`);
          for (const name of ["isOffline", "isExpired", "isForbidden", "isUnavailable"]) {
            assert.equal(error[name], name === flag, `${name} is wrong for ${status}`);
          }
          return true;
        });
      } finally {
        stub.restore();
      }
    });
  }
});

test("an unreachable API is a 0, not a silent empty journal", async () => {
  const stub = stubFetch(() => { throw new TypeError("Failed to fetch"); });
  try {
    await assert.rejects(
      fetchAudit({ token: "t", tenantId: "cust_42" }),
      (error) => error instanceof ApiError && error.status === 0 && error.isOffline,
    );
  } finally {
    stub.restore();
  }
});

test("an abort propagates instead of becoming an offline error", async () => {
  const stub = stubFetch(() => {
    const error = new Error("aborted");
    error.name = "AbortError";
    throw error;
  });
  try {
    await assert.rejects(fetchAudit({ token: "t", tenantId: "cust_42" }), (error) => error.name === "AbortError");
  } finally {
    stub.restore();
  }
});
