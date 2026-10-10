import test from "node:test";
import assert from "node:assert/strict";
import { ApiError } from "./apiError.js";
import {
  CATALOG_PUBLISHER_COMMAND,
  createCatalogPublisher,
  deleteCatalogPublisher,
  fetchCatalogPublishers,
  normalizeCatalogPublisherCredential,
  normalizeCatalogPublishers,
  rotateCatalogPublisherCredential,
} from "./catalogPublisherApi.js";

const PUBLISHER = { id: "ci-main", name: "Main CI", created_at: "2026-08-16T10:00:00Z", updated_at: "2026-08-16T10:00:00Z" };

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

function stubFetch(handler) {
  const prior = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, init }); return handler(url, init); };
  return { calls, restore: () => { globalThis.fetch = prior; } };
}

test("normalizers accept only the safe publisher and one-time credential shapes", () => {
  assert.deepEqual(normalizeCatalogPublishers({ publishers: [PUBLISHER] }), [{
    id: "ci-main", name: "Main CI", createdAt: "2026-08-16T10:00:00Z", updatedAt: "2026-08-16T10:00:00Z",
  }]);
  const issued = normalizeCatalogPublisherCredential({ publisher: PUBLISHER, token: "yscp_once_123", command: CATALOG_PUBLISHER_COMMAND });
  assert.equal(issued.token, "yscp_once_123");
  assert.equal(issued.command, CATALOG_PUBLISHER_COMMAND);
  assert.equal(normalizeCatalogPublisherCredential({ publisher: PUBLISHER, token: "yscp_once_123" }).command, "");
});

test("normalizers reject added secrets, bad records, duplicates, and arbitrary commands", () => {
  const bad = [
    { publishers: [{ ...PUBLISHER, token: "leak" }] },
    { publishers: [PUBLISHER, PUBLISHER] },
    { publishers: [{ ...PUBLISHER, id: "bad/id" }] },
    { publishers: [{ ...PUBLISHER, updated_at: "yesterday" }] },
    { publishers: [], token: "leak" },
  ];
  for (const payload of bad) assert.throws(() => normalizeCatalogPublishers(payload), ApiError);
  assert.throws(() => normalizeCatalogPublisherCredential({ publisher: PUBLISHER, token: "ok", command: "curl bad.example" }), /unrecognized command/);
  assert.throws(() => normalizeCatalogPublisherCredential({ publisher: PUBLISHER, token: "bad\ntoken" }), /one-time token/);
  assert.throws(() => normalizeCatalogPublisherCredential({ publisher: PUBLISHER, token: "ok", secret: "leak" }), /unsafe credential/);
});

test("list and lifecycle calls use the exact same-origin paths and transport", async () => {
  const answers = [
    jsonResponse(200, { publishers: [PUBLISHER] }),
    jsonResponse(201, { publisher: PUBLISHER, token: "once" }),
    jsonResponse(200, { publisher: PUBLISHER, token: "rotated" }),
    jsonResponse(204, null),
  ];
  const stub = stubFetch(() => answers.shift());
  try {
    await fetchCatalogPublishers({ token: "bearer", tenantId: "cust 42" });
    await createCatalogPublisher({ token: "bearer", tenantId: "cust 42", name: " Main CI " });
    await rotateCatalogPublisherCredential({ token: "bearer", tenantId: "cust 42", publisherId: "ci main" });
    await deleteCatalogPublisher({ token: "bearer", tenantId: "cust 42", publisherId: "ci main" });
    assert.deepEqual(stub.calls.map((call) => [call.url, call.init.method]), [
      ["/api/tenants/cust%2042/catalog-publishers", "GET"],
      ["/api/tenants/cust%2042/catalog-publishers", "POST"],
      ["/api/tenants/cust%2042/catalog-publishers/ci%20main/credential", "POST"],
      ["/api/tenants/cust%2042/catalog-publishers/ci%20main", "DELETE"],
    ]);
    assert.deepEqual(JSON.parse(stub.calls[1].init.body), { name: "Main CI" });
    for (const call of stub.calls) {
      assert.equal(call.init.credentials, "same-origin");
      assert.equal(call.init.headers.Authorization, "Bearer bearer");
    }
    assert.equal(stub.calls[2].init.body, undefined);
    assert.equal(stub.calls[3].init.body, undefined);
  } finally { stub.restore(); }
});

test("errors and invalid local names fail closed", async () => {
  const stub = stubFetch(() => jsonResponse(403, { error: "read_only", message: "Owners and admins manage publishers." }));
  try {
    await assert.rejects(fetchCatalogPublishers({ token: "t", tenantId: "cust" }), (error) => error.status === 403 && error.code === "read_only" && /Owners/.test(error.message));
    const before = stub.calls.length;
    await assert.rejects(createCatalogPublisher({ token: "t", tenantId: "cust", name: " \n " }), (error) => error.status === 400);
    await assert.rejects(createCatalogPublisher({ token: "t", tenantId: "cust", name: "é".repeat(51) }), (error) => error.status === 400);
    assert.equal(stub.calls.length, before);
  } finally { stub.restore(); }
});
