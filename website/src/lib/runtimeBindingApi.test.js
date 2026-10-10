import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ApiError } from "./apiError.js";
import { deleteRuntimeBinding, fetchRuntimeBindings, normalizeDeletedRuntimeBinding, normalizeRuntimeBinding, normalizeRuntimeBindings, putRuntimeBinding } from "./runtimeBindingApi.js";
import { resetDevPreview } from "./devPreview.js";

const ROW = { id: "rb_1", key: "DATABASE_URL", name: "Primary database", revision: "3", sync_state: "synced", created_at: "2026-08-16T09:00:00Z", updated_at: "2026-08-16T10:00:00Z" };
const LIST = { tenant_id: "cust_42", role: "owner", bindings: [ROW] };

function response(status, body) { return { ok: status >= 200 && status < 300, status, json: async () => body }; }
function stubFetch(handler) {
  const previous = globalThis.fetch; const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, init }); return handler(url, init); };
  return { calls, restore: () => { globalThis.fetch = previous; } };
}

beforeEach(resetDevPreview);

test("strict normalizers accept only safe summary contracts", () => {
  assert.equal(normalizeRuntimeBindings(LIST, "cust_42").bindings[0].syncState, "synced");
  assert.equal(normalizeRuntimeBinding({ tenant_id: "cust_42", role: "admin", binding: ROW }).binding.key, "DATABASE_URL");
  assert.deepEqual(normalizeDeletedRuntimeBinding({ key: "DATABASE_URL", deleted: true }, "DATABASE_URL"), { key: "DATABASE_URL", deleted: true });
  for (const payload of [
    { ...LIST, value: "leak" },
    { ...LIST, bindings: [{ ...ROW, ciphertext: "sealed" }] },
    { ...LIST, bindings: [{ ...ROW, nonce: "n" }] },
    { ...LIST, bindings: [{ ...ROW, hash: "h" }] },
    { ...LIST, bindings: [{ ...ROW, secret_material: "x" }] },
    { ...LIST, tenant_id: "other" },
    { ...LIST, bindings: [{ ...ROW, key: "lowercase" }] },
    { ...LIST, bindings: [{ ...ROW, revision: "01" }] },
    { ...LIST, bindings: [{ ...ROW, revision: "9007199254740992.0" }] },
  ]) assert.throws(() => normalizeRuntimeBindings(payload, "cust_42"), ApiError);
});

test("GET PUT DELETE use exact same-origin routes and exact write body", async () => {
  const stub = stubFetch((url, init) => init.method === "DELETE" ? response(200, { key: "DATABASE_URL", deleted: true }) : init.method === "PUT" ? response(200, { tenant_id: "cust 42", role: "owner", binding: ROW }) : response(200, { ...LIST, tenant_id: "cust 42" }));
  try {
    await fetchRuntimeBindings({ token: "bearer", tenantId: "cust 42" });
    await putRuntimeBinding({ token: "bearer", tenantId: "cust 42", key: "DATABASE_URL", name: "Primary database", value: "write-only" });
    await deleteRuntimeBinding({ token: "bearer", tenantId: "cust 42", key: "DATABASE_URL" });
    assert.deepEqual(stub.calls.map(({ url, init }) => [url, init.method]), [["/api/tenants/cust%2042/runtime-bindings", "GET"], ["/api/tenants/cust%2042/runtime-bindings/DATABASE_URL", "PUT"], ["/api/tenants/cust%2042/runtime-bindings/DATABASE_URL", "DELETE"]]);
    assert.deepEqual(JSON.parse(stub.calls[1].init.body), { name: "Primary database", value: "write-only" });
    for (const { init } of stub.calls) assert.equal(init.credentials, "same-origin");
  } finally { stub.restore(); }
});

test("invalid writes fail locally without sending plaintext", async () => {
  const stub = stubFetch(() => response(500, {}));
  try {
    for (const input of [
      { key: "lowercase", name: "Name", value: "x" },
      { key: "KEY", name: " Name ", value: "x" },
      { key: "KEY", name: "Name", value: "" },
      { key: "KEY", name: "Name", value: "a\0b" },
      { key: "KEY", name: "é".repeat(51), value: "x" },
      { key: "KEY", name: "Name", value: "x".repeat(8193) },
    ]) await assert.rejects(putRuntimeBinding({ token: "bearer", tenantId: "cust", ...input }), (error) => error.status === 400);
    await assert.rejects(deleteRuntimeBinding({ token: "bearer", tenantId: "cust", key: "bad-key" }), (error) => error.status === 400);
    assert.equal(stub.calls.length, 0);
  } finally { stub.restore(); }
});

test("preview is summary-only and never retains submitted plaintext", async () => {
  const previousWindow = globalThis.window;
  globalThis.window = { location: { hostname: "localhost" } };
  try {
    const created = await putRuntimeBinding({ token: "yscale-local-preview", tenantId: "tenant-acme-research", key: "NEW_KEY", name: "New key", value: "plaintext-sentinel" });
    assert.equal(JSON.stringify(created).includes("plaintext-sentinel"), false);
    const listed = await fetchRuntimeBindings({ token: "yscale-local-preview", tenantId: "tenant-acme-research" });
    assert.equal(JSON.stringify(listed).includes("plaintext-sentinel"), false);
    assert.ok(listed.bindings.some((binding) => binding.key === "NEW_KEY"));
    await deleteRuntimeBinding({ token: "yscale-local-preview", tenantId: "tenant-acme-research", key: "NEW_KEY" });
    await assert.rejects(putRuntimeBinding({ token: "yscale-local-preview", tenantId: "tenant-platform-lab", key: "NOPE", name: "Nope", value: "x" }), (error) => error.status === 403);
  } finally { globalThis.window = previousWindow; }
});
