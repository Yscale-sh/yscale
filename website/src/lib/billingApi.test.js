import assert from "node:assert/strict";
import test from "node:test";
import { ApiError } from "./apiError.js";
import { fetchBilling, formatMicroUSD, normalizeBilling } from "./billingApi.js";

const HOLD = { id: 41, workload_id: "wl_1", amount_micro_usd: 2500000, expires_at: "2026-08-16T13:00:00Z", created_at: "2026-08-16T12:00:00Z" };
const SAFE = { tenant_id: "cust_42", currency: "USD", balance_micro_usd: 10000000, held_micro_usd: 2500000, spendable_micro_usd: 7500000, debt_micro_usd: 0, frozen: false, updated_at: "2026-08-16T12:01:00Z", open_holds: [HOLD] };
function response(status, body) { return { ok: status >= 200 && status < 300, status, json: async () => body }; }

test("billing normalization accepts only exact USD micro-unit summaries", () => {
  const billing = normalizeBilling(SAFE, "cust_42");
  assert.equal(billing.openHolds[0].id, 41);
  assert.equal(billing.openHolds[0].workloadId, "wl_1");
  assert.equal(formatMicroUSD(1234567), "$1.234567");
  for (const payload of [{ ...SAFE, provider: "stripe" }, { ...SAFE, currency: "EUR" }, { ...SAFE, balance_micro_usd: 1.5 }, { ...SAFE, tenant_id: "other" }, { ...SAFE, open_holds: [{ ...HOLD, id: "hold_41" }] }, { ...SAFE, open_holds: [{ ...HOLD, id: 0 }] }, { ...SAFE, open_holds: [{ ...HOLD, payment_id: "x" }] }]) {
    assert.throws(() => normalizeBilling(payload, "cust_42"), ApiError);
  }
});

test("billing GET uses the exact same-origin bearer route", async () => {
  const old = globalThis.fetch; const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, init }); return response(200, { ...SAFE, tenant_id: "cust 42" }); };
  try {
    await fetchBilling({ token: "bearer", tenantId: "cust 42" });
    assert.equal(calls[0].url, "/api/tenants/cust%2042/billing");
    assert.deepEqual(calls[0].init.headers, { Authorization: "Bearer bearer", Accept: "application/json" });
    assert.equal(calls[0].init.credentials, "same-origin");
  } finally { globalThis.fetch = old; }
});
