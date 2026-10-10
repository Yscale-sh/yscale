import test from "node:test";
import assert from "node:assert/strict";
import {
  validTimestamp,
  deriveTimelineSteps,
  deriveCleanupPresentation,
  cleanupDetailRows,
} from "./workloadLifecycle.js";

const detailValues = (workload) => Object.fromEntries(cleanupDetailRows(workload).map(({ label, value }) => [label, value]));

test("cleanup detail uses the same canonical absence proof as the timeline", () => {
  const workload = { status: "succeeded", cleanup: { state: "terminated", deleted_at: "2026-08-10T14:46:12Z", durable_reap_receipt: true, reason: "workload completed" } };
  const values = detailValues(workload);
  assert.match(values["Provider resource"], /^confirmed absent /);
  assert.equal(values["Cleanup state"], "terminated");
  assert.equal(values["Teardown reason"], "workload completed");
  assert.equal(values["Durable cleanup receipt"], "recorded");
  assert.equal(values["Needs a human"], "no");
  assert.equal(deriveTimelineSteps(workload).at(-1).status, "done");
});

test("cleanup detail cannot infer provider absence from phase, aliases, or a durable receipt", () => {
  const time = "2026-08-10T14:46:12Z";
  for (const cleanup of [undefined, null, [], "terminated", { state: "terminated" }, { state: "terminated", deleted_at: "invalid" }, { state: "terminated", confirmed_at: time }, { state: "terminated", durable_reap_receipt: true }, ...["queued", "deleting", "retrying", "manual_attention", "unknown"].map((state) => ({ state, deleted_at: time, confirmed_at: time }))]) {
    const workload = { status: "succeeded", cleanup, provider_delete: { state: "terminated", confirmed_at: time }, lifecycle: { cleanup_state: "terminated", cleanup_confirmed_at: time } };
    assert.equal(detailValues(workload)["Provider resource"], "absence not declared");
    assert.notEqual(deriveTimelineSteps(workload).at(-1).status, "done");
  }
});

test("cleanup detail separates human action, provider absence, and durable reap", () => {
  assert.match(detailValues({ cleanup: { state: "manual_attention" } })["Needs a human"], /^yes/);
  assert.match(detailValues({ cleanup: { state: "retrying" } })["Needs a human"], /^no/);
  assert.equal(detailValues({ cleanup: { state: "terminated", deleted_at: "2026-08-10T14:46:12Z" }, failure: { code: "manual_attention" } })["Needs a human"], "no");
  assert.equal(detailValues({ cleanup: { state: "terminated", durable_reap_receipt: "false", reason: {} } })["Durable cleanup receipt"], "not recorded");
  assert.equal(detailValues({ cleanup: { state: "terminated", reason: {} } })["Teardown reason"], "not reported");
});

// --- validTimestamp ---

test("validTimestamp accepts ISO strings", () => {
  assert.equal(validTimestamp("2026-08-10T14:20:00Z"), "2026-08-10T14:20:00Z");
});

test("validTimestamp rejects null, undefined, empty string", () => {
  assert.equal(validTimestamp(null), null);
  assert.equal(validTimestamp(undefined), null);
  assert.equal(validTimestamp(""), null);
});

test("validTimestamp rejects the literal string 'unknown'", () => {
  assert.equal(validTimestamp("unknown"), null);
});

test("validTimestamp rejects non-string values", () => {
  assert.equal(validTimestamp(42), null);
  assert.equal(validTimestamp(true), null);
  assert.equal(validTimestamp({}), null);
});

test("validTimestamp rejects unparseable date strings", () => {
  assert.equal(validTimestamp("not-a-date"), null);
  assert.equal(validTimestamp("2026-13-45"), null);
});

// --- deriveTimelineSteps: active workloads ---

test("active workload with no cleanup has 4 steps, no cleanup step", () => {
  const steps = deriveTimelineSteps({
    status: "running",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
  });
  assert.equal(steps.length, 4);
  assert.equal(steps[0].label, "Requested");
  assert.equal(steps[0].status, "done");
  assert.equal(steps[1].label, "Capacity placed");
  assert.equal(steps[1].status, "done");
  assert.equal(steps[1].time, null);
  assert.equal(steps[2].label, "Running");
  assert.equal(steps[2].status, "done");
  assert.equal(steps[3].label, "Completion");
  assert.equal(steps[3].status, "current");
});

test("active workload pending capacity has capacity as current", () => {
  const steps = deriveTimelineSteps({
    status: "pending",
    created_at: "2026-08-10T14:20:00Z",
  });
  assert.equal(steps[1].label, "Capacity placed");
  assert.equal(steps[1].status, "current");
  assert.equal(steps[2].status, "pending");
  assert.equal(steps[3].status, "pending");
});

// --- deriveTimelineSteps: capacity placed ---

test("capacity placed with provider_created_at gets the timestamp", () => {
  const steps = deriveTimelineSteps({
    status: "running",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    burst_id: "burst_1",
    cleanup: { provider_created_at: "2026-08-10T14:20:45Z" },
  });
  assert.equal(steps[1].status, "done");
  assert.equal(steps[1].time, "2026-08-10T14:20:45Z");
});

test("capacity placed with burst_id but no provider_created_at is done with null time", () => {
  const steps = deriveTimelineSteps({
    status: "running",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    burst_id: "burst_1",
  });
  assert.equal(steps[1].status, "done");
  assert.equal(steps[1].time, null);
});

test("capacity placed with started_at but no burst or provider_created_at is done with null time", () => {
  const steps = deriveTimelineSteps({
    status: "running",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
  });
  assert.equal(steps[1].status, "done");
  assert.equal(steps[1].time, null);
});

test("capacity placed with no evidence is pending", () => {
  const steps = deriveTimelineSteps({
    status: "pending",
    created_at: "2026-08-10T14:20:00Z",
  });
  assert.equal(steps[1].status, "current");
  assert.equal(steps[1].time, null);
});

// --- deriveTimelineSteps: terminal workloads ---

test("terminal workload with no cleanup has unresolved teardown step", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
  });
  assert.equal(steps.length, 5);
  const teardown = steps[4];
  assert.equal(teardown.label, "Teardown evidence not recorded");
  assert.equal(teardown.status, "unresolved");
  assert.equal(teardown.time, null);
});

test("terminal workload completion step uses status label", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:45:00Z",
  });
  assert.equal(steps[3].label, "succeeded");
  assert.equal(steps[3].status, "done");
});

test("cancelled workload uses 'cancelled' as completion label", () => {
  const steps = deriveTimelineSteps({
    status: "cancelled",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:22:00Z",
  });
  assert.equal(steps[3].label, "cancelled");
  assert.equal(steps[3].status, "done");
});

// --- deriveTimelineSteps: cleanup states ---

test("cleanup queued is current", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "queued", requested_at: "2026-08-10T14:45:05Z" },
  });
  const teardown = steps[4];
  assert.equal(teardown.label, "Teardown queued");
  assert.equal(teardown.status, "current");
  assert.equal(teardown.time, null);
});

test("cleanup deleting is current", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "deleting" },
  });
  assert.equal(steps[4].label, "Teardown in progress");
  assert.equal(steps[4].status, "current");
});

test("cleanup retrying is current with timestamp", () => {
  const steps = deriveTimelineSteps({
    status: "failed",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "retrying", updated_at: "2026-08-10T15:00:00Z" },
  });
  assert.equal(steps[4].label, "Teardown retrying");
  assert.equal(steps[4].status, "current");
  assert.equal(steps[4].time, "2026-08-10T15:00:00Z");
});

test("cleanup manual_attention is attention, not done", () => {
  const steps = deriveTimelineSteps({
    status: "failed",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "manual_attention", updated_at: "2026-08-10T16:00:00Z" },
  });
  const teardown = steps[4];
  assert.equal(teardown.label, "Manual teardown required");
  assert.equal(teardown.status, "attention");
  assert.notEqual(teardown.status, "done");
  assert.equal(teardown.time, "2026-08-10T16:00:00Z");
});

test("cleanup terminated with valid deleted_at is done", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: {
      state: "terminated",
      deleted_at: "2026-08-10T14:46:12Z",
      durable_reap_receipt: true,
    },
  });
  const teardown = steps[4];
  assert.equal(teardown.label, "Provider capacity removed");
  assert.equal(teardown.status, "done");
  assert.equal(teardown.time, "2026-08-10T14:46:12Z");
});

test("cleanup terminated without deleted_at is unresolved, not done", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "terminated" },
  });
  const teardown = steps[4];
  assert.equal(teardown.label, "Provider removal unconfirmed");
  assert.equal(teardown.status, "unresolved");
  assert.notEqual(teardown.status, "done");
});

test("cleanup terminated with invalid deleted_at is unresolved", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "terminated", deleted_at: "not-a-date" },
  });
  assert.equal(steps[4].status, "unresolved");
  assert.equal(steps[4].time, null);
});

test("cleanup terminated with durable reap but no deleted_at does not claim provider absence", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_1",
    cleanup: { state: "terminated", durable_reap_receipt: true },
  });
  assert.equal(steps[4].label, "Provider removal unconfirmed");
  assert.equal(steps[4].status, "unresolved");
});

test("unknown cleanup state fails closed to unresolved", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    cleanup: { state: "some_future_state" },
  });
  assert.equal(steps[4].label, "Unknown cleanup state");
  assert.equal(steps[4].status, "unresolved");
});

test("cleanup with null state fails closed to unknown", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    cleanup: { state: null },
  });
  assert.equal(steps[4].label, "Unknown cleanup state");
  assert.equal(steps[4].status, "unresolved");
});

test("cleanup with numeric state fails closed to unknown", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    cleanup: { state: 42 },
  });
  assert.equal(steps[4].label, "Unknown cleanup state");
  assert.equal(steps[4].status, "unresolved");
});

test("malformed cleanup (string) is treated as absent", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    cleanup: "malformed",
  });
  assert.equal(steps[4].label, "Teardown evidence not recorded");
  assert.equal(steps[4].status, "unresolved");
});

test("null workload returns empty steps", () => {
  assert.deepEqual(deriveTimelineSteps(null), []);
  assert.deepEqual(deriveTimelineSteps(undefined), []);
});

test("invalid timestamps never produce string 'unknown' in step time", () => {
  const steps = deriveTimelineSteps({
    status: "succeeded",
    created_at: "bad",
    started_at: null,
    finished_at: undefined,
    cleanup: { state: "retrying", updated_at: "unknown" },
  });
  for (const step of steps) {
    assert.notEqual(step.time, "unknown", `step "${step.label}" must not have time "unknown"`);
  }
});

// --- deriveCleanupPresentation ---

test("active workload without cleanup returns null", () => {
  assert.equal(deriveCleanupPresentation({ status: "running" }), null);
});

test("terminal workload without cleanup is not available", () => {
  const result = deriveCleanupPresentation({ status: "succeeded" });
  assert.equal(result.available, false);
  assert.equal(result.stateLabel, "not recorded");
  assert.equal(result.tone, "warn");
  assert.ok(result.message);
});

test("cleanup queued presentation", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "queued", requested_at: "2026-08-10T14:45:05Z", reason: "run completed" },
  });
  assert.equal(result.available, true);
  assert.equal(result.stateLabel, "queued");
  assert.equal(result.reason, "run completed");
  assert.equal(result.requestedAt, "2026-08-10T14:45:05Z");
  assert.equal(result.providerAbsentConfirmed, false);
  assert.equal(result.providerAbsentText, "not yet");
  assert.equal(result.tone, "plain");
  assert.equal(result.message, null);
});

test("cleanup deleting presentation", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "deleting" },
  });
  assert.equal(result.stateLabel, "deleting");
  assert.equal(result.providerAbsentText, "not yet");
  assert.equal(result.tone, "plain");
});

test("cleanup retrying presentation", () => {
  const result = deriveCleanupPresentation({
    status: "failed",
    cleanup: { state: "retrying", reason: "provider timeout" },
  });
  assert.equal(result.stateLabel, "retrying");
  assert.equal(result.tone, "warn");
  assert.ok(result.message);
  assert.equal(result.providerAbsentConfirmed, false);
});

test("cleanup manual_attention presentation", () => {
  const result = deriveCleanupPresentation({
    status: "failed",
    cleanup: {
      state: "manual_attention",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      reason: "provider API refused deletion",
    },
  });
  assert.equal(result.stateLabel, "manual attention");
  assert.equal(result.tone, "danger");
  assert.ok(result.message);
  assert.equal(result.providerAbsentConfirmed, false);
  assert.equal(result.providerAbsentText, "unproven");
  assert.equal(result.hasProviderDetail, true);
  assert.equal(result.provider, "linode");
  assert.equal(result.region, "us-east");
  assert.equal(result.sku, "g2-gpu-rtx4000a1-s");
});

test("cleanup terminated with full evidence", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: {
      state: "terminated",
      deleted_at: "2026-08-10T14:46:12Z",
      durable_reap_receipt: true,
      requested_at: "2026-08-10T14:45:05Z",
    },
  });
  assert.equal(result.stateLabel, "terminated");
  assert.equal(result.providerAbsentConfirmed, true);
  assert.equal(result.providerAbsentTime, "2026-08-10T14:46:12Z");
  assert.equal(result.durableReap, true);
  assert.equal(result.tone, "plain");
  assert.equal(result.message, null);
});

test("cleanup terminated without deleted_at shows warning", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "terminated" },
  });
  assert.equal(result.providerAbsentConfirmed, false);
  assert.equal(result.providerAbsentText, "timestamp not recorded");
  assert.equal(result.tone, "warn");
  assert.ok(result.message);
});

test("cleanup terminated with durable reap but no deleted_at", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "terminated", durable_reap_receipt: true },
  });
  assert.equal(result.durableReap, true);
  assert.equal(result.providerAbsentConfirmed, false);
  assert.equal(result.providerAbsentText, "timestamp not recorded");
  assert.equal(result.tone, "warn");
});

test("cleanup with invalid deleted_at does not confirm provider absence", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "terminated", deleted_at: "not-a-date" },
  });
  assert.equal(result.providerAbsentConfirmed, false);
  assert.equal(result.providerAbsentText, "timestamp not recorded");
});

test("unknown cleanup state warns and shows unrecognized message", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "future_state" },
  });
  assert.equal(result.stateLabel, "future_state");
  assert.equal(result.tone, "warn");
  assert.ok(result.message.includes("Unrecognized"));
  assert.equal(result.providerAbsentConfirmed, false);
});

test("cleanup with missing state defaults to unknown", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { reason: "some reason" },
  });
  assert.equal(result.stateLabel, "unknown");
  assert.equal(result.tone, "warn");
});

test("cleanup with invalid requested_at returns null requestedAt", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "queued", requested_at: "bad-date" },
  });
  assert.equal(result.requestedAt, null);
});

test("cleanup null workload returns null", () => {
  assert.equal(deriveCleanupPresentation(null), null);
});

test("cleanup malformed (string) treated as absent", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: "malformed",
  });
  assert.equal(result.available, false);
});

test("cleanup with no provider detail has hasProviderDetail false", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "terminated", deleted_at: "2026-08-10T14:46:12Z" },
  });
  assert.equal(result.hasProviderDetail, false);
});

test("cleanup reason defaults to not reported", () => {
  const result = deriveCleanupPresentation({
    status: "succeeded",
    cleanup: { state: "queued" },
  });
  assert.equal(result.reason, "not reported");
});
