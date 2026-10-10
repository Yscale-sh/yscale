import test from "node:test";
import assert from "node:assert/strict";
import {
  orderedWorkloads,
  workloadPhase,
  workloadGpuLabel,
  workloadCluster,
  workloadTemplate,
  workloadSelectedProvider,
  workloadSelectedRegion,
  filterOptions,
  filterWorkloads,
  hasActiveFilters,
  EMPTY_FILTERS,
  elapsedLabel,
  gpuDisplay,
  workloadNeedsAttention,
  DATE_CHOICES,
} from "./workloadHistory.js";

const NOW = new Date("2026-08-22T12:00:00Z");

const running1 = {
  id: "wl_run1", status: "running", created_at: "2026-08-22T10:00:00Z",
  started_at: "2026-08-22T10:01:00Z",
  cluster_id: "cluster-a",
  spec: { spec: { gpu: { kind: "a100", count: 2 } } },
  template: { id: "gpu-train", version: 1 },
  placement: { granted_cluster_id: "cluster-a", receipt: { selected: { provider: "linode", region: "us-east", gpu_kind: "a100", gpu_count: 2 } } },
};

const pending1 = {
  id: "wl_pend1", status: "pending", created_at: "2026-08-22T09:30:00Z",
  cluster_id: "cluster-b",
  spec: { spec: {} },
  template: { id: "cpu-batch", version: 1 },
};

const selectedCpu = {
  ...pending1,
  placement: { granted_cluster_id: "cluster-b", receipt: { selected: { provider: "linode", region: "us-east", sku: "cpu-small" } } },
};

const succeeded1 = {
  id: "wl_succ1", status: "succeeded", created_at: "2026-08-21T14:00:00Z",
  started_at: "2026-08-21T14:01:00Z", finished_at: "2026-08-21T15:31:00Z",
  cluster_id: "cluster-a",
  spec: { spec: { gpu: { kind: "h100", count: 4 } } },
  template: { id: "gpu-train", version: 2 },
  cost: { usd: 12.50 },
  placement: { granted_cluster_id: "cluster-a", receipt: { selected: { provider: "aws", region: "us-west-2", gpu_kind: "h100", gpu_count: 4 } } },
};

const failed1 = {
  id: "wl_fail1", status: "failed", created_at: "2026-08-10T08:00:00Z",
  started_at: "2026-08-10T08:01:00Z", finished_at: "2026-08-10T08:05:00Z",
  spec: { spec: { gpu: { kind: "a100", count: 1 } } },
};

const legacy1 = {
  id: "wl_legacy1", status: "completed", created_at: "2026-07-01T10:00:00Z",
  spec: { spec: {} },
};

const records = [running1, pending1, succeeded1, failed1, legacy1];

// --- ordering ---

test("orderedWorkloads: active before terminal, newest first within groups", () => {
  const ordered = orderedWorkloads(records);
  assert.equal(ordered[0].id, "wl_run1", "newest active first");
  assert.equal(ordered[1].id, "wl_pend1", "second active next");
  assert.equal(ordered[2].id, "wl_succ1", "newest terminal after active");
  assert.equal(ordered[3].id, "wl_fail1");
  assert.equal(ordered[4].id, "wl_legacy1", "oldest terminal last");
});

test("orderedWorkloads: does not mutate input", () => {
  const input = [...records];
  const copy = [...input];
  orderedWorkloads(input);
  assert.deepEqual(input, copy);
});

test("orderedWorkloads: stable with identical timestamps", () => {
  const a = { id: "a", status: "running", created_at: "2026-08-22T10:00:00Z" };
  const b = { id: "b", status: "running", created_at: "2026-08-22T10:00:00Z" };
  const ordered = orderedWorkloads([a, b]);
  assert.equal(ordered.length, 2);
});

// --- phase ---

test("workloadPhase: returns the reported running phase", () => {
  assert.equal(workloadPhase(running1), "running");
});

test("workloadPhase: returns the reported pending phase", () => {
  assert.equal(workloadPhase(pending1), "pending");
});

test("workloadPhase: returns the reported succeeded phase", () => {
  assert.equal(workloadPhase(succeeded1), "succeeded");
});

test("workloadPhase: returns the reported failed phase", () => {
  assert.equal(workloadPhase(failed1), "failed");
});

test("workloadPhase: missing status is unknown", () => {
  assert.equal(workloadPhase({}), "unknown");
});

// --- GPU label ---

test("workloadGpuLabel: with GPU", () => {
  assert.equal(workloadGpuLabel(running1), "a100");
});

test("workloadGpuLabel: authoritative selected GPU wins over the request", () => {
  assert.equal(workloadGpuLabel({
    spec: { spec: { gpu: { kind: "requested", count: 1 } } },
    placement: { receipt: { selected: { provider: "provider", gpu_kind: "selected", gpu_count: 2 } } },
  }), "selected");
});

test("workloadGpuLabel: an authoritative CPU selection is CPU", () => {
  assert.equal(workloadGpuLabel(selectedCpu), "CPU");
});

test("workloadGpuLabel: absent placement is not recorded", () => {
  assert.equal(workloadGpuLabel(pending1), "not recorded");
  assert.equal(workloadGpuLabel({}), "not recorded");
});

// --- cluster ---

test("workloadCluster: from placement.granted_cluster_id", () => {
  assert.equal(workloadCluster(running1), "cluster-a");
});

test("workloadCluster: falls back to cluster_id", () => {
  assert.equal(workloadCluster(pending1), "cluster-b");
});

test("workloadCluster: empty when missing", () => {
  assert.equal(workloadCluster(failed1), "");
});

// --- template ---

test("workloadTemplate: from template.id", () => {
  assert.equal(workloadTemplate(running1), "gpu-train");
});

test("workloadTemplate: empty when no template", () => {
  assert.equal(workloadTemplate(legacy1), "");
});

// --- selected provider/region ---

test("workloadSelectedProvider: from placement receipt", () => {
  assert.equal(workloadSelectedProvider(running1), "linode");
});

test("workloadSelectedProvider: empty when missing", () => {
  assert.equal(workloadSelectedProvider(failed1), "");
});

test("workloadSelectedRegion: from placement receipt", () => {
  assert.equal(workloadSelectedRegion(running1), "us-east");
});

// --- filter options ---

test("filterOptions: collects from all records", () => {
  const opts = filterOptions(records);
  assert.ok(opts.phases.includes("running"));
  assert.ok(opts.phases.includes("succeeded"));
  assert.ok(opts.clusters.includes("cluster-a"));
  assert.ok(opts.clusters.includes("cluster-b"));
  assert.ok(opts.gpus.includes("a100"));
  assert.ok(opts.gpus.includes("not recorded"));
  assert.ok(opts.templates.includes("gpu-train"));
  assert.ok(opts.templates.includes("cpu-batch"));
});

test("filterOptions: no empty clusters or templates", () => {
  const opts = filterOptions(records);
  assert.ok(!opts.clusters.includes(""));
  assert.ok(!opts.templates.includes(""));
});

// --- filtering ---

test("filterWorkloads: no filters returns all", () => {
  const result = filterWorkloads(records, EMPTY_FILTERS, NOW);
  assert.equal(result.length, records.length);
});

test("filterWorkloads: phase filter", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, phase: "running" }, NOW);
  assert.equal(result.length, 1);
  assert.equal(result[0].id, "wl_run1");
});

test("filterWorkloads: cluster filter", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, cluster: "cluster-a" }, NOW);
  assert.ok(result.length >= 1);
  assert.ok(result.every((w) => w.cluster_id === "cluster-a" || w.placement?.granted_cluster_id === "cluster-a"));
});

test("filterWorkloads: gpu filter", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, gpu: "a100" }, NOW);
  assert.deepEqual(result.map((w) => w.id), ["wl_run1"]);
});

test("filterWorkloads: template filter", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, template: "gpu-train" }, NOW);
  assert.ok(result.length >= 1);
  assert.ok(result.every((w) => w.template?.id === "gpu-train"));
});

test("filterWorkloads: date filter 24h", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, date: "24h" }, NOW);
  const cutoff = new Date(NOW.getTime() - 86_400_000);
  assert.ok(result.every((w) => new Date(w.created_at) >= cutoff));
});

test("filterWorkloads: date filter 7d", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, date: "7d" }, NOW);
  assert.ok(result.length < records.length, "should exclude old records");
  assert.ok(!result.find((w) => w.id === "wl_legacy1"));
});

test("filterWorkloads: combined filters", () => {
  const result = filterWorkloads(records, { ...EMPTY_FILTERS, phase: "succeeded", gpu: "h100" }, NOW);
  assert.equal(result.length, 1);
  assert.equal(result[0].id, "wl_succ1");
});

test("filterWorkloads: does not mutate input", () => {
  const input = [...records];
  const copy = [...input];
  filterWorkloads(input, { ...EMPTY_FILTERS, phase: "running" }, NOW);
  assert.deepEqual(input, copy);
});

// --- hasActiveFilters ---

test("hasActiveFilters: false for empty", () => {
  assert.equal(hasActiveFilters(EMPTY_FILTERS), false);
});

test("hasActiveFilters: true when phase set", () => {
  assert.equal(hasActiveFilters({ ...EMPTY_FILTERS, phase: "running" }), true);
});

test("hasActiveFilters: true when date not all", () => {
  assert.equal(hasActiveFilters({ ...EMPTY_FILTERS, date: "7d" }), true);
});

// --- elapsed label ---

test("elapsedLabel: running workload shows elapsed to now", () => {
  const label = elapsedLabel(running1, NOW);
  assert.match(label, /h|m|s/);
});

test("elapsedLabel: terminal workload uses finished_at", () => {
  const label = elapsedLabel(succeeded1, NOW);
  assert.equal(label, "1h 30m");
});

test("elapsedLabel: missing timestamps", () => {
  assert.equal(elapsedLabel({}, NOW), "not recorded");
});

test("elapsedLabel: terminal without finished_at does not keep accumulating", () => {
  assert.equal(elapsedLabel({ status: "failed", created_at: "2026-08-22T10:00:00Z" }, NOW), "not recorded");
});

test("elapsedLabel: short duration", () => {
  const label = elapsedLabel(failed1, NOW);
  assert.equal(label, "4m 0s");
});

// --- gpuDisplay ---

test("gpuDisplay: GPU workload", () => {
  assert.equal(gpuDisplay(running1), "a100 × 2");
});

test("gpuDisplay: authoritative selected GPU wins over the request", () => {
  assert.equal(gpuDisplay({
    spec: { spec: { gpu: { kind: "requested", count: 1 } } },
    placement: { receipt: { selected: { provider: "provider", gpu_kind: "selected", gpu_count: 2 } } },
  }), "selected × 2");
});

test("gpuDisplay: authoritative CPU selection", () => {
  assert.equal(gpuDisplay(selectedCpu), "CPU");
});

test("gpuDisplay: requested or missing GPU without placement is not recorded", () => {
  assert.equal(gpuDisplay(pending1), "not recorded");
  assert.equal(gpuDisplay(failed1), "not recorded");
  assert.equal(gpuDisplay({}), "not recorded");
});

// --- legacy/missing fields ---

test("workloadPhase: preserves a legacy completed phase", () => {
  assert.equal(workloadPhase(legacy1), "completed");
});

test("workloadCluster: legacy with no placement or cluster_id", () => {
  assert.equal(workloadCluster(legacy1), "");
});

test("workloadTemplate: legacy with no template", () => {
  assert.equal(workloadTemplate(legacy1), "");
});

test("gpuDisplay: legacy with no placement", () => {
  assert.equal(gpuDisplay(legacy1), "not recorded");
});

test("DATE_CHOICES has expected keys", () => {
  const keys = DATE_CHOICES.map((c) => c.key);
  assert.deepEqual(keys, ["24h", "7d", "30d", "all"]);
});

// --- workloadNeedsAttention ---

test("workloadNeedsAttention: true for manual_attention cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "manual_attention" } }), true);
});

test("workloadNeedsAttention: false for retrying cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "retrying" } }), false);
});

test("workloadNeedsAttention: false for terminated cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "terminated" } }), false);
});

test("workloadNeedsAttention: false for queued cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "queued" } }), false);
});

test("workloadNeedsAttention: false for deleting cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "deleting" } }), false);
});

test("workloadNeedsAttention: false for unknown cleanup state", () => {
  assert.equal(workloadNeedsAttention({ cleanup: { state: "something_new" } }), false);
});

test("workloadNeedsAttention: false when cleanup is missing", () => {
  assert.equal(workloadNeedsAttention({}), false);
  assert.equal(workloadNeedsAttention({ cleanup: undefined }), false);
});

test("workloadNeedsAttention: false when cleanup is null", () => {
  assert.equal(workloadNeedsAttention({ cleanup: null }), false);
});

test("workloadNeedsAttention: false when cleanup is a string", () => {
  assert.equal(workloadNeedsAttention({ cleanup: "manual_attention" }), false);
});

test("workloadNeedsAttention: false when cleanup is an array", () => {
  assert.equal(workloadNeedsAttention({ cleanup: [{ state: "manual_attention" }] }), false);
});

test("workloadNeedsAttention: false when workload is null/undefined", () => {
  assert.equal(workloadNeedsAttention(null), false);
  assert.equal(workloadNeedsAttention(undefined), false);
});

test("workloadNeedsAttention: failed phase alone does not imply attention", () => {
  assert.equal(workloadNeedsAttention({ status: "failed" }), false);
  assert.equal(workloadNeedsAttention({ status: "failed", cleanup: { state: "retrying" } }), false);
});

test("workloadNeedsAttention: cancelled phase alone does not imply attention", () => {
  assert.equal(workloadNeedsAttention({ status: "cancelled" }), false);
});
