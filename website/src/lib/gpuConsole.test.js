import assert from "node:assert/strict";
import test from "node:test";
import {
  GPU_FAILURE_STATES,
  filterGPUWorkloads,
  sortWorkloadsActiveFirst,
  workloadCluster,
  workloadElapsed,
  workloadFailureState,
  workloadGPU,
  workloadIsTerminal,
  workloadNeedsAttention,
  workloadPlacement,
} from "./gpuConsole.js";

const run = (id, status, createdAt, extra = {}) => ({ id, status, created_at: createdAt, spec: { metadata: { name: id }, spec: {} }, ...extra });

test("workloads sort attention first, then active, then recent terminal runs", () => {
  const rows = sortWorkloadsActiveFirst([
    run("old-terminal", "succeeded", "2026-01-01T00:00:00Z"),
    run("active", "running", "2026-01-02T00:00:00Z"),
    run("attention", "failed", "2026-01-01T00:00:00Z", { cleanup: { state: "manual_attention" } }),
    run("new-terminal", "failed", "2026-01-03T00:00:00Z"),
  ]);
  assert.deepEqual(rows.map((row) => row.id), ["attention", "active", "new-terminal", "old-terminal"]);
});

test("GPU, cluster, placement, and elapsed labels use only reported fields", () => {
  const workload = run("train", "succeeded", "2026-08-20T10:00:00Z", {
    started_at: "2026-08-20T10:01:00Z",
    finished_at: "2026-08-20T11:02:03Z",
    cluster_id: "cluster-a",
    placement: { granted_cluster_id: "cluster-b", receipt: { selected: { provider: "linode", region: "us-east", sku: "g6-gpu-rtx4000ada1-s", gpu_kind: "rtx4000ada", gpu_count: 1 } } },
    spec: { metadata: { name: "train" }, spec: { gpu: { kind: "any", count: 2 } } },
  });
  assert.deepEqual(workloadGPU(workload), { label: "rtx4000ada × 1", kind: "rtx4000ada", count: 1 });
  assert.equal(workloadCluster(workload), "cluster-b");
  assert.deepEqual(workloadPlacement(workload), { provider: "linode", region: "us-east", sku: "g6-gpu-rtx4000ada1-s" });
  assert.equal(workloadElapsed(workload), "1h 1m");
  assert.equal(workloadIsTerminal({ lifecycle: { phase: "succeeded" } }), true);
  assert.equal(workloadIsTerminal({ status: "failed", lifecycle: { phase: "provider_delete_retrying" } }), true);
  assert.deepEqual(workloadPlacement(run("legacy", "running", "2026-08-20T10:00:00Z")), { provider: "not reported", region: "not reported", sku: "not reported" });
  assert.deepEqual(workloadGPU({ spec: { spec: { gpu: {} } } }), { label: "not reported", kind: "not reported", count: null });
});

test("filters preserve active-first order and never invent missing dimensions", () => {
  const gpu = run("train", "running", "2026-08-20T10:00:00Z", { cluster_id: "ml", template: { id: "pytorch" }, placement: { receipt: { selected: { provider: "aws", gpu_kind: "a100", gpu_count: 1 } } }, spec: { metadata: { name: "nightly" }, spec: { image: "trainer", gpu: { kind: "any", count: 1 } } } });
  const cpu = run("index", "succeeded", "2026-08-20T11:00:00Z", { cluster_id: "prod", template: { id: "container" } });
  assert.deepEqual(filterGPUWorkloads([cpu, gpu], { cluster: "ml", gpu: "a100", template: "pytorch", query: "night" }).map((row) => row.id), ["train"]);
  assert.equal(filterGPUWorkloads([run("unknown", "running", "2026-08-20T12:00:00Z")], { cluster: "not reported" }).length, 1);
  assert.deepEqual(filterGPUWorkloads([cpu, gpu], { date: "24h", now: Date.parse("2026-08-21T10:30:00Z") }).map((row) => row.id), ["index"]);
});

test("selected compute and placement never fall back to requested or obsolete flat fields", () => {
  const missing = { label: "not reported", kind: "not reported", count: null };
  const legacy = { provider: "aws", region: "us-west-2", sku: "requested-sku", placement: { provider: "aws" }, spec: { spec: { gpu: { kind: "a100", count: 4 } } } };
  assert.deepEqual(workloadGPU(legacy), missing);
  assert.deepEqual(workloadGPU({ spec: { spec: {} } }), missing);
  assert.deepEqual(workloadPlacement(legacy), { provider: "not reported", region: "not reported", sku: "not reported" });
  for (const selected of [null, [], "linode", { gpu_kind: "a100", gpu_count: 1 }, { provider: "linode", gpu_kind: "a100", gpu_count: true }, { provider: "linode", gpu_kind: "a100", gpu_count: "1" }, { provider: "linode", gpu_count: 1 }, { provider: "linode", gpu_kind: {}, gpu_count: 0 }, { provider: "linode", gpu_kind: null }]) {
    assert.deepEqual(workloadGPU({ placement: { receipt: { selected } } }), missing);
  }
  assert.deepEqual(workloadGPU({ placement: { receipt: { selected: { provider: "linode", sku: "cpu-small" } } } }), { label: "CPU only", kind: "cpu", count: 0 });
});

test("every workload and provider failure state has bounded actionable copy", () => {
  assert.deepEqual(GPU_FAILURE_STATES.map((state) => state.code), [
    "no_connected_cluster", "connector_offline", "no_eligible_placement", "price_above_cap",
    "placement_changed", "node_join_failed", "gpu_unhealthy", "pod_unschedulable",
    "provider_delete_retrying", "manual_attention",
  ]);
  assert.equal(new Set(GPU_FAILURE_STATES.map((state) => state.code)).size, GPU_FAILURE_STATES.length);
  for (const state of GPU_FAILURE_STATES) {
    assert.ok(state.title.length > 3 && state.title.length < 80);
    assert.ok(state.detail.length > 10 && state.detail.length < 220);
    assert.ok(state.action.length > 3 && state.action.length < 40);
  }
  const attention = { status: "failed", provider_delete: { state: "manual_attention" } };
  assert.equal(workloadNeedsAttention(attention), true);
  assert.equal(workloadFailureState(attention).code, "manual_attention");
  assert.equal(workloadFailureState({ phase: "gpu_unhealthy" }).code, "gpu_unhealthy");
  assert.equal(workloadNeedsAttention({ failure: { code: "node_join_failed" } }), true);
  assert.equal(workloadFailureState({ cleanup: { state: "retrying" } }).code, "provider_delete_retrying");
  assert.equal(workloadFailureState({ cleanup: { state: "deleting" } }), null);
});
