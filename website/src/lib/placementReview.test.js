import assert from "node:assert/strict";
import test from "node:test";

import {
  isPlacementReReviewError,
  normalizePreviewPlacement,
  executionRecipeRows,
  launchReviewRows,
  placementReceiptRows,
  placementReviewGuardrail,
  placementReviewShape,
  previewPlacementRows,
} from "./placementReview.js";

const hostedCluster = {
  id: "hosted-east",
  name: "Hosted east",
  source: "hosted",
  hostedNamespace: "ten-acme",
  state: "connected_here",
  connections: 1,
  eligible: true,
};

const tenantCluster = {
  id: "tenant-east",
  name: "Tenant east",
  source: "tenant",
  state: "connected_here",
  connections: 1,
  eligible: true,
};

test("review shape names backend and compact compute facts", () => {
  assert.deepEqual(
    placementReviewShape({
      template: { id: "train-gpu" },
      composedVersion: 7,
      values: {
        namespace: "ml",
        backend: "linode",
        region: "us-east",
        mode: "gpu",
        gpuKind: "l40s",
        gpuCount: 2,
      },
      selectedCluster: hostedCluster,
    }),
    {
      template: "train-gpu v7",
      namespace: "ml · reserved on Hosted east",
      backend: "linode / us-east",
      compute: "l40s x 2 GPU",
    },
  );
});

test("guardrail note separates hosted from customer-owned enforcement", () => {
  assert.deepEqual(
    placementReviewGuardrail({ values: { maxUSD: 12, deadline: "1h" }, selectedCluster: hostedCluster }),
    {
      limit: "$12.00 / 1h",
      note: "Central-owned capacity may be stopped at budget or deadline.",
    },
  );
  assert.equal(
    placementReviewGuardrail({ values: { maxUSD: 12, deadline: "1h" }, selectedCluster: tenantCluster }).note,
    "Customer-owned cluster budgets are recorded, not centrally enforced.",
  );
  assert.equal(
    placementReviewGuardrail({ values: { maxUSD: 12, deadline: "1h" }, selectedCluster: null }).note,
    "Enforcement follows the cluster central grants; the stored placement receipt is authoritative.",
  );
});

test("placement receipt rows show only the authoritative stored placement", () => {
  assert.deepEqual(
    placementReceiptRows({
      cluster_id: "tenant-east",
      placement: { mode: "auto", granted_cluster_id: "tenant-east" },
    }),
    [
      { label: "Granted cluster", value: "tenant-east" },
      { label: "Stored mode", value: "ordered automatic" },
      { label: "Requested cluster", value: "none recorded" },
    ],
  );
});

test("normalizePreviewPlacement extracts the selected route", () => {
  const preview = {
    status: "ok",
    placement: {
      version: 1,
      granted_cluster_id: "acme-lab",
      cluster_mode: "pinned",
      selected: {
        provider: "linode", region: "us-east", sku: "gpu-l40s-x2",
        gpu_kind: "l40s", gpu_count: 2, hourly_micro_usd: 2_500_000,
        maximum_duration_seconds: 3600, maximum_charge_micro_usd: 12_000_000,
      },
      candidates: [
        { provider: "linode", region: "us-east", sku: "gpu-l40s-x2", hourly_micro_usd: 2_500_000, selected: true },
        { provider: "aws", region: "us-west-2", sku: "gpu-a100-x8", hourly_micro_usd: 8_000_000, reason: "price_cap_exceeded" },
      ],
      availability_confidence: "create_time_only",
      pricing_version: 1,
      issued_at: "2026-08-22T00:00:00Z",
      expires_at: "2026-08-22T00:10:00Z",
      quote_id: "q_1",
      digest: "sha256:abc",
    },
    launch_token: "tok",
  };
  const n = normalizePreviewPlacement(preview);
  assert.equal(n.grantedClusterId, "acme-lab");
  assert.equal(n.provider, "linode");
  assert.equal(n.region, "us-east");
  assert.equal(n.sku, "gpu-l40s-x2");
  assert.equal(n.hourlyRate, 2.50);
  assert.equal(n.gpuKind, "l40s");
  assert.equal(n.gpuCount, 2);
  assert.equal(n.maximumDurationSeconds, 3600);
  assert.equal(n.maximumChargeUSD, 12);
  assert.equal(n.confidence, "create_time_only");
  assert.equal(n.candidates.length, 1);
  assert.equal(n.candidates[0].reason, "price_cap_exceeded");
});

test("normalizePreviewPlacement returns null for refusal", () => {
  assert.equal(normalizePreviewPlacement({ status: "rejected" }), null);
  assert.equal(normalizePreviewPlacement(null), null);
});

test("normalizePreviewPlacement keeps rejected candidates from a refusal receipt", () => {
  const normalized = normalizePreviewPlacement({
    status: "rejected",
    placement: {
      selected: {},
      candidates: [{ provider: "linode", region: "us-east", reason: "price_cap_exceeded" }],
    },
  });
  assert.equal(normalized.candidates.length, 1);
  assert.equal(normalized.candidates[0].reason, "price_cap_exceeded");
});

test("normalizePreviewPlacement bounds alternatives from an upstream receipt", () => {
  const candidates = Array.from({ length: 12 }, (_, index) => ({
    provider: `provider-${index}`,
    region: "us-east",
    sku: `sku-${index}`,
    reason: "not_selected",
  }));
  const normalized = normalizePreviewPlacement({
    status: "ok",
    placement: { selected: {}, candidates },
  });
  assert.equal(normalized.candidates.length, 8);
  assert.equal(normalized.candidates[7].provider, "provider-7");
});

test("previewPlacementRows renders a compact receipt", () => {
  const n = normalizePreviewPlacement({
    status: "ok",
    placement: {
      granted_cluster_id: "acme-lab",
      selected: {
        provider: "linode", region: "us-east", sku: "gpu-l40s-x2",
        gpu_kind: "l40s", gpu_count: 2, hourly_micro_usd: 2_500_000,
        maximum_duration_seconds: 3600, maximum_charge_micro_usd: 12_000_000,
      },
      candidates: [],
      availability_confidence: "create_time_only",
    },
  });
  const rows = previewPlacementRows(n);
  assert.ok(rows.some((r) => r.label === "GPU" && r.value === "l40s × 2"));
  assert.ok(rows.some((r) => r.label === "SKU" && r.value === "gpu-l40s-x2"));
  assert.ok(rows.some((r) => r.label === "Hourly rate" && r.value === "$2.50"));
  assert.ok(rows.some((r) => r.label === "Maximum duration" && r.value === "1h"));
  assert.ok(rows.some((r) => r.label === "Maximum charge" && r.value === "$12.00"));
  assert.ok(rows.some((r) => r.label === "Confidence" && r.value === "create time only"));
});

test("isPlacementReReviewError identifies the three re-review codes", () => {
  assert.ok(isPlacementReReviewError({ code: "placement_changed" }));
  assert.ok(isPlacementReReviewError({ code: "placement_preview_expired" }));
  assert.ok(isPlacementReReviewError({ code: "placement_digest_invalid" }));
  assert.ok(!isPlacementReReviewError({ code: "in_progress" }));
  assert.ok(!isPlacementReReviewError(null));
});

test("launch review rows carry the full summary a submitter reads", () => {
  assert.deepEqual(
    launchReviewRows({
      values: {
        name: "nightly-train",
        namespace: "ml",
        mode: "gpu",
        gpuKind: "l40s",
        gpuCount: 2,
        backend: "auto",
        region: "",
        dataEnabled: true,
        dataProvider: "r2",
        dataTarget: "/data/in",
        outputEnabled: true,
        outputProvider: "s3",
        outputTarget: "/data/out",
        maxUSD: 12,
        deadline: "1h",
      },
      template: { id: "train-gpu", title: "GPU training" },
      composedVersion: 7,
      clusterId: "",
      allowAutomatic: true,
      hostedTarget: null,
    }),
    [
      { label: "Name", value: "nightly-train" },
      { label: "Namespace", value: "ml" },
      { label: "Target", value: "Automatic" },
      { label: "Template", value: "GPU training · train-gpu v7" },
      { label: "Compute", value: "l40s × 2" },
      { label: "Backend", value: "automatic" },
      { label: "Program", value: "Image default" },
      { label: "Arguments", value: "Image default" },
      { label: "Input", value: "Pull R2 into /data/in" },
      { label: "Output", value: "Push /data/out to S3" },
      { label: "Budget", value: "$12.00 / 1h" },
    ],
  );
});

test("execution review names the program and keeps long arguments bounded", () => {
  const rows = executionRecipeRows({ command: ["python"], args: ["-c", "x".repeat(220)] });
  assert.match(rows[0].value, /python/);
  assert.match(rows[1].value, /^2 tokens/);
  assert.ok(rows[1].value.length <= 200);
  assert.deepEqual(executionRecipeRows({}), [
    { label: "Program", value: "Image default" },
    { label: "Arguments", value: "Image default" },
  ]);
  assert.deepEqual(executionRecipeRows({ execution: { mode: "custom", command_tokens: 1, argument_tokens: 2 } }), [
    { label: "Program", value: "Custom entrypoint · 1 token" },
    { label: "Arguments", value: "2 tokens · values hidden after submission" },
  ]);
});

test("launch review rows say what an unmade choice and a hosted target are", () => {
  const rows = launchReviewRows({
    values: {
      name: "cpu-job",
      namespace: "ten-acme",
      mode: "cpu",
      size: "small",
      backend: "linode",
      region: "us-east",
      dataEnabled: false,
      outputEnabled: false,
      maxUSD: 3.5,
      deadline: "30m",
    },
    template: { id: "serve-cpu", title: "CPU serve" },
    composedVersion: 2,
    clusterId: "",
    allowAutomatic: false,
    hostedTarget: hostedCluster,
  });
  assert.deepEqual(
    rows.filter((row) => ["Namespace", "Target", "Compute", "Backend", "Input", "Output"].includes(row.label)),
    [
      { label: "Namespace", value: "ten-acme · reserved on Hosted east" },
      { label: "Target", value: "no cluster selected" },
      { label: "Compute", value: "small CPU" },
      { label: "Backend", value: "linode / us-east" },
      { label: "Input", value: "None" },
      { label: "Output", value: "None" },
    ],
  );
});
