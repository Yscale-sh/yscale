import { ApiError } from "./apiError.js";
import { CLUSTER_ID_RE, HOSTED_CLUSTER_SOURCE, hostedNamespaceError, normalizeClusterObservation, normalizeClusterRow } from "./clusters.js";
import { isTerminal } from "./consoleData.js";
import { canManageTenant } from "./roles.js";
import { ACCEPTED_GPU_KINDS, LAUNCH_RELIABILITY, WORKLOAD_TEMPLATES } from "./workloadTemplates.js";

const PREVIEW_TOKEN = "yscale-local-preview";
const FIRST_WORKSPACE_PREVIEW_TOKEN = "yscale-local-preview-first-workspace";
const PREVIEW_ACCOUNT_KEY = "yscale.dev-preview.account";
const PREVIEW_HOSTS = new Set(["localhost", "127.0.0.1", "yscale-dev.yscale.sh"]);
// Central scopes the journal to a tenant's own managers; the preview refuses
// the same roles so the restricted state is reachable without a backend.
const AUDIT_ROLES = new Set(["owner", "admin"]);

const POPULATED_PREVIEW_ACCOUNT = {
  account_id: "acct_preview",
  subject: "preview-developer",
  email: "developer@example.test",
  email_verified: true,
  name: "Preview Developer",
  created_at: "2026-08-01T12:00:00Z",
  // The limit keys mirror what /v1/account actually reports (handlers.
  // TenantLimits), and the two roles exist so the manager-only surfaces and the
  // role-refused ones can both be walked from the tenant switcher.
  //
  // The namespace lists differ on purpose. The owner tenant has more than one,
  // so the launch form's select is a real choice; the viewer tenant has a
  // single namespace that is not "default", so a form that opened on a
  // hardcoded namespace instead of the tenant's first authorized one is visibly
  // wrong in the preview rather than only in production.
  //
  // ten-acme-research is the owner tenant's reserved namespace on the hosted
  // row below — central authorizes it when it assigns hosted capacity, and it
  // sits last so the form still opens on "default".
  tenants: [
    {
      customer_id: "tenant-acme-research",
      role: "owner",
      plan: "enterprise-preview",
      limits: { max_concurrent_bursts: 8, max_hourly_usd: 24 },
      workload_namespaces: ["default", "ml-team-a", "ten-acme-research"],
    },
    {
      customer_id: "tenant-platform-lab",
      role: "viewer",
      plan: "enterprise-preview",
      limits: { max_concurrent_bursts: 4, max_hourly_usd: 6 },
      workload_namespaces: ["platform-lab"],
    },
  ],
};

const ZERO_TENANT_PREVIEW_ACCOUNT = {
  account_id: "acct_preview_zero",
  subject: "preview-first-workspace",
  email: "first-workspace@example.test",
  email_verified: true,
  name: "First Workspace Preview",
  created_at: "2026-08-14T12:00:00Z",
  tenants: [],
};

let accountState = structuredClone(POPULATED_PREVIEW_ACCOUNT);

function previewStorage() {
  try {
    return typeof window === "undefined" ? null : window.sessionStorage;
  } catch {
    return null;
  }
}

function persistPreviewAccount() {
  try {
    previewStorage()?.setItem(PREVIEW_ACCOUNT_KEY, JSON.stringify(accountState));
  } catch {
    // Preview persistence is a convenience; memory remains the authority when
    // session storage is unavailable or full.
  }
}

function restorePreviewAccount(token) {
  if (token !== FIRST_WORKSPACE_PREVIEW_TOKEN) return;
  try {
    const parsed = JSON.parse(previewStorage()?.getItem(PREVIEW_ACCOUNT_KEY) || "null");
    if (parsed?.account_id === ZERO_TENANT_PREVIEW_ACCOUNT.account_id && Array.isArray(parsed.tenants)) {
      accountState = parsed;
      if (parsed.tenants[0]) seedFirstWorkspaceFixtures(parsed.tenants[0]);
      return;
    }
  } catch {
    // A malformed preview record is discarded below.
  }
  accountState = structuredClone(ZERO_TENANT_PREVIEW_ACCOUNT);
  persistPreviewAccount();
}

// The roster and the run list are fixtures the preview mutates: a removal and a
// launch have to behave like the real thing. Every write below replaces the
// binding rather than editing a fixture row, so PREVIEW_* stays the pristine
// starting state and resetDevPreview can hand it back whole.
const PREVIEW_MEMBERS = Object.freeze([
  { account_id: POPULATED_PREVIEW_ACCOUNT.account_id, email: POPULATED_PREVIEW_ACCOUNT.email, name: POPULATED_PREVIEW_ACCOUNT.name, role: "owner", created_at: POPULATED_PREVIEW_ACCOUNT.created_at },
  { account_id: "acct_preview_lena", email: "lena@example.test", name: "Lena Ortiz", role: "admin", created_at: "2026-08-02T09:30:00Z" },
  { account_id: "acct_preview_sam", email: "sam@example.test", name: "Sam Okafor", role: "member", created_at: "2026-08-04T15:12:00Z" },
  { account_id: "acct_preview_rai", email: "rai@example.test", name: "Rai Bergström", role: "viewer", created_at: "2026-08-07T11:45:00Z" },
]);

const PREVIEW_DIRECTORY = Object.freeze([
  { account_id: "acct_preview_mina", email: "mina@example.test", name: "Mina Patel", created_at: "2026-08-10T10:20:00Z" },
]);

let members = PREVIEW_MEMBERS;

// Journal rows in the shape handlers.TenantAuditEvent renders. Newest first,
// with ids in the cursor shape the real route accepts (aud_ + 32 hex) and
// descending in step with `at`, because the route's cursor is the last id of
// the page and paging older must mean paging down both orders at once.
const auditEvents = Object.freeze([
  {
    id: "aud_0000000000000000000000000000000f", at: "2026-08-12T20:21:11Z",
    action: "workload.started", outcome: "observed",
    actor: { kind: "system" },
    target_kind: "workload", target_id: "wl_preview_train",
    detail: { burst_id: "burst_preview_rtx4000ada" },
  },
  {
    id: "aud_0000000000000000000000000000000e", at: "2026-08-12T20:18:04Z",
    action: "workload.submit", outcome: "accepted",
    actor: { kind: "account", account_id: "acct_preview" },
    target_kind: "workload", target_id: "wl_preview_train",
    detail: { reason: "namespace_authorized", role: "owner", rule: "tenant-workload-namespaces", rule_version: "v1", requested_namespace: "ml-team-a", granted_namespace: "ml-team-a" },
  },
  {
    id: "aud_0000000000000000000000000000000d", at: "2026-08-11T16:17:41Z",
    action: "workload.completed", outcome: "observed",
    actor: { kind: "system" },
    target_kind: "workload", target_id: "wl_preview_cpu",
    detail: { status: "succeeded", burst_id: "burst_preview_cpu" },
  },
  {
    id: "aud_0000000000000000000000000000000c", at: "2026-08-11T16:04:02Z",
    action: "workload.submit", outcome: "accepted",
    actor: { kind: "account", account_id: "acct_preview_sam" },
    target_kind: "workload", target_id: "wl_preview_cpu",
    detail: { reason: "namespace_authorized", role: "member", rule: "tenant-workload-namespaces", rule_version: "v1", requested_namespace: "default", granted_namespace: "default" },
  },
  {
    id: "aud_0000000000000000000000000000000b", at: "2026-08-10T08:55:19Z",
    action: "workload.submit", outcome: "denied",
    actor: { kind: "account", account_id: "acct_preview_rai" },
    target_kind: "workload",
    detail: { reason: "role_read_only", role: "viewer" },
  },
  {
    id: "aud_0000000000000000000000000000000a", at: "2026-08-07T11:45:00Z",
    action: "membership.grant", outcome: "accepted",
    actor: { kind: "account", account_id: "acct_preview" },
    target_kind: "membership", target_id: "acct_preview_rai",
    detail: { role: "viewer" },
  },
  {
    id: "aud_00000000000000000000000000000009", at: "2026-08-05T13:02:36Z",
    action: "membership.role_change", outcome: "accepted",
    actor: { kind: "account", account_id: "acct_preview" },
    target_kind: "membership", target_id: "acct_preview_lena",
    detail: { role: "admin", previous_role: "member" },
  },
  {
    id: "aud_00000000000000000000000000000008", at: "2026-08-01T12:04:50Z",
    action: "tenant.workload_namespaces", outcome: "accepted",
    actor: { kind: "account", account_id: "acct_preview" },
    target_kind: "tenant", target_id: "tenant-acme-research",
    detail: { namespaces: ["default", "ml-team-a"] },
  },
]);

// The records cover all three cost states: a run still going, a torn-down run
// carrying an estimate, and terminal records with no observation.
function previewStoredPlacement({ clusterId, mode = "pinned", provider, region, sku, gpuKind = "", gpuCount = 0, hourlyMicroUSD, maximumChargeMicroUSD, deadlineSeconds = 14_400, maximumDurationSeconds = 14_400, quoteID, digest }) {
  const cpuMillis = gpuKind ? 8_000 : 2_000;
  const memoryMB = gpuKind ? 32_768 : 4_096;
  const hourly = hourlyMicroUSD || (gpuKind ? 520_000 : 100_000);
  const maximumCharge = maximumChargeMicroUSD || (gpuKind ? 8_000_000 : 1_000_000);
  const selected = {
    provider, region, sku, cpu_millis: cpuMillis, memory_mb: memoryMB,
    account_mode: "platform", hourly_micro_usd: hourly,
    maximum_duration_seconds: maximumDurationSeconds, maximum_charge_micro_usd: maximumCharge,
    ...(gpuKind ? { gpu_kind: gpuKind, gpu_count: gpuCount } : {}),
  };
  const candidate = {
    provider, region, sku, cpu_millis: cpuMillis, memory_mb: memoryMB,
    account_mode: "platform", hourly_micro_usd: hourly, selected: true,
    ...(gpuKind ? { gpu_kind: gpuKind, gpu_count: gpuCount } : {}),
  };
  return {
    ...(mode === "pinned" ? { requested_cluster_id: clusterId } : {}),
    granted_cluster_id: clusterId,
    mode,
    receipt: {
      version: 1,
      tenant: "tenant-acme-research",
      ...(mode === "pinned" ? { requested_cluster_id: clusterId } : {}),
      granted_cluster_id: clusterId,
      cluster_mode: mode,
      requested: {
        cpu_millis: cpuMillis,
        memory_mb: memoryMB,
        ...(gpuKind ? { gpu_kind: gpuKind, gpu_count: gpuCount, gpu_reliability: "reliable" } : {}),
      },
      constraints: {
        account_mode: "platform",
        account_eligible: true,
        max_charge_micro_usd: maximumCharge,
        deadline_seconds: deadlineSeconds,
      },
      candidates: [candidate],
      selected,
      pricing_version: 1,
      candidate_set_version: "v1",
      availability_confidence: "create_time_only",
      quote_id: quoteID,
      issued_at: "2026-08-12T20:18:04Z",
      expires_at: "2026-08-12T20:28:04Z",
      digest,
    },
  };
}

const PREVIEW_WORKLOADS = Object.freeze([
  {
    id: "wl_preview_train",
    status: "running",
    created_at: "2026-08-12T20:18:00Z",
    started_at: "2026-08-12T20:21:00Z",
    finished_at: null,
    burst_id: "burst_preview_rtx4000ada",
    cluster_id: "acme-ml-lab",
    placement: previewStoredPlacement({
      clusterId: "acme-ml-lab",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      gpuKind: "rtx4000ada",
      gpuCount: 1,
      quoteID: "quote_111111111111",
      digest: "1111111111111111111111111111111111111111111111111111111111111111",
    }),
    // Central records what a run was launched from, so this record shows exact
    // provenance while wl_preview_legacy below — written before the contract —
    // can only have a template inferred from its shape.
    template: { id: "pytorch-training", version: 3, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview" },
    authorization: {
      requested_namespace: "ml-team-a",
      granted_namespace: "ml-team-a",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "owner",
      decided_at: "2026-08-12T20:18:04Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "nightly-embeddings", namespace: "ml-team-a" },
      spec: {
        image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
        command: ["python"],
        args: ["-c", "import torch; d=\"cuda\"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec(\"for _ in range(3):\\n e=w*x+b-y\\n w-=.1*(e*x).mean()\\n b-=.1*e.mean()\"); print(float(w),float(b))"],
        size: "large",
        gpu: { kind: "rtx4000ada", count: 1, reliability: "reliable" },
        budget: { maxUSD: 8, deadline: "4h" },
      },
    },
  },
  {
    id: "wl_preview_keda_burst",
    status: "running",
    created_at: "2026-08-12T06:32:00Z",
    started_at: "2026-08-12T06:34:10Z",
    finished_at: null,
    burst_id: "burst_preview_pending_pod",
    // Node-only capacity the Connector asked for after observing a Pending pod:
    // the controller owns the pod, so there is no image and no Job here. The
    // submitter is the cluster credential, not a person, and nothing on the
    // record names which controller created the pod — the seam cannot see it.
    submission_origin: "pending-pod",
    cluster_id: "acme-ml-lab",
    placement: { requested_cluster_id: "acme-ml-lab", granted_cluster_id: "acme-ml-lab", mode: "pinned" },
    submitted_by: { kind: "cluster" },
    authorization: {
      requested_namespace: "ml-team-a",
      granted_namespace: "ml-team-a",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-12T06:32:03Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "inference-pool-node", namespace: "ml-team-a" },
      spec: {
        image: "",
        size: "medium",
        nodeOnly: true,
        gpu: { kind: "rtx4000ada", count: 1, reliability: "reliable" },
        budget: { maxUSD: 5, deadline: "1h" },
      },
    },
  },
  {
    id: "wl_preview_cpu",
    status: "succeeded",
    created_at: "2026-08-11T16:04:00Z",
    started_at: "2026-08-11T16:05:00Z",
    finished_at: "2026-08-11T16:17:00Z",
    burst_id: "burst_preview_cpu",
    cluster_id: "acme-prod-us-east",
    placement: previewStoredPlacement({
      clusterId: "acme-prod-us-east",
      mode: "auto",
      provider: "linode",
      region: "us-east",
      sku: "cpu-small",
      hourlyMicroUSD: 2_500_000,
      maximumChargeMicroUSD: 1_000_000,
      deadlineSeconds: 3_600,
      maximumDurationSeconds: 1_440,
      quoteID: "quote_222222222222",
      digest: "2222222222222222222222222222222222222222222222222222222222222222",
    }),
    cost: {
      usd: 0.5,
      hourly_usd: 2.5,
      runtime_seconds: 720,
      frozen_at: "2026-08-11T16:17:12Z",
      backend: "linode",
      burst_id: "burst_preview_cpu",
      basis: "hourly_rate_x_runtime_to_reap_claim",
    },
    template: { id: "container-job", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview_sam" },
    authorization: {
      requested_namespace: "default",
      granted_namespace: "default",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-11T16:04:02Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "catalog-refresh", namespace: "default" },
      spec: {
        image: "docker.io/library/busybox:1.36",
        size: "small",
        budget: { maxUSD: 1, deadline: "1h" },
      },
    },
  },
  {
    id: "wl_preview_export_success",
    status: "succeeded",
    created_at: "2026-08-10T14:20:00Z",
    started_at: "2026-08-10T14:21:00Z",
    finished_at: "2026-08-10T14:45:00Z",
    burst_id: "burst_preview_export_success",
    cluster_id: "acme-ml-lab",
    template: { id: "pytorch-training", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview" },
    authorization: {
      requested_namespace: "ml-team-a",
      granted_namespace: "ml-team-a",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "owner",
      decided_at: "2026-08-10T14:20:02Z",
    },
    outcome: {
      compute: { result: "succeeded" },
      artifacts: {
        result: "succeeded",
        objects_uploaded: 14,
        bytes_uploaded: 1610612736,
      },
    },
    cleanup: {
      state: "terminated",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      reason: "capacity freed after run completion",
      requested_at: "2026-08-10T14:45:05Z",
      updated_at: "2026-08-10T14:46:12Z",
      deleted_at: "2026-08-10T14:46:12Z",
      provider_created_at: "2026-08-10T14:20:45Z",
      durable_reap_receipt: true,
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "model-checkpoint-export", namespace: "ml-team-a" },
      spec: {
        image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
        size: "large",
        gpu: { kind: "a100", count: 1, reliability: "reliable" },
        data: {
          pull_before_run_inputs: 1,
          input_size_hint_gb: 48,
          push_after_run_outputs: 1,
          output_max_upload_gb: 10,
        },
        budget: { maxUSD: 5, deadline: "2h" },
      },
    },
  },
  {
    id: "wl_preview_export_failed",
    status: "failed",
    created_at: "2026-08-09T10:00:00Z",
    started_at: "2026-08-09T10:01:00Z",
    finished_at: "2026-08-09T10:15:00Z",
    burst_id: "burst_preview_export_failed",
    cluster_id: "acme-prod-us-east",
    template: { id: "container-job", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview_sam" },
    authorization: {
      requested_namespace: "default",
      granted_namespace: "default",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-09T10:00:02Z",
    },
    outcome: {
      compute: { result: "succeeded" },
      artifacts: {
        result: "failed",
        reason: "an artifact upload to the object store failed",
        objects_uploaded: 0,
        bytes_uploaded: 0,
      },
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "dataset-pack-export-failed", namespace: "default" },
      spec: {
        image: "docker.io/library/busybox:1.36",
        size: "small",
        data: {
          pull_before_run_inputs: 1,
          input_size_hint_gb: 5,
          push_after_run_outputs: 1,
          output_max_upload_gb: 2,
        },
        budget: { maxUSD: 1, deadline: "1h" },
      },
    },
  },
  {
    id: "wl_preview_cleanup_retrying",
    status: "failed",
    spent_usd: 1.25,
    created_at: "2026-08-08T11:00:00Z",
    started_at: "2026-08-08T11:01:00Z",
    finished_at: "2026-08-08T11:30:00Z",
    burst_id: "burst_preview_retrying",
    cluster_id: "acme-prod-us-east",
    template: { id: "container-job", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview_sam" },
    authorization: {
      requested_namespace: "default",
      granted_namespace: "default",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-08T11:00:02Z",
    },
    cleanup: {
      state: "retrying",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      reason: "provider API timeout during deletion",
      requested_at: "2026-08-08T11:30:05Z",
      updated_at: "2026-08-08T12:15:00Z",
      provider_created_at: "2026-08-08T11:00:45Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "gpu-job-retrying-cleanup", namespace: "default" },
      spec: {
        image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
        size: "large",
        gpu: { kind: "a100", count: 1, reliability: "reliable" },
        budget: { maxUSD: 3, deadline: "2h" },
      },
    },
  },
  {
    id: "wl_preview_manual_attention",
    status: "failed",
    spent_usd: 2.5,
    created_at: "2026-08-07T08:00:00Z",
    started_at: "2026-08-07T08:01:00Z",
    finished_at: "2026-08-07T08:45:00Z",
    burst_id: "burst_preview_attention",
    cluster_id: "acme-ml-lab",
    template: { id: "pytorch-training", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview" },
    authorization: {
      requested_namespace: "ml-team-a",
      granted_namespace: "ml-team-a",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "owner",
      decided_at: "2026-08-07T08:00:02Z",
    },
    cleanup: {
      state: "manual_attention",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      reason: "provider API refused deletion after 5 retries",
      requested_at: "2026-08-07T08:45:05Z",
      updated_at: "2026-08-07T10:30:00Z",
      provider_created_at: "2026-08-07T08:00:40Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "training-stuck-cleanup", namespace: "ml-team-a" },
      spec: {
        image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
        size: "large",
        gpu: { kind: "a100", count: 1, reliability: "reliable" },
        budget: { maxUSD: 5, deadline: "2h" },
      },
    },
  },
  {
    id: "wl_preview_terminated_durable",
    status: "succeeded",
    created_at: "2026-08-06T14:00:00Z",
    started_at: "2026-08-06T14:01:00Z",
    finished_at: "2026-08-06T14:30:00Z",
    burst_id: "burst_preview_durable",
    cluster_id: "acme-prod-us-east",
    template: { id: "container-job", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview_sam" },
    authorization: {
      requested_namespace: "default",
      granted_namespace: "default",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-06T14:00:02Z",
    },
    cleanup: {
      state: "terminated",
      provider: "linode",
      region: "us-east",
      sku: "g2-gpu-rtx4000a1-s",
      reason: "capacity freed after run completion",
      requested_at: "2026-08-06T14:30:05Z",
      updated_at: "2026-08-06T14:31:00Z",
      deleted_at: "2026-08-06T14:31:00Z",
      provider_created_at: "2026-08-06T14:00:40Z",
      durable_reap_receipt: true,
    },
    cost: {
      usd: 1.25,
      hourly_usd: 2.5,
      runtime_seconds: 1800,
      frozen_at: "2026-08-06T14:31:02Z",
      backend: "linode",
      burst_id: "burst_preview_durable",
      basis: "hourly_rate_x_runtime_to_reap_claim",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "batch-export-clean", namespace: "default" },
      spec: {
        image: "docker.io/library/busybox:1.36",
        size: "medium",
        gpu: { kind: "a100", count: 1, reliability: "reliable" },
        budget: { maxUSD: 3, deadline: "2h" },
      },
    },
  },
  {
    id: "wl_preview_no_cleanup",
    status: "failed",
    created_at: "2026-08-05T16:00:00Z",
    started_at: "2026-08-05T16:01:00Z",
    finished_at: "2026-08-05T16:10:00Z",
    burst_id: "burst_preview_no_cleanup",
    cluster_id: "acme-prod-us-east",
    template: { id: "container-job", version: 2, catalog_revision: "1" },
    submitted_by: { kind: "account", account_id: "acct_preview_sam" },
    authorization: {
      requested_namespace: "default",
      granted_namespace: "default",
      rule: "tenant-workload-namespaces",
      rule_version: "v1",
      role: "member",
      decided_at: "2026-08-05T16:00:02Z",
    },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "job-missing-cleanup", namespace: "default" },
      spec: {
        image: "docker.io/library/busybox:1.36",
        size: "small",
        budget: { maxUSD: 1, deadline: "1h" },
      },
    },
  },
  {
    id: "wl_preview_legacy",
    status: "failed",
    created_at: "2026-08-04T09:12:00Z",
    started_at: "2026-08-04T09:13:00Z",
    finished_at: "2026-08-04T09:14:30Z",
    burst_id: "",
    submitted_by: { kind: "account", account_id: "acct_preview_lena" },
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name: "index-rebuild", namespace: "default" },
      spec: {
        image: "docker.io/library/alpine:3.20",
        size: "small",
        budget: { maxUSD: 2, deadline: "1h" },
      },
    },
  },
]);

let workloads = PREVIEW_WORKLOADS;

const PREVIEW_WORKLOAD_LOGS = Object.freeze({
  wl_preview_export_success: Object.freeze([
    Object.freeze({
      pod: "model-checkpoint-export-7d9k2",
      container: "exporter",
      output: "2026-08-10T14:21:10Z [preview fixture] starting run\n2026-08-10T14:44:50Z [preview fixture] uploaded 14 objects (1.5 GiB)\n",
    }),
  ]),
  wl_preview_export_failed: Object.freeze([
    Object.freeze({
      pod: "dataset-pack-export-failed-3x2y1",
      container: "exporter",
      output: "2026-08-09T10:01:10Z [preview fixture] starting run\n2026-08-09T10:14:55Z [preview fixture] error: artifact upload quota_exceeded\n",
    }),
  ]),
  wl_preview_train: Object.freeze([
    Object.freeze({
      pod: "nightly-embeddings-preview-7d9k2",
      container: "trainer",
      output: "2026-08-12T20:18:10Z [preview fixture] loading shard 18/32\n2026-08-12T20:18:18Z [preview fixture] epoch 4 · loss 0.184\n",
    }),
  ]),
  wl_preview_cpu: Object.freeze([
    Object.freeze({
      pod: "catalog-refresh-preview-2mn8p",
      container: "main",
      output: "2026-08-11T16:04:08Z [preview fixture] refreshed 2,418 catalog entries\n2026-08-11T16:04:09Z [preview fixture] completed successfully\n",
    }),
  ]),
});

const PREVIEW_CLUSTER_POLICIES = Object.freeze({
  "tenant-acme-research": Object.freeze({
    tenant_id: "tenant-acme-research",
    role: "owner",
    changed: false,
    policy: Object.freeze({
      // Hosted is allowed so an explicit hosted pin passes the policy filter;
      // previewPlacement skips hosted rows on the automatic path regardless
      // of where this order puts them, exactly as central does.
      auto: "ordered",
      allow: Object.freeze(["acme-ml-lab", "acme-prod-us-east", "acme-disconnected-preauth", "yscale-hosted-us-east"]),
      deny: Object.freeze(["acme-edge-fra"]),
    }),
  }),
  "tenant-platform-lab": Object.freeze({
    tenant_id: "tenant-platform-lab",
    role: "viewer",
    changed: false,
    policy: Object.freeze({
      auto: "require_pin",
      allow: Object.freeze([]),
      deny: Object.freeze([]),
    }),
  }),
});

let clusterPolicies = structuredClone(PREVIEW_CLUSTER_POLICIES);

const PREVIEW_HOSTED_CAPACITY = Object.freeze({
  "tenant-acme-research": Object.freeze({
    tenant_id: "tenant-acme-research",
    role: "owner",
    status: "assigned",
  }),
  "tenant-platform-lab": Object.freeze({
    tenant_id: "tenant-platform-lab",
    role: "viewer",
    status: "not_requested",
  }),
});

let hostedCapacity = structuredClone(PREVIEW_HOSTED_CAPACITY);

const PREVIEW_CLOUD_ACCOUNTS = Object.freeze({
  "tenant-acme-research": Object.freeze({ tenant_id: "tenant-acme-research", role: "owner", account: null }),
  "tenant-platform-lab": Object.freeze({
    tenant_id: "tenant-platform-lab", role: "viewer",
    account: Object.freeze({ id: "ca_preview_viewer", provider: "linode", provider_account_id: "linode-preview-viewer", region: "us-west", cpu_image_ready: true, gpu_image_ready: false, updated_at: "2026-08-15T15:30:00Z" }),
  }),
});

let cloudAccounts = structuredClone(PREVIEW_CLOUD_ACCOUNTS);
let previewDisconnectBlocked = true;

// The durable cluster registry in the shape GET /v1/tenants/{tenant_id}/clusters
// returns. The rows cover every state the fleet page has to render honestly:
// connected_here rows with and without the optional live fields, a disconnected
// row the policy pre-authorizes, and a registered-but-never-connected row. The
// owner tenant keeps several launchable clusters so the launch form must make
// the reader choose one, while the viewer tenant has exactly one, which the
// form may preselect. live_partial is true because a single replica answering
// for the live columns is what central actually reports.
const PREVIEW_OBSERVED_AT = "2026-08-12T20:31:04Z";

const PREVIEW_CLUSTER_REGISTRY = Object.freeze({
  "tenant-acme-research": [
    {
      cluster_id: "acme-prod-us-east", name: "Prod US East", source: "console", state: "connected_here",
      registered_at: "2026-08-05T09:00:00Z", first_connected_at: "2026-08-09T04:12:00Z",
      live: {
        connected_at: "2026-08-09T04:12:00Z", last_seen: "2026-08-12T20:30:58Z", agent_version: "0.9.3", connections: 2,
        node_inventory_observed: true, node_inventory_observed_at: "2026-08-12T20:30:40Z", node_count: 12, burst_count: 2,
        pod_inventory_observed: true, pod_inventory_observed_at: "2026-08-12T20:30:58Z", pending_pods: 0,
      },
    },
    {
      cluster_id: "acme-ml-lab", name: "ML Lab", source: "console", state: "connected_here",
      registered_at: "2026-08-03T15:30:00Z", first_connected_at: "2026-08-11T22:47:31Z",
      live: {
        connected_at: "2026-08-11T22:47:31Z", last_seen: "2026-08-12T20:30:41Z", agent_version: "0.9.3", connections: 1,
        node_inventory_observed: true, node_inventory_observed_at: "2026-08-12T20:30:20Z", node_count: 8, burst_count: 1,
      },
    },
    {
      // agent_version and connections stay absent here because both are
      // optional on the contract; the policy denies this row.
      cluster_id: "acme-edge-fra", name: "Edge Frankfurt", source: "connector", state: "connected_here",
      registered_at: "2026-08-12T17:58:00Z", first_connected_at: "2026-08-12T18:02:09Z",
      live: {
        connected_at: "2026-08-12T18:02:09Z", last_seen: "2026-08-12T20:29:12Z",
        pod_inventory_observed: true, pod_inventory_observed_at: "2026-08-12T20:29:12Z", pending_pods: 3,
      },
    },
    {
      // Pre-authorized in the allow list but offline: visible, never launchable.
      cluster_id: "acme-disconnected-preauth", name: "Batch EU", source: "console", state: "disconnected",
      registered_at: "2026-07-28T11:00:00Z", first_connected_at: "2026-07-29T08:12:00Z",
      last_disconnected_at: "2026-08-11T07:40:00Z",
      live: {
        node_inventory_observed: true, node_inventory_observed_at: "2026-08-11T07:39:52Z", node_count: 0, burst_count: 0,
      },
    },
    {
      cluster_id: "acme-staging-eu", name: "Staging EU", source: "console", state: "never_connected",
      registered_at: "2026-08-12T14:05:00Z",
    },
    {
      // The platform-assigned hosted row: a virtual cluster on Yscale-run
      // shared capacity carrying this tenant's reserved namespace. source
      // "hosted" is what the console keys platform-managed behavior on — no
      // tenant connector credential, no rotate, no delete.
      cluster_id: "yscale-hosted-us-east", name: "Yscale Hosted", source: "hosted", state: "connected_here",
      hosted_namespace: "ten-acme-research",
      registered_at: "2026-08-10T06:00:00Z", first_connected_at: "2026-08-10T06:00:12Z",
      live: {
        connected_at: "2026-08-10T06:00:12Z", last_seen: "2026-08-12T20:30:47Z", agent_version: "0.9.3", connections: 1,
        node_inventory_observed: true, node_inventory_observed_at: "2026-08-12T20:30:47Z", node_count: 4, burst_count: 4,
        pod_inventory_observed: true, pod_inventory_observed_at: "2026-08-12T20:30:46Z", pending_pods: 1,
      },
    },
  ],
  "tenant-platform-lab": [
    {
      cluster_id: "platform-lab-sandbox", name: "Sandbox", source: "console", state: "connected_here",
      registered_at: "2026-08-06T09:45:00Z", first_connected_at: "2026-08-06T10:15:00Z",
      live: {
        connected_at: "2026-08-06T10:15:00Z", last_seen: "2026-08-12T20:30:52Z", agent_version: "0.9.1", connections: 1,
        pod_inventory_observed: true, pod_inventory_observed_at: "2026-08-12T20:30:52Z", pending_pods: 0,
      },
    },
  ],
});

let clusterRegistry = structuredClone(PREVIEW_CLUSTER_REGISTRY);

// The template catalog in the wire shape GET /v1/tenants/{tenant_id}/templates
// answers with. Both tenants start on the platform default catalog — the three
// built-in shapes — so a preview reader sees the same entries the launch form
// has always offered, and a save is visibly a tenant catalog replacing it.
function previewCatalogPayload(tenantId, role, { source = "default", revision = "1", templates = WORKLOAD_TEMPLATES, changed = false } = {}) {
  return {
    tenant_id: tenantId,
    catalog_revision: revision,
    source,
    role,
    changed,
    templates: templates.map((template) => ({
      id: template.id,
      version: template.version,
      mark: template.mark,
      title: template.title,
      kind: template.kind,
      description: template.description,
      nodeOnly: template.nodeOnly === true,
      disabled: template.disabled === true,
      defaults: {
        name: template.defaults.name,
        image: template.defaults.image,
        size: template.defaults.size,
        mode: template.defaults.mode,
        ...(template.defaults.mode === "gpu" ? { gpuKind: template.defaults.gpuKind || "any" } : {}),
        ...(!template.nodeOnly && template.defaults.command?.length ? { command: [...template.defaults.command] } : {}),
        ...(!template.nodeOnly && template.defaults.args?.length ? { args: [...template.defaults.args] } : {}),
        ...(!template.nodeOnly && template.defaults.env?.length ? { env: structuredClone(template.defaults.env) } : {}),
      },
    })),
  };
}

const PREVIEW_TEMPLATE_CATALOGS = Object.freeze({
  "tenant-acme-research": previewCatalogPayload("tenant-acme-research", "owner", { templates: WORKLOAD_TEMPLATES.map((template, index) => index === 0 ? {
    ...template,
    defaults: { ...template.defaults, env: [{ name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "yscale-runtime-bindings", key: "DATABASE_URL" } } }] },
  } : template) }),
  "tenant-platform-lab": previewCatalogPayload("tenant-platform-lab", "viewer"),
});

let templateCatalogs = structuredClone(PREVIEW_TEMPLATE_CATALOGS);

// The GitOps source registry in the wire shape
// GET /v1/tenants/{tenant_id}/gitops/sources answers with. Every row is
// configuration a reader typed — repository, ref, path, reconciler, target
// cluster — and nothing observed, because central observes nothing here.
//
// The owner tenant carries a source naming a cluster its fleet observation no
// longer reports. That is the state the console has to keep visible and
// editable rather than drop: a registration outliving a cluster is how a
// registry actually goes wrong, and it is unreachable in the preview if no
// fixture has it.
const PREVIEW_GITOPS_SOURCES = Object.freeze({
  "tenant-acme-research": {
    tenant_id: "tenant-acme-research",
    sources_revision: "3",
    role: "owner",
    changed: false,
    sources: [
      { id: "batch-eu", name: "Batch EU", reconciler: "flux", repo_url: "https://github.com/acme-research/batch-eu.git", path: "", ref: "main", cluster_id: "acme-retired-fra" },
      { id: "ml-platform", name: "ML platform", reconciler: "argo", repo_url: "git@github.com:acme-research/ml-platform.git", path: "envs/lab", ref: "release-2026-08", cluster_id: "acme-ml-lab" },
      { id: "prod-infra", name: "Prod infrastructure", reconciler: "flux", repo_url: "https://github.com/acme-research/infra.git", path: "clusters/prod", ref: "main", cluster_id: "acme-prod-us-east" },
    ],
  },
  "tenant-platform-lab": {
    tenant_id: "tenant-platform-lab",
    sources_revision: "1",
    role: "viewer",
    changed: false,
    sources: [
      { id: "sandbox-apps", name: "Sandbox apps", reconciler: "argo", repo_url: "https://gitlab.com/platform-lab/sandbox.git", path: "apps", ref: "main", cluster_id: "platform-lab-sandbox" },
    ],
  },
});

let gitOpsSources = structuredClone(PREVIEW_GITOPS_SOURCES);

const PREVIEW_USAGE = Object.freeze({
  "tenant-acme-research": {
    tenant_id: "tenant-acme-research",
    observed_at: "2026-08-12T20:30:00Z",
    running_bursts: 2,
    hourly_usd: 7.25,
    projected_daily_usd: 174,
    limits: { max_concurrent_bursts: 8, max_hourly_usd: 24 },
  },
  "tenant-platform-lab": {
    tenant_id: "tenant-platform-lab",
    observed_at: "2026-08-12T20:30:00Z",
    running_bursts: 0,
    hourly_usd: 0,
    projected_daily_usd: 0,
    limits: { max_concurrent_bursts: 4, max_hourly_usd: 6 },
  },
});

let usage = structuredClone(PREVIEW_USAGE);

const PREVIEW_OPERATOR_TENANTS = Object.freeze([
  { tenant_id: "tenant-acme-research", tenant_name: "Acme Research", plan: "enterprise-preview", running_bursts: 2, hourly_usd: 7.25, projected_daily_usd: 174, limits: { max_concurrent_bursts: 8, max_hourly_usd: 24 } },
  { tenant_id: "tenant-platform-lab", tenant_name: "Platform Lab", plan: "enterprise-preview", running_bursts: 0, hourly_usd: 0, projected_daily_usd: 0, limits: { max_concurrent_bursts: 4, max_hourly_usd: 6 } },
]);

const PREVIEW_CATALOG_PUBLISHERS = {
  "tenant-acme-research": [
    { id: "release-catalog", name: "Release catalog CI", created_at: "2026-08-05T09:00:00Z", updated_at: "2026-08-12T15:30:00Z" },
  ],
  "tenant-platform-lab": [
    { id: "platform-catalog", name: "Platform catalog CI", created_at: "2026-08-07T11:00:00Z", updated_at: "2026-08-07T11:00:00Z" },
  ],
};
let catalogPublishers = structuredClone(PREVIEW_CATALOG_PUBLISHERS);

// Runtime binding fixtures deliberately contain summaries only. Submitted
// values are validated and discarded by previewPutRuntimeBinding; they never
// enter module state, storage, or a later response.
const PREVIEW_RUNTIME_BINDINGS = Object.freeze({
  "tenant-acme-research": {
    tenant_id: "tenant-acme-research", role: "owner", bindings: [
      { id: "rb_preview_database", key: "DATABASE_URL", name: "Primary database", revision: "4", sync_state: "synced", created_at: "2026-08-09T08:00:00Z", updated_at: "2026-08-15T14:20:00Z" },
      { id: "rb_preview_api", key: "PARTNER_API_KEY", name: "Partner API", revision: "2", sync_state: "pending", created_at: "2026-08-11T10:00:00Z", updated_at: "2026-08-16T08:30:00Z" },
    ],
  },
  "tenant-platform-lab": {
    tenant_id: "tenant-platform-lab", role: "viewer", bindings: [
      { id: "rb_preview_webhook", key: "WEBHOOK_TOKEN", name: "Deployment webhook", revision: "1", sync_state: "synced", created_at: "2026-08-07T11:10:00Z", updated_at: "2026-08-07T11:10:00Z" },
    ],
  },
});
let runtimeBindings = structuredClone(PREVIEW_RUNTIME_BINDINGS);

// Puts the mutable fixtures back to their starting state. The preview is a
// module singleton, so without this one removal or one launch decides what the
// next reader — or the next test — sees.
export function resetDevPreview() {
  accountState = structuredClone(POPULATED_PREVIEW_ACCOUNT);
  members = PREVIEW_MEMBERS;
  workloads = PREVIEW_WORKLOADS;
  clusterPolicies = structuredClone(PREVIEW_CLUSTER_POLICIES);
  hostedCapacity = structuredClone(PREVIEW_HOSTED_CAPACITY);
	cloudAccounts = structuredClone(PREVIEW_CLOUD_ACCOUNTS);
	previewDisconnectBlocked = true;
  templateCatalogs = structuredClone(PREVIEW_TEMPLATE_CATALOGS);
  gitOpsSources = structuredClone(PREVIEW_GITOPS_SOURCES);
  clusterRegistry = structuredClone(PREVIEW_CLUSTER_REGISTRY);
  usage = structuredClone(PREVIEW_USAGE);
  catalogPublishers = structuredClone(PREVIEW_CATALOG_PUBLISHERS);
  runtimeBindings = structuredClone(PREVIEW_RUNTIME_BINDINGS);
  try {
    previewStorage()?.removeItem(PREVIEW_ACCOUNT_KEY);
  } catch {
    // See persistPreviewAccount: storage is optional for the preview.
  }
}

export function canUseDevPreview(hostname = typeof window === "undefined" ? "" : window.location.hostname) {
  return import.meta.env?.DEV === true || PREVIEW_HOSTS.has(hostname);
}

export function isDevPreviewToken(token) {
  return canUseDevPreview() && (token === PREVIEW_TOKEN || token === FIRST_WORKSPACE_PREVIEW_TOKEN);
}

export function startDevPreviewSession(mode = "populated") {
  if (!canUseDevPreview()) return null;
  resetDevPreview();
  if (mode === "first-workspace") {
    accountState = structuredClone(ZERO_TENANT_PREVIEW_ACCOUNT);
    members = [];
    workloads = [];
    persistPreviewAccount();
  }
  return {
    accessToken: mode === "first-workspace" ? FIRST_WORKSPACE_PREVIEW_TOKEN : PREVIEW_TOKEN,
    idToken: null,
    expiresAt: Date.now() + 60 * 60 * 1000,
  };
}

export function previewAccount(token) {
  restorePreviewAccount(token);
  return structuredClone(accountState);
}

function seedFirstWorkspaceFixtures(tenant) {
  const tenantId = tenant.customer_id;
  members = [{ account_id: accountState.account_id, email: accountState.email, name: accountState.name, role: "owner", created_at: new Date().toISOString() }];
  workloads = [];
  clusterPolicies = {
    [tenantId]: { tenant_id: tenantId, role: "owner", changed: false, policy: { auto: "require_pin", allow: [], deny: [] } },
  };
  hostedCapacity = { [tenantId]: { tenant_id: tenantId, role: "owner", status: "not_requested" } };
  cloudAccounts = { [tenantId]: { tenant_id: tenantId, role: "owner", account: null } };
  clusterRegistry = { [tenantId]: [] };
  templateCatalogs = { [tenantId]: previewCatalogPayload(tenantId, "owner") };
  gitOpsSources = { [tenantId]: { tenant_id: tenantId, sources_revision: "1", role: "owner", changed: false, sources: [] } };
  runtimeBindings = { [tenantId]: { tenant_id: tenantId, role: "owner", bindings: [] } };
  usage = {
    [tenantId]: {
      tenant_id: tenantId,
      observed_at: new Date().toISOString(),
      running_bursts: 0,
      hourly_usd: 0,
      projected_daily_usd: 0,
      limits: { max_concurrent_bursts: 1, max_hourly_usd: 1 },
    },
  };
}

export function previewOperatorTenants() {
  return Promise.resolve({ tenants: structuredClone(PREVIEW_OPERATOR_TENANTS) });
}

export function previewHostedRequests() { return Promise.resolve({ requests: [] }); }
export function previewHostedClusters() { return Promise.resolve({ clusters: [] }); }

export function previewCreateTenant(name, token) {
  restorePreviewAccount(token);
  const displayName = String(name || "").trim();
  if (!displayName) return Promise.reject(new ApiError(400, "Enter a workspace name."));
  if ([...displayName].length > 64 || [...displayName].some((char) => /[\u0000-\u001f\u007f-\u009f]/u.test(char))) {
    return Promise.reject(new ApiError(400, "Workspace names must be 1 to 64 characters with no control characters."));
  }
  if (accountState.tenants.length > 0) {
    return Promise.reject(new ApiError(409, "This account already belongs to a workspace."));
  }
  const slug = displayName.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 36) || "workspace";
  const tenantId = `tenant-trial-${slug}`;
  const tenant = {
    customer_id: tenantId,
    display_name: displayName,
    role: "owner",
    plan: "trial",
    limits: { max_concurrent_bursts: 1, max_hourly_usd: 1 },
    workload_namespaces: ["default"],
  };
  accountState = { ...accountState, tenants: [tenant] };
  persistPreviewAccount();
  seedFirstWorkspaceFixtures(tenant);
  return Promise.resolve(structuredClone(accountState));
}

export function previewMembers(tenantId) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (tenant && !canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Reading the member roster requires the owner or admin role in this tenant."));
  }
  return {
    members: tenant ? structuredClone(members) : [],
    next_after: "",
  };
}

export function previewAddMember(tenantId, email, role) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Changing the member roster requires the owner or admin role in this tenant."));
  }
  const allowed = tenant.role === "owner" ? ["owner", "admin", "member", "viewer"] : ["admin", "member", "viewer"];
  if (!allowed.includes(role)) return Promise.reject(new ApiError(403, "That role is not available to this account."));
  const normalized = String(email || "").trim().toLowerCase();
  const existing = members.find((member) => member.email.toLowerCase() === normalized);
  if (existing) {
    const updated = { ...existing, role };
    members = members.map((member) => (member.account_id === updated.account_id ? updated : member));
    return Promise.resolve(structuredClone(updated));
  }
  const found = PREVIEW_DIRECTORY.find((member) => member.email.toLowerCase() === normalized);
  if (!found) return Promise.reject(new ApiError(404, "That Yscale account was not found. The person must sign in once before you can add them."));
  const member = { ...found, role };
  members = [...members, member];
  return Promise.resolve(structuredClone(member));
}

export function previewUpdateMemberRole(tenantId, accountId, role) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Changing member roles requires the owner or admin role in this tenant."));
  }
  const member = members.find((item) => item.account_id === accountId);
  if (!member) return Promise.reject(new ApiError(404, "That member does not exist."));
  const allowed = tenant.role === "owner" ? ["owner", "admin", "member", "viewer"] : ["admin", "member", "viewer"];
  if (tenant.role === "admin" && member.role === "owner") {
    return Promise.reject(new ApiError(403, "Admins cannot change an owner."));
  }
  if (!allowed.includes(role)) return Promise.reject(new ApiError(403, "That role is not available to this account."));
  const updated = { ...member, role };
  members = members.map((item) => (item.account_id === accountId ? updated : item));
  return Promise.resolve(structuredClone(updated));
}

export function previewRemoveMember(tenantId, accountId) {
  if (tenantId !== accountState.tenants[0].customer_id) return false;
  const before = members.length;
  members = members.filter((member) => member.account_id !== accountId);
  return members.length < before;
}

export function previewUsage(tenantId) {
  const row = usage[tenantId];
  if (!row) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return structuredClone(row);
}

export function previewClusterPolicy(tenantId) {
  const policy = clusterPolicies[tenantId];
  if (!policy) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve(structuredClone(policy));
}

export function previewPutClusterPolicy(tenantId, policy) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Changing the cluster policy requires the owner or admin role in this tenant."));
  }
  clusterPolicies = {
    ...clusterPolicies,
    [tenantId]: {
      tenant_id: tenantId,
      role: tenant.role,
      changed: true,
      policy: {
        auto: policy.auto,
        allow: [...policy.allow],
        deny: [...policy.deny],
      },
    },
  };
  return Promise.resolve(structuredClone(clusterPolicies[tenantId]));
}

export function previewTemplateCatalog(tenantId) {
  const catalog = templateCatalogs[tenantId];
  if (!catalog) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve(structuredClone(catalog));
}

// A save replaces the catalog whole and moves the revision on, the way central
// does: the reader has to be able to see that what they are launching from is
// now this tenant's own catalog rather than the platform default.
export function previewPutTemplateCatalog(tenantId, body) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Changing the workload template catalog requires the owner or admin role in this tenant."));
  }
  const templates = body?.templates;
  if (!Array.isArray(templates)) {
    return Promise.reject(new ApiError(400, "A catalog must be a list of templates."));
  }
  const previous = templateCatalogs[tenantId];
  if (!previous) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  // Compare-and-set on the revision, as central does it. A write composed
  // against a catalog that has since been replaced is refused rather than
  // silently overwriting whatever the other manager published.
  const sent = typeof body?.catalog_revision === "number" ? String(body.catalog_revision) : String(body?.catalog_revision || "").trim();
  if (sent !== String(previous.catalog_revision)) {
    return Promise.reject(new ApiError(409, "The template catalog changed since it was read. Reload it and apply the change again."));
  }
  const revision = String(Number(previous.catalog_revision || 0) + 1);
  templateCatalogs = {
    ...templateCatalogs,
    [tenantId]: {
      tenant_id: tenantId,
      catalog_revision: revision,
      source: "tenant",
      role: tenant.role,
      changed: true,
      templates: structuredClone(templates),
    },
  };
  return Promise.resolve(structuredClone(templateCatalogs[tenantId]));
}

// Every tenant member may read the registry — the coordinates are not a secret
// and a viewer has to be able to see what their clusters are reconciled from.
export function previewGitOpsSources(tenantId) {
  const registry = gitOpsSources[tenantId];
  if (!registry) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  return Promise.resolve({ ...structuredClone(registry), role: tenant?.role || registry.role });
}

// A save replaces the registry whole and moves the revision on, the way central
// does. The compare-and-set refusal carries the registry as it stands now,
// because a console that only said "stale" would leave the reader guessing at
// what moved — and the draft they are holding is not this fixture's to discard.
export function previewPutGitOpsSources(tenantId, body) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Changing the GitOps source registry requires the owner or admin role in this tenant."));
  }
  const sources = body?.sources;
  if (!Array.isArray(sources)) {
    return Promise.reject(new ApiError(400, "A registry must be a list of sources."));
  }
  const previous = gitOpsSources[tenantId];
  if (!previous) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  const sent = typeof body?.sources_revision === "number" ? String(body.sources_revision) : String(body?.sources_revision || "").trim();
  if (sent !== String(previous.sources_revision)) {
    const conflict = new ApiError(409, "The GitOps source registry changed since it was read. Review the latest registry before applying this change.");
    conflict.payload = { ...structuredClone(previous), role: tenant.role };
    return Promise.reject(conflict);
  }
  const normalizedSources = structuredClone(sources).sort((a, b) => String(a?.id || "").localeCompare(String(b?.id || "")));
  const previousSources = structuredClone(previous.sources).sort((a, b) => String(a?.id || "").localeCompare(String(b?.id || "")));
  const unchanged = JSON.stringify(previousSources) === JSON.stringify(normalizedSources);
  const revision = unchanged ? String(previous.sources_revision) : String(Number(previous.sources_revision || 0) + 1);
  gitOpsSources = {
    ...gitOpsSources,
    [tenantId]: {
      tenant_id: tenantId,
      sources_revision: revision,
      role: tenant.role,
      changed: !unchanged,
      sources: normalizedSources,
    },
  };
  return Promise.resolve(structuredClone(gitOpsSources[tenantId]));
}

// previewAudit pages exactly as the route does: `after` is the id of the last
// row already seen, and next_after is present only while older rows remain, so
// a client that stops on an absent cursor stops in the preview too.
export function previewAudit(tenantId, { after = "", limit = 50 } = {}) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!AUDIT_ROLES.has(tenant.role)) {
    return Promise.reject(new ApiError(403, "Reading the audit journal requires the owner or admin role in this tenant."));
  }
  const size = Math.min(Math.max(Number(limit) || 50, 1), 200);
  let start = 0;
  if (after) {
    const seen = auditEvents.findIndex((event) => event.id === after);
    if (seen < 0) return Promise.reject(new ApiError(400, "after must be a cursor returned by this route."));
    start = seen + 1;
  }
  const page = auditEvents.slice(start, start + size);
  const more = start + size < auditEvents.length;
  return Promise.resolve({
    events: structuredClone(page),
    next_after: more && page.length ? page[page.length - 1].id : "",
  });
}

// The preview answers the same normalized observation the network path does, so
// a form that works here is working against the shape central sends. Eligibility
// is the policy's answer only — whether a row is connected is a separate fact
// the launch form gates on itself, exactly as it must against production.
export function previewClusters(tenantId) {
  const rows = clusterRegistry[tenantId];
  if (!rows) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  const policy = clusterPolicies[tenantId]?.policy;
  const allow = new Set(policy?.allow || []);
  const deny = new Set(policy?.deny || []);
  const withEligibility = rows.map((row) => {
    let eligible = true;
    let reason = "";
    if (deny.has(row.cluster_id)) {
      eligible = false;
      reason = "Denied by tenant cluster policy.";
    } else if (allow.size && !allow.has(row.cluster_id)) {
      eligible = false;
      reason = "Not in the tenant's allowed cluster order.";
    }
    return { ...structuredClone(row), eligible, reason };
  });
  return Promise.resolve(normalizeClusterObservation({
    tenant_id: tenantId,
    observed_at: PREVIEW_OBSERVED_AT,
    live_partial: true,
    policy: structuredClone(policy),
    role: clusterPolicies[tenantId]?.role || "",
    changed: clusterPolicies[tenantId]?.changed || "",
    clusters: withEligibility,
  }));
}

export function previewHostedCapacity(tenantId) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  const state = hostedCapacity[tenantId];
  if (!tenant || !state) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve(structuredClone({ ...state, role: tenant.role }));
}

export function previewRequestHostedCapacity(tenantId) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) {
    return Promise.reject(new ApiError(403, "Requesting shared capacity requires the owner or admin role in this tenant."));
  }
  const current = hostedCapacity[tenantId] || { tenant_id: tenantId, role: tenant.role, status: "not_requested" };
  if (current.status === "requested" || current.status === "assigned") {
    return Promise.resolve(structuredClone({ ...current, role: tenant.role }));
  }
  const requested = {
    tenant_id: tenantId,
    role: tenant.role,
    status: "requested",
    requested_at: new Date().toISOString(),
  };
  hostedCapacity = { ...hostedCapacity, [tenantId]: requested };
  return Promise.resolve(structuredClone(requested));
}

export function previewLinodeCloudAccount(tenantId) {
  const state = cloudAccounts[tenantId];
  if (!state) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve(structuredClone(state));
}

export function previewPutLinodeCloudAccount(tenantId, body) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) return Promise.reject(new ApiError(403, "Only a tenant owner or admin can connect this cloud account."));
  if (typeof body?.token !== "string" || !body.token || typeof body?.region !== "string" || !body.region) {
    return Promise.reject(new ApiError(400, "A Linode token and region are required."));
  }
  const previous = cloudAccounts[tenantId]?.account;
  const account = {
    id: previous?.id || "ca_preview_linode",
    provider: "linode",
    provider_account_id: previous?.provider_account_id || "linode-preview-owner",
    region: body.region,
    cpu_image_ready: true,
    gpu_image_ready: Boolean(body.gpu_image),
    updated_at: new Date().toISOString(),
  };
  cloudAccounts = { ...cloudAccounts, [tenantId]: { tenant_id: tenantId, role: tenant.role, account, changed: true } };
  return Promise.resolve(structuredClone(cloudAccounts[tenantId]));
}

export function previewDeleteLinodeCloudAccount(tenantId) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  if (!canManageTenant(tenant.role)) return Promise.reject(new ApiError(403, "Only a tenant owner or admin can disconnect this cloud account."));
  if (previewDisconnectBlocked) {
    previewDisconnectBlocked = false;
    return Promise.reject(new ApiError(409, "Preview refusal: a live burst or lease still holds this account.", { code: "cloud_account_in_use" }));
  }
  cloudAccounts = { ...cloudAccounts, [tenantId]: { tenant_id: tenantId, role: tenant.role, account: null, changed: true } };
  return Promise.resolve(structuredClone(cloudAccounts[tenantId]));
}

export function previewRuntimeBindings(tenantId) {
  const record = runtimeBindings[tenantId];
  if (!record) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve(structuredClone(record));
}

export function previewPutRuntimeBinding(tenantId, key, body) {
  let tenant;
  try { tenant = previewManagedTenant(tenantId, "Changing a runtime binding"); } catch (error) { return Promise.reject(error); }
  if (!/^[A-Z_][A-Z0-9_]{0,62}$/.test(key) || typeof body?.name !== "string" || !body.name.trim() || typeof body?.value !== "string" || !body.value || body.value.includes("\0")) {
    return Promise.reject(new ApiError(400, "Enter a valid key, display name, and value."));
  }
  const record = runtimeBindings[tenantId] || { tenant_id: tenantId, role: tenant.role, bindings: [] };
  const previous = record.bindings.find((binding) => binding.key === key);
  const now = new Date().toISOString();
  const binding = {
    id: previous?.id || `rb_preview_${key.toLowerCase()}`,
    key,
    name: body.name.trim(),
    revision: String(Number(previous?.revision || 0) + 1),
    sync_state: "pending",
    created_at: previous?.created_at || now,
    updated_at: now,
  };
  runtimeBindings = { ...runtimeBindings, [tenantId]: { tenant_id: tenantId, role: tenant.role, bindings: [...record.bindings.filter((row) => row.key !== key), binding] } };
  return Promise.resolve({ tenant_id: tenantId, role: tenant.role, binding: structuredClone(binding) });
}

export function previewDeleteRuntimeBinding(tenantId, key) {
  let tenant;
  try { tenant = previewManagedTenant(tenantId, "Deleting a runtime binding"); } catch (error) { return Promise.reject(error); }
  const record = runtimeBindings[tenantId];
  if (!record?.bindings.some((binding) => binding.key === key)) return Promise.reject(new ApiError(404, "That runtime binding no longer exists."));
  runtimeBindings = { ...runtimeBindings, [tenantId]: { tenant_id: tenantId, role: tenant.role, bindings: record.bindings.filter((binding) => binding.key !== key) } };
  return Promise.resolve({ key, deleted: true });
}

function previewConnectedClusters(tenantId) {
  return (clusterRegistry[tenantId] || [])
    .filter((row) => row.state === "connected_here")
    .map((row) => row.cluster_id);
}

function previewManagedTenant(tenantId, action) {
  const tenant = accountState.tenants.find((item) => item.customer_id === tenantId);
  if (!tenant) throw new ApiError(404, "That tenant does not exist for this account.");
  if (!canManageTenant(tenant.role)) {
    throw new ApiError(403, `${action} requires the owner or admin role in this tenant.`);
  }
  return tenant;
}

// Preview credentials are minted fresh on every call and returned once, the way
// central does it: nothing below stores the token, so a later registry read can
// never hand it back.
function previewConnectorToken() {
  let token = "ysc_preview";
  while (token.length < 40) token += Math.random().toString(36).slice(2);
  return token.slice(0, 40);
}

function previewPublisherToken() {
  let token = "yscp_preview_";
  while (token.length < 44) token += Math.random().toString(36).slice(2);
  return token.slice(0, 44);
}

export function previewCatalogPublishers(tenantId) {
  if (!accountState.tenants.some((tenant) => tenant.customer_id === tenantId)) return Promise.reject(new ApiError(404, "That tenant does not exist for this account."));
  return Promise.resolve({ publishers: structuredClone(catalogPublishers[tenantId] || []) });
}

export function previewCreateCatalogPublisher(tenantId, { name } = {}) {
  try { previewManagedTenant(tenantId, "Creating a catalog publisher"); } catch (error) { return Promise.reject(error); }
  const cleanName = String(name || "").trim();
  const rows = catalogPublishers[tenantId] || [];
  let id = cleanName.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48) || "catalog-publisher";
  let suffix = 2;
  const base = id;
  while (rows.some((row) => row.id === id)) id = `${base}-${suffix++}`;
  const now = new Date().toISOString();
  const publisher = { id, name: cleanName, created_at: now, updated_at: now };
  catalogPublishers = { ...catalogPublishers, [tenantId]: [...rows, publisher] };
  return Promise.resolve({ publisher: structuredClone(publisher), token: previewPublisherToken() });
}

export function previewRotateCatalogPublisher(tenantId, publisherId) {
  try { previewManagedTenant(tenantId, "Rotating a catalog publisher"); } catch (error) { return Promise.reject(error); }
  const rows = catalogPublishers[tenantId] || [];
  const current = rows.find((row) => row.id === publisherId);
  if (!current) return Promise.reject(new ApiError(404, "That publisher no longer exists."));
  const publisher = { ...current, updated_at: new Date().toISOString() };
  catalogPublishers = { ...catalogPublishers, [tenantId]: rows.map((row) => row.id === publisherId ? publisher : row) };
  return Promise.resolve({ publisher: structuredClone(publisher), token: previewPublisherToken() });
}

export function previewDeleteCatalogPublisher(tenantId, publisherId) {
  try { previewManagedTenant(tenantId, "Revoking a catalog publisher"); } catch (error) { return Promise.reject(error); }
  const rows = catalogPublishers[tenantId] || [];
  if (!rows.some((row) => row.id === publisherId)) return Promise.reject(new ApiError(404, "That publisher no longer exists."));
  catalogPublishers = { ...catalogPublishers, [tenantId]: rows.filter((row) => row.id !== publisherId) };
  return Promise.resolve(null);
}

function previewHelmCommand(verb, clusterId) {
  if (verb === "upgrade") {
    return `helm upgrade yscale-agent ./deploy/helm/yscale-agent \\\n  --namespace yscale-system --reuse-values \\\n  --set clusterID=${clusterId} \\\n  --set token="$YSCALE_CONNECTOR_TOKEN"`;
  }
  return `helm install yscale-agent ./deploy/helm/yscale-agent \\\n  --namespace yscale-system --create-namespace \\\n  --set clusterID=${clusterId} \\\n  --set token="$YSCALE_CONNECTOR_TOKEN"`;
}

function previewClusterID(rows) {
  let id;
  do {
    id = `cluster-${Math.random().toString(36).slice(2, 14)}`;
  } while (rows.some((row) => row.cluster_id === id));
  return id;
}

export function previewRegisterCluster(tenantId, { name, clusterId = "" } = {}) {
  let tenant;
  try {
    tenant = previewManagedTenant(tenantId, "Registering a cluster");
  } catch (error) {
    return Promise.reject(error);
  }
  const displayName = String(name || "").trim();
  if (!displayName) return Promise.reject(new ApiError(400, "A display name is required."));
  if ([...displayName].length > 64) return Promise.reject(new ApiError(400, "The display name must be at most 64 characters."));
  const rows = clusterRegistry[tenantId] || [];
  const id = String(clusterId || "").trim() || previewClusterID(rows);
  if (!CLUSTER_ID_RE.test(id)) return Promise.reject(new ApiError(400, "That cluster id is not usable."));
  if (rows.some((row) => row.cluster_id === id)) {
    return Promise.reject(new ApiError(409, "A cluster with that id is already registered for this tenant.", { code: "cluster_exists" }));
  }
  const row = { cluster_id: id, name: displayName, source: "registered", state: "never_connected", registered_at: new Date().toISOString() };
  clusterRegistry = { ...clusterRegistry, [tenantId]: [...rows, row] };
  const token = previewConnectorToken();
  return Promise.resolve({
    tenant_id: tenantId,
    cluster: structuredClone(row),
    connector_token: token,
    helm_install: previewHelmCommand("install", id),
    role: tenant.role,
  });
}

export function previewRotateClusterCredential(tenantId, clusterId) {
  let tenant;
  try {
    tenant = previewManagedTenant(tenantId, "Rotating a connector credential");
  } catch (error) {
    return Promise.reject(error);
  }
  const rows = clusterRegistry[tenantId] || [];
  const row = rows.find((item) => item.cluster_id === clusterId);
  if (!row) return Promise.reject(new ApiError(404, "That cluster is not registered for this tenant."));
  // Rotation revokes the credential the open connector is holding, so a
  // connected row drops to disconnected until the upgrade command runs.
  let rotated = structuredClone(row);
  if (row.state === "connected_here") {
    rotated = { ...rotated, state: "disconnected", last_disconnected_at: new Date().toISOString() };
    delete rotated.live;
  }
  clusterRegistry = { ...clusterRegistry, [tenantId]: rows.map((item) => (item.cluster_id === clusterId ? rotated : item)) };
  const token = previewConnectorToken();
  return Promise.resolve({
    tenant_id: tenantId,
    cluster: structuredClone(rotated),
    connector_token: token,
    helm_install: previewHelmCommand("upgrade", clusterId),
    role: tenant.role,
  });
}

export function previewDeleteCluster(tenantId, clusterId) {
  try {
    previewManagedTenant(tenantId, "Deleting a cluster");
  } catch (error) {
    return Promise.reject(error);
  }
  const rows = clusterRegistry[tenantId] || [];
  if (!rows.some((item) => item.cluster_id === clusterId)) {
    return Promise.reject(new ApiError(404, "That cluster is not registered for this tenant."));
  }
  const policy = clusterPolicies[tenantId]?.policy;
  if ([...(policy?.allow || []), ...(policy?.deny || [])].includes(clusterId)) {
    return Promise.reject(new ApiError(409, "That cluster is named in this tenant's placement policy. Remove it from the policy first.", { code: "policy_conflict" }));
  }
  clusterRegistry = { ...clusterRegistry, [tenantId]: rows.filter((item) => item.cluster_id !== clusterId) };
  return Promise.resolve({ deleted: true, tenant_id: tenantId, cluster_id: clusterId });
}

function previewEligibleClusterIds(tenantId) {
  const connected = new Set(previewConnectedClusters(tenantId));
  const policy = clusterPolicies[tenantId]?.policy || { allow: [], deny: [], auto: "require_pin" };
  const deny = new Set(policy.deny || []);
  if (policy.allow?.length) {
    return policy.allow.filter((id) => connected.has(id) && !deny.has(id));
  }
  return [...connected].filter((id) => !deny.has(id)).sort();
}

// Central attaches spent_usd, gpu_util_percent, and last_heartbeat_at to a
// tenant's live burst records. The preview mirrors that on the running GPU
// fixture so the live-spend panel, the freshness label, and the utilization
// chip have real inputs to exercise — including a heartbeat that always looks
// recent from the reader's clock, so the freshness label is not stuck at
// "12 days ago" whenever the fixture predates them.
function withLivePreviewSignals(workload) {
  if (workload?.id !== "wl_preview_train" || isTerminal(workload?.status)) return workload;
  return {
    ...workload,
    spent_usd: 2.34,
    gpu_util_percent: 87,
    last_heartbeat_at: new Date().toISOString(),
  };
}

export function previewWorkloads(tenantId) {
  if (tenantId !== accountState.tenants[0].customer_id) return { workloads: [] };
  return { workloads: structuredClone(workloads).map(withLivePreviewSignals) };
}

export function previewWorkload(tenantId, workloadId) {
  const workload = previewWorkloads(tenantId).workloads.find((item) => item.id === workloadId);
  if (!workload) return null;
  return workload;
}

export function previewWorkloadLogs(tenantId, workloadId, tail = 200) {
  if (!previewWorkload(tenantId, workloadId)) return null;
  const count = Number.isInteger(tail) && tail >= 1 && tail <= 1000 ? tail : 200;
  const streams = (PREVIEW_WORKLOAD_LOGS[workloadId] || []).map((stream) => {
    const lines = stream.output.replace(/\n$/, "").split("\n");
    const output = lines.slice(-count).join("\n");
    return { ...stream, output: output ? `${output}\n` : "" };
  });
  return {
    observed_at: new Date().toISOString(),
    streams: structuredClone(streams),
    truncated: false,
    fixture: true,
  };
}

// The registry rows central marks platform-managed, by id. Automatic placement
// must skip them no matter where the policy's allow order puts them.
function previewHostedClusterIds(tenantId) {
  return new Set((clusterRegistry[tenantId] || [])
    .filter((row) => row.source === HOSTED_CLUSTER_SOURCE)
    .map((row) => row.cluster_id));
}

function previewPlacement(tenantId, clusterId) {
  const policy = clusterPolicies[tenantId]?.policy || { allow: [], deny: [], auto: "require_pin" };
  const eligible = previewEligibleClusterIds(tenantId);
  let placedClusterId = clusterId || "";
  if (!placedClusterId) {
    if (policy.auto !== "ordered") {
      throw new ApiError(400, "A target cluster is required on every workload submit.");
    }
    // Central never chooses hosted capacity on the tenant's behalf: hosted
    // rows are namespace-pinned, so automatic placement skips them, and a
    // policy whose only eligible rows are hosted answers central's 409.
    const hosted = previewHostedClusterIds(tenantId);
    placedClusterId = eligible.find((id) => !hosted.has(id)) || "";
    if (!placedClusterId) {
      if (eligible.length) {
        throw new ApiError(409, "hosted clusters require X-Cluster-ID so central can enforce their reserved namespace");
      }
      throw new ApiError(400, "No eligible connected cluster is available for automatic placement.");
    }
  }
  if (!previewConnectedClusters(tenantId).includes(placedClusterId)) {
    throw new ApiError(400, "That cluster is not connected for this tenant.");
  }
  if (!eligible.includes(placedClusterId)) {
    throw new ApiError(400, "That cluster is not eligible for this tenant's policy.");
  }
  return {
    clusterId: placedClusterId,
    placement: {
      ...(clusterId ? { requested_cluster_id: clusterId } : {}),
      granted_cluster_id: placedClusterId,
      mode: clusterId ? "pinned" : "auto",
    },
  };
}

// The template selection arrives as the X-Template-ID / X-Template-Version pair
// and is provenance, so the preview refuses the same submissions central would:
// a template this tenant's catalog does not carry, one it has disabled, and one
// at a version the catalog has already moved past.
function previewTemplateSelection(tenantId, selection) {
  if (!selection?.id) return null;
  const catalog = templateCatalogs[tenantId];
  const entry = catalog?.templates.find((template) => template.id === selection.id);
  if (!entry) throw new ApiError(400, "That template is not in this tenant's catalog.");
  if (entry.disabled) throw new ApiError(400, "That template is disabled in this tenant's catalog.");
  if (Number(selection.version) !== entry.version) {
    throw new ApiError(409, `That template is now at version ${entry.version}. Reload the catalog and submit again.`);
  }
  return { id: entry.id, version: entry.version, catalog_revision: catalog.catalog_revision };
}

export const DEV_LAUNCH_TOKEN = "yscale-dev-preview-placement-token";

export function previewPlacementPreview(tenantId, yaml, clusterId) {
  if (!accountState.tenants.some((tenant) => tenant.customer_id === tenantId)) return null;
  const resolved = previewPlacement(tenantId, clusterId);
  const gpuKind = yaml.match(new RegExp(`^\\s*kind:\\s*(${ACCEPTED_GPU_KINDS.join("|")})\\s*$`, "m"))?.[1] || "";
  const gpuCount = Number(yaml.match(/^\s*count:\s*([0-9]+)\s*$/m)?.[1] || 1);
  const maxUSD = Number(yaml.match(/^\s*maxUSD:\s*([0-9.]+)\s*$/m)?.[1] || 0);
  const deadline = yaml.match(/^\s*deadline:\s*([^\s]+)\s*$/m)?.[1] || "";
  const deadlineMatch = deadline.match(/^(\d+)([hms])$/);
  const deadlineSeconds = deadlineMatch
    ? Number(deadlineMatch[1]) * ({ h: 3600, m: 60, s: 1 }[deadlineMatch[2]])
    : 0;
  const backend = yaml.match(/^\s*backend:\s*([^\s]+)\s*$/m)?.[1] || "auto";
  const region = yaml.match(/^\s*region:\s*"([^"]+)"/m)?.[1] || "";
  const size = yaml.match(/^\s*size:\s*([^\s]+)\s*$/m)?.[1] || "small";
  const now = new Date().toISOString();
  const expires = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  return {
    status: "ok",
    placement: {
      version: 1,
      requested_cluster_id: clusterId || null,
      granted_cluster_id: resolved.clusterId,
      cluster_mode: clusterId ? "pinned" : "auto",
      requested: {
        gpu_kind: gpuKind,
        gpu_count: gpuKind ? gpuCount : 0,
        gpu_reliability: gpuKind ? "reliable" : "",
        cpu_millis: size === "large" ? 8_000 : 2_000,
        memory_mb: size === "large" ? 32_768 : 4_096,
      },
      constraints: {
        backend_pin: backend === "auto" ? "" : backend,
        region_pin: region,
        account_mode: "platform",
        account_eligible: true,
        max_charge_micro_usd: Math.round(maxUSD * 1_000_000),
        deadline_seconds: deadlineSeconds,
      },
      candidates: [
        {
          provider: backend === "auto" ? "linode" : backend,
          region: region || "us-east",
          sku: gpuKind ? `gpu-${gpuKind}-x${gpuCount}` : `cpu-${size}`,
          gpu_kind: gpuKind,
          gpu_count: gpuKind ? gpuCount : 0,
          hourly_micro_usd: gpuKind ? 2_500_000 : 100_000,
          selected: true,
        },
      ],
      selected: {
        provider: backend === "auto" ? "linode" : backend,
        region: region || "us-east",
        sku: gpuKind ? `gpu-${gpuKind}-x${gpuCount}` : `cpu-${size}`,
        gpu_kind: gpuKind,
        gpu_count: gpuKind ? gpuCount : 0,
        hourly_micro_usd: gpuKind ? 2_500_000 : 100_000,
        maximum_duration_seconds: deadlineSeconds,
        maximum_charge_micro_usd: Math.round(maxUSD * 1_000_000),
      },
      pricing_version: 1,
      candidate_set_version: "v1",
      availability_confidence: "create_time_only",
      quote_id: "quote_0123456789ab",
      issued_at: now,
      expires_at: expires,
      digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    launch_token: DEV_LAUNCH_TOKEN,
  };
}

function previewYAMLStringArray(yaml, field) {
  const encoded = yaml.match(new RegExp(`^\\s*${field}:\\s*(\\[[^\\n]*\\])\\s*$`, "m"))?.[1];
  if (!encoded) return [];
  try {
    const parsed = JSON.parse(encoded);
    return Array.isArray(parsed) && parsed.every((token) => typeof token === "string") ? parsed : [];
  } catch {
    return [];
  }
}

// clusterId arrives as the X-Cluster-ID header on the real path, never in the
// YAML, so the preview checks it the way the seam and central would: exactly
// one cluster, and one this tenant is actually observed to have connected.
export function previewCreateWorkload(tenantId, yaml, clusterId, template = null, placementToken = null) {
  if (tenantId !== accountState.tenants[0].customer_id) return null;
  if (!placementToken || placementToken !== DEV_LAUNCH_TOKEN) {
    throw new ApiError(400, "A valid placement token is required. Preview placement first.");
  }
  const resolved = previewPlacement(tenantId, clusterId);
  const selection = previewTemplateSelection(tenantId, template);
  const name = yaml.match(/^\s*name:\s*"([^"]+)"/m)?.[1] || "preview-workload";
  // The record has to show the namespace that was actually submitted, or the
  // preview cannot show a launch landing anywhere but "default".
  const namespace = yaml.match(/^\s*namespace:\s*"([^"]+)"/m)?.[1] || "default";
  // Hosted capacity admits a workload only into the tenant's reserved
  // namespace; central refuses the mismatch as a 409 conflict rather than
  // relocating the run, so the preview does too.
  const hostedRefusal = hostedNamespaceError(
    normalizeClusterRow((clusterRegistry[tenantId] || []).find((row) => row.cluster_id === resolved.clusterId)),
    namespace,
  );
  if (hostedRefusal) throw new ApiError(409, hostedRefusal);
  const image = yaml.match(/^\s*image:\s*"([^"]+)"/m)?.[1] || "";
  const size = yaml.match(/^\s*size:\s*([^\s]+)\s*$/m)?.[1] || "small";
  const backend = yaml.match(/^\s*backend:\s*([^\s]+)\s*$/m)?.[1] || "";
  const region = yaml.match(/^\s*region:\s*"([^"]+)"/m)?.[1] || "";
  const nodeOnly = /^\s*nodeOnly:\s*true\s*$/m.test(yaml);
  const command = nodeOnly ? [] : previewYAMLStringArray(yaml, "command");
  const args = nodeOnly ? [] : previewYAMLStringArray(yaml, "args");
  // Anchored to the catalog so the preview cannot silently drop the gpu block
  // for a kind the launch form is allowed to emit — and never matches the
  // envelope's own `kind: Workload`.
  const gpuKind = yaml.match(new RegExp(`^\\s*kind:\\s*(${ACCEPTED_GPU_KINDS.join("|")})\\s*$`, "m"))?.[1] || "";
  const gpuCount = Number(yaml.match(/^\s*count:\s*([0-9]+)\s*$/m)?.[1] || 1);
  // Mirror central admission: omitted means reliable, while every explicit
  // mode other than reliable is refused. Silently rewriting spot/any would let
  // the preview claim a submission succeeded that production rejects.
  const reliability = yaml.match(/^\s*reliability:\s*([^\s#]+)\s*$/m)?.[1] || LAUNCH_RELIABILITY;
  if (gpuKind && reliability !== LAUNCH_RELIABILITY) {
    throw new ApiError(400, "Only reliable on-demand capacity can be launched; spot and any are not supported.");
  }
  const maxUSD = Number(yaml.match(/^\s*maxUSD:\s*([0-9.]+)\s*$/m)?.[1] || 0);
  const deadline = yaml.match(/^\s*deadline:\s*([^\s]+)\s*$/m)?.[1] || "";
  const hasObjectInput = /^\s*storage:\s*$/m.test(yaml) && /^\s*cache:\s*$/m.test(yaml);
  const hasObjectOutput = /^\s*storage:\s*$/m.test(yaml) && /^\s*artifacts:\s*$/m.test(yaml);
  const inputSizeHintGB = Number(yaml.match(/^\s*sizeHintGB:\s*([0-9]+)\s*$/m)?.[1] || 0);
  const outputMaxUploadGB = Number(yaml.match(/^\s*maxSizeGB:\s*([0-9]+)\s*$/m)?.[1] || 0);
  const data = hasObjectInput || hasObjectOutput ? {
    pull_before_run_inputs: hasObjectInput ? 1 : 0,
    ...(inputSizeHintGB ? { input_size_hint_gb: inputSizeHintGB } : {}),
    push_after_run_outputs: hasObjectOutput ? 1 : 0,
    ...(outputMaxUploadGB ? { output_max_upload_gb: outputMaxUploadGB } : {}),
  } : null;
  const id = `wl_preview_${Date.now().toString(36)}`;
  workloads = [{
    id,
    status: "provisioning",
    created_at: new Date().toISOString(),
    started_at: null,
    finished_at: null,
    burst_id: "",
    cluster_id: resolved.clusterId,
    placement: resolved.placement,
    ...(selection ? { template: selection } : {}),
    spec: {
      apiVersion: "yscale.sh/v1",
      kind: "Workload",
      metadata: { name, namespace },
      spec: {
        image,
        size,
        nodeOnly,
        ...(command.length ? { command: [...command] } : {}),
        ...(args.length ? { args: [...args] } : {}),
        ...(backend ? { backend } : {}),
        ...(region ? { region } : {}),
        ...(gpuKind ? { gpu: { kind: gpuKind, count: gpuCount, reliability: LAUNCH_RELIABILITY } } : {}),
        ...(data ? { data } : {}),
        budget: { maxUSD, deadline },
      },
    },
  }, ...workloads];
  return { id, status: "provisioning" };
}

export function previewRetryWorkload(tenantId, workloadId) {
  if (tenantId !== accountState.tenants[0].customer_id) return null;
  const source = workloads.find((item) => item.id === workloadId);
  if (!source) return null;
  if (!isTerminal(source.status)) {
    throw new ApiError(409, "This workload must be terminal before retry can start a new run.");
  }
  const requested = source.placement?.mode === "pinned" ? source.placement.requested_cluster_id : "";
  const resolved = previewPlacement(tenantId, requested);
  const id = `wl_preview_retry_${Date.now().toString(36)}`;
  workloads = [{
    ...structuredClone(source),
    id,
    retry_of: source.id,
    status: "provisioning",
    created_at: new Date().toISOString(),
    started_at: null,
    finished_at: null,
    burst_id: "",
    cluster_id: resolved.clusterId,
    placement: resolved.placement,
    cost: undefined,
    submitted_by: { kind: "account", account_id: accountState.account_id },
    authorization: {
      ...structuredClone(source.authorization || {}),
      role: accountState.tenants[0].role,
      decided_at: new Date().toISOString(),
    },
  }, ...workloads];
  return { id, status: "provisioning" };
}

export function previewCancelWorkload(tenantId, workloadId) {
  if (tenantId !== accountState.tenants[0].customer_id) return false;
  if (!workloads.some((item) => item.id === workloadId)) return false;
  workloads = workloads.map((item) => (
    item.id === workloadId ? { ...item, status: "cancelled", finished_at: new Date().toISOString() } : item
  ));
  return true;
}
