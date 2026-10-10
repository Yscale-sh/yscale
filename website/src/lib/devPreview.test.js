import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ApiError } from "./apiError.js";
import { clusterInventoryTotals, hostedCluster, isHostedCluster, launchableClusters, preselectedClusterId } from "./clusters.js";
import { PENDING_POD_ORIGIN, controllerTriggeredWorkloads, workloadNamespaces } from "./consoleData.js";
import {
  canUseDevPreview,
  DEV_LAUNCH_TOKEN,
  previewAccount,
  previewAudit,
  previewAddMember,
  previewCancelWorkload,
  previewClusters,
  previewClusterPolicy,
  previewCreateTenant,
  previewCreateWorkload,
  previewDeleteCluster,
  previewHostedCapacity,
  previewMembers,
  previewPlacementPreview,
  previewPutClusterPolicy,
  previewRegisterCluster,
  previewRemoveMember,
  previewRequestHostedCapacity,
  previewRetryWorkload,
  previewRotateClusterCredential,
  previewUpdateMemberRole,
  previewUsage,
  previewWorkloads,
  previewWorkloadLogs,
  resetDevPreview,
  startDevPreviewSession,
} from "./devPreview.js";
import { initialWorkloadForm, workloadYAML, WORKLOAD_TEMPLATES } from "./workloadTemplates.js";

// The preview is a module singleton. Without this, the roster one test removes
// from is the roster the next test starts with, and the order the runner picks
// decides whether they pass.
beforeEach(resetDevPreview);

// One of the owner tenant's observed eligible clusters, so a preview launch has the
// target the real submit carries in X-Cluster-ID.
const PREVIEW_CLUSTER = "acme-prod-us-east";
const DENIED_CLUSTER = "acme-edge-fra";
const TOK = DEV_LAUNCH_TOKEN;
// The owner tenant's platform-managed row and the namespace reserved on it.
const HOSTED_CLUSTER = "yscale-hosted-us-east";
const HOSTED_NAMESPACE = "ten-acme-research";

test("dev preview is limited to local and dev hosts", () => {
  assert.equal(canUseDevPreview("localhost"), true);
  assert.equal(canUseDevPreview("127.0.0.1"), true);
  assert.equal(canUseDevPreview("yscale-dev.yscale.sh"), true);
  assert.equal(canUseDevPreview("yscale.sh"), false);
  assert.equal(canUseDevPreview("www.yscale.sh"), false);
  assert.equal(canUseDevPreview("yscale-dev.yscale.sh.attacker.test"), false);
});

test("first-workspace preview starts with no tenants and create mutates the account envelope", async () => {
  const previousWindow = globalThis.window;
  globalThis.window = { location: { hostname: "localhost" } };
  try {
    assert.ok(startDevPreviewSession("first-workspace"));
    assert.deepEqual(previewAccount().tenants, []);

    const envelope = await previewCreateTenant(" Trial Lab ");
    assert.equal(envelope.tenants.length, 1);
    assert.equal(envelope.tenants[0].display_name, "Trial Lab");
    assert.equal(envelope.tenants[0].role, "owner");
    assert.deepEqual(envelope.tenants[0].limits, { max_concurrent_bursts: 1, max_hourly_usd: 1 });
    assert.deepEqual(previewAccount(), envelope);
    assert.deepEqual(previewWorkloads(envelope.tenants[0].customer_id), { workloads: [] });

    const clusters = await previewClusters(envelope.tenants[0].customer_id);
    assert.deepEqual(clusters.clusters, []);
    const initialCapacity = await previewHostedCapacity(envelope.tenants[0].customer_id);
    assert.equal(initialCapacity.status, "not_requested");
    const requested = await previewRequestHostedCapacity(envelope.tenants[0].customer_id);
    assert.equal(requested.status, "requested");
    assert.ok(requested.requested_at);
    const duplicate = await previewRequestHostedCapacity(envelope.tenants[0].customer_id);
    assert.equal(duplicate.requested_at, requested.requested_at, "a duplicate keeps the original queue time");
    await assert.rejects(previewCreateTenant("Another"), (error) => error.status === 409);
  } finally {
    globalThis.window = previousWindow;
  }
});

test("the preview journal pages with a cursor and stops when it ends", async () => {
  const first = await previewAudit("tenant-acme-research", { limit: 3 });
  assert.equal(first.events.length, 3);
  assert.ok(first.next_after, "a truncated page must carry a cursor");
  assert.equal(first.next_after, first.events[2].id);

  const second = await previewAudit("tenant-acme-research", { after: first.next_after, limit: 3 });
  assert.equal(second.events.length, 3);
  assert.equal(second.events.some((event) => first.events.some((seen) => seen.id === event.id)), false);

  const last = await previewAudit("tenant-acme-research", { after: second.next_after, limit: 100 });
  assert.equal(last.next_after, "", "the final page must not carry a cursor");
  assert.ok(last.events.length > 0);

  // Every row is in the shape the real route renders.
  for (const event of [...first.events, ...second.events, ...last.events]) {
    assert.match(event.id, /^aud_[0-9a-f]{32}$/);
    assert.ok(["accepted", "denied", "observed"].includes(event.outcome), `unknown outcome ${event.outcome}`);
    assert.ok(event.action.includes("."), "an action is namespaced by the thing it acts on");
    assert.ok(event.actor?.kind);
    assert.ok(event.detail);
  }
});

test("the preview journal refuses the roles central refuses", async () => {
  await assert.rejects(previewAudit("tenant-platform-lab"), (error) => error.status === 403);
  await assert.rejects(previewAudit("tenant-does-not-exist"), (error) => error.status === 404);
  await assert.rejects(
    previewAudit("tenant-acme-research", { after: "aud_ffffffffffffffffffffffffffffffff" }),
    (error) => error.status === 400,
  );
});

// The console renders the journal in the order the route hands it back and
// pages with the last id it saw, so the fixture has to agree with itself: a row
// that is newer must also sort later by id, or "load older" walks backwards.
test("the preview journal is newest first and its cursors agree with its clock", async () => {
  const { events } = await previewAudit("tenant-acme-research", { limit: 200 });
  assert.ok(events.length > 1);
  for (let i = 1; i < events.length; i += 1) {
    const previous = events[i - 1];
    const current = events[i];
    assert.ok(
      new Date(previous.at) >= new Date(current.at),
      `${previous.id} (${previous.at}) is listed above the newer ${current.id} (${current.at})`,
    );
    assert.ok(previous.id > current.id, `${previous.id} does not sort above ${current.id}`);
  }
});

test("the preview roster removal is scoped to the tenant it names", () => {
  const before = previewMembers("tenant-acme-research").members.length;
  assert.equal(previewRemoveMember("tenant-platform-lab", "acct_preview_sam"), false);
  assert.equal(previewRemoveMember("tenant-acme-research", "acct_not_a_member"), false);
  assert.equal(previewRemoveMember("tenant-acme-research", "acct_preview_sam"), true);
  assert.equal(previewMembers("tenant-acme-research").members.length, before - 1);
});

test("the preview roster can add an existing account and edit roles", async () => {
  const added = await previewAddMember("tenant-acme-research", "mina@example.test", "member");
  assert.equal(added.account_id, "acct_preview_mina");
  assert.equal(added.role, "member");
  assert.ok(previewMembers("tenant-acme-research").members.some((member) => member.account_id === "acct_preview_mina"));

  const updated = await previewUpdateMemberRole("tenant-acme-research", "acct_preview_mina", "admin");
  assert.equal(updated.role, "admin");
  assert.equal(previewMembers("tenant-acme-research").members.find((member) => member.account_id === "acct_preview_mina").role, "admin");

  await assert.rejects(previewAddMember("tenant-acme-research", "unknown@example.test", "member"), (error) => error.status === 404);
  await assert.rejects(previewUpdateMemberRole("tenant-platform-lab", "acct_preview_mina", "viewer"), (error) => error.status === 403);
});

test("the preview live usage is deterministic and available to viewer tenants", async () => {
  const owner = await previewUsage("tenant-acme-research");
  const viewer = await previewUsage("tenant-platform-lab");
  assert.equal(owner.running_bursts, 2);
  assert.equal(owner.hourly_usd, 7.25);
  assert.equal(viewer.running_bursts, 0);
  assert.equal(viewer.limits.max_hourly_usd, 6);
});

// The preview is where the launch form's namespace select is walked without a
// backend, so the two tenants have to disagree: one with a real choice, one
// whose single namespace is not "default". A form that opened on a hardcoded
// namespace passes against the owner tenant and fails against the viewer one.
test("the preview tenants carry distinct authorized namespaces", () => {
  const [owner, viewer] = previewAccount().tenants;

  assert.equal(owner.role, "owner");
  assert.deepEqual(workloadNamespaces(owner), ["default", "ml-team-a", HOSTED_NAMESPACE]);
  assert.ok(workloadNamespaces(owner).length > 1, "the owner tenant must offer a real choice");

  assert.equal(viewer.role, "viewer");
  assert.deepEqual(workloadNamespaces(viewer), ["platform-lab"]);
  assert.equal(
    workloadNamespaces(viewer).includes(workloadNamespaces(owner)[0]),
    false,
    "the viewer tenant must not share the owner tenant's first namespace",
  );

  // The seeded record and the journal row that admitted it agree with the list.
  const record = previewWorkloads(owner.customer_id).workloads.find((item) => item.id === "wl_preview_train");
  assert.equal(record.spec.metadata.namespace, "ml-team-a");
  assert.equal(record.authorization.granted_namespace, "ml-team-a");
  assert.ok(workloadNamespaces(owner).includes(record.spec.metadata.namespace));
  assert.equal(record.placement.receipt.version, 1);
  assert.equal(record.placement.receipt.selected.provider, "linode");
  assert.equal(record.placement.receipt.selected.gpu_kind, record.spec.spec.gpu.kind);
  assert.equal(record.placement.receipt.selected.gpu_count, record.spec.spec.gpu.count);
  assert.match(record.placement.receipt.quote_id, /^quote_[0-9a-f]{12}$/);
  assert.match(record.placement.receipt.digest, /^[0-9a-f]{64}$/);
});

test("preview workload logs are explicit fixtures scoped to an owned record", () => {
  const snapshot = previewWorkloadLogs("tenant-acme-research", "wl_preview_train", 1);
  assert.equal(snapshot.fixture, true);
  assert.equal(snapshot.streams.length, 1);
  assert.match(snapshot.streams[0].output, /\[preview fixture\]/);
  assert.equal(snapshot.streams[0].output.trim().split("\n").length, 1);
  assert.equal(previewWorkloadLogs("tenant-platform-lab", "wl_preview_train"), null);
  assert.equal(previewWorkloadLogs("tenant-acme-research", "wl_missing"), null);
});

test("a preview launch is recorded in the namespace it submitted", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const namespaces = workloadNamespaces(owner);
  const values = { ...initialWorkloadForm(template, namespaces), namespace: "ml-team-a" };

  assert.ok(previewCreateWorkload(owner.customer_id, workloadYAML(template, values), PREVIEW_CLUSTER, null, TOK)?.id);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].spec.metadata.namespace, "ml-team-a");

  // The form's own default lands where central would have put it anyway.
  previewCreateWorkload(owner.customer_id, workloadYAML(template, initialWorkloadForm(template, namespaces)), PREVIEW_CLUSTER, null, TOK);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].spec.metadata.namespace, namespaces[0]);
});

// The selection arrives as the X-Template-ID / X-Template-Version pair and is
// what central stores as provenance, so the preview records it on the run and
// refuses the selections central would.
test("a preview launch records the template it was composed from", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));

  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, { id: template.id, version: template.version }, TOK)?.id);
  const record = previewWorkloads(owner.customer_id).workloads[0];
  assert.deepEqual(record.template, { id: template.id, version: template.version, catalog_revision: "1" });

  // A submission that names no template is still accepted; it simply carries no
  // provenance for the list to read back.
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, TOK)?.id);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].template, undefined);
});

test("a preview launch and retry preserve the exact execution recipe", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES.find((entry) => entry.id === "pytorch-training");
  const values = initialWorkloadForm(template, workloadNamespaces(owner));
  const accepted = previewCreateWorkload(owner.customer_id, workloadYAML(template, values), PREVIEW_CLUSTER, { id: template.id, version: template.version }, TOK);
  const record = previewWorkloads(owner.customer_id).workloads.find((item) => item.id === accepted.id);
  assert.deepEqual(record.spec.spec.command, template.defaults.command);
  assert.deepEqual(record.spec.spec.args, template.defaults.args);
  assert.notEqual(record.spec.spec.command, template.defaults.command, "the preview record owns its recipe arrays");

  previewCancelWorkload(owner.customer_id, accepted.id);
  const retried = previewRetryWorkload(owner.customer_id, accepted.id);
  const retryRecord = previewWorkloads(owner.customer_id).workloads.find((item) => item.id === retried.id);
  assert.deepEqual(retryRecord.spec.spec.command, template.defaults.command);
  assert.deepEqual(retryRecord.spec.spec.args, template.defaults.args);
});

test("a preview launch refuses a template the catalog cannot back", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));
  const before = previewWorkloads(owner.customer_id).workloads.length;

  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, { id: "not-in-catalog", version: 1 }, TOK),
    (error) => error.status === 400,
  );
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, { id: template.id, version: template.version + 9 }, TOK),
    (error) => error.status === 409,
  );
  assert.equal(previewWorkloads(owner.customer_id).workloads.length, before, "a refused selection starts no run");
});

test("a preview data launch keeps only the sanitized transfer receipt", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const values = {
    ...initialWorkloadForm(template, workloadNamespaces(owner)),
    dataEnabled: true,
    dataProvider: "r2",
    dataBucket: "private-bucket",
    dataPrefix: "private-prefix/",
    dataEndpoint: "https://account123.r2.cloudflarestorage.com",
    dataCredentialsSecret: "private-secret-ref",
    dataSizeHintGB: "48",
  };
  const yaml = workloadYAML(template, values);
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, TOK)?.id);
  const record = previewWorkloads(owner.customer_id).workloads[0];
  assert.deepEqual(record.spec.spec.data, {
    pull_before_run_inputs: 1,
    input_size_hint_gb: 48,
    push_after_run_outputs: 0,
  });
  const summary = JSON.stringify(record);
  for (const secret of ["private-bucket", "private-prefix", "account123", "private-secret-ref"]) {
    assert.doesNotMatch(summary, new RegExp(secret));
  }
});

test("a preview artifact launch keeps only the sanitized output receipt", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const values = {
    ...initialWorkloadForm(template, workloadNamespaces(owner)),
    outputEnabled: true,
    outputProvider: "r2",
    outputBucket: "private-results-bucket",
    outputPrefix: "private-results-prefix/",
    outputEndpoint: "https://account123.r2.cloudflarestorage.com",
    outputCredentialsSecret: "private-results-secret",
    outputMaxFiles: "250",
    outputMaxSizeGB: "12",
  };
  const yaml = workloadYAML(template, values);
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, TOK)?.id);
  const record = previewWorkloads(owner.customer_id).workloads[0];
  assert.deepEqual(record.spec.spec.data, {
    pull_before_run_inputs: 0,
    push_after_run_outputs: 1,
    output_max_upload_gb: 12,
  });
  const summary = JSON.stringify(record);
  for (const secret of ["private-results-bucket", "private-results-prefix", "account123", "private-results-secret"]) {
    assert.doesNotMatch(summary, new RegExp(secret));
  }
});

// The target rides as X-Cluster-ID, so the preview has to check it the way the
// seam and central do — otherwise a form bug that sends no cluster, or one this
// tenant has never connected, looks like a successful launch here.
test("a preview launch requires a cluster this tenant is observed to have", async () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));

  const auto = previewCreateWorkload(owner.customer_id, yaml, undefined, null, TOK);
  assert.ok(auto?.id);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].placement.mode, "auto");
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].placement.granted_cluster_id, "acme-ml-lab");
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].cluster_id, "acme-ml-lab");
  const acceptedCount = previewWorkloads(owner.customer_id).workloads.length;
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, "platform-lab-sandbox", null, TOK),
    (error) => error?.status === 400 && /not connected for this tenant/i.test(error.message),
  );
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, DENIED_CLUSTER, null, TOK),
    (error) => error?.status === 400 && /not eligible/i.test(error.message),
  );
  assert.equal(previewWorkloads(owner.customer_id).workloads.length, acceptedCount, "refused launches must not be recorded");

  const observation = await previewClusters(owner.customer_id);
  // The launch form offers only connected eligible rows, so the preview must
  // accept exactly those — an eligible-but-offline row is not a target.
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, launchableClusters(observation.clusters)[0].id, null, TOK)?.id);
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, "acme-disconnected-preauth", null, TOK),
    (error) => error?.status === 400 && /not connected for this tenant/i.test(error.message),
  );
});

// The dummy login has to be able to walk both branches of the selector — the
// owner tenant must force an explicit choice, the viewer tenant must be
// preselectable — and every durable state the fleet page renders: connected
// with and without the optional live fields, disconnected, never connected.
test("the preview registry carries realistic multi-state fleet data", async () => {
  const owner = await previewClusters("tenant-acme-research");
  assert.equal(owner.livePartial, true);
  assert.ok(owner.observedAt);
  assert.ok(owner.clusters.length > 1, "the owner tenant must make the form ask");
  assert.equal(preselectedClusterId(launchableClusters(owner.clusters)), "");
  assert.equal(owner.policy.auto, "ordered");
  assert.ok(owner.clusters.some((cluster) => cluster.id === DENIED_CLUSTER && cluster.eligible === false && /Denied/.test(cluster.reason)));
  // agent_version and connections are optional on the contract, so at least one
  // connected row has to exercise the absent case.
  assert.ok(owner.clusters.some((cluster) => cluster.state === "connected_here" && cluster.agentVersion && cluster.connections !== null));
  assert.ok(owner.clusters.some((cluster) => cluster.state === "connected_here" && !cluster.agentVersion && cluster.connections === null));
  assert.ok(owner.clusters.some((cluster) => cluster.nodeInventoryObserved && cluster.podInventoryObserved), "both inventory scopes are visible");
  assert.ok(owner.clusters.some((cluster) => cluster.nodeInventoryObserved && !cluster.podInventoryObserved), "node-only inventory is visible");
  assert.ok(owner.clusters.some((cluster) => !cluster.nodeInventoryObserved && cluster.podInventoryObserved), "pod-only inventory is visible");
  assert.ok(owner.clusters.some((cluster) => !cluster.nodeInventoryObserved && !cluster.podInventoryObserved), "rows with no inventory are visible");
  assert.deepEqual(clusterInventoryTotals(owner.clusters), {
    nodes: 24,
    nodeRows: 4,
    burstNodes: 7,
    pendingPods: 4,
    podRows: 3,
    totalRows: 6,
    newestObservedAt: "2026-08-12T20:30:58Z",
  });
  // Registered offline rows are part of the durable fleet the page must render.
  const offline = owner.clusters.find((cluster) => cluster.id === "acme-disconnected-preauth");
  assert.equal(offline.state, "disconnected");
  assert.equal(offline.eligible, true, "policy pre-authorization survives disconnection");
  assert.ok(offline.registeredAt && offline.lastDisconnectedAt);
  assert.equal(offline.nodeInventoryObserved, true, "offline rows can still carry last observed node inventory");
  assert.equal(offline.nodeCount, 0);
  assert.equal(offline.burstCount, 0);
  const fresh = owner.clusters.find((cluster) => cluster.id === "acme-staging-eu");
  assert.equal(fresh.state, "never_connected");
  assert.ok(fresh.registeredAt);
  // Every row carries a display name distinct from its id.
  assert.ok(owner.clusters.every((cluster) => cluster.name && cluster.name !== cluster.id));

  const viewer = await previewClusters("tenant-platform-lab");
  assert.equal(viewer.clusters.length, 1);
  assert.equal(preselectedClusterId(launchableClusters(viewer.clusters)), viewer.clusters[0].id);
  assert.equal(viewer.clusters[0].podInventoryObserved, true);
  assert.equal(viewer.clusters[0].pendingPods, 0);

  await assert.rejects(previewClusters("tenant-nope"), (error) => error.status === 404);
});

// The owner tenant carries exactly one hosted row so the dummy login can walk
// the platform-managed surfaces: connected, policy-eligible, launchable, and
// its reserved namespace on the tenant's own authorized list so the pinned
// value passes the form's namespace validation. The read-only second tenant
// stays without hosted capacity so the not-enabled state is reachable too.
test("the preview hosted row is connected, eligible, and namespace-authorized", async () => {
  const [owner, viewer] = previewAccount().tenants;

  const observation = await previewClusters(owner.customer_id);
  assert.equal(observation.clusters.filter(isHostedCluster).length, 1);
  const hosted = hostedCluster(observation.clusters);
  assert.equal(hosted.id, HOSTED_CLUSTER);
  assert.equal(hosted.hostedNamespace, HOSTED_NAMESPACE);
  assert.equal(hosted.eligible, true);
  assert.equal(hosted.state, "connected_here");
  assert.equal(hosted.nodeInventoryObserved, true);
  assert.equal(hosted.podInventoryObserved, true);
  assert.ok(launchableClusters(observation.clusters).some((cluster) => cluster.id === HOSTED_CLUSTER));
  assert.ok(workloadNamespaces(owner).includes(HOSTED_NAMESPACE));
  // Hosted sits last in the authorized list so the form still opens on the
  // tenant's own first namespace.
  assert.notEqual(workloadNamespaces(owner)[0], HOSTED_NAMESPACE);
  assert.equal((await previewHostedCapacity(owner.customer_id)).status, "assigned");
  assert.equal((await previewRequestHostedCapacity(owner.customer_id)).status, "assigned");

  const viewerObservation = await previewClusters(viewer.customer_id);
  assert.equal(viewerObservation.clusters.some(isHostedCluster), false);
  assert.equal(workloadNamespaces(viewer).includes(HOSTED_NAMESPACE), false);
  assert.equal((await previewHostedCapacity(viewer.customer_id)).status, "not_requested");
  await assert.rejects(previewRequestHostedCapacity(viewer.customer_id), (error) => error.status === 403);
});

// The submit path mirrors production: a hosted target admits only its
// reserved namespace, records it on the run, and a mismatch is a refusal
// rather than a silent relocation.
test("a preview hosted launch records the reserved namespace and refuses a mismatch", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const namespaces = workloadNamespaces(owner);
  const pinned = { ...initialWorkloadForm(template, namespaces), namespace: HOSTED_NAMESPACE };

  assert.ok(previewCreateWorkload(owner.customer_id, workloadYAML(template, pinned), HOSTED_CLUSTER, null, TOK)?.id);
  const record = previewWorkloads(owner.customer_id).workloads[0];
  assert.equal(record.cluster_id, HOSTED_CLUSTER);
  assert.equal(record.placement.mode, "pinned");
  assert.equal(record.spec.metadata.namespace, HOSTED_NAMESPACE);

  const before = previewWorkloads(owner.customer_id).workloads.length;
  // The form's default namespace is not the reserved one, so submitting it
  // against the hosted row is exactly the mismatch central refuses — and
  // central answers it as a 409 conflict, so the preview must too.
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, workloadYAML(template, initialWorkloadForm(template, namespaces)), HOSTED_CLUSTER, null, TOK),
    (error) => error?.status === 409 && /reserved namespace/.test(error.message),
  );
  assert.equal(previewWorkloads(owner.customer_id).workloads.length, before, "refused launches must not be recorded");

  // Tenant-owned rows keep accepting every authorized namespace unchanged.
  assert.ok(previewCreateWorkload(owner.customer_id, workloadYAML(template, initialWorkloadForm(template, namespaces)), PREVIEW_CLUSTER, null, TOK)?.id);
});

// Central never chooses hosted capacity on the tenant's behalf: the automatic
// path skips hosted rows wherever the allow order puts them, and a policy
// whose only eligible row is hosted answers 409 instead of guessing — the
// explicit pin stays the one way onto hosted capacity.
test("preview automatic placement skips hosted rows like central", async () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const namespaces = workloadNamespaces(owner);
  const yaml = workloadYAML(template, initialWorkloadForm(template, namespaces));

  // Hosted first in the allow order must not win automatic placement.
  await previewPutClusterPolicy(owner.customer_id, { auto: "ordered", allow: [HOSTED_CLUSTER, "acme-ml-lab"], deny: [] });
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, undefined, null, TOK)?.id);
  const record = previewWorkloads(owner.customer_id).workloads[0];
  assert.equal(record.placement.mode, "auto");
  assert.equal(record.placement.granted_cluster_id, "acme-ml-lab");

  // Only hosted rows eligible: the same 409 central answers, not a placement.
  await previewPutClusterPolicy(owner.customer_id, { auto: "ordered", allow: [HOSTED_CLUSTER], deny: [] });
  const before = previewWorkloads(owner.customer_id).workloads.length;
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, undefined, null, TOK),
    (error) => error?.status === 409 && /X-Cluster-ID/.test(error.message),
  );
  assert.equal(previewWorkloads(owner.customer_id).workloads.length, before, "refused launches must not be recorded");

  // The explicit pin with the reserved namespace still lands.
  const pinned = { ...initialWorkloadForm(template, namespaces), namespace: HOSTED_NAMESPACE };
  assert.ok(previewCreateWorkload(owner.customer_id, workloadYAML(template, pinned), HOSTED_CLUSTER, null, TOK)?.id);
});

test("preview register mints a durable row and a credential it never stores", async () => {
  const issued = await previewRegisterCluster("tenant-acme-research", { name: "Fresh Edge" });
  assert.equal(issued.cluster.name, "Fresh Edge");
  assert.equal(issued.cluster.state, "never_connected");
  assert.ok(issued.cluster.cluster_id);
  assert.match(issued.connector_token, /^ysc_preview/);
  assert.match(issued.helm_install, /helm install/);
  assert.match(issued.helm_install, /\$YSCALE_CONNECTOR_TOKEN/);
  assert.ok(!issued.helm_install.includes(issued.connector_token), "the command must not put the secret in shell history");
  assert.equal(issued.role, "owner");

  // The row is durable; the token is not — a later fleet read must carry the
  // registration and no trace of the revealed credential.
  const observation = await previewClusters("tenant-acme-research");
  const row = observation.clusters.find((cluster) => cluster.id === issued.cluster.cluster_id);
  assert.equal(row.state, "never_connected");
  assert.doesNotMatch(JSON.stringify(observation), new RegExp(issued.connector_token));

  // A custom id is honoured when the advanced flow sends one.
  const custom = await previewRegisterCluster("tenant-acme-research", { name: "Named Edge", clusterId: "edge-custom-01" });
  assert.equal(custom.cluster.cluster_id, "edge-custom-01");
});

test("preview register refuses what central would", async () => {
  await assert.rejects(previewRegisterCluster("tenant-nope", { name: "X" }), (error) => error.status === 404);
  await assert.rejects(previewRegisterCluster("tenant-platform-lab", { name: "X" }), (error) => {
    assert.equal(error.status, 403);
    assert.match(error.message, /owner or admin/);
    return true;
  });
  await assert.rejects(previewRegisterCluster("tenant-acme-research", { name: "   " }), (error) => error.status === 400);
  await assert.rejects(previewRegisterCluster("tenant-acme-research", { name: "X", clusterId: "-bad" }), (error) => error.status === 400);
  await assert.rejects(
    previewRegisterCluster("tenant-acme-research", { name: "X", clusterId: "acme-ml-lab" }),
    (error) => error.status === 409,
  );
  await assert.rejects(previewRegisterCluster("tenant-acme-research", { name: "x".repeat(65) }), (error) => error.status === 400);
});

test("preview rotate reveals a fresh credential and disconnects the open connector", async () => {
  const first = await previewRegisterCluster("tenant-acme-research", { name: "Rotate Target" });
  const rotated = await previewRotateClusterCredential("tenant-acme-research", first.cluster.cluster_id);
  assert.match(rotated.helm_install, /helm upgrade/);
  assert.notEqual(rotated.connector_token, first.connector_token, "rotation must mint a new token");

  // A connected cluster loses its live connection until the upgrade runs.
  const live = await previewRotateClusterCredential("tenant-acme-research", "acme-prod-us-east");
  assert.equal(live.cluster.state, "disconnected");
  const observation = await previewClusters("tenant-acme-research");
  const row = observation.clusters.find((cluster) => cluster.id === "acme-prod-us-east");
  assert.equal(row.state, "disconnected");
  assert.equal(row.connections, null);
  assert.doesNotMatch(JSON.stringify(observation), new RegExp(live.connector_token));

  await assert.rejects(previewRotateClusterCredential("tenant-acme-research", "no-such-cluster"), (error) => error.status === 404);
  await assert.rejects(previewRotateClusterCredential("tenant-platform-lab", "platform-lab-sandbox"), (error) => error.status === 403);
});

test("preview delete removes the row and surfaces the policy conflict", async () => {
  // A cluster the placement policy names cannot be deleted out from under it.
  await assert.rejects(
    previewDeleteCluster("tenant-acme-research", "acme-ml-lab"),
    (error) => error.status === 409 && /placement policy/.test(error.message),
  );

  // Staging is registered but not in the policy, so it deletes cleanly.
  const answer = await previewDeleteCluster("tenant-acme-research", "acme-staging-eu");
  assert.equal(answer.deleted, true);
  const observation = await previewClusters("tenant-acme-research");
  assert.equal(observation.clusters.some((cluster) => cluster.id === "acme-staging-eu"), false);

  await assert.rejects(previewDeleteCluster("tenant-acme-research", "acme-staging-eu"), (error) => error.status === 404);
  await assert.rejects(previewDeleteCluster("tenant-platform-lab", "platform-lab-sandbox"), (error) => error.status === 403);
});

test("the preview cluster policy is readable by all roles but mutable only by managers", async () => {
  const owner = await previewClusterPolicy("tenant-acme-research");
  const viewer = await previewClusterPolicy("tenant-platform-lab");
  assert.equal(owner.policy.auto, "ordered");
  assert.deepEqual(owner.policy.allow.slice(0, 2), ["acme-ml-lab", "acme-prod-us-east"]);
  assert.equal(viewer.policy.auto, "require_pin");

  const saved = await previewPutClusterPolicy("tenant-acme-research", { auto: "require_pin", allow: ["acme-prod-us-east"], deny: [DENIED_CLUSTER] });
  assert.equal(saved.policy.auto, "require_pin");
  await assert.rejects(
    previewPutClusterPolicy("tenant-platform-lab", { auto: "ordered", allow: ["platform-lab-sandbox"], deny: [] }),
    (error) => error.status === 403,
  );
});

test("preview require_pin refuses automatic but accepts explicit eligible targets", async () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));
  await previewPutClusterPolicy(owner.customer_id, { auto: "require_pin", allow: [PREVIEW_CLUSTER], deny: [DENIED_CLUSTER] });

  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, undefined, null, TOK),
    (error) => error?.status === 400 && /target cluster is required/i.test(error.message),
  );
  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, TOK)?.id);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].placement.mode, "pinned");
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].placement.requested_cluster_id, PREVIEW_CLUSTER);
});

// Central places on-demand capacity and nothing else. Preview must reject the
// same unsupported documents rather than rewrite them into apparent success.
test("a preview launch accepts reliable and refuses unsupported capacity", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES.find((item) => item.defaults.mode === "gpu");
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));
  assert.match(yaml, /\n {4}reliability: reliable\n/);

  assert.ok(previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, TOK)?.id);
  assert.equal(previewWorkloads(owner.customer_id).workloads[0].spec.spec.gpu.reliability, "reliable");

  for (const mode of ["spot", "any", "preemptible"]) {
    assert.throws(
      () => previewCreateWorkload(owner.customer_id, yaml.replace("reliability: reliable", `reliability: ${mode}`), PREVIEW_CLUSTER, null, TOK),
      (error) => error?.status === 400 && /Only reliable/.test(error.message),
    );
  }
});

test("preview retry creates a new run only from terminal workloads", () => {
  const [owner] = previewAccount().tenants;
  const accepted = previewRetryWorkload(owner.customer_id, "wl_preview_cpu");
  assert.ok(accepted?.id);
  const retried = previewWorkloads(owner.customer_id).workloads[0];
  assert.equal(retried.id, accepted.id);
  assert.equal(retried.retry_of, "wl_preview_cpu");
  assert.equal(retried.status, "provisioning");
  assert.equal(retried.cost, undefined);
  assert.equal(retried.cluster_id, "acme-ml-lab");
  assert.equal(retried.submitted_by.account_id, "acct_preview");
  assert.equal(retried.authorization.role, "owner");

  assert.throws(
    () => previewRetryWorkload(owner.customer_id, "wl_preview_train"),
    (error) => error?.status === 409 && /terminal/.test(error.message),
  );
});

test("the preview roster refuses the roles central refuses", async () => {
  assert.ok(previewMembers("tenant-acme-research").members.length > 0);
  await assert.rejects(previewMembers("tenant-platform-lab"), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, 403);
    assert.equal(error.isForbidden, true);
    assert.match(error.message, /owner or admin/);
    return true;
  });
});

// Proves the reset above is real rather than decorative: this test removes the
// same member again and still sees the full roster to start from.
test("each test starts from the pristine preview fixtures", async () => {
  assert.ok(previewMembers("tenant-acme-research").members.some((m) => m.account_id === "acct_preview_sam"));
  assert.equal(previewRemoveMember("tenant-acme-research", "acct_preview_sam"), true);

  const running = previewWorkloads("tenant-acme-research").workloads.find((item) => item.id === "wl_preview_train");
  assert.equal(running.status, "running");
  assert.equal(previewCancelWorkload("tenant-acme-research", "wl_preview_train"), true);
  assert.equal(previewWorkloads("tenant-acme-research").workloads[0].status, "cancelled");
  await previewAddMember("tenant-acme-research", "mina@example.test", "viewer");
  await previewDeleteCluster("tenant-acme-research", "acme-staging-eu");

  resetDevPreview();
  assert.equal(previewWorkloads("tenant-acme-research").workloads[0].status, "running");
  assert.equal(previewMembers("tenant-acme-research").members.some((m) => m.account_id === "acct_preview_mina"), false);
  const observation = await previewClusters("tenant-acme-research");
  assert.equal(observation.clusters.some((cluster) => cluster.id === "acme-staging-eu"), true);
});

// The GitOps page's live panel has nothing to render in the populated preview
// unless one record carries the origin, and the record has to read as capacity
// a controller owns — not as a training Job somebody launched from the console.
test("a preview fixture is a controller-triggered node-only capacity request", () => {
  const workloads = previewWorkloads("tenant-acme-research").workloads;
  const record = workloads.find((item) => item.id === "wl_preview_keda_burst");
  assert.ok(record, "wl_preview_keda_burst fixture must exist");
  assert.equal(record.submission_origin, PENDING_POD_ORIGIN);
  assert.equal(record.spec.spec.nodeOnly, true);
  assert.equal(record.spec.spec.image, "", "node-only capacity carries no image; the controller owns the pod");
  assert.equal(record.submitted_by.account_id, undefined, "a controller submission is not attributed to a person");
  assert.equal(record.submitted_by.kind, "cluster");

  // Every field the panel prints has to be there, or the row renders as an
  // unrecorded placement the fixture never meant to demonstrate.
  assert.ok(record.burst_id, "the row shows a burst receipt when one is present");
  assert.equal(record.placement.granted_cluster_id, "acme-ml-lab");
  assert.equal(record.placement.mode, "pinned");
  assert.equal(record.spec.metadata.name, "inference-pool-node");
  assert.ok(Number.isFinite(new Date(record.created_at).getTime()), "created_at must be readable as a request time");

  // The exact-origin filter is what the page calls, so the fixture is only
  // useful if that filter — not a shape guess — finds it and nothing else.
  const triggered = controllerTriggeredWorkloads(workloads);
  assert.deepEqual(triggered.map((item) => item.id), ["wl_preview_keda_burst"]);
  assert.ok(workloads.length > triggered.length, "the other fixtures must stay out of the controller panel");
});

test("preview fixtures cover successful export and compute-succeeded/export-failed outcome states", () => {
  const workloads = previewWorkloads("tenant-acme-research").workloads;
  const successRecord = workloads.find((w) => w.id === "wl_preview_export_success");
  assert.ok(successRecord, "wl_preview_export_success fixture must exist");
  assert.equal(successRecord.outcome?.compute?.result, "succeeded");
  assert.equal(successRecord.outcome?.artifacts?.result, "succeeded");
  assert.equal(successRecord.outcome?.artifacts?.objects_uploaded, 14);
  assert.equal(successRecord.outcome?.artifacts?.bytes_uploaded, 1610612736);

  const exportFailedRecord = workloads.find((w) => w.id === "wl_preview_export_failed");
  assert.ok(exportFailedRecord, "wl_preview_export_failed fixture must exist");
  assert.equal(exportFailedRecord.status, "failed");
  assert.equal(exportFailedRecord.outcome?.compute?.result, "succeeded");
  assert.equal(exportFailedRecord.outcome?.artifacts?.result, "failed");
  assert.equal(exportFailedRecord.outcome?.artifacts?.reason, "an artifact upload to the object store failed");
  assert.equal(exportFailedRecord.outcome?.artifacts?.objects_uploaded, 0);
  assert.equal(exportFailedRecord.outcome?.artifacts?.bytes_uploaded, 0);
});

test("cleanup fixtures retain accrued spend after compute failure without inventing a frozen receipt", () => {
  const workloads = previewWorkloads("tenant-acme-research").workloads;
  for (const [id, spent] of [["wl_preview_cleanup_retrying", 1.25], ["wl_preview_manual_attention", 2.5]]) {
    const workload = workloads.find((item) => item.id === id);
    assert.equal(workload.status, "failed");
    assert.equal(workload.spent_usd, spent);
    assert.equal(workload.cost, undefined);
    assert.equal(workload.cleanup.deleted_at, undefined);
  }
});

test("preview placement preview returns an authoritative-shaped receipt", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));
  const result = previewPlacementPreview(owner.customer_id, yaml, PREVIEW_CLUSTER);
  assert.equal(result.status, "ok");
  assert.ok(result.launch_token);
  assert.equal(result.placement.granted_cluster_id, PREVIEW_CLUSTER);
  assert.equal(result.placement.cluster_mode, "pinned");
  assert.ok(result.placement.candidates.length >= 1);
  assert.ok(result.placement.selected);
  assert.ok(result.placement.issued_at);
  assert.ok(result.placement.expires_at);
});

test("viewer tenants can obtain a placement preview without gaining launch access", () => {
  const viewer = previewAccount().tenants.find((tenant) => tenant.role === "viewer");
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(viewer)));
  const result = previewPlacementPreview(viewer.customer_id, yaml, "platform-lab-sandbox");
  assert.equal(result.status, "ok");
  assert.equal(result.placement.granted_cluster_id, "platform-lab-sandbox");
  assert.ok(result.launch_token, "preview remains readable even though the role cannot create");
});

test("preview create refuses without a valid placement token", () => {
  const [owner] = previewAccount().tenants;
  const template = WORKLOAD_TEMPLATES[0];
  const yaml = workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(owner)));
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER),
    (error) => error?.status === 400 && /placement token/i.test(error.message),
  );
  assert.throws(
    () => previewCreateWorkload(owner.customer_id, yaml, PREVIEW_CLUSTER, null, "wrong-token"),
    (error) => error?.status === 400 && /placement token/i.test(error.message),
  );
});
