import assert from "node:assert/strict";
import test from "node:test";

import {
  automaticPlacementAvailable,
  CLUSTER_OBSERVATION_NOTE,
  clusterChoiceError,
  clusterConnectionTotal,
  clusterDetail,
  clusterInventorySummary,
  clusterInventoryTotals,
  clusterOptionLabel,
  clusterStateLabel,
  clusterUnavailableReason,
  findCluster,
  hostedCluster,
  hostedNamespaceError,
  isConnectedHere,
  isHostedCluster,
  launchableClusters,
  namespaceForCluster,
  newestClusterActivity,
  normalizeClusterObservation,
  preselectedClusterId,
  registerDraftError,
  requiredNamespace,
} from "./clusters.js";

// The previous wire shape: connected rows flat, one top-level `partial`.
const OBSERVATION = {
  tenant_id: "tenant-acme-research",
  observed_at: "2026-08-12T20:31:04Z",
  partial: true,
  clusters: [
    { cluster_id: "b-cluster", connected_at: "2026-08-09T04:12:00Z", last_seen: "2026-08-12T20:30:58Z", agent_version: "0.9.3", connections: 2 },
    { cluster_id: "a-cluster", connected_at: "2026-08-11T22:47:31Z", eligible: false, reason: "Denied by policy" },
  ],
};

// The durable registry shape: names, states, lifecycle stamps, live nested.
const REGISTRY = {
  tenant_id: "tenant-acme-research",
  observed_at: "2026-08-12T20:31:04Z",
  live_partial: true,
  clusters: [
    {
      cluster_id: "reg-prod", name: "Prod US East", source: "console", state: "connected_here",
      registered_at: "2026-08-05T09:00:00Z", first_connected_at: "2026-08-09T04:12:00Z",
      live: { connected_at: "2026-08-09T04:12:00Z", last_seen: "2026-08-12T20:30:58Z", agent_version: "0.9.3", connections: 2 },
    },
    {
      cluster_id: "reg-batch", name: "Batch EU", source: "console", state: "disconnected",
      registered_at: "2026-07-28T11:00:00Z", last_disconnected_at: "2026-08-11T07:40:00Z",
    },
    {
      cluster_id: "reg-staging", name: "Staging EU", source: "console", state: "never_connected",
      registered_at: "2026-08-12T14:05:00Z", eligible: false, reason: "Not in the allowed order",
    },
  ],
};

test("the durable registry payload is read as central sends it", () => {
  const observed = normalizeClusterObservation(REGISTRY);
  assert.equal(observed.tenantId, "tenant-acme-research");
  assert.equal(observed.livePartial, true);
  // Stable name-first order across reloads, id as the tiebreak.
  assert.deepEqual(observed.clusters.map((cluster) => cluster.id), ["reg-batch", "reg-prod", "reg-staging"]);

  const prod = findCluster(observed.clusters, "reg-prod");
  assert.equal(prod.name, "Prod US East");
  assert.equal(prod.source, "console");
  assert.equal(prod.state, "connected_here");
  assert.equal(prod.registeredAt, "2026-08-05T09:00:00Z");
  assert.equal(prod.firstConnectedAt, "2026-08-09T04:12:00Z");
  // The nested live block lands on the same flat fields the previous contract
  // filled, so every consumer reads one shape.
  assert.equal(prod.connections, 2);
  assert.equal(prod.agentVersion, "0.9.3");
  assert.equal(prod.lastSeen, "2026-08-12T20:30:58Z");
  assert.equal(prod.eligible, true);

  const batch = findCluster(observed.clusters, "reg-batch");
  assert.equal(batch.state, "disconnected");
  assert.equal(batch.lastDisconnectedAt, "2026-08-11T07:40:00Z");
  assert.equal(batch.connections, null);
  assert.equal(batch.lastSeen, "");

  const staging = findCluster(observed.clusters, "reg-staging");
  assert.equal(staging.state, "never_connected");
  assert.equal(staging.eligible, false);
  assert.equal(staging.reason, "Not in the allowed order");
});

// The previous flat payload keeps working through a rolling deploy: a row's
// presence meant a registered connection on the answering replica, so those
// rows normalize to connected_here with the id standing in for the name.
test("the previous flat payload normalizes unchanged", () => {
  const observed = normalizeClusterObservation(OBSERVATION);
  assert.equal(observed.tenantId, "tenant-acme-research");
  assert.equal(observed.observedAt, "2026-08-12T20:31:04Z");
  assert.equal(observed.livePartial, true);
  assert.deepEqual(observed.clusters.map((cluster) => cluster.id), ["a-cluster", "b-cluster"]);

  const [a, b] = observed.clusters;
  assert.equal(a.name, "a-cluster");
  assert.equal(a.state, "connected_here");
  assert.equal(b.state, "connected_here");
  assert.equal(b.connections, 2);
  assert.equal(b.agentVersion, "0.9.3");
  // Both optional fields absent is a shape the contract allows, and "not
  // reported" must survive as null and "" rather than becoming 0 or "unknown".
  assert.equal(a.connections, null);
  assert.equal(a.agentVersion, "");
  assert.equal(a.eligible, false);
  assert.equal(a.reason, "Denied by policy");
  assert.equal(b.eligible, true);
});

// live_partial is central's word for "this replica's live view"; the previous
// contract spelled the same claim `partial`. Anything that is not a literal
// true on either spelling is not that claim.
test("live_partial is only true when central said true", () => {
  assert.equal(normalizeClusterObservation(REGISTRY).livePartial, true);
  assert.equal(normalizeClusterObservation({ ...REGISTRY, live_partial: false }).livePartial, false);
  assert.equal(normalizeClusterObservation({ ...REGISTRY, live_partial: "true" }).livePartial, false);
  assert.equal(normalizeClusterObservation({ ...OBSERVATION, partial: false }).livePartial, false);
  assert.equal(normalizeClusterObservation({ ...OBSERVATION, partial: "true" }).livePartial, false);
  assert.equal(normalizeClusterObservation({ clusters: [] }).livePartial, false);
});

// Only rows with a live connection on this replica may be launch targets.
// Registered offline rows stay visible with a reason, never selectable.
test("launch eligibility is connected-here only", () => {
  const clusters = normalizeClusterObservation(REGISTRY).clusters;
  assert.deepEqual(launchableClusters(clusters).map((cluster) => cluster.id), ["reg-prod"]);

  assert.equal(isConnectedHere(findCluster(clusters, "reg-prod")), true);
  assert.equal(isConnectedHere(findCluster(clusters, "reg-batch")), false);
  assert.equal(isConnectedHere(findCluster(clusters, "reg-staging")), false);

  assert.equal(clusterUnavailableReason(findCluster(clusters, "reg-prod")), "");
  assert.match(clusterUnavailableReason(findCluster(clusters, "reg-batch")), /no live connection on this replica/);
  assert.match(clusterUnavailableReason(findCluster(clusters, "reg-staging")), /Not in the allowed order/);

  // An eligible disconnected row is refused by the choice check even when it
  // is explicitly named — a target nobody can reach is not a target.
  assert.match(clusterChoiceError(clusters, "reg-batch"), /no live connection on this replica/);

  // Legacy rows stay launchable exactly as before: presence meant connected.
  const legacy = normalizeClusterObservation(OBSERVATION).clusters;
  assert.deepEqual(launchableClusters(legacy).map((cluster) => cluster.id), ["b-cluster"]);

  // A state this console does not recognise is not a claim of connection —
  // unless the live signal itself reports open connections.
  const odd = normalizeClusterObservation({
    clusters: [
      { cluster_id: "c1", state: "draining" },
      { cluster_id: "c2", state: "draining", live: { connections: 1 } },
    ],
  }).clusters;
  assert.equal(isConnectedHere(findCluster(odd, "c1")), false);
  assert.equal(isConnectedHere(findCluster(odd, "c2")), true);
});

test("state labels and option labels say the honest thing", () => {
  const clusters = normalizeClusterObservation(REGISTRY).clusters;
  assert.equal(clusterStateLabel(findCluster(clusters, "reg-prod")), "connected here");
  assert.equal(clusterStateLabel(findCluster(clusters, "reg-batch")), "disconnected");
  assert.equal(clusterStateLabel(findCluster(clusters, "reg-staging")), "never connected");
  assert.equal(clusterOptionLabel(findCluster(clusters, "reg-prod")), "Prod US East (reg-prod)");
  // Name falls back to the id, and then the label does not repeat it.
  const [legacy] = normalizeClusterObservation({ clusters: [{ cluster_id: "only-one" }] }).clusters;
  assert.equal(clusterOptionLabel(legacy), "only-one");
});

test("the register draft check refuses what the server would", () => {
  assert.match(registerDraftError(""), /display name/);
  assert.match(registerDraftError("   "), /display name/);
  assert.equal(registerDraftError("Prod US East"), "");
  assert.equal(registerDraftError("Prod US East", "prod-us-east"), "");
  assert.match(registerDraftError("Prod", "-bad-id"), /cluster id/);
  assert.match(registerDraftError("Prod", "has space"), /cluster id/);
  assert.match(registerDraftError(`x${"y".repeat(64)}`), /64 characters or fewer/);
});

// A row this browser could not carry in X-Cluster-ID would be a target the seam
// refuses at submit time, so it is never offered.
test("rows this console could not target are dropped, not shown", () => {
  const observed = normalizeClusterObservation({
    clusters: [
      { cluster_id: "good-1" },
      { cluster_id: "" },
      { cluster_id: "   " },
      { cluster_id: "-leading-hyphen" },
      { cluster_id: "has space" },
      { cluster_id: "has/slash" },
      { cluster_id: "../admin" },
      { cluster_id: `x${"y".repeat(200)}` },
      { cluster_id: 42 },
      { cluster_id: "good-1", connections: 9 },
      null,
    ],
  });
  assert.deepEqual(observed.clusters.map((cluster) => cluster.id), ["good-1"]);
  // The duplicate keeps the first row rather than merging two answers into one.
  assert.equal(observed.clusters[0].connections, null);
});

test("an unusable timestamp is absent rather than rendered as a fact", () => {
  const [cluster] = normalizeClusterObservation({
    clusters: [{ cluster_id: "c1", connected_at: "whenever", last_seen: 17 }],
  }).clusters;
  assert.equal(cluster.connectedAt, "");
  assert.equal(cluster.lastSeen, "");
  assert.equal(normalizeClusterObservation({ observed_at: "not a date" }).observedAt, "");
});

test("inventory scopes normalize independently with exact counts and newest observation", () => {
  const clusters = normalizeClusterObservation({
    clusters: [
      {
        cluster_id: "both",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-08-12T20:30:10Z",
          node_count: 3,
          burst_count: 1,
          pod_inventory_observed: true,
          pod_inventory_observed_at: "2026-08-12T20:31:10Z",
          pending_pods: 0,
        },
      },
      {
        cluster_id: "node-only",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-08-12T20:29:00Z",
          node_count: 0,
          burst_count: 0,
        },
      },
      {
        cluster_id: "pod-only",
        pod_inventory_observed: true,
        pod_inventory_observed_at: "2026-08-12T20:32:00Z",
        pending_pods: 2,
      },
      { cluster_id: "neither" },
    ],
  }).clusters;

  const both = findCluster(clusters, "both");
  assert.equal(both.nodeInventoryObserved, true);
  assert.equal(both.podInventoryObserved, true);
  assert.equal(both.nodeCount, 3);
  assert.equal(both.burstCount, 1);
  assert.equal(both.pendingPods, 0);
  assert.deepEqual(clusterInventorySummary(both), {
    text: "3 nodes · 1 burst node · 0 pending pods",
    newestObservedAt: "2026-08-12T20:31:10Z",
    nodeObserved: true,
    podObserved: true,
  });

  const nodeOnly = findCluster(clusters, "node-only");
  assert.equal(nodeOnly.nodeInventoryObserved, true);
  assert.equal(nodeOnly.podInventoryObserved, false);
  assert.equal(nodeOnly.nodeCount, 0);
  assert.equal(nodeOnly.burstCount, 0);
  assert.equal(nodeOnly.pendingPods, null);
  assert.equal(clusterInventorySummary(nodeOnly).text, "0 nodes · 0 burst nodes");

  const podOnly = findCluster(clusters, "pod-only");
  assert.equal(podOnly.nodeInventoryObserved, false);
  assert.equal(podOnly.podInventoryObserved, true);
  assert.equal(podOnly.pendingPods, 2);
  assert.equal(clusterInventorySummary(podOnly).text, "2 pending pods");

  const neither = findCluster(clusters, "neither");
  assert.deepEqual(clusterInventorySummary(neither), {
    text: "",
    newestObservedAt: "",
    nodeObserved: false,
    podObserved: false,
  });
  assert.deepEqual(clusterInventorySummary(null), {
    text: "",
    newestObservedAt: "",
    nodeObserved: false,
    podObserved: false,
  });

  assert.deepEqual(clusterInventoryTotals(clusters), {
    nodes: 3,
    nodeRows: 2,
    burstNodes: 1,
    pendingPods: 2,
    podRows: 2,
    totalRows: 4,
    newestObservedAt: "2026-08-12T20:32:00Z",
  });
  assert.deepEqual(clusterInventoryTotals([]), {
    nodes: 0,
    nodeRows: 0,
    burstNodes: 0,
    pendingPods: 0,
    podRows: 0,
    totalRows: 0,
    newestObservedAt: "",
  });
  assert.deepEqual(clusterInventoryTotals(null), {
    nodes: 0,
    nodeRows: 0,
    burstNodes: 0,
    pendingPods: 0,
    podRows: 0,
    totalRows: 0,
    newestObservedAt: "",
  });
});

test("inventory scopes fail closed on invalid flags and counts while dropping only bad optional timestamps", () => {
  const clusters = normalizeClusterObservation({
    clusters: [
      {
        cluster_id: "string-flags",
        live: {
          node_inventory_observed: "true",
          node_inventory_observed_at: "2026-08-12T20:30:10Z",
          node_count: 3,
          burst_count: 1,
          pod_inventory_observed: "true",
          pod_inventory_observed_at: "2026-08-12T20:31:10Z",
          pending_pods: 2,
        },
      },
      {
        cluster_id: "counts-without-flags",
        live: { node_count: 3, burst_count: 1, pending_pods: 2 },
      },
      {
        cluster_id: "bad-node-timestamp",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-08-12 20:30:10",
          node_count: 3,
          burst_count: 1,
          pod_inventory_observed: true,
          pod_inventory_observed_at: "2026-08-12T20:31:10Z",
          pending_pods: 2,
        },
      },
      {
        cluster_id: "bad-rollover-timestamp",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-02-31T20:30:10Z",
          node_count: 3,
          burst_count: 1,
        },
      },
      {
        cluster_id: "bad-node-count",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-08-12T20:30:10Z",
          node_count: 3.5,
          burst_count: 1,
        },
      },
      {
        cluster_id: "bad-burst-count",
        live: {
          node_inventory_observed: true,
          node_inventory_observed_at: "2026-08-12T20:30:10Z",
          node_count: 3,
          burst_count: -1,
        },
      },
      {
        cluster_id: "bad-pod-count",
        live: {
          pod_inventory_observed: true,
          pod_inventory_observed_at: "2026-08-12T20:31:10Z",
          pending_pods: "2",
        },
      },
    ],
  }).clusters;

  for (const id of ["string-flags", "counts-without-flags", "bad-node-count", "bad-burst-count"]) {
    const cluster = findCluster(clusters, id);
    assert.equal(cluster.nodeInventoryObserved, false, id);
    assert.equal(cluster.nodeInventoryObservedAt, "");
    assert.equal(cluster.nodeCount, null);
    assert.equal(cluster.burstCount, null);
  }
  for (const id of ["string-flags", "counts-without-flags", "bad-pod-count"]) {
    const cluster = findCluster(clusters, id);
    assert.equal(cluster.podInventoryObserved, false, id);
    assert.equal(cluster.podInventoryObservedAt, "");
    assert.equal(cluster.pendingPods, null);
  }

  const independent = findCluster(clusters, "bad-node-timestamp");
  assert.equal(independent.nodeInventoryObserved, true);
  assert.equal(independent.nodeInventoryObservedAt, "");
  assert.equal(independent.nodeCount, 3);
  assert.equal(independent.burstCount, 1);
  assert.equal(independent.podInventoryObserved, true);
  assert.equal(independent.podInventoryObservedAt, "2026-08-12T20:31:10Z");
  assert.equal(independent.pendingPods, 2);

  const rollover = findCluster(clusters, "bad-rollover-timestamp");
  assert.equal(rollover.nodeInventoryObserved, true);
  assert.equal(rollover.nodeInventoryObservedAt, "");
  assert.equal(rollover.nodeCount, 3);
  assert.equal(rollover.burstCount, 1);
});

test("a payload without a cluster list reads as no rows, never as invented ones", () => {
  for (const payload of [null, undefined, {}, { clusters: "many" }, { clusters: null }]) {
    assert.deepEqual(normalizeClusterObservation(payload).clusters, []);
  }
});

// The whole point of the selector: one is unambiguous, several is the reader's
// call, none is a dead end.
test("only a single observed cluster may be preselected", () => {
  const one = normalizeClusterObservation({ clusters: [{ cluster_id: "only-one" }] }).clusters;
  assert.equal(preselectedClusterId(one), "only-one");
  assert.equal(preselectedClusterId(normalizeClusterObservation(OBSERVATION).clusters), "");
  assert.equal(preselectedClusterId([]), "");
});

test("the form refuses every target it cannot stand behind", () => {
  const clusters = normalizeClusterObservation(OBSERVATION).clusters;
  assert.match(clusterChoiceError([], "a-cluster"), /No eligible connected cluster/);
  assert.match(clusterChoiceError(clusters, ""), /Choose the cluster/);
  // A cluster that left the observation while the form sat open is refused here
  // rather than sent to a destination this console can no longer see.
  assert.match(clusterChoiceError(clusters, "gone-cluster"), /no longer in the observation/);
  assert.equal(clusterChoiceError(clusters, "b-cluster"), "");
  assert.equal(clusterChoiceError(clusters, "", { allowAutomatic: true }), "");
  assert.match(clusterChoiceError(clusters, "a-cluster"), /Denied by policy/);
});

// The hosted contract: source "hosted" marks the platform-managed row, and
// hosted_namespace is the namespace central reserved for the tenant on it.
const HOSTED = {
  tenant_id: "tenant-acme-research",
  observed_at: "2026-08-12T20:31:04Z",
  clusters: [
    { cluster_id: "self-run", name: "Prod", source: "console", state: "connected_here" },
    {
      cluster_id: "hosted-1", name: "Yscale Hosted", source: "hosted", state: "connected_here",
      hosted_namespace: "ten-acme", live: { connections: 1 },
    },
  ],
};

test("hosted rows normalize the reserved namespace and are platform-managed", () => {
  const clusters = normalizeClusterObservation(HOSTED).clusters;
  const hosted = findCluster(clusters, "hosted-1");
  assert.equal(hosted.hostedNamespace, "ten-acme");
  assert.equal(isHostedCluster(hosted), true);
  assert.equal(hostedCluster(clusters), hosted);

  const own = findCluster(clusters, "self-run");
  assert.equal(own.hostedNamespace, "");
  assert.equal(isHostedCluster(own), false);
  // A fleet without a hosted row has no platform-managed row to show.
  assert.equal(hostedCluster([own]), null);
  assert.equal(hostedCluster([]), null);

  // A namespace central sent malformed is dropped rather than pinned into a
  // document this console could never submit.
  for (const bad of ["Not A Namespace", "-leading", "trailing-", "a".repeat(64), "dot.ted", 42, ""]) {
    const [row] = normalizeClusterObservation({
      clusters: [{ cluster_id: "h", source: "hosted", state: "connected_here", hosted_namespace: bad }],
    }).clusters;
    assert.equal(row.hostedNamespace, "", `expected ${JSON.stringify(bad)} to be dropped`);
  }
});

test("automatic placement is offered only when a tenant-owned target is launchable", () => {
  const mixed = normalizeClusterObservation(HOSTED).clusters;
  const hostedOnly = mixed.filter((cluster) => isHostedCluster(cluster));
  const ownOnly = mixed.filter((cluster) => !isHostedCluster(cluster));

  assert.equal(automaticPlacementAvailable(mixed, "ordered"), true);
  assert.equal(automaticPlacementAvailable(ownOnly, "ordered"), true);
  assert.equal(automaticPlacementAvailable(hostedOnly, "ordered"), false);
  assert.equal(automaticPlacementAvailable(mixed, "require_pin"), false);
});

// A hosted target and its reserved namespace travel together; every other
// target leaves the namespace as the reader's own authorized choice.
test("the namespace rules pin hosted targets and only hosted targets", () => {
  const clusters = normalizeClusterObservation(HOSTED).clusters;
  const hosted = findCluster(clusters, "hosted-1");
  const own = findCluster(clusters, "self-run");

  assert.equal(requiredNamespace(hosted), "ten-acme");
  assert.equal(requiredNamespace(own), "");
  assert.equal(requiredNamespace(null), "");

  assert.equal(hostedNamespaceError(hosted, "ten-acme"), "");
  assert.match(hostedNamespaceError(hosted, "default"), /reserved namespace ten-acme/);
  assert.equal(hostedNamespaceError(own, "anything"), "");
  // Automatic placement names no cluster, so nothing is pinned or refused:
  // central may choose a different cluster than any hosted guess.
  assert.equal(hostedNamespaceError(null, "default"), "");

  const authorized = ["default", "ml-team-a", "ten-acme"];
  assert.equal(namespaceForCluster(hosted, "default", authorized), "ten-acme");
  assert.equal(namespaceForCluster(own, "ml-team-a", authorized), "ml-team-a");
  assert.equal(namespaceForCluster(null, "ml-team-a", authorized), "ml-team-a");
  // Leaving a hosted target keeps its reserved namespace: a live assignment
  // keeps that namespace on the tenant's authorized list, so it is now an
  // ordinary choice rather than a pin.
  assert.equal(namespaceForCluster(own, "ten-acme", authorized), "ten-acme");
  // A namespace the authorized list does not carry — the observation and the
  // namespace fetch straddling an assignment change — falls back to the first
  // authorized one instead of stranding the select.
  assert.equal(namespaceForCluster(own, "ten-acme", ["default", "ml-team-a"]), "default");
});

// A hosted row central named no usable namespace for stays visible but is
// never a target: offering it would be guessing where the workload lands.
test("a hosted row without a reserved namespace is never a target", () => {
  const clusters = normalizeClusterObservation({
    clusters: [
      { cluster_id: "hosted-ok", name: "Hosted", source: "hosted", state: "connected_here", hosted_namespace: "ten-acme", live: { connections: 1 } },
      { cluster_id: "hosted-bare", name: "Hosted bare", source: "hosted", state: "connected_here", live: { connections: 1 } },
    ],
  }).clusters;
  assert.deepEqual(launchableClusters(clusters).map((cluster) => cluster.id), ["hosted-ok"]);
  assert.equal(clusterUnavailableReason(findCluster(clusters, "hosted-ok")), "");
  assert.match(clusterUnavailableReason(findCluster(clusters, "hosted-bare")), /no reserved namespace/);
  assert.equal(clusterChoiceError(clusters, "hosted-ok"), "");
  assert.match(clusterChoiceError(clusters, "hosted-bare"), /reserved namespace/);
  assert.match(hostedNamespaceError(findCluster(clusters, "hosted-bare"), "default"), /reserved namespace/);
});

test("the detail line names what was reported and what was not", () => {
  const clusters = normalizeClusterObservation(OBSERVATION).clusters;
  const withCounts = clusterDetail(findCluster(clusters, "b-cluster"));
  assert.match(withCounts, /2 agent connections/);
  assert.match(withCounts, /agent 0\.9\.3/);
  assert.match(withCounts, /last seen /);

  const without = clusterDetail(findCluster(clusters, "a-cluster"));
  assert.match(without, /connections not reported/);
  assert.match(without, /agent version not reported/);
  assert.match(without, /last seen not reported/);
  assert.equal(clusterDetail(null), "");

  const [one] = normalizeClusterObservation({ clusters: [{ cluster_id: "c1", connections: 1 }] }).clusters;
  assert.match(clusterDetail(one), /1 agent connection ·/);
});

// A cluster that reported nothing is not a cluster with zero connections, so
// the count says how many rows it actually covers.
test("connection totals only add up what was reported", () => {
  const clusters = normalizeClusterObservation(OBSERVATION).clusters;
  assert.deepEqual(clusterConnectionTotal(clusters), { total: 2, reported: 1 });
  assert.deepEqual(clusterConnectionTotal([]), { total: 0, reported: 0 });
  const zeroed = normalizeClusterObservation({ clusters: [{ cluster_id: "c1", connections: 0 }] }).clusters;
  assert.deepEqual(clusterConnectionTotal(zeroed), { total: 0, reported: 1 });
});

test("the newest activity is the newest reported one", () => {
  const clusters = normalizeClusterObservation(OBSERVATION).clusters;
  assert.equal(newestClusterActivity(clusters), "2026-08-12T20:30:58Z");
  assert.equal(newestClusterActivity([]), "");
  assert.equal(newestClusterActivity(normalizeClusterObservation({ clusters: [{ cluster_id: "c1" }] }).clusters), "");
});

// The one sentence that has to travel with these rows. It is asserted here so
// a later edit cannot quietly widen the live signal into a fleet-completeness
// claim or a health claim: the registry is durable, the live columns are this
// replica's view only.
test("the observation note refuses both claims it must never make", () => {
  assert.match(CLUSTER_OBSERVATION_NOTE, /durable fleet/);
  assert.match(CLUSTER_OBSERVATION_NOTE, /this central replica/);
  assert.match(CLUSTER_OBSERVATION_NOTE, /may be connected through another replica/);
  assert.match(CLUSTER_OBSERVATION_NOTE, /health check/);
});
