// The tenant's durable cluster registry as central answers it
// (GET /v1/tenants/{tenant_id}/clusters). Registration is a durable fleet
// fact: a row exists whether or not its connector is connected anywhere right
// now. The live block is different — it is only what the central replica that
// answered this read observes, so `live_partial` scopes the live columns, not
// the registry. Neither is a health check, and every surface that renders
// these rows is expected to say so.
//
// The previous contract sent connected rows flat (connected_at, last_seen,
// agent_version, connections on the row) with a top-level `partial`. Both
// shapes are normalized here so a rolling deploy of central cannot strand the
// console on either side.

import { formatDate } from "./format.js";

// The opaque shape server/proxy.go accepts in X-Cluster-ID (pathSegmentRe). A
// row this browser could not carry in that header is dropped rather than
// offered: choosing it would earn a 400 from our own seam at submit time.
export const CLUSTER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export const CLUSTER_STATES = Object.freeze(["never_connected", "connected_here", "disconnected"]);

const STATE_LABELS = Object.freeze({
  never_connected: "never connected",
  connected_here: "connected here",
  disconnected: "disconnected",
});

const MAX_TEXT = 128;
const MAX_NAME = 64;

// The namespace central reserves for a tenant on hosted capacity — a DNS-1123
// label, the same shape the workload YAML's metadata.namespace must carry. A
// malformed value is dropped rather than kept: pinning a namespace this
// console could never submit would only move the refusal to the server.
const NAMESPACE_RE = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;

// The one sentence that has to travel with these rows wherever they are shown.
export const CLUSTER_OBSERVATION_NOTE =
  "Registered rows are the tenant's durable fleet. Live signal is only what this central replica observes — a cluster without live signal here may be connected through another replica — and none of this is a health check.";

export const CLUSTER_PARTIAL_NOTE =
  "Central marked the live signal as this replica's partial view.";

function text(value, max = MAX_TEXT) {
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  return trimmed.length > max ? trimmed.slice(0, max) : trimmed;
}

function clusterID(value) {
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  return CLUSTER_ID_RE.test(trimmed) ? trimmed : "";
}

// Never truncated: a shortened namespace would still look valid while naming
// a different place than central reserved.
function hostedNamespaceOf(value) {
  if (typeof value !== "string") return "";
  const namespace = value.trim();
  return NAMESPACE_RE.test(namespace) ? namespace : "";
}

export function isClusterId(value) {
  return clusterID(value) !== "";
}

// A timestamp this console cannot parse is worse than an absent one: it renders
// as "Unknown" and reads like a fact. Only a usable instant is kept.
function timestamp(value) {
  const at = text(value, 64);
  return at && !Number.isNaN(new Date(at).getTime()) ? at : "";
}

// connections is optional on the contract, so anything that is not a reported
// non-negative number stays null — "not reported" and 0 are different answers.
function count(value) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

function inventoryCount(value) {
  return Number.isInteger(value) && value >= 0 ? value : null;
}

const RFC3339_RE = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:[Zz]|[+-](\d{2}):(\d{2}))$/;

function inventoryTimestamp(value) {
  const at = text(value, 64);
  const match = RFC3339_RE.exec(at);
  if (!match || Number.isNaN(new Date(at).getTime())) return "";
  const [, year, month, day, hour, minute, second, offsetHour = "00", offsetMinute = "00"] = match;
  const daysInMonth = new Date(Date.UTC(Number(year), Number(month), 0)).getUTCDate();
  if (Number(month) < 1 || Number(month) > 12 || Number(day) < 1 || Number(day) > daysInMonth) return "";
  if (Number(hour) > 23 || Number(minute) > 59 || Number(second) > 59) return "";
  if (Number(offsetHour) > 23 || Number(offsetMinute) > 59) return "";
  return at;
}

function nodeInventoryOf(live) {
  const nodeCount = inventoryCount(live?.node_count);
  const burstCount = inventoryCount(live?.burst_count);
  const observed = live?.node_inventory_observed === true && nodeCount !== null && burstCount !== null;
  return {
    observed,
    observedAt: observed ? inventoryTimestamp(live?.node_inventory_observed_at) : "",
    nodeCount: observed ? nodeCount : null,
    burstCount: observed ? burstCount : null,
  };
}

function podInventoryOf(live) {
  const pendingPods = inventoryCount(live?.pending_pods);
  const observed = live?.pod_inventory_observed === true && pendingPods !== null;
  return {
    observed,
    observedAt: observed ? inventoryTimestamp(live?.pod_inventory_observed_at) : "",
    pendingPods: observed ? pendingPods : null,
  };
}

function newestTimestamp(values) {
  let newest = "";
  for (const value of values) {
    if (!value) continue;
    if (!newest || new Date(value) > new Date(newest)) newest = value;
  }
  return newest;
}

function counted(value, singular, plural = `${singular}s`) {
  return `${value} ${value === 1 ? singular : plural}`;
}

// A row with no state field is the previous contract, where presence itself
// meant a registered connection on the answering replica. A state string this
// console does not recognise is kept as central's word rather than upgraded to
// a claim of connection — launch eligibility fails closed on it.
function stateOf(row) {
  const state = text(row?.state, 32);
  return state || "connected_here";
}

// One registry row, from either wire shape. The live signal nests under `live`
// on the durable contract and sits flat on the previous one; nested wins so a
// flat echo cannot shadow it.
export function normalizeClusterRow(row) {
  const id = clusterID(row?.cluster_id);
  if (!id) return null;
  const live = row?.live && typeof row.live === "object" ? row.live : row;
  const nodeInventory = nodeInventoryOf(live);
  const podInventory = podInventoryOf(live);
  return {
    id,
    name: text(row.name, MAX_NAME) || id,
    source: text(row.source, 32),
    hostedNamespace: hostedNamespaceOf(row.hosted_namespace),
    state: stateOf(row),
    registeredAt: timestamp(row.registered_at),
    firstConnectedAt: timestamp(row.first_connected_at),
    lastConnectedAt: timestamp(row.last_connected_at),
    lastDisconnectedAt: timestamp(row.last_disconnected_at || row.disconnected_at),
    connectedAt: timestamp(live?.connected_at),
    lastSeen: timestamp(live?.last_seen),
    agentVersion: text(live?.agent_version, 64),
    connections: count(live?.connections),
    nodeInventoryObserved: nodeInventory.observed,
    podInventoryObserved: podInventory.observed,
    nodeInventoryObservedAt: nodeInventory.observedAt,
    podInventoryObservedAt: podInventory.observedAt,
    nodeCount: nodeInventory.nodeCount,
    burstCount: nodeInventory.burstCount,
    pendingPods: podInventory.pendingPods,
    eligible: row.eligible !== false,
    reason: text(row.reason || row.policy_reason, 160),
  };
}

// The rows are reshaped rather than rendered raw because two things depend on
// it: the selector needs a stable name-keyed order across reloads, and a row
// without a usable cluster_id is not selectable at all.
export function normalizeClusterObservation(payload) {
  const rows = Array.isArray(payload?.clusters) ? payload.clusters : [];
  const seen = new Set();
  const clusters = [];
  for (const row of rows) {
    const cluster = normalizeClusterRow(row);
    if (!cluster || seen.has(cluster.id)) continue;
    seen.add(cluster.id);
    clusters.push(cluster);
  }
  clusters.sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
  return {
    tenantId: text(payload?.tenant_id),
    observedAt: timestamp(payload?.observed_at),
    // live_partial is the durable contract's word; `partial` carried the same
    // claim on the previous one. Either way it scopes only the live signal.
    livePartial: payload?.live_partial === true || payload?.partial === true,
    policy: payload?.policy || null,
    clusters,
  };
}

export function findCluster(clusters, id) {
  return clusters.find((cluster) => cluster.id === id) || null;
}

// A workload can only land where a connector is actually connected to the
// replica answering this console: state connected_here, or a reported live
// connection when the state itself does not carry that claim.
export function isConnectedHere(cluster) {
  if (!cluster) return false;
  if (cluster.state === "connected_here") return true;
  return cluster.connections !== null && cluster.connections > 0;
}

// central's word for who operates a row. "hosted" is Yscale-run shared
// capacity — a virtual cluster with a namespace reserved for this tenant —
// so the tenant holds no connector credential and rotate/delete are not its
// calls to make. Every other source is a cluster the tenant runs itself.
export const HOSTED_CLUSTER_SOURCE = "hosted";

export function isHostedCluster(cluster) {
  return cluster?.source === HOSTED_CLUSTER_SOURCE;
}

// The tenant's platform-managed row. central assigns at most one; if several
// ever arrive, the first in the observation's stable order is the one shown.
export function hostedCluster(clusters) {
  return clusters.find(isHostedCluster) || null;
}

// A hosted row central named no usable namespace for cannot be a target: the
// console would be guessing where the workload lands.
function hostedNamespaceMissing(cluster) {
  return isHostedCluster(cluster) && !cluster.hostedNamespace;
}

// The only rows a launch selector or automode helper may offer. Registered
// rows without a live connection stay visible elsewhere but are never targets.
export function launchableClusters(clusters) {
  return clusters.filter((cluster) => cluster.eligible && isConnectedHere(cluster) && !hostedNamespaceMissing(cluster));
}

export function clusterStateLabel(cluster) {
  return STATE_LABELS[cluster?.state] || text(cluster?.state, 32).replaceAll("_", " ") || "connected here";
}

// Why a row is not a launch target right now, in the words the selector shows
// beside it. Empty string means it is one.
export function clusterUnavailableReason(cluster) {
  if (!cluster) return "";
  if (!cluster.eligible) return cluster.reason || "Not eligible under this tenant's cluster policy.";
  if (!isConnectedHere(cluster)) {
    return cluster.state === "never_connected"
      ? "Registered, never connected."
      : "Registered, no live connection on this replica.";
  }
  if (hostedNamespaceMissing(cluster)) return "Hosted, but central reported no reserved namespace.";
  return "";
}

// Name first, id as the technical secondary — unless they are the same word.
export function clusterOptionLabel(cluster) {
  if (!cluster) return "";
  return cluster.name === cluster.id ? cluster.id : `${cluster.name} (${cluster.id})`;
}

// The register form's own check, before anything crosses the seam. The name is
// the only required answer; a custom id is optional and shape-checked so the
// server's 400 is never the first time the reader hears about it.
export function registerDraftError(name, customId = "") {
  if (!text(name, MAX_NAME)) return "Give this cluster a display name.";
  if (typeof name === "string" && [...name.trim()].length > MAX_NAME) return `Keep the display name at ${MAX_NAME} characters or fewer.`;
  const id = String(customId || "").trim();
  if (id && !isClusterId(id)) return "A cluster id starts with a letter or digit and uses only letters, digits, dots, dashes, and underscores.";
  return "";
}

// Exactly one launchable cluster is an unambiguous target, so a form may open
// on it. None, or several, is a choice the console must not make for the reader.
export function preselectedClusterId(clusters) {
  return clusters.length === 1 ? clusters[0].id : "";
}

// The form's own check, run before review and again before submit. An id that
// has left the registry — or lost its live connection — between those two
// points is refused here rather than sent.
export function clusterChoiceError(clusters, id, { allowAutomatic = false } = {}) {
  const eligible = launchableClusters(clusters);
  if (!eligible.length) return "No eligible connected cluster is observed for this tenant.";
  if (!id) return allowAutomatic ? "" : "Choose the cluster this workload should run on.";
  const cluster = findCluster(clusters, id);
  if (!cluster) return "That cluster is no longer in the observation. Choose one of the clusters listed.";
  if (!cluster.eligible) return cluster.reason || "That cluster is not eligible for this tenant's policy.";
  if (!isConnectedHere(cluster)) return "That cluster is registered but has no live connection on this replica right now.";
  if (hostedNamespaceMissing(cluster)) return "That hosted cluster has no reported reserved namespace, so this console cannot place a workload on it.";
  return "";
}

// Automatic placement is safe only when central can choose at least one
// tenant-owned connector. Hosted rows are namespace-pinned and therefore must
// always travel with an explicit X-Cluster-ID.
export function automaticPlacementAvailable(clusters, autoMode) {
  return autoMode === "ordered" && launchableClusters(clusters).some((cluster) => !isHostedCluster(cluster));
}

// The namespace a target insists on: hosted workloads land only in the row's
// reserved namespace, while tenant-owned rows impose nothing. Empty means the
// reader's own namespace choice stands.
export function requiredNamespace(cluster) {
  return isHostedCluster(cluster) ? cluster.hostedNamespace : "";
}

// The form's namespace rule for the chosen target, run before review and again
// before submit: a hosted target and its reserved namespace travel together,
// and a mismatch is refused here rather than by central's admission after the
// reader believed the YAML was accepted. No cluster chosen — automatic
// placement — imposes nothing, because central may choose a different cluster
// and a namespace guessed for one hosted row would be wrong on every other.
export function hostedNamespaceError(cluster, namespace) {
  if (!isHostedCluster(cluster)) return "";
  if (!cluster.hostedNamespace) return "That hosted cluster has no reported reserved namespace, so this console cannot place a workload on it.";
  if (namespace !== cluster.hostedNamespace) return `Workloads on ${cluster.name} run only in its reserved namespace ${cluster.hostedNamespace}.`;
  return "";
}

// The namespace the form carries after the target changes: a hosted target
// pins its reserved namespace; anything else keeps the reader's choice while
// the tenant still authorizes it. A live assignment keeps its reserved
// namespace on the authorized list, so leaving a hosted target keeps the
// namespace too; the remaining fallback to the tenant's first authorized
// namespace — the one central decides unqualified submissions against — can
// fire only when the observation and the namespace list disagree, as when
// the two fetches straddle an assignment change.
export function namespaceForCluster(cluster, current, authorized) {
  const required = requiredNamespace(cluster);
  if (required) return required;
  if (authorized.includes(current)) return current;
  return authorized[0] || current;
}

// What the form shows beside the selector: everything the observation reported
// about the chosen cluster, and "not reported" where it reported nothing.
export function clusterDetail(cluster) {
  if (!cluster) return "";
  return [
    cluster.connections === null
      ? "connections not reported"
      : `${cluster.connections} agent connection${cluster.connections === 1 ? "" : "s"}`,
    cluster.agentVersion ? `agent ${cluster.agentVersion}` : "agent version not reported",
    cluster.lastSeen ? `last seen ${formatDate(cluster.lastSeen)}` : "last seen not reported",
  ].join(" · ");
}

// Only the rows that reported a count are added up, and how many did is part of
// the answer — otherwise an unreported cluster reads as a cluster with none.
export function clusterConnectionTotal(clusters) {
  let total = 0;
  let reported = 0;
  for (const cluster of clusters) {
    if (cluster.connections === null) continue;
    total += cluster.connections;
    reported += 1;
  }
  return { total, reported };
}

export function clusterInventorySummary(cluster) {
  if (!cluster) {
    return { text: "", newestObservedAt: "", nodeObserved: false, podObserved: false };
  }
  const parts = [];
  if (cluster.nodeInventoryObserved === true) {
    parts.push(counted(cluster.nodeCount, "node"));
    parts.push(counted(cluster.burstCount, "burst node"));
  }
  if (cluster.podInventoryObserved === true) parts.push(counted(cluster.pendingPods, "pending pod"));
  return {
    text: parts.join(" · "),
    newestObservedAt: newestTimestamp([cluster.nodeInventoryObservedAt, cluster.podInventoryObservedAt]),
    nodeObserved: cluster.nodeInventoryObserved === true,
    podObserved: cluster.podInventoryObserved === true,
  };
}

export function clusterInventoryTotals(clusters) {
  const rows = Array.isArray(clusters) ? clusters : [];
  const totals = {
    nodes: 0,
    nodeRows: 0,
    burstNodes: 0,
    pendingPods: 0,
    podRows: 0,
    totalRows: rows.length,
    newestObservedAt: "",
  };
  for (const cluster of rows) {
    if (cluster.nodeInventoryObserved === true) {
      totals.nodes += cluster.nodeCount;
      totals.burstNodes += cluster.burstCount;
      totals.nodeRows += 1;
    }
    if (cluster.podInventoryObserved === true) {
      totals.pendingPods += cluster.pendingPods;
      totals.podRows += 1;
    }
    totals.newestObservedAt = newestTimestamp([
      totals.newestObservedAt,
      cluster.nodeInventoryObservedAt,
      cluster.podInventoryObservedAt,
    ]);
  }
  return totals;
}

export function newestClusterActivity(clusters) {
  let newest = "";
  for (const cluster of clusters) {
    if (!cluster.lastSeen) continue;
    if (!newest || new Date(cluster.lastSeen) > new Date(newest)) newest = cluster.lastSeen;
  }
  return newest;
}
