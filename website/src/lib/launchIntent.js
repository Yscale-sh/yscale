import { isClusterId, preselectedClusterId } from "./clusters.js";

export const LAUNCH_CLUSTER_QUERY = "cluster";

function cleanSearch(rawSearch = "") {
  if (typeof rawSearch !== "string") return "";
  const trimmed = rawSearch.trim();
  if (!trimmed) return "";
  if (trimmed.includes("?")) return trimmed.split("?").slice(1).join("?");
  return trimmed;
}

export function parseLaunchClusterId(rawSearch = "") {
  const query = cleanSearch(rawSearch);
  if (!query) return "";
  const params = new URLSearchParams(query);
  const next = params.get(LAUNCH_CLUSTER_QUERY);
  return isClusterId(next) ? next : "";
}

export function launchClusterQuery(clusterId) {
  const query = new URLSearchParams();
  const normalized = String(clusterId || "").trim();
  if (!isClusterId(normalized)) return "";
  query.set(LAUNCH_CLUSTER_QUERY, normalized);
  const next = query.get(LAUNCH_CLUSTER_QUERY);
  return next ? `?${query.toString()}` : "";
}

export function launchTemplatesPath(clusterId = "") {
  return `/workloads/templates${launchClusterQuery(clusterId)}`;
}

export function launchFormPath(templateId, clusterId = "") {
  return `/workloads/new/${encodeURIComponent(String(templateId || ""))}${launchClusterQuery(clusterId)}`;
}

export function resolveLaunchClusterId({
  eligibleClusters,
  requestedClusterId,
  currentClusterId,
  allowAutomatic,
}) {
  const valid = (value) => isClusterId(value);
  const isLaunchable = (clusterId) => {
    if (!valid(clusterId)) return false;
    return eligibleClusters?.some((cluster) => cluster?.id === clusterId) || false;
  };

  if (isLaunchable(requestedClusterId)) return requestedClusterId;
  if (isLaunchable(currentClusterId)) return currentClusterId;
  if (allowAutomatic && (eligibleClusters?.length || 0) > 0) return "";
  return preselectedClusterId(Array.isArray(eligibleClusters) ? eligibleClusters : []);
}
