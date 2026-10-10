// The console's route table. The sidebar, the mobile drawer, the command
// palette, and the route switch all read this one list, so a section cannot be
// reachable from one of them and missing from another.
//
// Every fixed page lives under a named second segment; a segment that is not
// one of them is a workload id, which is what keeps the detail route a bare
// /workloads/{id}.

export const NAV_GROUPS = [
  {
    label: "Workspace",
    items: [
      { label: "Workloads", icon: "workloads", to: "/workloads", hint: "Active and recent GPU runs" },
      { label: "Launch", icon: "plus", to: "/workloads/launch", hint: "Start a guided workload" },
      { label: "Clusters", icon: "clusters", to: "/workloads/clusters", hint: "The registered fleet and its live connections" },
      { label: "Data", icon: "gitops", to: "/workloads/data", hint: "Inputs, artifacts, and runtime bindings" },
      { label: "Usage", icon: "usage", to: "/workloads/usage", hint: "Runs, spend, and tenant limits" },
      { label: "Settings", icon: "account", to: "/workloads/settings", hint: "Tenant configuration and access" },
    ],
  },
  {
    label: "Advanced",
    items: [
      { label: "Templates", icon: "templates", to: "/workloads/templates", hint: "Approved launch shapes" },
      { label: "Policies", icon: "policies", to: "/workloads/policies", hint: "Guardrails and automatic placement" },
      { label: "GitOps", icon: "gitops", to: "/workloads/gitops", hint: "Manifests, KEDA, and provenance" },
      { label: "Team", icon: "team", to: "/workloads/team", hint: "Members and roles" },
      { label: "Audit", icon: "audit", to: "/workloads/audit", hint: "Governance journal" },
      { label: "Account", icon: "account", to: "/workloads/account", hint: "Identity and role" },
    ],
  },
];

export const NAV_LINKS = NAV_GROUPS.flatMap((group) => group.items);

export const OPERATIONS_NAV_GROUP = {
  label: "Operations",
  items: [
    { label: "Hosted capacity", icon: "clusters", to: "/workloads/hosted-requests", hint: "Assign and manage shared capacity" },
    { label: "Tenant guardrails", icon: "policies", to: "/workloads/operator-tenants", hint: "Monitor tenant usage and admission ceilings" },
  ],
};

// Operator access is server-authoritative. Tenant roles never enter this
// decision: callers pass true only after the authenticated operator probe has
// returned 200.
export function consoleNavGroups(operatorAvailable = false, tenantAvailable = true) {
  const groups = tenantAvailable ? NAV_GROUPS : [];
  return operatorAvailable ? [...groups, OPERATIONS_NAV_GROUP] : groups;
}

export function consoleNavLinks(operatorAvailable = false, tenantAvailable = true) {
  return consoleNavGroups(operatorAvailable, tenantAvailable).flatMap((group) => group.items);
}

const SECTION_PATHS = new Set([
  ...NAV_LINKS,
  ...OPERATIONS_NAV_GROUP.items,
  // Keep the pre-#95 history URL routable for existing bookmarks. Workloads
  // is now the active-first history surface, so this route is intentionally
  // absent from the primary navigation.
  { to: "/workloads/history" },
].map((item) => item.to));

// Every console page holds what it read for one tenant in component state, and
// React keeps that state when only a prop changes. So the instance key names
// the tenant: a key that survives a switch keeps the previous tenant's rows,
// its cursor, and its pending confirmation under the new tenant's header. The
// detail page names the record too, so moving between two workloads cannot
// carry one's error or cancel confirmation onto the other.
export function routeInstanceKey(route, tenantId) {
  const scope = `${route.name}:${tenantId || ""}`;
  if (route.name === "detail") return `${scope}:${route.workloadId}`;
  if (route.name === "new") return `${scope}:${route.templateId}`;
  return scope;
}

// The route name for a fixed page is its own second segment ("history",
// "audit", …); /workloads itself is "overview".
export function matchConsoleRoute(path) {
  if (path === "/workloads") return { name: "overview" };
  const single = path.match(/^\/workloads\/([^/]+)$/);
  if (single) {
    if (SECTION_PATHS.has(path)) return { name: single[1] };
    return { name: "detail", workloadId: decodeURIComponent(single[1]) };
  }
  const created = path.match(/^\/workloads\/new\/([^/]+)$/);
  if (created) return { name: "new", templateId: decodeURIComponent(created[1]) };
  return { name: "notFound" };
}

// Templates owns the launch form and Workloads owns a run's detail page, so
// each stays highlighted while the reader is inside it.
export function navActive(path, to) {
  const { name } = matchConsoleRoute(path);
  if (to === "/workloads/launch") return name === "launch" || name === "new";
  if (to === "/workloads") return name === "overview" || name === "history" || name === "detail";
  return to === `/workloads/${name}`;
}
