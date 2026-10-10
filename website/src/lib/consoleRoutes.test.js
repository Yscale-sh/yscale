import test from "node:test";
import assert from "node:assert/strict";
import { NAV_GROUPS, NAV_LINKS, consoleNavGroups, consoleNavLinks, matchConsoleRoute, navActive, routeInstanceKey } from "./consoleRoutes.js";

test("every nav item routes and every route is reachable from the nav", () => {
  for (const item of NAV_LINKS) {
    assert.ok(item.to, `${item.label} has no path`);
    assert.ok(item.icon, `${item.label} has no icon`);
    assert.equal(matchConsoleRoute(item.to).name === "notFound", false, `${item.to} does not route`);
    assert.equal(matchConsoleRoute(item.to).name === "detail", false, `${item.to} is read as a workload id`);
    assert.equal(navActive(item.to, item.to), true, `${item.to} does not highlight its own nav item`);
  }
  const labels = NAV_GROUPS.flatMap((group) => group.items.map((item) => item.label));
  for (const section of ["Workloads", "Launch", "Clusters", "Data", "Usage", "Settings", "Templates", "Policies", "GitOps", "Team", "Audit"]) {
    assert.ok(labels.includes(section), `${section} is missing from the console nav`);
  }
});

test("the GPU-first workspace and advanced sections resolve to their own routes", () => {
  for (const name of ["launch", "clusters", "data", "usage", "settings", "templates", "policies", "gitops", "team", "audit"]) {
    assert.deepEqual(matchConsoleRoute(`/workloads/${name}`), { name });
  }
  assert.deepEqual(matchConsoleRoute("/workloads"), { name: "overview" });
  assert.deepEqual(matchConsoleRoute("/workloads/history"), { name: "history" });
  assert.deepEqual(matchConsoleRoute("/workloads/templates"), { name: "templates" });
  assert.deepEqual(matchConsoleRoute("/workloads/account"), { name: "account" });
  assert.deepEqual(matchConsoleRoute("/workloads/hosted-requests"), { name: "hosted-requests" });
  assert.deepEqual(matchConsoleRoute("/workloads/operator-tenants"), { name: "operator-tenants" });
});

test("the Operations group is gated only by the operator capability", () => {
  assert.equal(consoleNavGroups(false).some((group) => group.label === "Operations"), false);
  assert.equal(consoleNavLinks(false).some((item) => item.to === "/workloads/hosted-requests"), false);
  assert.equal(consoleNavGroups(true).at(-1).label, "Operations");
  assert.equal(consoleNavLinks(true).find((item) => item.to === "/workloads/hosted-requests").label, "Hosted capacity");
  assert.equal(consoleNavLinks(true).find((item) => item.to === "/workloads/operator-tenants").label, "Tenant guardrails");
  assert.equal(navActive("/workloads/hosted-requests", "/workloads/hosted-requests"), true);
});

test("an operator without a tenant sees only global operations", () => {
  assert.deepEqual(consoleNavGroups(true, false).map((group) => group.label), ["Operations"]);
  assert.deepEqual(consoleNavLinks(true, false).map((item) => item.to), ["/workloads/hosted-requests", "/workloads/operator-tenants"]);
  assert.deepEqual(consoleNavLinks(false, false), []);
});

test("a segment that is not a section is a workload id", () => {
  assert.deepEqual(matchConsoleRoute("/workloads/wl_123"), { name: "detail", workloadId: "wl_123" });
  assert.deepEqual(matchConsoleRoute("/workloads/wl%20123"), { name: "detail", workloadId: "wl 123" });
  assert.deepEqual(matchConsoleRoute("/workloads/new/pytorch-training"), { name: "new", templateId: "pytorch-training" });
  assert.deepEqual(matchConsoleRoute("/workloads/audit/extra"), { name: "notFound" });
  assert.deepEqual(matchConsoleRoute("/account"), { name: "notFound" });
});

// Coverage is taken from the nav itself plus the two parameterised routes, so a
// page added to NAV_GROUPS is checked here the day it lands rather than when
// somebody remembers to extend a hand-kept list.
test("every console page gets a new instance when the tenant changes", () => {
  const routes = [
    ...NAV_LINKS.map((item) => matchConsoleRoute(item.to)),
    matchConsoleRoute("/workloads/hosted-requests"),
    matchConsoleRoute("/workloads/wl_123"),
    matchConsoleRoute("/workloads/new/container-job"),
  ];
  assert.equal(routes.length, NAV_LINKS.length + 3);
  for (const route of routes) {
    assert.notEqual(
      routeInstanceKey(route, "tenant-acme-research"),
      routeInstanceKey(route, "tenant-platform-lab"),
      `${route.name} reuses one instance across tenants`,
    );
    assert.equal(routeInstanceKey(route, "tenant-acme-research"), routeInstanceKey(route, "tenant-acme-research"));
    // An absent tenant is still its own instance, not the previous tenant's.
    assert.notEqual(routeInstanceKey(route, undefined), routeInstanceKey(route, "tenant-acme-research"));
  }
  // No two pages share an instance under one tenant.
  const keys = routes.map((route) => routeInstanceKey(route, "tenant-acme-research"));
  assert.equal(new Set(keys).size, keys.length);
});

test("the detail page gets a new instance per record as well as per tenant", () => {
  const one = matchConsoleRoute("/workloads/wl_one");
  const two = matchConsoleRoute("/workloads/wl_two");
  assert.notEqual(routeInstanceKey(one, "tenant-acme"), routeInstanceKey(two, "tenant-acme"));
  assert.notEqual(routeInstanceKey(one, "tenant-acme"), routeInstanceKey(one, "tenant-lab"));
  // The tenant and the record occupy their own fields: swapping them is not
  // the same instance, so a cancel can never inherit the other pairing.
  assert.notEqual(
    routeInstanceKey(matchConsoleRoute("/workloads/b"), "a"),
    routeInstanceKey(matchConsoleRoute("/workloads/a"), "b"),
  );
  // The launch form is scoped to its template for the same reason.
  assert.notEqual(
    routeInstanceKey(matchConsoleRoute("/workloads/new/container-job"), "tenant-acme"),
    routeInstanceKey(matchConsoleRoute("/workloads/new/pytorch-training"), "tenant-acme"),
  );
});

// The launch form reads the authorized namespaces once, when it mounts. That is
// only safe while a tenant switch is a remount: a shared instance would keep
// tenant A's selected namespace in a form now submitting as tenant B.
test("the launch form is a new instance per tenant, whatever the template", () => {
  for (const templateId of ["container-job", "pytorch-training", "node-capacity"]) {
    const route = matchConsoleRoute(`/workloads/new/${templateId}`);
    assert.equal(route.name, "new");
    assert.notEqual(
      routeInstanceKey(route, "tenant-acme-research"),
      routeInstanceKey(route, "tenant-platform-lab"),
      `${templateId} keeps one launch form across tenants`,
    );
  }
});

test("nav highlighting follows the section a reader is inside", () => {
  assert.equal(navActive("/workloads/new/container-job", "/workloads/launch"), true);
  assert.equal(navActive("/workloads/wl_123", "/workloads"), true);
  assert.equal(navActive("/workloads/audit", "/workloads"), false);
  assert.equal(navActive("/workloads/audit", "/workloads"), false);
  assert.equal(navActive("/workloads", "/workloads"), true);
  // A section path must never light up the workload detail nav item.
  for (const name of ["launch", "clusters", "data", "usage", "settings", "templates", "policies", "gitops", "team", "audit"]) {
    assert.equal(navActive(`/workloads/${name}`, `/workloads/${name}`), true);
    assert.equal(navActive(`/workloads/${name}`, "/workloads"), false);
  }
});
