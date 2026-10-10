// What a tenant role may do, in one place. The console shell and the section
// pages both gate controls on it, and a second copy of the list is how the two
// start disagreeing about who may submit. Central is still the authority — this
// only decides whether the console offers the control at all.

const MUTATING_ROLES = new Set(["owner", "admin", "member"]);
const MANAGER_ROLES = new Set(["owner", "admin"]);

export function canMutateWorkloads(role) {
  return MUTATING_ROLES.has(role);
}

export function canMutateWorkload(role, accountId, workload) {
  if (MANAGER_ROLES.has(role)) return true;
  return role === "member" && !!accountId && workload?.submitted_by?.account_id === accountId;
}

export function canManageTenant(role) {
  return MANAGER_ROLES.has(role);
}
