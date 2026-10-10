import test from "node:test";
import assert from "node:assert/strict";

import { canManageTenant, canMutateWorkload, canMutateWorkloads } from "./roles.js";

test("only the roles central lets submit are offered a mutating control", () => {
  for (const role of ["owner", "admin", "member"]) {
    assert.equal(canMutateWorkloads(role), true, `${role} may submit and cancel`);
  }
  // A viewer, an unreported role, and anything that merely looks like a role
  // are all read-only here; the console must not offer a control central
  // would refuse.
  for (const role of ["viewer", "", " member", "Owner", undefined, null]) {
    assert.equal(canMutateWorkloads(role), false, `${String(role)} is read-only`);
  }
});

test("only owners and admins receive tenant governance controls", () => {
  for (const role of ["owner", "admin"]) assert.equal(canManageTenant(role), true);
  for (const role of ["member", "viewer", "", undefined, null]) assert.equal(canManageTenant(role), false);
});

test("a member only receives controls for a workload they submitted", () => {
  const mine = { submitted_by: { kind: "account", account_id: "acct_me" } };
  const theirs = { submitted_by: { kind: "account", account_id: "acct_them" } };
  for (const role of ["owner", "admin"]) assert.equal(canMutateWorkload(role, "acct_me", theirs), true);
  assert.equal(canMutateWorkload("member", "acct_me", mine), true);
  assert.equal(canMutateWorkload("member", "acct_me", theirs), false);
  assert.equal(canMutateWorkload("member", "acct_me", {}), false);
  assert.equal(canMutateWorkload("viewer", "acct_me", mine), false);
});
