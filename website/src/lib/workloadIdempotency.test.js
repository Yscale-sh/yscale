import test from "node:test";
import assert from "node:assert/strict";
import { createIdempotencyKeys, isIdempotencyKey, newIdempotencyKey } from "./workloadIdempotency.js";

const YAML_A = "kind: Workload\nmetadata:\n  name: \"trainer\"\n";
const YAML_B = "kind: Workload\nmetadata:\n  name: \"trainer-2\"\n";

test("a minted key satisfies central's contract and is not guessable", () => {
  const keys = new Set();
  for (let i = 0; i < 500; i += 1) {
    const key = newIdempotencyKey();
    assert.ok(isIdempotencyKey(key), `${key} is outside 8..255 printable non-space ASCII`);
    keys.add(key);
  }
  assert.equal(keys.size, 500, "keys repeated across mints");
});

test("the contract check rejects everything central would", () => {
  for (const bad of ["", "short7c", "has space", "tab\tkey0", "kéy01234", "ctrlkey", "x".repeat(256), null, undefined, 12345678]) {
    assert.equal(isIdempotencyKey(bad), false, `${String(bad)} should be rejected`);
  }
  assert.equal(isIdempotencyKey("x".repeat(255)), true);
  assert.equal(isIdempotencyKey("12345678"), true);
});

// The whole point: a double click, a page-level retry, or a proxy timeout on the
// same reviewed YAML is one run, not several.
test("one submission keeps one key however many times it is retried", () => {
  const keys = createIdempotencyKeys();
  const first = keys.keyFor(YAML_A);
  assert.equal(keys.keyFor(YAML_A), first);
  assert.equal(keys.keyFor(YAML_A), first);
  assert.ok(isIdempotencyKey(first));
});

test("editing the YAML and resubmitting is a new run, so it gets a new key", () => {
  const keys = createIdempotencyKeys();
  const first = keys.keyFor(YAML_A);
  const second = keys.keyFor(YAML_B);
  assert.notEqual(second, first);
  // Going back to the earlier YAML is still a later submission, never a replay
  // of the first one.
  assert.notEqual(keys.keyFor(YAML_A), first);
});

// The cluster never appears in the YAML, so a key scoped to the document alone
// would let a reader change destination and have central replay the first run
// against the first cluster.
test("changing the target cluster is a new logical run", () => {
  const keys = createIdempotencyKeys();
  const toA = keys.keyFor(YAML_A, "cluster-a");
  assert.equal(keys.keyFor(YAML_A, "cluster-a"), toA, "retrying one destination must reuse its key");

  const toB = keys.keyFor(YAML_A, "cluster-b");
  assert.notEqual(toB, toA);
  assert.ok(isIdempotencyKey(toB));
  // Going back to the first cluster is a later submission, never a replay.
  assert.notEqual(keys.keyFor(YAML_A, "cluster-a"), toA);
  // And the document still counts: same destination, edited YAML, new run.
  const edited = keys.keyFor(YAML_B, "cluster-a");
  assert.notEqual(edited, keys.keyFor(YAML_A, "cluster-a"));
});

test("switching between automode and explicit pin changes the idempotency scope", () => {
  const keys = createIdempotencyKeys();
  const auto = keys.keyFor(YAML_A, "");
  assert.equal(keys.keyFor(YAML_A, ""), auto);
  const pinned = keys.keyFor(YAML_A, "cluster-a");
  assert.notEqual(pinned, auto);
  assert.notEqual(keys.keyFor(YAML_A, ""), auto);
});

// The template selection rides as a header too, so a reader who reloads a
// changed catalog entry and submits the same document is launching from a
// different entry — not replaying the first run.
test("changing the template selection is a new logical run", () => {
  const keys = createIdempotencyKeys();
  const atV1 = keys.keyFor(YAML_A, "cluster-a", "container-job@1");
  assert.equal(keys.keyFor(YAML_A, "cluster-a", "container-job@1"), atV1, "retrying one selection must reuse its key");
  const atV2 = keys.keyFor(YAML_A, "cluster-a", "container-job@2");
  assert.notEqual(atV2, atV1);
  const other = keys.keyFor(YAML_A, "cluster-a", "pytorch-training@2");
  assert.notEqual(other, atV2);
  assert.ok(isIdempotencyKey(other));
});

test("a freshly mounted form is a deliberate new run even for identical YAML", () => {
  const first = createIdempotencyKeys().keyFor(YAML_A);
  const second = createIdempotencyKeys().keyFor(YAML_A);
  assert.notEqual(second, first);
});
