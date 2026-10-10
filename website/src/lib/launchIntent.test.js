import assert from "node:assert/strict";
import test from "node:test";
import { launchFormPath, launchTemplatesPath, parseLaunchClusterId, resolveLaunchClusterId } from "./launchIntent.js";

test("launch-cluster query parsing rejects malformed values", () => {
  assert.equal(parseLaunchClusterId("?cluster=hosted-01"), "hosted-01");
  assert.equal(parseLaunchClusterId("cluster=hosted-01"), "hosted-01");
  assert.equal(parseLaunchClusterId("/workloads/templates?cluster=hosted-01"), "hosted-01");
  assert.equal(parseLaunchClusterId("?cluster=has space"), "");
  assert.equal(parseLaunchClusterId(""), "");
  assert.equal(parseLaunchClusterId("?cluster=bad/slash"), "");
});

test("launch-cluster query serializes only valid ids and is deterministic", () => {
  assert.equal(launchTemplatesPath("hosted-01"), "/workloads/templates?cluster=hosted-01");
  assert.equal(launchFormPath("container-job", "hosted-01"), "/workloads/new/container-job?cluster=hosted-01");
  assert.equal(launchFormPath("container job", "hosted-01"), "/workloads/new/container%20job?cluster=hosted-01");
  assert.equal(launchTemplatesPath("bad id"), "/workloads/templates");
  assert.equal(launchFormPath("container-job", "bad id"), "/workloads/new/container-job");
});

test("launch cluster intent resolves stale and invalid request ids safely", () => {
  const eligible = [{ id: "hosted-01" }, { id: "cluster-02" }];

  assert.equal(
    resolveLaunchClusterId({
      eligibleClusters: eligible,
      requestedClusterId: "hosted-01",
      currentClusterId: "cluster-02",
      allowAutomatic: false,
    }),
    "hosted-01",
  );
  assert.equal(
    resolveLaunchClusterId({
      eligibleClusters: eligible,
      requestedClusterId: "no-longer-launchable",
      currentClusterId: "",
      allowAutomatic: false,
    }),
    "",
  );
  assert.equal(
    resolveLaunchClusterId({
      eligibleClusters: [{ id: "single-hosted" }],
      requestedClusterId: "gone-now",
      currentClusterId: "",
      allowAutomatic: false,
    }),
    "single-hosted",
  );
  assert.equal(
    resolveLaunchClusterId({
      eligibleClusters: [{ id: "hosted-01" }],
      requestedClusterId: "hosted-01",
      currentClusterId: "tenant-legacy",
      allowAutomatic: true,
    }),
    "hosted-01",
  );
});
