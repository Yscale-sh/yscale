import test from "node:test";
import assert from "node:assert/strict";
import {
  CONTROLLER_TRIGGERED_LIMIT,
  PENDING_POD_ORIGIN,
  burstTargets,
  controllerTriggeredWorkloads,
  costBasisText,
  costCellLabel,
  costDetailRows,
  costObservation,
  freshnessLabel,
  isTerminal,
  lastActivityAt,
  limitRows,
  liveDetailRows,
  liveSpendObservation,
  liveTelemetryObservation,
  mergeWorkloadDetail,
  provenanceSummary,
  summarizeRuns,
  shouldPollWorkload,
  usageByMonth,
  usageByTemplate,
  workloadNamespaces,
  workloadReceiptDetails,
} from "./consoleData.js";
import { formatDate } from "./format.js";

// wl_a is still running, so central has nothing frozen for it; wl_b was torn
// down and carries the observation; wl_c is terminal and was never observed.
const records = [
  {
    id: "wl_a", status: "running", created_at: "2026-08-12T20:18:00Z", burst_id: "burst_1",
    submitted_by: { kind: "account", account_id: "acct_1" },
    authorization: { requested_namespace: "default", granted_namespace: "default", rule: "tenant-workload-namespaces", rule_version: "v1" },
    spec: { spec: { image: "img", size: "large", gpu: { kind: "a100", count: 1 }, backend: "linode", region: "us-east" } },
  },
  {
    id: "wl_b", status: "succeeded", created_at: "2026-08-11T16:04:00Z", burst_id: "burst_2",
    cost: {
      usd: 1.34, hourly_usd: 2.5, runtime_seconds: 1930, frozen_at: "2026-08-11T16:36:22Z",
      backend: "linode", burst_id: "burst_2", basis: "hourly_rate_x_runtime_to_reap_claim",
    },
    submitted_by: { kind: "account", account_id: "acct_2" },
    authorization: { rule: "tenant-workload-namespaces", rule_version: "v1" },
    spec: { spec: { image: "img", size: "small" } },
  },
  {
    id: "wl_c", status: "failed", created_at: "2026-07-30T09:00:00Z", burst_id: "", spec: { spec: { nodeOnly: true, size: "medium" } },
  },
];

test("run totals count every record and only the frozen cost observations", () => {
  const stats = summarizeRuns(records);
  // The estimate is a float sum of what central froze, so it is compared the
  // way the console renders it — to the cent — not by exact binary equality.
  assert.equal(stats.estimatedUSD.toFixed(2), "1.34");
  assert.deepEqual(
    { ...stats, estimatedUSD: 0 },
    { active: 1, succeeded: 1, failed: 1, estimatedUSD: 0, estimateRecords: 1, total: 3 },
  );
  assert.equal(isTerminal("CANCELLED"), true);
  assert.equal(isTerminal("provisioning"), false);
  assert.deepEqual(summarizeRuns([]), { active: 0, succeeded: 0, failed: 0, estimatedUSD: 0, estimateRecords: 0, total: 0 });
});

// Every shape here is a record central could send — or a legacy one it already
// has — and none of them may become a $0.00 the platform never reported.
test("a cost that is missing, malformed, or negative stays unknown instead of zero", () => {
  const unusable = [
    { id: "n1", status: "succeeded" },
    { id: "n2", status: "succeeded", cost: null },
    { id: "n3", status: "succeeded", cost: "0.42" },
    { id: "n4", status: "succeeded", cost: {} },
    { id: "n5", status: "succeeded", cost: { usd: null } },
    { id: "n6", status: "succeeded", cost: { usd: "not-a-number" } },
    { id: "n7", status: "succeeded", cost: { usd: Number.NaN } },
    { id: "n8", status: "succeeded", cost: { usd: Number.POSITIVE_INFINITY } },
    { id: "n9", status: "succeeded", cost: { usd: -0.5 } },
  ];
  for (const record of unusable) {
    assert.equal(costObservation(record), null, `${record.id} was treated as an observation`);
  }
  const stats = summarizeRuns(unusable);
  assert.equal(stats.estimatedUSD, 0);
  assert.equal(stats.estimateRecords, 0, "an unusable cost must not be counted as a recorded one");

  // A zero central actually froze is an observation and is counted.
  assert.deepEqual(costObservation({ cost: { usd: 0, backend: "fly" } }), { usd: 0, backend: "fly" });
  assert.equal(summarizeRuns([{ status: "succeeded", cost: { usd: 0 } }]).estimateRecords, 1);

  // A string central sends as a number is still a number it reported.
  assert.equal(costObservation({ cost: { usd: "0.25" } }).usd, 0.25);
});

test("a row says running or not recorded rather than a bare dash", () => {
  assert.equal(costCellLabel(records[0]), "running");
  assert.equal(costCellLabel(records[1]), "$1.34");
  assert.equal(costCellLabel(records[2]), "not recorded");
  assert.equal(costCellLabel({ status: "queued" }), "running");
  assert.equal(costCellLabel({ status: "provisioning", cost: { usd: -1 } }), "running");
  assert.equal(costCellLabel({ status: "cancelled" }), "not recorded");
  assert.equal(costCellLabel({ status: "cancelled", cost: { usd: "bad" } }), "not recorded");
  assert.equal(costCellLabel({}), "running");

  // A live burst carries an accrued number central reports, so the row prints
  // it instead of "running". A terminal record ignores spent_usd — the
  // contract says central only sends it while the burst is live, and any
  // trailing value on a terminal record is not the frozen truth.
  assert.equal(costCellLabel({ status: "running", spent_usd: 1.5 }), "$1.50");
  assert.equal(costCellLabel({ status: "queued", spent_usd: 0 }), "$0.00");
  assert.equal(costCellLabel({ status: "succeeded", spent_usd: 9.99, cost: { usd: 1 } }), "$1.00");
  // A negative or nonfinite live number is refused; the row must not print
  // one central never sent.
  assert.equal(costCellLabel({ status: "running", spent_usd: -0.1 }), "running");
  assert.equal(costCellLabel({ status: "running", spent_usd: Number.NaN }), "running");
});

test("the detail explains the number it prints", () => {
  assert.deepEqual(costDetailRows(records[1]), [
    { label: "Est. cost", value: "$1.34" },
    { label: "Quoted rate", value: "$2.50 / hour" },
    { label: "Measured runtime", value: "32m 10s" },
    { label: "Frozen", value: formatDate("2026-08-11T16:36:22Z") },
    { label: "Backend", value: "linode" },
    { label: "Burst receipt", value: "burst_2" },
    { label: "Basis", value: "quoted hourly rate × measured runtime, frozen when teardown was claimed" },
  ]);

  // Provenance central left out is named, never filled in.
  assert.deepEqual(costDetailRows({ status: "succeeded", cost: { usd: 3 } }), [
    { label: "Est. cost", value: "$3.00" },
    { label: "Quoted rate", value: "not reported" },
    { label: "Measured runtime", value: "not reported" },
    { label: "Frozen", value: "not reported" },
    { label: "Backend", value: "not reported" },
    { label: "Burst receipt", value: "not reported" },
    { label: "Basis", value: "not reported" },
  ]);

  // Without an observation the detail collapses to the same two honest states
  // the list rows use.
  assert.deepEqual(costDetailRows(records[0]), [{ label: "Est. cost", value: "running" }]);
  assert.deepEqual(costDetailRows(records[2]), [{ label: "Est. cost", value: "not recorded" }]);

  const long = costDetailRows({ status: "succeeded", cost: { usd: 12, runtime_seconds: 7830 } });
  assert.equal(long.find((row) => row.label === "Measured runtime").value, "2h 10m");
  const short = costDetailRows({ status: "succeeded", cost: { usd: 0.01, runtime_seconds: 42.4 } });
  assert.equal(short.find((row) => row.label === "Measured runtime").value, "42s");

  // A basis this console has not been taught is shown, not dropped and not
  // printed as raw underscored jargon.
  assert.equal(costBasisText("hourly_rate_x_runtime_to_reap_claim"), "quoted hourly rate × measured runtime, frozen when teardown was claimed");
  assert.equal(costBasisText("hourly_rate_x_runtime_to_provider_delete"), "quoted hourly rate × measured runtime, frozen at confirmed provider deletion");
  assert.equal(costBasisText("metered_gpu_seconds"), "metered gpu seconds");
  assert.equal(costBasisText(""), "not reported");
  assert.equal(costBasisText(undefined), "not reported");
});

test("placements group by the backend and region actually requested", () => {
  const targets = burstTargets(records);
  assert.equal(targets.length, 2);
  const [auto, linode] = [targets.find((t) => t.backend === "auto"), targets.find((t) => t.backend === "linode")];
  assert.deepEqual(
    { runs: auto.runs, region: auto.region, gpu: auto.gpu, bursts: auto.bursts },
    { runs: 2, region: "", gpu: false, bursts: 1 },
  );
  assert.deepEqual(
    { runs: linode.runs, region: linode.region, gpu: linode.gpu, active: linode.active },
    { runs: 1, region: "us-east", gpu: true, active: 1 },
  );
  assert.equal(lastActivityAt(records), "2026-08-12T20:18:00Z");
  assert.equal(lastActivityAt([]), null);
});

test("usage buckets by template and by the UTC month requested", () => {
  const byTemplate = usageByTemplate(records);
  assert.deepEqual(byTemplate.map((row) => [row.id, row.runs]).sort(), [
    ["container-job", 1], ["node-capacity", 1], ["pytorch-training", 1],
  ]);
  // Only the torn-down record carries an observation, so its buckets report one
  // estimate over the runs they hold and the buckets around it report none.
  const container = byTemplate.find((row) => row.id === "container-job");
  assert.deepEqual(
    { runs: container.runs, estimatedUSD: container.estimatedUSD.toFixed(2), estimateRecords: container.estimateRecords },
    { runs: 1, estimatedUSD: "1.34", estimateRecords: 1 },
  );
  assert.equal(byTemplate.find((row) => row.id === "pytorch-training").estimateRecords, 0);

  const byMonth = usageByMonth(records);
  assert.deepEqual(byMonth.map((row) => row.key), ["2026-08", "2026-07"]);
  assert.equal(byMonth[0].estimatedUSD.toFixed(2), "1.34");
  assert.deepEqual(
    { ...byMonth[0], estimatedUSD: 0 },
    { key: "2026-08", runs: 2, succeeded: 1, failed: 0, estimatedUSD: 0, estimateRecords: 1 },
  );
  // A record with no usable timestamp is bucketed, never dropped.
  const withUnknown = usageByMonth([...records, { id: "wl_d", status: "queued", created_at: "" }]);
  assert.equal(withUnknown.reduce((sum, row) => sum + row.runs, 0), 4);
  assert.ok(withUnknown.some((row) => row.key === "unknown"));
});

test("provenance counts only the records that carry a decision", () => {
  const summary = provenanceSummary(records);
  assert.equal(summary.total, 3);
  assert.equal(summary.recorded, 2);
  assert.equal(summary.submitters, 2);
  assert.deepEqual(summary.rules, [{ key: "tenant-workload-namespaces@v1", rule: "tenant-workload-namespaces", version: "v1", runs: 2 }]);
});

// The order central reports is the order the form offers, because the first
// entry is the one an unqualified submission is decided against.
test("authorized namespaces keep central's order and never come back empty", () => {
  assert.deepEqual(workloadNamespaces({ workload_namespaces: ["ml-team-a", "default", "ml-team-b"] }), ["ml-team-a", "default", "ml-team-b"]);
  assert.deepEqual(workloadNamespaces({ workload_namespaces: ["platform-lab"] }), ["platform-lab"]);

  // A tenant summary from a central that predates the field, or a shape it
  // could never send, still leaves the launch form something to offer.
  for (const tenant of [undefined, {}, { workload_namespaces: null }, { workload_namespaces: [] }, { workload_namespaces: "default" }, { workload_namespaces: [""] }, { workload_namespaces: [null, 7] }]) {
    assert.deepEqual(workloadNamespaces(tenant), ["default"], `${JSON.stringify(tenant)} did not fall back`);
  }

  // A repeated or padded entry would otherwise become a second option that
  // submits the same namespace, or an option no <select> value can match.
  assert.deepEqual(workloadNamespaces({ workload_namespaces: ["default", "ml-team-a", "default"] }), ["default", "ml-team-a"]);
  assert.deepEqual(workloadNamespaces({ workload_namespaces: [" ml-team-a ", 42, "", "default"] }), ["ml-team-a", "default"]);
});

test("limits render what the tenant summary reported, and zero means no ceiling", () => {
  const rows = limitRows({ max_concurrent_bursts: 8, max_hourly_usd: 0, unknown_future_key: 3, plan_name: "enterprise" });
  assert.deepEqual(rows.map((row) => [row.key, row.label, row.unit, row.unlimited]), [
    ["max_concurrent_bursts", "Concurrent bursts", "count", false],
    ["max_hourly_usd", "Hourly spend ceiling", "usd", true],
    ["unknown_future_key", "unknown future key", "count", false],
  ]);
  assert.deepEqual(limitRows(undefined), []);
  assert.deepEqual(limitRows({}), []);
});

// Everything here except the exact string is a shape a human can produce by
// hand, so anything but an exact match would attribute to a controller a
// submission nobody's controller made.
test("only an exact pending-pod origin counts as controller-triggered", () => {
  assert.equal(PENDING_POD_ORIGIN, "pending-pod");

  const notTriggered = [
    { id: "x1", created_at: "2026-08-12T00:00:00Z" },
    { id: "x2", created_at: "2026-08-12T00:00:00Z", submission_origin: null },
    { id: "x3", created_at: "2026-08-12T00:00:00Z", submission_origin: "" },
    { id: "x4", created_at: "2026-08-12T00:00:00Z", submission_origin: "Pending-Pod" },
    { id: "x5", created_at: "2026-08-12T00:00:00Z", submission_origin: "pending-pod " },
    { id: "x6", created_at: "2026-08-12T00:00:00Z", submission_origin: "pending_pod" },
    { id: "x7", created_at: "2026-08-12T00:00:00Z", submission_origin: "pending-pod-scaler" },
    { id: "x8", created_at: "2026-08-12T00:00:00Z", submission_origin: "console" },
    // A node-only capacity request submitted by hand from the launch form is
    // the same shape as one a controller triggered, minus the origin.
    { id: "x9", created_at: "2026-08-12T00:00:00Z", spec: { spec: { nodeOnly: true } }, submitted_by: { kind: "cluster" } },
    // The legacy record: written before central reported the field at all.
    { id: "x10", created_at: "2026-07-01T00:00:00Z", burst_id: "burst_legacy", spec: { spec: { nodeOnly: true } } },
    null,
    undefined,
  ];
  assert.deepEqual(controllerTriggeredWorkloads(notTriggered), []);

  const mixed = [...notTriggered, { id: "keda", created_at: "2026-08-12T00:00:00Z", submission_origin: PENDING_POD_ORIGIN }];
  assert.deepEqual(controllerTriggeredWorkloads(mixed).map((row) => row.id), ["keda"]);

  // A shape the console could be handed but central never sends must not throw
  // its way onto the page.
  for (const input of [undefined, null, "", 0, {}]) {
    assert.deepEqual(controllerTriggeredWorkloads(input), [], `${JSON.stringify(input)} was not handled`);
  }
});

test("controller-triggered records come back newest first and bounded", () => {
  const records = [
    { id: "older", created_at: "2026-08-09T10:00:00Z", submission_origin: PENDING_POD_ORIGIN },
    { id: "newest", created_at: "2026-08-14T10:00:00Z", submission_origin: PENDING_POD_ORIGIN },
    { id: "middle", created_at: "2026-08-11T10:00:00Z", submission_origin: PENDING_POD_ORIGIN },
    // No usable timestamp: kept, because the origin is the evidence, but it
    // cannot claim to be recent so it sorts last.
    { id: "undated", created_at: "", submission_origin: PENDING_POD_ORIGIN },
  ];
  assert.deepEqual(controllerTriggeredWorkloads(records).map((row) => row.id), ["newest", "middle", "older", "undated"]);
  assert.deepEqual(controllerTriggeredWorkloads(records, 2).map((row) => row.id), ["newest", "middle"]);
  assert.deepEqual(controllerTriggeredWorkloads(records, 0), []);

  // The default bound applies to a limit no caller should have sent, so a bad
  // argument shortens the list rather than printing the whole history.
  for (const limit of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, "3", null]) {
    assert.equal(controllerTriggeredWorkloads(records, limit).length, 4, `${String(limit)} was not rejected`);
  }

  const many = Array.from({ length: 40 }, (_, index) => ({
    id: `wl_${index}`,
    created_at: `2026-08-${String((index % 28) + 1).padStart(2, "0")}T00:00:00Z`,
    submission_origin: PENDING_POD_ORIGIN,
  }));
  assert.equal(CONTROLLER_TRIGGERED_LIMIT, 6, "the panel prints this bound as copy");
  assert.equal(controllerTriggeredWorkloads(many).length, CONTROLLER_TRIGGERED_LIMIT);
  assert.equal(controllerTriggeredWorkloads(many)[0].created_at, "2026-08-28T00:00:00Z");

  // The caller's array is read, never reordered underneath it.
  const source = [...records];
  controllerTriggeredWorkloads(source);
  assert.deepEqual(source.map((row) => row.id), ["older", "newest", "middle", "undated"]);
});

test("workloadReceiptDetails formats requested vs observed contract and makes compute and artifact results separately legible", () => {
  // Successful compute & artifact export
  const successRecord = {
    status: "succeeded",
    outcome: {
      compute: { result: "succeeded" },
      artifacts: { result: "succeeded", objects_uploaded: 14, bytes_uploaded: 1610612736 },
    },
    spec: { spec: { data: { pull_before_run_inputs: 1, input_size_hint_gb: 48, push_after_run_outputs: 1, output_max_upload_gb: 10 } } },
  };
  const successDetails = workloadReceiptDetails(successRecord);
  assert.equal(successDetails.requestedInputs, "1 object input");
  assert.equal(successDetails.inputLimit, "48 GiB");
  assert.equal(successDetails.requestedOutputs, "1 object output");
  assert.equal(successDetails.uploadLimit, "10 GiB");
  assert.equal(successDetails.computeStatusText, "succeeded");
  assert.equal(successDetails.computeBadgeTone, "ok");
  assert.equal(successDetails.computeLabel, "succeeded");
  assert.equal(successDetails.artifactStatusText, "succeeded");
  assert.equal(successDetails.artifactBadgeTone, "ok");
  assert.equal(successDetails.hasArtifactOutcome, true);
  assert.equal(successDetails.objectsUploaded, 14);
  assert.equal(successDetails.bytesUploaded, 1610612736);

  // Compute succeeded, artifact export failed
  const exportFailedRecord = {
    status: "failed",
    outcome: {
      compute: { result: "succeeded" },
      artifacts: { result: "failed", reason: "an artifact upload to the object store failed", objects_uploaded: 0, bytes_uploaded: 0 },
    },
    spec: { spec: { data: { pull_before_run_inputs: 1, input_size_hint_gb: 5, push_after_run_outputs: 1, output_max_upload_gb: 2 } } },
  };
  const failedDetails = workloadReceiptDetails(exportFailedRecord);
  assert.equal(failedDetails.computeStatusText, "succeeded");
  assert.equal(failedDetails.computeBadgeTone, "ok");
  assert.equal(failedDetails.artifactStatusText, "failed");
  assert.equal(failedDetails.artifactBadgeTone, "danger");
  assert.equal(failedDetails.artifactLabel, "failed · an artifact upload to the object store failed");
  assert.equal(failedDetails.hasArtifactOutcome, true);
  assert.equal(failedDetails.objectsUploaded, 0);
  assert.equal(failedDetails.bytesUploaded, 0);

  // Pending / in-flight record
  const runningRecord = {
    status: "running",
    spec: { spec: { data: { pull_before_run_inputs: 1, push_after_run_outputs: 1 } } },
  };
  const runningDetails = workloadReceiptDetails(runningRecord);
  assert.equal(runningDetails.computeStatusText, "in flight");
  assert.equal(runningDetails.artifactStatusText, "not observed yet");
  assert.equal(runningDetails.hasArtifactOutcome, false);

  // Legacy record (terminal without outcome)
  const legacyRecord = {
    status: "succeeded",
    spec: { spec: { data: { pull_before_run_inputs: 1 } } },
  };
  const legacyDetails = workloadReceiptDetails(legacyRecord);
  assert.equal(legacyDetails.computeStatusText, "no outcome recorded");
  assert.equal(legacyDetails.artifactStatusText, "not requested");
  assert.equal(legacyDetails.hasArtifactOutcome, false);

  // Outcome-only record (no request spec.data)
  const outcomeOnlyRecord = {
    status: "succeeded",
    outcome: { compute: { result: "succeeded" }, artifacts: { result: "skipped" } },
  };
  const outcomeOnlyDetails = workloadReceiptDetails(outcomeOnlyRecord);
  assert.equal(outcomeOnlyDetails.hasRequestSpec, false);
  assert.equal(outcomeOnlyDetails.requestedInputs, "None requested");
  assert.equal(outcomeOnlyDetails.computeStatusText, "succeeded");
  assert.equal(outcomeOnlyDetails.artifactStatusText, "skipped");

  const observedWithoutResult = workloadReceiptDetails({
    status: "succeeded",
    outcome: { compute: { reason: "legacy observer" }, artifacts: { objects_uploaded: 3, bytes_uploaded: 12 } },
  });
  assert.equal(observedWithoutResult.computeStatusText, "observed");
  assert.equal(observedWithoutResult.artifactStatusText, "observed");

  const completeAlias = workloadReceiptDetails({
    status: "succeeded",
    outcome: { compute: { result: "complete" }, artifacts: { result: "complete" } },
  });
  assert.equal(completeAlias.computeBadgeTone, "ok");
  assert.equal(completeAlias.artifactBadgeTone, "ok");

  const normalizedCounts = workloadReceiptDetails({
    status: "running",
    spec: { spec: { data: { pull_before_run_inputs: "1", push_after_run_outputs: "2" } } },
  });
  assert.equal(normalizedCounts.requestedInputs, "1 object input");
  assert.equal(normalizedCounts.requestedOutputs, "2 object outputs");
  assert.equal(workloadReceiptDetails({ spec: { spec: { data: { pull_before_run_inputs: 1.5 } } } }).requestedInputs, "None requested");
});

test("liveSpendObservation follows provider lifetime and fails closed on bad numbers", () => {
  assert.deepEqual(liveSpendObservation({ status: "running", spent_usd: 2.34 }), { usd: 2.34 });
  assert.deepEqual(liveSpendObservation({ status: "queued", spent_usd: 0.5 }), { usd: 0.5 });
  // Compute completion does not prove provider absence or frozen cost.
  assert.deepEqual(liveSpendObservation({ status: "succeeded", spent_usd: 5 }), { usd: 5 });
  assert.deepEqual(liveSpendObservation({ status: "cancelled", spent_usd: 0 }), { usd: 0 });
  // Fail closed on missing, malformed, negative, and nonfinite spend.
  for (const bad of [undefined, null, "", "0.5", -0.01, Number.NaN, Number.POSITIVE_INFINITY, "not-a-number"]) {
    assert.equal(liveSpendObservation({ status: "running", spent_usd: bad }), null, `${bad} was accepted`);
  }
  assert.equal(liveSpendObservation({ status: "running" }), null);
  assert.equal(liveSpendObservation(null), null);
});

test("spend remains visible through cleanup but never overrides frozen cost or deletion proof", () => {
  const deletedAt = "2026-08-25T20:30:00Z";
  for (const status of ["running", "succeeded", "failed", "cancelled"]) {
    for (const state of ["queued", "deleting", "retrying", "manual_attention", "unknown"]) {
      const workload = { status, spent_usd: 1.25, cleanup: { state, deleted_at: deletedAt } };
      assert.deepEqual(liveSpendObservation(workload), { usd: 1.25 });
      assert.equal(costCellLabel(workload), status === "running" ? "$1.25" : "$1.25 · accruing");
      assert.equal(liveDetailRows(workload)[0].value, "$1.25");
    }
    const absent = { status, spent_usd: 9.99, cleanup: { state: "terminated", deleted_at: deletedAt } };
    assert.equal(liveSpendObservation(absent), null);
    assert.equal(costCellLabel(absent), "not recorded");
    const frozen = { status, spent_usd: 9.99, cost: { usd: 0.75 } };
    assert.equal(liveSpendObservation(frozen), null);
    assert.equal(costCellLabel(frozen), "$0.75");
  }
  for (const deleted_at of [undefined, null, "invalid"]) {
    assert.deepEqual(liveSpendObservation({ status: "succeeded", spent_usd: 1.25, cleanup: { state: "terminated", deleted_at } }), { usd: 1.25 });
  }
});

test("detail polling continues through teardown and receipt repair, then stops", () => {
  assert.equal(shouldPollWorkload(null), false);
  assert.equal(shouldPollWorkload({ status: "running" }), true);
  assert.equal(shouldPollWorkload({ status: "failed" }), false);
  const workload = { status: "succeeded", burst_id: "burst_cleanup" };
  for (const state of [undefined, "queued", "deleting", "retrying", "manual_attention", "unknown", "terminated"]) {
    assert.equal(shouldPollWorkload({ ...workload, cleanup: { state } }), true);
  }
  const absent = { ...workload, cleanup: { state: "terminated", deleted_at: "2026-08-25T20:30:00Z" } };
  assert.equal(shouldPollWorkload(absent), true, "provider gone but receipt repair still pending");
  assert.equal(shouldPollWorkload({ ...absent, cost: { usd: 0.75 } }), false);
  assert.equal(shouldPollWorkload({ ...absent, cost: { usd: 0 } }), false, "observed zero is a receipt");
  assert.equal(shouldPollWorkload({ ...absent, cost: { usd: "bad" } }), true);
  assert.equal(shouldPollWorkload({ ...workload, cost: { usd: 0.75 } }), true, "cleanup evidence is still unknown");
});

test("a fresh detail snapshot clears stale list spend, cleanup, cost, and telemetry", () => {
  const list = {
    id: "wl_cleanup", status: "running", spec: { kind: "Workload" },
    spent_usd: 1.25, gpu_util_percent: 90, last_heartbeat_at: "2026-08-25T20:30:00Z",
    cost: { usd: 0.75 }, cleanup: { state: "terminated", deleted_at: "2026-08-25T20:30:00Z" },
  };
  const detail = { id: "wl_cleanup", status: "succeeded", burst_id: "burst_cleanup" };
  const merged = mergeWorkloadDetail(list, detail);
  for (const field of ["spent_usd", "gpu_util_percent", "last_heartbeat_at", "cost", "cleanup"]) {
    assert.equal(Object.hasOwn(merged, field), false, `${field} revived from stale list`);
  }
  assert.equal(merged.spec, list.spec);
  assert.equal(liveSpendObservation(merged), null);
  assert.equal(mergeWorkloadDetail(list, null), list);
  assert.equal(mergeWorkloadDetail(null, detail), detail);
  const current = mergeWorkloadDetail(list, { ...detail, spent_usd: 0, cleanup: { state: "retrying" } });
  assert.deepEqual(liveSpendObservation(current), { usd: 0 });
  assert.equal(current.cleanup.state, "retrying");
});

test("liveTelemetryObservation distinguishes absent, malformed, partial, and observed 0%", () => {
  const observed = liveTelemetryObservation({ gpu_util_percent: 87, last_heartbeat_at: "2026-08-25T20:30:00Z" });
  assert.equal(observed.present, true);
  assert.equal(observed.percent, 87);
  assert.equal(observed.heartbeatAt, "2026-08-25T20:30:00Z");

  // Observed 0% is a legitimate signal, not "absent": the utilization is
  // present and the heartbeat is fresh.
  const idle = liveTelemetryObservation({ gpu_util_percent: 0, last_heartbeat_at: "2026-08-25T20:30:00Z" });
  assert.equal(idle.present, true);
  assert.equal(idle.percent, 0);

  // Absent — neither half of the pair — is not the same state as malformed.
  const absent = liveTelemetryObservation({});
  assert.equal(absent.present, false);
  assert.equal(absent.malformed, false);

  // A partial pair, an out-of-range percent, and an unparseable timestamp all
  // fail closed.
  for (const partial of [
    { gpu_util_percent: 12 },
    { last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: 101, last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: -0.1, last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: Number.NaN, last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: "50", last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: "not-a-number", last_heartbeat_at: "2026-08-25T20:30:00Z" },
    { gpu_util_percent: 50, last_heartbeat_at: "not-a-timestamp" },
    { gpu_util_percent: 50, last_heartbeat_at: "2026-08-25 20:30:00Z" },
    { gpu_util_percent: 50, last_heartbeat_at: "" },
  ]) {
    const result = liveTelemetryObservation(partial);
    assert.equal(result.present, false, `${JSON.stringify(partial)} was accepted`);
    assert.equal(result.malformed, true, `${JSON.stringify(partial)} was not flagged malformed`);
  }
});

test("freshnessLabel returns a coarse relative label anchored to a caller-supplied clock", () => {
  const now = new Date("2026-08-25T20:31:00Z");
  assert.equal(freshnessLabel("2026-08-25T20:30:55Z", now), "5s ago");
  assert.equal(freshnessLabel("2026-08-25T20:29:00Z", now), "2m ago");
  assert.equal(freshnessLabel("2026-08-25T18:15:00Z", now), "2h 16m ago");
  // A negative delta from clock skew clamps at 0 rather than printing "-5s".
  assert.equal(freshnessLabel("2026-08-25T20:31:30Z", now), "0s ago");
  // Missing or unparseable timestamps read as "not reported", not "Invalid".
  assert.equal(freshnessLabel(undefined, now), "not reported");
  assert.equal(freshnessLabel("not-a-timestamp", now), "not reported");
});

test("liveDetailRows prints accrued spend, coarse GPU utilization, and freshness with fail-closed labels", () => {
  const now = new Date("2026-08-25T20:31:00Z");
  const rows = liveDetailRows({
    status: "running",
    spent_usd: 2.34,
    gpu_util_percent: 87,
    last_heartbeat_at: "2026-08-25T20:30:45Z",
  }, now);
  assert.deepEqual(rows, [
    { label: "Accrued spend", value: "$2.34" },
    { label: "GPU utilization", value: "87%" },
    { label: "Telemetry updated", value: "15s ago" },
  ]);

  // 0% is observed idle rather than the same "not reported" as absent data.
  const idle = liveDetailRows({
    status: "running",
    spent_usd: 0.05,
    gpu_util_percent: 0,
    last_heartbeat_at: "2026-08-25T20:30:55Z",
  }, now);
  assert.deepEqual(idle.find((row) => row.label === "GPU utilization").value, "0% (idle)");
  assert.deepEqual(idle.find((row) => row.label === "Telemetry updated").value, "5s ago");

  // Absent telemetry: spend can still print but GPU + freshness read as "not
  // reported", never as $0 or 0%.
  const noTelemetry = liveDetailRows({ status: "running", spent_usd: 1.25 }, now);
  assert.deepEqual(noTelemetry, [
    { label: "Accrued spend", value: "$1.25" },
    { label: "GPU utilization", value: "not reported" },
    { label: "Telemetry updated", value: "not reported" },
  ]);

  // Malformed / partial telemetry is labelled distinctly so the reader knows
  // the data was bad rather than merely absent.
  const bad = liveDetailRows({
    status: "running",
    spent_usd: 1.25,
    gpu_util_percent: 150,
    last_heartbeat_at: "2026-08-25T20:30:55Z",
  }, now);
  assert.deepEqual(bad.find((row) => row.label === "GPU utilization").value, "not reported · signal malformed");
  assert.deepEqual(bad.find((row) => row.label === "Telemetry updated").value, "not reported · signal malformed");

  // Nothing at all: every row is honest about the absence, and no $0.00 is
  // invented in place of the missing accrued number.
  const nothing = liveDetailRows({ status: "running" }, now);
  assert.deepEqual(nothing, [
    { label: "Accrued spend", value: "not reported" },
    { label: "GPU utilization", value: "not reported" },
    { label: "Telemetry updated", value: "not reported" },
  ]);
});
