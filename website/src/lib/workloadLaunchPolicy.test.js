import assert from "node:assert/strict";
import test from "node:test";

import {
  canInspectRawWorkload,
  canOverrideTemplateRuntime,
  templateCatalogSummary,
  templateOwnedLaunchValues,
} from "./workloadLaunchPolicy.js";

const TEMPLATE = {
  nodeOnly: false,
  defaults: {
    image: "registry.example/approved:v4",
    command: ["python"],
    args: ["train.py"],
    size: "large",
    mode: "gpu",
    gpuKind: "l4",
    env: [{ name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "runtime", key: "database-url" } } }],
  },
};

test("only catalog builders see runtime overrides and raw workload YAML", () => {
  for (const role of ["owner", "admin"]) {
    assert.equal(canOverrideTemplateRuntime(role), true);
    assert.equal(canInspectRawWorkload(role), true);
  }
  for (const role of ["member", "viewer", "", undefined]) {
    assert.equal(canOverrideTemplateRuntime(role), false);
    assert.equal(canInspectRawWorkload(role), false);
  }
});

test("non-builders compose the exact template-owned runtime values", () => {
  const edited = {
    name: "reader-chosen-name",
    namespace: "team-a",
    image: "registry.example/unapproved:latest",
    command: ["sh"],
    args: ["-c", "bad"],
    size: "xlarge",
  };
  for (const role of ["member", "viewer"]) {
    const composed = templateOwnedLaunchValues(role, TEMPLATE, edited);
    assert.deepEqual(composed, {
      ...edited,
      image: TEMPLATE.defaults.image,
      command: TEMPLATE.defaults.command,
      args: TEMPLATE.defaults.args,
      env: TEMPLATE.defaults.env,
    });
    assert.notEqual(composed.command, TEMPLATE.defaults.command);
    assert.notEqual(composed.args, TEMPLATE.defaults.args);
    assert.notEqual(composed.env, TEMPLATE.defaults.env);
    assert.notEqual(composed.env[0].valueFrom.secretKeyRef, TEMPLATE.defaults.env[0].valueFrom.secretKeyRef);
    assert.equal(composed.name, "reader-chosen-name");
    assert.equal(composed.size, "xlarge");
  }
});

test("a catalog entry removed under an open form does not crash composition", () => {
  const values = { name: "still-open", image: "registry.example/old:v1" };
  assert.equal(templateOwnedLaunchValues("member", null, values), values);
});

test("builders keep per-run overrides and catalog cards summarize the approved shape", () => {
  const edited = { image: "registry.example/one-run:v5", command: ["run"], args: [] };
  assert.equal(templateOwnedLaunchValues("owner", TEMPLATE, edited), edited);
  assert.deepEqual(templateCatalogSummary(TEMPLATE), { runtime: "Container job", compute: "large · NVIDIA L4", references: "1 environment reference" });
  assert.deepEqual(templateCatalogSummary({ nodeOnly: true, defaults: { size: "medium", mode: "cpu" } }), {
    runtime: "Node capacity",
    compute: "medium · CPU",
    references: "0 environment references",
  });
});
