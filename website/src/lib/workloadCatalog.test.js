import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ApiError } from "./apiError.js";
import { previewPutTemplateCatalog, previewTemplateCatalog, resetDevPreview } from "./devPreview.js";
import {
  catalogTemplateById,
  fetchTemplateCatalog,
  managedRuntimeBindingEnv,
  normalizeTemplateEnv,
  normalizeTemplateCatalog,
  putTemplateCatalog,
  templateCatalogBody,
  templateDraftChanged,
  templateDraftErrors,
  templateDraftRow,
  templateReceipt,
  templateSelectionError,
  templateSourceLabel,
  versionedTemplateDraft,
} from "./workloadCatalog.js";
import { initialWorkloadForm, workloadYAML } from "./workloadTemplates.js";

// The client is exercised against a stub fetch: these tests are about the shape
// it accepts, the request it builds, and the selections it refuses.
function stubFetch(handler) {
  const calls = [];
  const previous = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({ url, init });
    return handler(url, init);
  };
  return {
    calls,
    restore() { globalThis.fetch = previous; },
  };
}

function jsonResponse(status, body) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

const CPU_ENTRY = {
  id: "container-job",
  version: 2,
  mark: "01",
  title: "Generic container job",
  kind: "Run-once job",
  description: "Run an OCI image to completion.",
  nodeOnly: false,
  disabled: false,
  defaults: { name: "container-job", image: "docker.io/library/busybox:1.36", size: "small", mode: "cpu" },
};

const GPU_ENTRY = {
  id: "pytorch-training",
  version: 5,
  title: "PyTorch training job",
  kind: "Run-once GPU job",
  nodeOnly: false,
  disabled: true,
  defaults: { name: "pytorch-train", image: "docker.io/pytorch/pytorch:2.4.1", size: "large", mode: "gpu", gpuKind: "rtx4000ada" },
};

const CATALOG = {
  tenant_id: "cust_42",
  catalog_revision: "7",
  source: "tenant",
  role: "owner",
  changed: true,
  templates: [CPU_ENTRY, GPU_ENTRY],
};

beforeEach(resetDevPreview);

test("a catalog normalizes into the shape the launch form already speaks", () => {
  const catalog = normalizeTemplateCatalog(CATALOG);
  assert.equal(catalog.tenantId, "cust_42");
  assert.equal(catalog.catalogRevision, "7");
  assert.equal(catalog.source, "tenant");
  assert.equal(catalog.role, "owner");
  assert.equal(catalog.changed, true);
  assert.equal(catalog.templates.length, 2);
  const [cpu, gpu] = catalog.templates;
  assert.equal(cpu.nodeOnly, false);
  assert.equal(cpu.disabled, false);
  assert.deepEqual(cpu.defaults, { name: "container-job", image: "docker.io/library/busybox:1.36", size: "small", mode: "cpu", command: [], args: [], env: [] });
  assert.equal(gpu.disabled, true);
  assert.equal(gpu.defaults.gpuKind, "rtx4000ada");
  // The badge is optional on the wire, so an entry without one still renders.
  assert.equal(gpu.mark, "PY");
  // An integer revision is as valid an answer as a string one.
  assert.equal(normalizeTemplateCatalog({ ...CATALOG, catalog_revision: 12 }).catalogRevision, "12");
});

test("catalog recipes round-trip exactly without sharing token arrays", () => {
  const source = {
    ...CPU_ENTRY,
    defaults: { ...CPU_ENTRY.defaults, command: ["sh"], args: ["-c", "echo one two"] },
  };
  const [normalized] = normalizeTemplateCatalog({ ...CATALOG, templates: [source] }).templates;
  const draft = templateDraftRow(normalized);
  const body = templateCatalogBody([draft], "7");
  assert.deepEqual(body.templates[0].defaults.command, ["sh"]);
  assert.deepEqual(body.templates[0].defaults.args, ["-c", "echo one two"]);
  source.defaults.command[0] = "changed";
  normalized.defaults.args[1] = "changed";
  assert.deepEqual(draft.defaults.command, ["sh"]);
  assert.deepEqual(body.templates[0].defaults.args, ["-c", "echo one two"]);
});

test("environment references normalize and round-trip without sharing nested objects", () => {
  const env = [
    { name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "app-runtime", key: "database-url" } } },
    { name: "LOG_LEVEL", valueFrom: { configMapKeyRef: { name: "app-settings", key: "log.level" } } },
  ];
  const normalizedEnv = normalizeTemplateEnv(env, "container-job");
  const source = { ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, env } };
  const [normalized] = normalizeTemplateCatalog({ ...CATALOG, templates: [source] }).templates;
  const draft = templateDraftRow(normalized);
  const body = templateCatalogBody([draft], "7");
  assert.deepEqual(body.templates[0].defaults.env, normalizedEnv);
  env[0].valueFrom.secretKeyRef.key = "changed";
  normalized.defaults.env[1].valueFrom.configMapKeyRef.name = "changed";
  assert.equal(draft.defaults.env[0].valueFrom.secretKeyRef.key, "database-url");
  assert.equal(body.templates[0].defaults.env[1].valueFrom.configMapKeyRef.name, "app-settings");
});

test("environment references reject duplicates, literals, unknown shapes, and backend bounds", () => {
  const secret = { name: "TOKEN", valueFrom: { secretKeyRef: { name: "app-runtime", key: "token" } } };
  assert.throws(() => normalizeTemplateEnv([secret, structuredClone(secret)]), /duplicate environment variable/i);
  assert.throws(() => normalizeTemplateEnv([{ name: "TOKEN", value: "plaintext" }]), /unknown environment reference shape/i);
  assert.throws(() => normalizeTemplateEnv([{ name: "TOKEN", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } }]), /invalid reference source/i);
  assert.throws(() => normalizeTemplateEnv(Array.from({ length: 65 }, (_, index) => ({
    name: `ENV_${index}`,
    valueFrom: { configMapKeyRef: { name: "app-settings", key: `key-${index}` } },
  }))), /more than 64/i);
  assert.throws(() => normalizeTemplateEnv([{ name: "bad-name", valueFrom: { secretKeyRef: { name: "app-runtime", key: "token" } } }]), /invalid environment variable/i);
});

test("environment references accept Kubernetes keys used by common Secrets", () => {
  assert.deepEqual(normalizeTemplateEnv([{
    name: "DOCKER_CONFIG",
    valueFrom: { secretKeyRef: { name: "registry-auth", key: ".dockerconfigjson" } },
  }]), [{
    name: "DOCKER_CONFIG",
    valueFrom: { secretKeyRef: { name: "registry-auth", key: ".dockerconfigjson" } },
  }]);
});

test("managed binding selection emits only the fixed Secret name and selected key", () => {
  const entry = managedRuntimeBindingEnv("DATABASE_URL", "DATABASE_URL");
  assert.deepEqual(entry, { name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "yscale-runtime-bindings", key: "DATABASE_URL" } } });
  assert.equal(JSON.stringify(entry).includes("value\""), false);
  assert.throws(() => managedRuntimeBindingEnv("DATABASE_URL", "lowercase"), ApiError);
});

test("managed binding references must still exist in the loaded safe inventory", () => {
  const row = templateDraftRow({
    ...CPU_ENTRY,
    defaults: { ...CPU_ENTRY.defaults, env: [managedRuntimeBindingEnv("DATABASE_URL", "DATABASE_URL")] },
  });
  assert.deepEqual(templateDraftErrors([row], [{ key: "DATABASE_URL" }])[0], {});
  assert.match(templateDraftErrors([row], [{ key: "OTHER_KEY" }])[0].env[0].key, /still exists/);
  assert.deepEqual(
    row.defaults.env[0],
    { name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "yscale-runtime-bindings", key: "DATABASE_URL" } } },
    "a stale managed reference remains explicit instead of being rewritten as an arbitrary Secret",
  );
});

test("node-only drafts and canonical payloads clear environment references", () => {
  const withEnv = {
    ...CPU_ENTRY,
    defaults: { ...CPU_ENTRY.defaults, env: [{ name: "TOKEN", valueFrom: { secretKeyRef: { name: "runtime", key: "token" } } }] },
  };
  const draft = templateDraftRow({ ...withEnv, nodeOnly: true });
  assert.deepEqual(draft.defaults.env, []);
  assert.doesNotMatch(JSON.stringify(templateCatalogBody([draft], "7")), /TOKEN|secretKeyRef/);
});

test("catalog recipes match central's token, byte, and control bounds", () => {
  const row = templateDraftRow(CPU_ENTRY);
  const errorsFor = (field, value) => templateDraftErrors([{ ...row, defaults: { ...row.defaults, [field]: value } }])[0][field];
  assert.match(errorsFor("command", Array.from({ length: 17 }, () => "x")), /at most 16/);
  assert.match(errorsFor("command", ["ok", ""]), /empty line/);
  assert.match(errorsFor("args", ["bad\u0000token"]), /control/);
  assert.match(errorsFor("args", ["é".repeat(129)]), /256 bytes/);
  assert.match(errorsFor("args", Array.from({ length: 9 }, () => "x".repeat(256))), /2,048 bytes/);
  assert.throws(() => normalizeTemplateCatalog({
    ...CATALOG,
    templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, command: [""] } }],
  }), ApiError);
});

test("node-only catalog entries never carry an execution recipe", () => {
  assert.throws(() => normalizeTemplateCatalog({
    ...CATALOG,
    templates: [{ ...CPU_ENTRY, id: "node-capacity", nodeOnly: true, defaults: { ...CPU_ENTRY.defaults, image: "", command: ["sh"] } }],
  }), ApiError);
  const row = templateDraftRow({ ...CPU_ENTRY, id: "node-capacity", nodeOnly: true, defaults: { ...CPU_ENTRY.defaults, image: "", command: ["ignored"], args: ["ignored"] } });
  assert.deepEqual(row.defaults.command, []);
  assert.equal(templateCatalogBody([row], "7").templates[0].defaults.command, undefined);
});

test("a normalized entry drives the launch form's defaults and YAML", () => {
  const [cpu] = normalizeTemplateCatalog(CATALOG).templates;
  const values = initialWorkloadForm(cpu, ["ml-team-a"]);
  assert.equal(values.name, "container-job");
  assert.equal(values.namespace, "ml-team-a");
  assert.equal(values.image, "docker.io/library/busybox:1.36");
  assert.equal(values.size, "small");
  assert.equal(values.mode, "cpu");
  const yaml = workloadYAML(cpu, values);
  assert.match(yaml, /image: "docker.io\/library\/busybox:1.36"/);
  assert.match(yaml, /size: small/);
});

test("a malformed catalog is refused rather than half-rendered", () => {
  const bad = [
    null,
    {},
    { ...CATALOG, templates: "nope" },
    { ...CATALOG, tenant_id: "" },
    { ...CATALOG, catalog_revision: "" },
    { ...CATALOG, source: "" },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, id: "Container Job" }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, version: 0 }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, version: "2" }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, title: "" }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, kind: "" }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: undefined }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, name: "Bad Name" } }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, image: "" } }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, size: "enormous" } }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, mode: "quantum" } }] },
    { ...CATALOG, templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, mode: "gpu", gpuKind: "rtx9999" } }] },
    { ...CATALOG, templates: [CPU_ENTRY, CPU_ENTRY] },
  ];
  for (const payload of bad) {
    assert.throws(() => normalizeTemplateCatalog(payload), ApiError);
  }
});

test("a GPU template without a kind uses central's cheapest-GPU default", () => {
  const catalog = normalizeTemplateCatalog({
    ...CATALOG,
    templates: [{ ...CPU_ENTRY, defaults: { ...CPU_ENTRY.defaults, mode: "gpu" } }],
  });
  assert.equal(catalog.templates[0].defaults.gpuKind, "any");
});

// A node-only entry legitimately has no image, and refusing it would make the
// capacity-request shape unrepresentable.
test("a node-only entry needs no default image", () => {
  const catalog = normalizeTemplateCatalog({
    ...CATALOG,
    templates: [{ ...CPU_ENTRY, id: "node-capacity", nodeOnly: true, defaults: { ...CPU_ENTRY.defaults, image: "" } }],
  });
  assert.equal(catalog.templates[0].nodeOnly, true);
  assert.equal(catalog.templates[0].defaults.image, "");
});

test("draft errors name every incomplete field at once", () => {
  const rows = [
    templateDraftRow({ ...CPU_ENTRY, id: "container-job", nodeOnly: false, defaults: { name: "container-job", image: "busybox", size: "small", mode: "cpu" } }),
    { ...templateDraftRow(), id: "Bad Id", title: "", kind: "", defaults: { name: "Bad Name", image: "", size: "huge", mode: "gpu", gpuKind: "rtx9999" } },
  ];
  const errors = templateDraftErrors(rows);
  assert.deepEqual(errors[0], {});
  assert.ok(errors[1].id && errors[1].title && errors[1].kind);
  assert.ok(errors[1].name && errors[1].image && errors[1].size && errors[1].gpuKind);
});

test("two entries cannot claim one id", () => {
  const row = templateDraftRow({ ...CPU_ENTRY, defaults: { name: "container-job", image: "busybox", size: "small", mode: "cpu" } });
  const errors = templateDraftErrors([row, { ...row }]);
  assert.match(errors[0].id, /already uses this id/);
  assert.match(errors[1].id, /already uses this id/);
});

// Versions are what a record's provenance points at, so an untouched entry must
// keep the one its runs were launched under.
test("only edited entries earn the next version", () => {
  const loaded = normalizeTemplateCatalog(CATALOG).templates;
  const rows = loaded.map((template) => templateDraftRow(template));
  rows[0] = { ...rows[0], title: "Renamed container job" };
  const versioned = versionedTemplateDraft([...rows, templateDraftRow({ ...CPU_ENTRY, id: "brand-new" })], loaded);
  assert.equal(versioned[0].version, 3, "an edited entry moves on from 2");
  assert.equal(versioned[1].version, 5, "an untouched entry keeps its version");
  assert.equal(versioned[2].version, 1, "a new entry starts at 1");
});

test("the wire body matches central and refuses an incomplete catalog", () => {
  const rows = versionedTemplateDraft(normalizeTemplateCatalog(CATALOG).templates.map(templateDraftRow), normalizeTemplateCatalog(CATALOG).templates);
  const body = templateCatalogBody(rows, "7");
  assert.equal(body.catalog_revision, "7");
  assert.equal(body.templates[0].nodeOnly, false);
  assert.equal(body.templates[0].defaults.gpuKind, undefined, "a CPU entry carries no GPU kind");
  assert.equal(body.templates[1].defaults.gpuKind, "rtx4000ada");
  assert.equal(body.templates[1].disabled, true);
  assert.deepEqual(templateCatalogBody([], 7), { catalog_revision: "7", templates: [] });
  assert.throws(() => templateCatalogBody([templateDraftRow()], "7"), /incomplete/);
  // A catalog nobody read is a catalog nobody may replace.
  assert.throws(() => templateCatalogBody(rows), /revision it was read at/);
});

// Whitespace and a GPU kind the mode has put to sleep both emit the document
// already on record, so neither may move a version a run's provenance points at.
test("only a change to the emitted entry earns the next version", () => {
  const loaded = normalizeTemplateCatalog(CATALOG).templates;
  const rows = loaded.map(templateDraftRow);
  const padded = { ...rows[0], title: `  ${rows[0].title}  `, description: `${rows[0].description} ` };
  const dormantGpu = { ...rows[0], defaults: { ...rows[0].defaults, gpuKind: "h100" } };
  assert.equal(versionedTemplateDraft([padded], loaded)[0].version, 2, "a retyped-identical title keeps its version");
  assert.equal(versionedTemplateDraft([dormantGpu], loaded)[0].version, 2, "a GPU kind a CPU entry never emits keeps its version");
  assert.equal(templateCatalogBody([padded], "7").templates[0].title, "Generic container job");
  assert.equal(templateCatalogBody([dormantGpu], "7").templates[0].defaults.gpuKind, undefined);
  // The same fields, once they reach the wire, do move it.
  const woken = { ...rows[0], defaults: { ...rows[0].defaults, mode: "gpu", gpuKind: "h100" } };
  assert.equal(versionedTemplateDraft([woken], loaded)[0].version, 3);
  assert.equal(versionedTemplateDraft([{ ...rows[0], title: "Renamed" }], loaded)[0].version, 3);
});

// What the editor asks the reader about before it throws their work away.
test("unsaved edits are measured against the document a save would publish", () => {
  const loaded = normalizeTemplateCatalog(CATALOG).templates;
  const rows = loaded.map(templateDraftRow);
  assert.equal(templateDraftChanged(rows, loaded), false);
  assert.equal(templateDraftChanged([{ ...rows[0], title: "  Generic container job  " }, rows[1]], loaded), false);
  assert.equal(templateDraftChanged([{ ...rows[0], defaults: { ...rows[0].defaults, gpuKind: "a100" } }, rows[1]], loaded), false);
  assert.equal(templateDraftChanged([{ ...rows[0], title: "Renamed" }, rows[1]], loaded), true);
  assert.equal(templateDraftChanged([rows[0]], loaded), true, "a removed entry is a change");
  assert.equal(templateDraftChanged([...rows, templateDraftRow()], loaded), true, "a new entry is a change");
  assert.equal(templateDraftChanged([rows[1], rows[0]], loaded), true, "a reorder is a change");
});

test("GET and PUT use the same-origin catalog seam", async () => {
  const stub = stubFetch(() => jsonResponse(200, CATALOG));
  try {
    const read = await fetchTemplateCatalog({ token: "bearer-value", tenantId: "cust 42" });
    assert.equal(read.templates.length, 2);
    assert.equal(stub.calls[0].url, "/api/tenants/cust%2042/templates");
    assert.equal(stub.calls[0].init.credentials, "same-origin");
    assert.equal(stub.calls[0].init.headers.Authorization, "Bearer bearer-value");
    assert.equal(stub.calls[0].init.body, undefined);

    await putTemplateCatalog({
      token: "bearer-value",
      tenantId: "cust 42",
      templates: read.templates.map(templateDraftRow),
      catalogRevision: read.catalogRevision,
    });
    assert.equal(stub.calls[1].init.method, "PUT");
    assert.equal(stub.calls[1].init.headers["Content-Type"], "application/json");
    const sent = JSON.parse(stub.calls[1].init.body);
    assert.equal(sent.templates.length, 2);
    assert.equal(sent.templates[0].id, "container-job");
    // The write says which catalog it was composed against, so central can
    // refuse one aimed at a revision it has already moved past.
    assert.equal(sent.catalog_revision, "7");

    // A write with no revision never reaches the network.
    assert.throws(
      () => putTemplateCatalog({ token: "bearer-value", tenantId: "cust 42", templates: read.templates.map(templateDraftRow) }),
      /revision it was read at/,
    );
    assert.equal(stub.calls.length, 2);
  } finally {
    stub.restore();
  }
});

test("catalog refusals keep their status", async () => {
  const stub = stubFetch(() => jsonResponse(403, { error: "owner_required" }));
  try {
    await assert.rejects(
      fetchTemplateCatalog({ token: "t", tenantId: "cust_42" }),
      (error) => error.status === 403 && error.code === "owner_required",
    );
  } finally {
    stub.restore();
  }
});

test("a selection is refused when the catalog no longer backs it", () => {
  const catalog = normalizeTemplateCatalog(CATALOG);
  assert.equal(templateSelectionError(catalog, "container-job", 2), "");
  assert.match(templateSelectionError(null, "container-job"), /has not been read/);
  assert.match(templateSelectionError(catalog, "gone-away"), /no longer in the tenant catalog/);
  assert.match(templateSelectionError(catalog, "pytorch-training", 5), /disabled/);
  assert.match(templateSelectionError(catalog, "container-job", 1), /changed to version 2/);
  assert.equal(catalogTemplateById(catalog, "container-job").title, "Generic container job");
  assert.equal(catalogTemplateById(catalog, "gone-away"), null);
});

// The two readings of "what was this launched from" must never look alike: one
// is what central recorded, the other is a guess about an older record.
test("a receipt is exact only when the record carries provenance", () => {
  const catalog = normalizeTemplateCatalog(CATALOG);
  const exact = templateReceipt({
    template: { id: "container-job", version: 2, catalog_revision: "7" },
    spec: { spec: { image: "busybox" } },
  }, catalog);
  assert.equal(exact.exact, true);
  assert.equal(exact.id, "container-job");
  assert.equal(exact.version, 2);
  assert.equal(exact.catalogRevision, "7");
  assert.equal(exact.title, "Generic container job");
  assert.match(exact.note, /Recorded by central/);

  const legacy = templateReceipt({ spec: { spec: { gpu: { kind: "a100", count: 8 } } } }, catalog);
  assert.equal(legacy.exact, false);
  assert.equal(legacy.version, null);
  assert.equal(legacy.catalogRevision, "");
  assert.match(legacy.note, /Inferred from the request shape/);

  // A recorded id the catalog has since dropped is still provenance: the record
  // says where it came from even when the entry is gone.
  const dropped = templateReceipt({ template: { id: "retired-job", version: 1 }, spec: { spec: {} } }, catalog);
  assert.equal(dropped.exact, true);
  assert.equal(dropped.title, "retired-job");

  const olderVersion = templateReceipt({ template: { id: "container-job", version: 1 }, spec: { spec: {} } }, catalog);
  assert.equal(olderVersion.title, "container-job", "a newer catalog title must not relabel an older exact receipt");

  // A malformed template block is not provenance, so it falls back to the guess.
  for (const template of [{ id: "Bad Id" }, { version: 2 }, "container-job"]) {
    assert.equal(templateReceipt({ template, spec: { spec: {} } }, catalog).exact, false);
  }
});

test("an unknown source is shown rather than relabelled", () => {
  assert.equal(templateSourceLabel("tenant"), "Tenant catalog");
  assert.equal(templateSourceLabel("default"), "Platform default catalog");
  assert.equal(templateSourceLabel("platform"), "Platform default catalog");
  assert.equal(templateSourceLabel("something-new"), "something-new");
});

// The dummy preview has to behave like the seam it stands in for: a manager
// replaces the whole catalog, the revision moves, and the source stops being
// the platform default.
test("the dev preview replaces a session-local catalog", async () => {
  const before = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  assert.equal(before.source, "default");
  assert.equal(before.catalogRevision, "1");
  assert.ok(before.templates.length >= 1);

  const rows = before.templates.map(templateDraftRow).slice(0, 1);
  rows[0] = { ...rows[0], title: "Tenant container job" };
  const body = templateCatalogBody(versionedTemplateDraft(rows, before.templates), before.catalogRevision);
  const saved = normalizeTemplateCatalog(await previewPutTemplateCatalog("tenant-acme-research", body));
  assert.equal(saved.source, "tenant");
  assert.equal(saved.catalogRevision, "2");
  assert.equal(saved.changed, true);
  assert.equal(saved.templates.length, 1);
  assert.equal(saved.templates[0].title, "Tenant container job");
  assert.equal(saved.templates[0].version, before.templates[0].version + 1);

  const reread = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  assert.equal(reread.templates.length, 1, "the replacement is what the next read sees");
  assert.equal(reread.catalogRevision, "2");

  // The preview is a module singleton, so a replaced catalog must not decide
  // what the next reader — or the next test — starts from.
  resetDevPreview();
  const restored = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  assert.equal(restored.source, "default");
  assert.equal(restored.catalogRevision, "1");
  assert.equal(restored.templates.length, before.templates.length);
});

test("the dev preview refuses a catalog write from a role that cannot manage the tenant", async () => {
  const viewer = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-platform-lab"));
  assert.equal(viewer.role, "viewer");
  await assert.rejects(
    previewPutTemplateCatalog("tenant-platform-lab", templateCatalogBody(viewer.templates.map(templateDraftRow), viewer.catalogRevision)),
    (error) => error.status === 403,
  );
  await assert.rejects(previewTemplateCatalog("tenant-does-not-exist"), (error) => error.status === 404);
});

// Two managers, one catalog: the second write was composed against a document
// the first has already replaced, so it is refused rather than applied over it.
test("the dev preview refuses a write composed against a stale revision", async () => {
  const read = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  const rows = read.templates.map(templateDraftRow);
  const first = { ...rows[0], title: "First manager's title" };
  await previewPutTemplateCatalog("tenant-acme-research", templateCatalogBody(versionedTemplateDraft([first, ...rows.slice(1)], read.templates), read.catalogRevision));

  const second = { ...rows[0], title: "Second manager's title" };
  const stale = templateCatalogBody(versionedTemplateDraft([second, ...rows.slice(1)], read.templates), read.catalogRevision);
  await assert.rejects(
    previewPutTemplateCatalog("tenant-acme-research", stale),
    (error) => error.status === 409 && /changed since it was read/.test(error.message),
  );
  // The refused write left nothing behind.
  const after = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  assert.equal(after.catalogRevision, "2");
  assert.equal(after.templates[0].title, "First manager's title");

  // A body with no revision at all is the same refusal: it cannot have been
  // composed against what this tenant is being served.
  await assert.rejects(
    previewPutTemplateCatalog("tenant-acme-research", { templates: [] }),
    (error) => error.status === 409,
  );
});

test("the dev preview can publish an empty tenant catalog", async () => {
  const read = normalizeTemplateCatalog(await previewTemplateCatalog("tenant-acme-research"));
  const empty = normalizeTemplateCatalog(await previewPutTemplateCatalog("tenant-acme-research", templateCatalogBody([], read.catalogRevision)));
  assert.deepEqual(empty.templates, []);
  assert.equal(empty.source, "tenant");
});
