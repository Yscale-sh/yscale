import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ApiError } from "./apiError.js";
import { previewGitOpsSources, previewPutGitOpsSources, resetDevPreview } from "./devPreview.js";
import {
  fetchGitOpsSources,
  gitOpsConflict,
  gitOpsDraftChanged,
  gitOpsDraftErrors,
  gitOpsDraftRow,
  gitOpsRegistryBody,
  normalizeGitOpsRegistry,
  putGitOpsSources,
  reconcilerLabel,
} from "./gitOpsApi.js";

// The client is exercised against a stub fetch: these tests are about the shape
// it accepts, the request it builds, and the refusals it will not paper over.
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

const FLUX_SOURCE = {
  id: "prod-infra",
  name: "Prod infrastructure",
  reconciler: "flux",
  repo_url: "https://github.com/acme/infra.git",
  path: "clusters/prod",
  ref: "main",
  cluster_id: "acme-prod-us-east",
};

const ARGO_SOURCE = {
  id: "ml-platform",
  name: "ML platform",
  reconciler: "argo",
  repo_url: "git@github.com:acme/ml-platform.git",
  path: "",
  ref: "release-2026-08",
  cluster_id: "acme-ml-lab",
};

const REGISTRY = {
  tenant_id: "cust_42",
  sources_revision: "4",
  role: "owner",
  changed: true,
  sources: [FLUX_SOURCE, ARGO_SOURCE],
};

beforeEach(resetDevPreview);

test("a registry normalizes into the shape the console renders", () => {
  const registry = normalizeGitOpsRegistry(REGISTRY);
  assert.equal(registry.tenantId, "cust_42");
  assert.equal(registry.sourcesRevision, "4");
  assert.equal(registry.role, "owner");
  assert.equal(registry.changed, true);
  assert.deepEqual(registry.sources[0], {
    id: "prod-infra",
    name: "Prod infrastructure",
    reconciler: "flux",
    repoUrl: "https://github.com/acme/infra.git",
    path: "clusters/prod",
    ref: "main",
    clusterId: "acme-prod-us-east",
  });
  // An empty path is the repository root, which is a real answer rather than a
  // missing one, so it survives normalization as an empty string.
  assert.equal(registry.sources[1].path, "");
  assert.equal(registry.sources[1].repoUrl, "git@github.com:acme/ml-platform.git");
});

test("an empty registry is a registry, not an absent one", () => {
  const registry = normalizeGitOpsRegistry({ ...REGISTRY, sources: [] });
  assert.deepEqual(registry.sources, []);
  assert.equal(registry.sourcesRevision, "4");
});

test("a numeric revision is accepted and shown as the marker central sent", () => {
  assert.equal(normalizeGitOpsRegistry({ ...REGISTRY, sources_revision: 7 }).sourcesRevision, "7");
});

test("source ids and repository paths preserve central's exact contract", () => {
  const id = `a${"b".repeat(62)}z`;
  const path = " environments/production ";
  const registry = normalizeGitOpsRegistry({
    ...REGISTRY,
    sources: [{ ...FLUX_SOURCE, id, path }],
  });
  assert.equal(registry.sources[0].id, id);
  assert.equal(registry.sources[0].path, path);
  assert.equal(gitOpsRegistryBody(registry.sources.map(gitOpsDraftRow), "4").sources[0].path, path);
  assert.throws(
    () => normalizeGitOpsRegistry({ ...REGISTRY, sources: [{ ...FLUX_SOURCE, id: `${id}x` }] }),
    ApiError,
  );
});

test("malformed registry responses are refused rather than rendered", () => {
  const bad = [
    null,
    {},
    { tenant_id: "cust_42", sources_revision: "4" },
    { tenant_id: "", sources_revision: "4", sources: [] },
    { tenant_id: "cust_42", sources_revision: "", sources: [] },
    { tenant_id: "cust_42", sources: [] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, id: "Prod_Infra" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, id: "" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, name: "" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, reconciler: "kustomize" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, repo_url: "not a url" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, repo_url: "http://github.com/acme/infra.git" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, repo_url: "https://token@github.com/acme/infra.git" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, repo_url: "" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, ref: "" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, ref: "main branch" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, path: "../clusters/prod" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, path: "/clusters/prod" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, ref: "main/" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, ref: "release.lock" }] },
    { ...REGISTRY, sources: [{ ...FLUX_SOURCE, cluster_id: "bad/id" }] },
    { ...REGISTRY, sources: [FLUX_SOURCE, FLUX_SOURCE] },
    { ...REGISTRY, sources: [null] },
    { ...REGISTRY, sources: Array.from({ length: 33 }, (_, index) => ({ ...FLUX_SOURCE, id: `source-${index}` })) },
  ];
  for (const payload of bad) {
    assert.throws(() => normalizeGitOpsRegistry(payload), ApiError, JSON.stringify(payload)?.slice(0, 90));
  }
});

test("GET and PUT use the same-origin registry seam and send the exact document", async () => {
  const stub = stubFetch(() => jsonResponse(200, REGISTRY));
  try {
    const read = await fetchGitOpsSources({ token: "bearer-value", tenantId: "cust 42" });
    assert.equal(read.sources.length, 2);
    assert.equal(stub.calls[0].url, "/api/tenants/cust%2042/gitops/sources");
    assert.equal(stub.calls[0].init.credentials, "same-origin");
    assert.equal(stub.calls[0].init.headers.Authorization, "Bearer bearer-value");
    assert.equal(stub.calls[0].init.body, undefined);

    await putGitOpsSources({
      token: "bearer-value",
      tenantId: "cust 42",
      sources: read.sources,
      sourcesRevision: read.sourcesRevision,
    });
    assert.equal(stub.calls[1].init.method, "PUT");
    assert.equal(stub.calls[1].init.headers["Content-Type"], "application/json");
    assert.deepEqual(JSON.parse(stub.calls[1].init.body), {
      sources_revision: "4",
      sources: [FLUX_SOURCE, ARGO_SOURCE],
    });
  } finally {
    stub.restore();
  }
});

// Removing the last source is an explicit empty list. A client that treated it
// as "nothing to send" would make the final source unremovable.
test("an empty source list is a document the client will send", async () => {
  const stub = stubFetch(() => jsonResponse(200, { ...REGISTRY, sources_revision: "5", sources: [] }));
  try {
    const saved = await putGitOpsSources({ token: "t", tenantId: "cust_42", sources: [], sourcesRevision: "4" });
    assert.deepEqual(JSON.parse(stub.calls[0].init.body), { sources_revision: "4", sources: [] });
    assert.deepEqual(saved.sources, []);
  } finally {
    stub.restore();
  }
});

test("a write without the revision it was read at is refused before the network", () => {
  assert.throws(() => gitOpsRegistryBody([], ""), /revision it was read at/);
  assert.throws(() => gitOpsRegistryBody("not a list", "4"), /list of sources/);
  assert.throws(() => gitOpsRegistryBody([gitOpsDraftRow()], "4"), /Source 1 is incomplete/);
});

test("a failed read never becomes an empty registry", async () => {
  for (const status of [401, 403, 404, 500, 502, 503]) {
    const stub = stubFetch(() => jsonResponse(status, { error: "nope" }));
    try {
      await assert.rejects(
        fetchGitOpsSources({ token: "t", tenantId: "cust_42" }),
        (error) => error instanceof ApiError && error.status === status,
      );
    } finally {
      stub.restore();
    }
  }
  // A 200 the client cannot read is a refusal too, not an empty list.
  const broken = stubFetch(() => jsonResponse(200, { tenant_id: "cust_42", sources_revision: "4", sources: "all of them" }));
  try {
    await assert.rejects(fetchGitOpsSources({ token: "t", tenantId: "cust_42" }), (error) => error.status === 502);
  } finally {
    broken.restore();
  }
});

test("an unreachable seam reports as offline rather than as an answer", async () => {
  const stub = stubFetch(() => { throw new TypeError("network"); });
  try {
    await assert.rejects(fetchGitOpsSources({ token: "t", tenantId: "cust_42" }), (error) => error.status === 0 && error.isOffline);
  } finally {
    stub.restore();
  }
});

test("a 409 carries the registry central is serving now", async () => {
  const current = { ...REGISTRY, sources_revision: "9", changed: false, sources: [ARGO_SOURCE] };
  const stub = stubFetch(() => jsonResponse(409, current));
  try {
    await assert.rejects(
      putGitOpsSources({ token: "t", tenantId: "cust_42", sources: [gitOpsDraftRow(normalizeGitOpsRegistry(REGISTRY).sources[0])], sourcesRevision: "4" }),
      (error) => {
        const latest = gitOpsConflict(error);
        assert.equal(error.status, 409);
        assert.equal(latest.sourcesRevision, "9");
        assert.deepEqual(latest.sources.map((source) => source.id), ["ml-platform"]);
        return true;
      },
    );
  } finally {
    stub.restore();
  }
});

test("a 409 whose body will not normalize offers no registry rather than half of one", async () => {
  const stub = stubFetch(() => jsonResponse(409, { error: "stale_revision" }));
  try {
    await assert.rejects(
      putGitOpsSources({ token: "t", tenantId: "cust_42", sources: [], sourcesRevision: "4" }),
      (error) => error.status === 409 && gitOpsConflict(error) === null,
    );
  } finally {
    stub.restore();
  }
});

test("draft validation reports every field of every row", () => {
  const errors = gitOpsDraftErrors([
    gitOpsDraftRow(),
    { id: "Prod Infra", name: "", reconciler: "kustomize", repoUrl: "http://github.com/acme/infra", path: "../outside", ref: "a b", clusterId: "bad/id" },
    { id: "dup", name: "One", reconciler: "flux", repoUrl: "https://github.com/acme/a.git", path: "", ref: "main", clusterId: "acme-prod-us-east" },
    { id: "dup", name: "Two", reconciler: "argo", repoUrl: "https://github.com/acme/b.git", path: "", ref: "main", clusterId: "acme-ml-lab" },
  ]);
  assert.equal(errors[0].id, "Give this source an id.");
  assert.equal(errors[0].name, "Give this source a name.");
  assert.ok(errors[0].repoUrl);
  assert.ok(errors[0].clusterId);
  assert.ok(!errors[0].ref, "the default row carries a usable ref");
  assert.ok(!errors[0].path, "an empty path means the repository root");
  assert.match(errors[1].id, /lowercase/);
  assert.match(errors[1].reconciler, /Flux or Argo/);
  assert.match(errors[1].repoUrl, /clone URL/);
  assert.ok(errors[1].path);
  assert.ok(errors[1].ref);
  assert.match(errors[1].clusterId, /cluster id central mints/);
  assert.match(errors[2].id, /Another source already uses this id/);
  assert.match(errors[3].id, /Another source already uses this id/);
});

test("draft validation matches central's URL, path, and Git ref grammar", () => {
  const valid = [
    { ...gitOpsDraftRow(), id: "https", name: "HTTPS", repoUrl: "https://github.com/acme/infra.git", path: "clusters/prod us", ref: "v1.2.3+build", clusterId: "cl-prod" },
    { ...gitOpsDraftRow(), id: "ssh", name: "SSH", repoUrl: "ssh://git@git.example.com:2222/acme/infra.git", path: "", ref: "refs/tags/v2", clusterId: "cl-prod" },
    { ...gitOpsDraftRow(), id: "scp", name: "SCP", repoUrl: "github.com:acme/infra.git", path: "apps/team", ref: "main", clusterId: "cl-prod" },
  ];
  assert.deepEqual(gitOpsDraftErrors(valid), [{}, {}, {}]);

  for (const patch of [
    { repoUrl: "git://github.com/acme/infra.git" },
    { repoUrl: "https://token@github.com/acme/infra.git" },
    { path: "./clusters" },
    { path: "clusters//prod" },
    { ref: "main/" },
    { ref: "foo.lock" },
    { ref: "foo//bar" },
  ]) {
    const row = { ...valid[0], ...patch };
    assert.ok(Object.keys(gitOpsDraftErrors([row])[0]).length > 0, JSON.stringify(patch));
  }
});

test("a draft is dirty only when a save would publish something different", () => {
  const loaded = normalizeGitOpsRegistry(REGISTRY).sources;
  const rows = loaded.map(gitOpsDraftRow);
  assert.equal(gitOpsDraftChanged(rows, loaded), false);
  assert.equal(gitOpsDraftChanged(rows.map((row, index) => (index ? row : { ...row, name: "  Prod infrastructure  " })), loaded), false);
  assert.equal(gitOpsDraftChanged(rows.map((row, index) => (index ? row : { ...row, ref: "release" })), loaded), true);
  assert.equal(gitOpsDraftChanged(rows.slice(0, 1), loaded), true);
  assert.equal(gitOpsDraftChanged([...rows].reverse(), loaded), false);
});

test("reconciler labels name the tool and pass an unknown one through", () => {
  assert.equal(reconcilerLabel("flux"), "Flux");
  assert.equal(reconcilerLabel("argo"), "Argo CD");
  assert.equal(reconcilerLabel("kustomize"), "kustomize");
  assert.equal(reconcilerLabel(""), "unreported reconciler");
});

/* ------------------------------------------------------------- dev preview */

test("the preview registry is readable by every role and mutable only by managers", async () => {
  const owner = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  const viewer = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-platform-lab"));
  assert.equal(owner.role, "owner");
  assert.deepEqual(owner.sources.map((source) => source.id), ["batch-eu", "ml-platform", "prod-infra"]);
  assert.equal(viewer.role, "viewer");
  assert.equal(viewer.sources.length, 1);

  await assert.rejects(
    previewPutGitOpsSources("tenant-platform-lab", { sources_revision: "1", sources: [] }),
    (error) => error.status === 403,
  );
  await assert.rejects(previewGitOpsSources("tenant-nope"), (error) => error.status === 404);
});

// The registry has to be able to outlive a cluster: a source naming a cluster
// the fleet no longer reports is the state the console must keep visible.
test("the preview owner registry names a cluster the fleet observation does not carry", async () => {
  const owner = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  const orphan = owner.sources.find((source) => source.id === "batch-eu");
  assert.equal(orphan.clusterId, "acme-retired-fra");
  assert.equal(orphan.path, "", "an empty path exercises the repository-root rendering");
});

// The preview is driven through the same document the network client builds,
// because a fixture that accepts a shape the client never sends proves nothing.
test("a preview save replaces the registry whole and moves the revision on", async () => {
  const before = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  const kept = before.sources.filter((source) => source.id !== "batch-eu").map(gitOpsDraftRow);
  const saved = normalizeGitOpsRegistry(
    await previewPutGitOpsSources("tenant-acme-research", gitOpsRegistryBody(kept, before.sourcesRevision)),
  );
  assert.equal(saved.sourcesRevision, "4");
  assert.equal(saved.changed, true);
  assert.deepEqual(saved.sources.map((source) => source.id), ["ml-platform", "prod-infra"]);

  const reread = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  assert.deepEqual(reread.sources.map((source) => source.id), ["ml-platform", "prod-infra"]);
});

test("an identical preview save is a no-op with the same revision", async () => {
  const before = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  const saved = normalizeGitOpsRegistry(
    await previewPutGitOpsSources("tenant-acme-research", gitOpsRegistryBody(before.sources.map(gitOpsDraftRow), before.sourcesRevision)),
  );
  assert.equal(saved.sourcesRevision, before.sourcesRevision);
  assert.equal(saved.changed, false);
});

test("preview source order is not content and is stored in central's id order", async () => {
  const before = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  const saved = normalizeGitOpsRegistry(
    await previewPutGitOpsSources(
      "tenant-acme-research",
      gitOpsRegistryBody(before.sources.map(gitOpsDraftRow).reverse(), before.sourcesRevision),
    ),
  );
  assert.equal(saved.sourcesRevision, before.sourcesRevision);
  assert.equal(saved.changed, false);
  assert.deepEqual(saved.sources.map((source) => source.id), ["batch-eu", "ml-platform", "prod-infra"]);
});

test("a preview save can empty the registry, which is how the last source leaves", async () => {
  const emptied = normalizeGitOpsRegistry(
    await previewPutGitOpsSources("tenant-acme-research", gitOpsRegistryBody([], "3")),
  );
  assert.deepEqual(emptied.sources, []);
  assert.equal(emptied.sourcesRevision, "4");
});

test("a preview save at a stale revision refuses and hands back the current registry", async () => {
  await previewPutGitOpsSources("tenant-acme-research", gitOpsRegistryBody([], "3"));
  await assert.rejects(
    previewPutGitOpsSources("tenant-acme-research", gitOpsRegistryBody([gitOpsDraftRow(normalizeGitOpsRegistry(REGISTRY).sources[0])], "3")),
    (error) => {
      assert.equal(error.status, 409);
      // The refusal carries the envelope the client turns into `error.current`,
      // so the editor can name what moved without discarding the local draft.
      const latest = normalizeGitOpsRegistry(error.payload);
      assert.equal(latest.sourcesRevision, "4");
      assert.deepEqual(latest.sources, []);
      return true;
    },
  );
});

test("the preview registry resets with the rest of the fixtures", async () => {
  await previewPutGitOpsSources("tenant-acme-research", { sources_revision: "3", sources: [] });
  assert.equal((await previewGitOpsSources("tenant-acme-research")).sources.length, 0);
  resetDevPreview();
  const restored = normalizeGitOpsRegistry(await previewGitOpsSources("tenant-acme-research"));
  assert.equal(restored.sourcesRevision, "3");
  assert.equal(restored.sources.length, 3);
});
