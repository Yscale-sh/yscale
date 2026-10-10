import test from "node:test";
import assert from "node:assert/strict";
import {DOC_PAGES, docLink, docPage, renderDoc} from "./releaseDocs.js";

test("release docs keep local pages, anchors and examples on this deployment", () => {
  assert.equal(docLink("docs/README.md", "getting-started.md"), "/docs/getting-started.html");
  assert.equal(docLink("docs/README.md", "quickstart.md"), "/docs/quickstart.html");
  assert.equal(docLink("docs/quickstart.md", "../deploy/runbooks/factory.md"), "/docs/factory-environment.html");
  assert.equal(docLink("docs/getting-started.md", "self-hosted-mesh.md#operator-tenant-and-connector-recipe"), "/docs/self-hosted-mesh.html#operator-tenant-and-connector-recipe");
  assert.equal(docLink("docs/articles/introducing-yscale.md", "../../LICENSE"), "/docs/license.html");
  assert.equal(docLink("docs/self-hosted-mesh.md", "../.env.factory.example"), "/docs/files/.env.factory.example");
  assert.equal(docLink("docs/first-workload.md", "#submit"), "#submit");
});
test("docs reject injected markup and unsafe links", () => {
  const result = renderDoc("docs/README.md", '<script>alert(1)</script>\n\n[x](javascript:alert(1))');
  assert(!result.includes("<script>"));
  assert(!result.includes('href="javascript:'));
  assert.equal(docLink("docs/README.md", "javascript:alert(1)"), "#");
  assert.equal(docLink("docs/README.md", "../../../secrets"), "#");
});
test("docs emit heading anchors, scrollable code and tables", () => {
  const result = renderDoc("docs/README.md", "## Operator tenant and connector recipe\n\n```sh\nmake build\n```\n\n| One | Two |\n|---|---|\n| a | b |");
  assert(result.includes('id="operator-tenant-and-connector-recipe"'));
  assert(result.includes('<pre tabindex="0"'));
  assert(result.includes('class="table-scroll"'));
});
test("LAN docs retain noindex, qualification guidance and no hosted signup", () => {
  for (const [source, url] of DOC_PAGES) {
    const result = docPage(source, "# Example");
    assert(result.includes('content="noindex,nofollow"'));
    assert(result.includes(`href="${url}" aria-current="page"`));
    assert(result.includes('id="main"'));
    assert(result.includes("You operate Yscale and pay providers directly"));
    assert(result.includes("Local smoke tests use fake provisioning"));
    assert(result.includes('href="/docs/configuration-status.html"'));
    assert(!result.includes("DEV PREVIEW"));
    assert(!result.includes("create account"));
  }
});
