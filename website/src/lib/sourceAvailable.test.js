import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync, existsSync} from "node:fs";
import {DOC_PAGES, DOC_FILES, docLink} from "./releaseDocs.js";

const read = name => readFileSync(new URL(`../../${name}`, import.meta.url), "utf8");
test("release license and customer surfaces use finalized terms", () => {
  for (const file of ["content/release/LICENSE", "content/release/COMMERCIAL.md", "content/release/docs/articles/introducing-yscale.md", "src/pages/SourceAvailable.jsx", "src/components/Footer.jsx", "src/lib/releaseDocs.js"]) {
    const content = read(file);
    assert.doesNotMatch(content, /draft license|license draft|license.*still a draft|DRAFT —|not yet in effect|does not yet grant|pending legal review|proposed terms|intended terms/i, file);
  }
  const license = read("content/release/LICENSE");
  assert(license.includes("separate paid written license"));
  assert(license.includes("Internal organization use"));
  assert(license.includes("Third-party components retain their respective licenses"));
  assert.equal(DOC_PAGES.find(([file]) => file === "LICENSE")[2], "License");
});
test("hero distinguishes validated Linode bursts from experimental adapters", () => {
  const home = read("src/pages/SourceAvailable.jsx");
  const hero = home.slice(home.indexOf('<section className="source-hero'), home.indexOf('</section>'));
  const headline = hero.match(/<h1[^>]*>([\s\S]*?)<\/h1>/)[1].replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim();
  assert.equal(headline, "Add Linode CPU and GPU nodes to your cluster when the workload demands it.");
  for (const phrase of ["Live-validated on k3s with Linode CPU and GPU bursts", "Fly, AWS, GCP and Azure adapters are included but experimental", "Provider support and network performance vary"]) assert(hero.includes(phrase), phrase);
  assert.doesNotMatch(home, /any cloud|GPU access across supported clouds/i);
  const supportingCopy = hero.slice(hero.indexOf('<p className="source-lede">'), hero.indexOf('<aside'));
  const paragraphs = [...supportingCopy.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].map(([, text]) => text.replace(/<[^>]*>/g, "")).join(" ");
  assert(paragraphs.split(/\s+/).length <= 40, "Keep hero supporting copy to 40 words or fewer");
  assert(hero.includes('href="/docs/configuration-status.html"'));
  assert(!/endless|unlimited|guaranteed (?:capacity|speed)|automatic (?:GPU )?failover/i.test(hero));
});
test("public release routes use full-source positioning without removing application routes", () => {
  const app = read("src/App.jsx");
  assert.match(app, /case "\/open-source":\s*case "\/source-available":\s*return <SourceAvailable/);
  assert.match(app, /default:\s*return <SourceAvailable/);
  for (const component of ["Account", "Console", "Callback", "Workloads"]) assert(app.includes(`<${component}`));
});
test("homepage, navigation and metadata do not offer hosted subscriptions", () => {
  const files = ["src/pages/SourceAvailable.jsx", "src/components/Nav.jsx", "src/components/Footer.jsx", "index.html"];
  for (const file of files) {
    const content = read(file);
    for (const forbidden of [/SIGNUP[_]URL/, /cost\s*\+\s*2%/i, /billed by the second/i, /create Yscale ID/i, /open source self-hosted/i]) assert(!forbidden.test(content), `${file}: ${forbidden}`);
  }
  const home = read(files[0]);
  for (const phrase of ["business logic", "cloud cost estimates", "No. You operate", "not OSI open source", "paid written license", "provider-enforced spending caps", "Azure and GCP GPU support is outside this release scope"]) assert(home.includes(phrase), phrase);
  assert(home.includes('href="/docs/getting-started.html"'));
  assert.match(read("src/styles/source-release.css"), /body:has\(\.source-release\)\{min-width:0\}/);
});
test("updated website surfaces omit hosted identity and point to self-hosting resources", () => {
  const files = ["src/App.jsx", "src/lib/identity.js", "src/lib/router.jsx", "src/pages/SourceAvailable.jsx", "src/pages/Account.jsx", "src/pages/Callback.jsx", "src/pages/Console.jsx", "src/components/WhoItsFor.jsx", "src/components/Nav.jsx", "src/components/Footer.jsx"];
  for (const file of files) {
    assert.doesNotMatch(read(file), /create\s+account|Yscale ID|id\.kubagachi\.com|SIGNUP[_]URL|\/unsubscribe/i, file);
  }
  for (const file of ["src/pages/Account.jsx", "src/pages/Callback.jsx"]) {
    const content = read(file);
    assert(content.includes("sign in with your installation's identity provider"), file);
    assert(content.includes("This deployment has no identity provider configured."), file);
    assert(content.includes('href="/docs/getting-started.html"'), file);
  }
  assert.equal((read("src/components/WhoItsFor.jsx").match(/label: "read the quickstart →", href: "\/docs\/quickstart.html"/g) || []).length, 3);
  assert.match(read("src/pages/Console.jsx"), /href="\/docs\/quickstart.html">connect a cluster/);
  for (const file of ["src/components/Nav.jsx", "src/components/Footer.jsx"]) {
    assert(read(file).includes('href="https://github.com/Yscale-sh/yscale"'), file);
  }
  assert.doesNotMatch(read("src/components/Footer.jsx"), /Kubagachi|KubeKritter/i);
});
test("identity is opt-in through the installation environment", async () => {
  const source = read("src/lib/identity.js");
  for (const [configuredURL, expectedURL] of [[undefined, ""], ["", ""], ["   ", ""], [" https://identity.example.test/// ", "https://identity.example.test"]]) {
    const moduleSource = source.replaceAll("import.meta.env", JSON.stringify({VITE_YSCALE_ID_URL: configuredURL}));
    const identity = await import(`data:text/javascript;base64,${Buffer.from(moduleSource).toString("base64")}`);
    assert.equal(identity.ID_URL, expectedURL);
    assert.equal(identity.identityConfigured, Boolean(expectedURL));
  }
});
test("public docs are curated and omit the private editorial handoff", () => {
  for (const file of [...DOC_PAGES.map(([file]) => file), ...DOC_FILES]) {
    assert(existsSync(new URL(`../../content/release/${file}`, import.meta.url)), file);
    assert(!/(?:^|\/)(?:notes|scratch|internal|internals)(?:\/|$)/i.test(file), file);
    assert(!file.includes("RELEASE_READINESS"));
  }
  const article = read("content/release/docs/articles/burst-where-it-makes-sense.md");
  assert(!article.includes("Editorial verification notes"));
  assert(!article.includes("Some media changes are uncommitted"));
  assert.equal(docLink("docs/articles/burst-where-it-makes-sense.md", "../../deploy/runbooks/factory.md#environment-variables"), "/docs/factory-environment.html#environment-variables");
});

test("self-hosted docs require explicit ops Headscale, not a Tailscale account", () => {
  const home = read("src/pages/SourceAvailable.jsx");
  assert.match(home, /no Tailscale account or subscription is required/i);
  assert(home.includes("Tailscale client software still runs on infrastructure nodes"));
  const runbook = read("content/release/deploy/runbooks/factory.md");
  const env = read("content/release/.env.factory.example");
  assert(runbook.includes("FACTORY_OPS_LOGIN_SERVER"));
  assert(runbook.includes("### Self-hosted ops setup"));
  assert(env.includes("FACTORY_OPS_LOGIN_SERVER=https://ops-headscale.example.invalid"));
  for (const file of ["docs/networking.md", "docs/getting-started.md", "docs/self-hosted-mesh.md", "docs/articles/introducing-yscale.md", "docs/articles/burst-where-it-makes-sense.md", "deploy/runbooks/factory.md"]) {
    const content = read(`content/release/${file}`);
    assert(!/requires (?:your|the operator's) (?:own )?Tailscale account/i.test(content), file);
    assert(!content.includes("self-hosted ops coordinator is\n  not configurable"), file);
  }
});

test("customer copy avoids canned slogans and internal publication instructions", () => {
  const files = ["src/pages/SourceAvailable.jsx", "src/components/Nav.jsx", "src/components/Footer.jsx", "index.html", "src/lib/releaseDocs.js", "content/release/docs/README.md", "content/release/docs/getting-started.md", "content/release/docs/quickstart.md", "content/release/docs/articles/introducing-yscale.md", "content/release/docs/articles/burst-where-it-makes-sense.md"];
  for (const file of files) {
    const content = read(file);
    for (const forbidden of [/seamless(?:ly)?/i, /game[- ]changing/i, /unlock (?:the|your) potential/i, /Keep your cluster\.\s*(?:<br\s*\/>\s*<span>)?Burst beyond it/i, /Publication draft/i, /Private stays private/i, /private Git history/i, /must not be announced/i]) {
      assert(!forbidden.test(content), `${file}: ${forbidden}`);
    }
  }
});

test("use-case article preserves measured storage results and non-turnkey limits", () => {
  const article = read("content/release/docs/articles/burst-where-it-makes-sense.md");
  for (const value of ["266.9 MiB/s", "167.6 MiB/s", "48.9 MiB/s", "36.1 MiB/s", "180.2 MiB/s", "127.6 MiB/s", "35.6 seconds", "63.5 seconds", "31.6 seconds"]) assert(article.includes(value), value);
  assert(article.includes("synthetic I/O"));
  assert(article.includes("spec.modelVolume"));
  assert(article.includes("spec.storage.cache"));
  assert.match(article, /template|integration/);
  assert(article.includes("kubectl apply -f first-burst.yaml"));
});
