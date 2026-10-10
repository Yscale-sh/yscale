import MarkdownIt from "markdown-it";
import path from "node:path";

export const DOC_PAGES = [
  ["docs/README.md", "/docs/", "Overview"],
  ["docs/quickstart.md", "/docs/quickstart.html", "Quickstart"],
  ["docs/getting-started.md", "/docs/getting-started.html", "Getting started"],
  ["docs/self-hosted-mesh.md", "/docs/self-hosted-mesh.html", "Operator setup"],
  ["docs/first-workload.md", "/docs/first-workload.html", "First workload"],
  ["docs/cluster-connector.md", "/docs/cluster-connector.html", "Cluster Connector"],
  ["docs/articles/introducing-yscale.md", "/docs/articles/introducing-yscale.html", "Introducing Yscale"],
  ["docs/articles/burst-where-it-makes-sense.md", "/docs/articles/burst-where-it-makes-sense.html", "Workloads and media workers"],
  ["deploy/runbooks/factory.md", "/docs/factory-environment.html", "Factory environment"],
  ["docs/storage.md", "/docs/storage.html", "Storage"],
  ["docs/networking.md", "/docs/networking.html", "Full networking"],
  ["docs/operations.md", "/docs/operations.html", "Operations and recovery"],
  ["docs/model-volumes.md", "/docs/model-volumes.html", "Model volumes"],
  ["docs/release/supported-configurations.md", "/docs/configuration-status.html", "Configuration status"],
  ["docs/benchmarks/storage-2026-09-24.md", "/docs/benchmarks/storage-2026-09-24.html", "Storage benchmark"],
  ["LICENSE", "/docs/license.html", "License"],
  ["COMMERCIAL.md", "/docs/commercial.html", "Commercial terms"],
];
export const DOC_FILES = [
  "go.mod", ".env.cloud.example", ".env.factory.example", ".env.agent.example",
  "examples/local-managed-mesh-smoke.mjs", "examples/workloads/hello.yaml",
];
const sourceURL = "https://github.com/yscale-sh/yscale/tree/main/";
const escape = text => String(text).replace(/[&<>"']/g, c => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[c]));

export function docLink(source, href) {
  if (/^(https?:|mailto:|#)/i.test(href)) return href;
  if (/^[a-z][a-z\d+.-]*:/i.test(href) || href.startsWith("/")) return "#";
  const split = href.search(/[?#]/);
  const target = split < 0 ? href : href.slice(0, split);
  const suffix = split < 0 ? "" : href.slice(split);
  const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(source), target));
  if (resolved.startsWith("../")) return "#";
  const page = DOC_PAGES.find(([file]) => file === resolved);
  if (page) return page[1] + suffix;
  const encoded = resolved.split("/").map(encodeURIComponent).join("/");
  if (DOC_FILES.includes(resolved)) return "/docs/files/" + encoded + suffix;
  return sourceURL + encoded + suffix;
}

export function renderDoc(source, markdown) {
  const md = new MarkdownIt({html: false, linkify: false});
  const link = md.renderer.rules.link_open || ((tokens, idx, options, env, self) => self.renderToken(tokens, idx, options));
  md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
    tokens[idx].attrSet("href", docLink(source, tokens[idx].attrGet("href") || ""));
    return link(tokens, idx, options, env, self);
  };
  const seen = new Map();
  md.renderer.rules.heading_open = (tokens, idx, options, env, self) => {
    const base = tokens[idx + 1].content.toLowerCase().replace(/[^\w\- ]/g, "").replace(/ /g, "-");
    const count = seen.get(base) || 0;
    seen.set(base, count + 1);
    tokens[idx].attrSet("id", base + (count ? `-${count}` : ""));
    return self.renderToken(tokens, idx, options);
  };
  md.renderer.rules.table_open = () => '<div class="table-scroll" tabindex="0" role="region" aria-label="Reference table"><table>\n';
  md.renderer.rules.table_close = () => '</table></div>\n';
  const fence = md.renderer.rules.fence;
  md.renderer.rules.fence = (...args) => fence(...args).replace("<pre>", '<pre tabindex="0" aria-label="Code example">');
  return md.render(markdown);
}

export function docPage(source, markdown) {
  const page = DOC_PAGES.find(([file]) => file === source);
  if (!page) throw new Error(`Unknown document: ${source}`);
  const nav = DOC_PAGES.map(([file, url, title], index) =>
    `<a href="${url}"${file === source ? ' aria-current="page"' : ""}><span>${String(index + 1).padStart(2, "0")}</span>${escape(title)}</a>`).join("");
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>${escape(page[2])} · Yscale documentation</title>
<link rel="icon" href="data:,"><link rel="stylesheet" href="/docs/docs.css"></head>
<body><a class="skip" href="#main">Skip to content</a>
<header><a class="wordmark" href="/docs/">yscale<span> / documentation</span></a><span class="status">SELF-HOSTED</span></header>
<div class="layout"><aside><p class="eyebrow">INSTALLATION AND OPERATIONS</p><nav aria-label="Documentation">${nav}</nav>
<p class="aside-note">Start with installation, then review networking and provider requirements. You build your images and manage the supporting infrastructure.</p></aside>
<main id="main" tabindex="-1"><div class="notice">This release is experimental. Check <a href="/docs/configuration-status.html">configuration status</a> for the provider and cluster you plan to run. Local smoke tests use fake provisioning.</div>
<article>${renderDoc(source, markdown)}</article>
<footer>You operate Yscale and pay providers directly. Budget settings are not provider billing caps; charges continue until resources are deleted. Source-code links may require repository access.</footer></main></div></body></html>`;
}
