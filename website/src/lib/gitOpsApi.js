import { ApiError } from "./apiError.js";
import { CLUSTER_ID_RE } from "./clusters.js";
import {
  isDevPreviewToken,
  previewGitOpsSources,
  previewPutGitOpsSources,
} from "./devPreview.js";

// The tenant's GitOps source registry, as central serves it from
// /v1/tenants/{tenant_id}/gitops/sources. A source is configuration
// coordinates and nothing more: which repository, at which ref and path, is
// reconciled by which reconciler onto which cluster. Central does not read the
// repository, hold a credential for it, observe reconciliation, or resolve
// drift, and neither does anything in this module.

export const GITOPS_RECONCILERS = Object.freeze(["flux", "argo"]);

const RECONCILER_LABELS = {
  flux: "Flux",
  argo: "Argo CD",
};

export function reconcilerLabel(reconciler) {
  return RECONCILER_LABELS[reconciler] || reconciler || "unreported reconciler";
}

// Source ids are minted per tenant and shown beside the row they name, so they
// stay in the same narrow lowercase shape every other console id uses.
const SOURCE_ID = /^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;

const MAX_NAME = 120;
const MAX_REPO_URL = 400;
const MAX_PATH = 256;
const MAX_REF = 128;
// Central's registry can hold more sources than a console can present as rows
// somebody reads. The ceiling is the seam's rather than a product rule: a
// bigger registry is refused loudly instead of silently truncated.
const MAX_SOURCES = 32;

// Bounded, single-line strings only. Written as a scan rather than a character
// class so the range it refuses stays legible: C0, DEL, and C1.
function hasControl(value) {
  for (const char of value) {
    const code = char.codePointAt(0);
    if (code < 0x20 || code === 0x7f) return true;
  }
  return false;
}

function byteLength(value) {
  return new TextEncoder().encode(value).length;
}

// The two forms a clone URL actually takes. Neither is dereferenced here — this
// is a shape check on a value about to be shown and handed back to central, so
// a string carrying whitespace or a control byte is refused rather than
// rendered as if it were an address.
const REPO_SCP = /^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9.-]+:[A-Za-z0-9._~/-]+$/;

const MESSAGES = {
  0: "Could not reach the GitOps source registry service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot change the GitOps source registry.",
  404: "That tenant does not exist for this account.",
  409: "The GitOps source registry changed since it was read. Review the latest registry before applying this change.",
  413: "The GitOps source registry is too large.",
  415: "The GitOps source registry must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The GitOps source registry service did not answer.",
  503: "The GitOps source registry service is unavailable in this environment.",
};

function text(value) {
  return typeof value === "string" ? value.trim() : "";
}

function invalid(detail) {
  return new ApiError(502, `The GitOps source registry answered with ${detail}.`);
}

// A revision is an opaque marker the console shows and compares, never orders,
// so a counter and an etag-like string are both acceptable answers.
function revisionOf(value) {
  if (typeof value === "string" && value.trim()) return value.trim().slice(0, 64);
  if (Number.isInteger(value) && value >= 0) return String(value);
  return "";
}

export function isRepoURL(value) {
  const url = text(value);
  if (!url || byteLength(url) > MAX_REPO_URL || hasControl(url) || /[ \t?#]/.test(url)) return false;
  if (!url.includes("://")) return REPO_SCP.test(url);
  try {
    const parsed = new URL(url);
    if (!["https:", "ssh:"].includes(parsed.protocol) || !parsed.hostname || parsed.search || parsed.hash) return false;
    if (parsed.password || (parsed.protocol === "https:" && parsed.username)) return false;
    return !parsed.username || parsed.protocol === "ssh:";
  } catch {
    return false;
  }
}

export function isGitRef(value) {
  const ref = text(value);
  if (!ref || byteLength(ref) > MAX_REF || ref === "@" || ref.startsWith("/") || ref.endsWith("/") ||
      ref.endsWith(".") || ref.includes("//") || ref.includes("..") || ref.includes("@{") ||
      /[ ~^:?*[\\]/.test(ref) || hasControl(ref)) return false;
  return ref.split("/").every((component) => !component.startsWith(".") && !component.endsWith(".lock"));
}

// A clean path inside the repository, matching central's stored grammar.
// Empty is a legitimate answer: it means the repository root.
export function isSourcePath(value) {
  if (typeof value !== "string") return false;
  const path = value;
  if (byteLength(path) > MAX_PATH || hasControl(path)) return false;
  if (!path) return true;
  if (path.startsWith("/") || path.includes("\\")) return false;
  return path.split("/").every((segment) => segment && segment !== "." && segment !== "..");
}

export function isSourceName(value) {
  const name = text(value);
  return !!name && byteLength(name) <= MAX_NAME && !hasControl(name);
}

function normalizeSource(entry) {
  if (!entry || typeof entry !== "object") throw invalid("a source that is not an object");
  const id = text(entry.id);
  if (!SOURCE_ID.test(id)) throw invalid(`an invalid source id: ${id || "(empty)"}`);
  const name = text(entry.name);
  if (!isSourceName(name)) throw invalid(`an invalid name for ${id}`);
  const reconciler = text(entry.reconciler);
  if (!GITOPS_RECONCILERS.includes(reconciler)) {
    throw invalid(`an unsupported reconciler for ${id}: ${reconciler || "(empty)"}`);
  }
  const repoUrl = text(entry.repo_url);
  if (!isRepoURL(repoUrl)) throw invalid(`an unusable repository URL for ${id}`);
  const path = typeof entry.path === "string" ? entry.path : "";
  if (!isSourcePath(path)) throw invalid(`an invalid repository path for ${id}`);
  const ref = text(entry.ref);
  if (!isGitRef(ref)) throw invalid(`an invalid ref for ${id}`);
  const clusterId = text(entry.cluster_id);
  if (!CLUSTER_ID_RE.test(clusterId)) throw invalid(`an invalid cluster id for ${id}`);
  return { id, name, reconciler, repoUrl, path, ref, clusterId };
}

export function normalizeGitOpsRegistry(payload) {
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.sources)) {
    throw invalid("no source list");
  }
  if (payload.sources.length > MAX_SOURCES) throw invalid(`more than ${MAX_SOURCES} sources`);
  const tenantId = text(payload.tenant_id);
  if (!tenantId) throw invalid("no tenant");
  const sourcesRevision = revisionOf(payload.sources_revision);
  if (!sourcesRevision) throw invalid("no registry revision");
  const sources = [];
  const seen = new Set();
  for (const entry of payload.sources) {
    const source = normalizeSource(entry);
    if (seen.has(source.id)) throw invalid(`a duplicate source id: ${source.id}`);
    seen.add(source.id);
    sources.push(source);
  }
  return {
    tenantId,
    sourcesRevision,
    role: text(payload.role),
    changed: payload.changed === true,
    sources,
  };
}

// One draft row for the registry editor. The editable fields are the
// normalized ones, so a loaded registry is edited in place rather than
// translated into a second shape that has to be kept in step with the first.
export function gitOpsDraftRow(source = null) {
  return {
    id: source?.id || "",
    name: source?.name || "",
    reconciler: source?.reconciler || "flux",
    repoUrl: source?.repoUrl || "",
    path: source?.path || "",
    ref: source?.ref || "main",
    clusterId: source?.clusterId || "",
  };
}

// Per-row, per-field messages rather than one thrown sentence: the whole
// registry is written at once, and a save that reports only the first problem
// makes the reader submit the form once per mistake.
export function gitOpsDraftErrors(rows) {
  const errors = rows.map(() => ({}));
  const counts = new Map();
  rows.forEach((row) => {
    const id = text(row.id);
    if (id) counts.set(id, (counts.get(id) || 0) + 1);
  });
  rows.forEach((row, index) => {
    const at = errors[index];
    const id = text(row.id);
    if (!id) at.id = "Give this source an id.";
    else if (!SOURCE_ID.test(id)) at.id = "Use 1–64 lowercase letters, numbers, or hyphens.";
    else if (counts.get(id) > 1) at.id = "Another source already uses this id.";
    const name = text(row.name);
    if (!name) at.name = "Give this source a name.";
    else if (!isSourceName(name)) at.name = `Use at most ${MAX_NAME} characters on one line.`;
    if (!GITOPS_RECONCILERS.includes(text(row.reconciler))) at.reconciler = "Choose Flux or Argo CD.";
    const repoUrl = text(row.repoUrl);
    if (!repoUrl) at.repoUrl = "Give the clone URL of the repository.";
    else if (!isRepoURL(repoUrl)) at.repoUrl = "Use an HTTPS, SSH, or git@host:path clone URL with no embedded credential.";
    if (!isSourcePath(row.path)) at.path = "Use a clean repository-relative path with no . or .. segments. Leave it empty for the repository root.";
    const ref = text(row.ref);
    if (!ref) at.ref = "Name the branch, tag, or commit to reconcile from.";
    else if (!isGitRef(ref)) at.ref = "Use a valid Git branch, tag, or commit ref.";
    const clusterId = text(row.clusterId);
    if (!clusterId) at.clusterId = "Choose the cluster this source is reconciled onto.";
    else if (!CLUSTER_ID_RE.test(clusterId)) at.clusterId = "That is not a cluster id central mints.";
  });
  return errors;
}

// One source exactly as it crosses the wire. Emitting and comparing through the
// same function is what keeps the two readings of "this source" from drifting.
function canonicalSource(row) {
  return {
    id: text(row?.id),
    name: text(row?.name),
    reconciler: text(row?.reconciler),
    repo_url: text(row?.repoUrl),
    path: typeof row?.path === "string" ? row.path : "",
    ref: text(row?.ref),
    cluster_id: text(row?.clusterId),
  };
}

// Whether the editor holds anything a save would actually change. Central
// stores the registry as a set keyed by source id, so order alone is not a
// change; additions, removals, and field edits still are.
export function gitOpsDraftChanged(rows, loaded = []) {
  if (!Array.isArray(rows)) return false;
  const before = Array.isArray(loaded) ? loaded : [];
  if (rows.length !== before.length) return true;
  const canonical = (items) => items
    .map((row) => canonicalSource(row))
    .sort((a, b) => a.id.localeCompare(b.id));
  return JSON.stringify(canonical(rows)) !== JSON.stringify(canonical(before.map(gitOpsDraftRow)));
}

// The wire shape. The registry is replaced whole: a PUT carrying only the
// edited rows would delete every source the manager did not touch. That is also
// what makes removal work — an explicit empty list is how the last source
// leaves the registry — so an empty array is a legal document here rather than
// something the client quietly upgrades to "no change".
export function gitOpsRegistryBody(rows, sourcesRevision = "") {
  if (!Array.isArray(rows)) throw new Error("A registry must be a list of sources.");
  if (rows.length > MAX_SOURCES) throw new Error(`A registry holds at most ${MAX_SOURCES} sources.`);
  const revision = revisionOf(sourcesRevision);
  if (!revision) throw new Error("A registry write must carry the revision it was read at.");
  const errors = gitOpsDraftErrors(rows);
  const firstBad = errors.findIndex((row) => Object.keys(row).length > 0);
  if (firstBad >= 0) throw new Error(`Source ${firstBad + 1} is incomplete: ${Object.values(errors[firstBad])[0]}`);
  return { sources_revision: revision, sources: rows.map(canonicalSource) };
}

// A conflict answers with the registry as it stands now. It is attached to the
// error rather than thrown away so the editor can show the reader what moved
// while still holding their draft; a body that will not normalize leaves it
// absent instead of handing the console a half-read registry.
function conflictRegistry(payload) {
  try {
    return normalizeGitOpsRegistry(payload);
  } catch {
    return null;
  }
}

export function gitOpsConflict(error) {
  return error?.status === 409 && error.current ? error.current : null;
}

function refusal(status, payload) {
  const detail = typeof payload?.error === "string" ? payload.error : "";
  const error = new ApiError(status, MESSAGES[status] || detail || `Request failed (HTTP ${status}).`, {
    code: detail,
  });
  if (status === 409) error.current = conflictRegistry(payload);
  return error;
}

async function call(path, { token, method = "GET", body, signal } = {}) {
  let response;
  try {
    response = await fetch(path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(0, MESSAGES[0]);
  }
  const payload = await response.json().catch(() => null);
  // A read that did not succeed throws. It never falls through to an empty
  // registry: "this tenant has configured nothing" and "this console could not
  // find out" are answers a manager would act on differently.
  if (!response.ok) throw refusal(response.status, payload);
  return normalizeGitOpsRegistry(payload);
}

// The preview rejects with the same ApiError the network path does, carrying
// the conflicting registry as the raw envelope so the fixtures never have to
// import this module's normalizer.
function fromPreview(promise) {
  return promise.then(normalizeGitOpsRegistry, (error) => {
    if (error?.status === 409) error.current = conflictRegistry(error.payload);
    throw error;
  });
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/gitops/sources`;
}

export function fetchGitOpsSources({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return fromPreview(previewGitOpsSources(tenantId));
  return call(base(tenantId), { token, signal });
}

export function putGitOpsSources({ token, tenantId, sources, sourcesRevision, signal }) {
  const body = gitOpsRegistryBody(sources, sourcesRevision);
  if (isDevPreviewToken(token)) return fromPreview(previewPutGitOpsSources(tenantId, body));
  return call(base(tenantId), { token, method: "PUT", body, signal });
}
