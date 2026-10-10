import { ApiError } from "./apiError.js";
import {
  isDevPreviewToken,
  previewPutTemplateCatalog,
  previewTemplateCatalog,
} from "./devPreview.js";
import {
  GPU_KINDS,
  SIZE_OPTIONS,
  isWorkloadName,
  recipeTokensError,
  templateForWorkload,
} from "./workloadTemplates.js";

// The tenant's workload template catalog, as central serves it from
// /v1/tenants/{tenant_id}/templates. The console reads it before it offers a
// launch and submits the entry it actually used back as provenance, so this
// module owns three things: the shape central answers with, the shape a manager
// may write, and whether a selection the reader is holding is still launchable.

export const TEMPLATE_MODES = Object.freeze(["cpu", "gpu"]);

// Catalog ids are interpolated into a same-origin path and handed to central as
// a header, so they stay in the narrow lowercase shape every other id uses.
const TEMPLATE_ID = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

const MAX_TITLE = 120;
const MAX_KIND = 64;
const MAX_DESCRIPTION = 400;
const MAX_MARK = 8;
const MAX_IMAGE = 512;
export const MAX_TEMPLATE_ENV = 64;
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const OBJECT_NAME = /^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$/;
const OBJECT_KEY = /^[A-Za-z0-9._-]{1,253}$/;
// Central's catalog can hold more than a console can present as cards without
// becoming a list nobody reads. The ceiling is the seam's, not a product rule:
// a bigger catalog is refused loudly rather than silently truncated.
const MAX_TEMPLATES = 32;

const MESSAGES = {
  0: "Could not reach the template catalog service from this browser.",
  401: "This session is no longer valid. Sign in again.",
  403: "This tenant role cannot change the workload template catalog.",
  404: "That tenant does not exist for this account.",
  409: "The template catalog changed since it was read. Reload it and apply the change again.",
  413: "The template catalog is too large.",
  415: "The template catalog must be JSON.",
  429: "Too many requests. Try again in a moment.",
  502: "The template catalog service did not answer.",
  503: "The template catalog service is unavailable in this environment.",
};

// Where the catalog this tenant is being served came from. Unknown values are
// shown as central sent them rather than relabelled: a source this console has
// never heard of is still a fact about the tenant.
const SOURCE_LABELS = {
  default: "Platform default catalog",
  platform: "Platform default catalog",
  tenant: "Tenant catalog",
  inherited: "Inherited catalog",
};

export function templateSourceLabel(source) {
  return SOURCE_LABELS[source] || source || "unreported source";
}

function text(value) {
  return typeof value === "string" ? value.trim() : "";
}

function invalid(detail) {
  return new ApiError(502, `The template catalog service answered with ${detail}.`);
}

function normalizeRecipeTokens(value, id, field) {
  if (value === undefined || value === null) return [];
  const error = recipeTokensError(value, field);
  if (error) throw invalid(`${error.toLowerCase()} for ${id}`);
  return [...value];
}

export function normalizeTemplateEnv(value, id = "template") {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invalid(`environment references that are not a list for ${id}`);
  if (value.length > MAX_TEMPLATE_ENV) throw invalid(`more than ${MAX_TEMPLATE_ENV} environment references for ${id}`);
  const seen = new Set();
  return value.map((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)
      || Object.keys(entry).some((key) => !["name", "valueFrom"].includes(key))
      || Object.keys(entry).length !== 2) throw invalid(`an unknown environment reference shape for ${id}`);
    const name = text(entry.name);
    if (!ENV_NAME.test(name)) throw invalid(`an invalid environment variable name for ${id}`);
    if (seen.has(name)) throw invalid(`a duplicate environment variable name: ${name}`);
    seen.add(name);
    const valueFrom = entry.valueFrom;
    if (!valueFrom || typeof valueFrom !== "object" || Array.isArray(valueFrom)) throw invalid(`an invalid reference for ${name}`);
    const sourceKeys = Object.keys(valueFrom);
    if (sourceKeys.length !== 1 || !["secretKeyRef", "configMapKeyRef"].includes(sourceKeys[0])) {
      throw invalid(`an invalid reference source for ${name}`);
    }
    const sourceType = sourceKeys[0];
    const source = valueFrom[sourceType];
    if (!source || typeof source !== "object" || Array.isArray(source)
      || Object.keys(source).length !== 2 || !Object.hasOwn(source, "name") || !Object.hasOwn(source, "key")) {
      throw invalid(`an invalid ${sourceType} for ${name}`);
    }
    const objectName = text(source.name);
    const key = text(source.key);
    if (!OBJECT_NAME.test(objectName)) throw invalid(`an invalid object name for ${name}`);
    if (!OBJECT_KEY.test(key)) throw invalid(`an invalid object key for ${name}`);
    return { name, valueFrom: { [sourceType]: { name: objectName, key } } };
  });
}

export function managedRuntimeBindingEnv(name, key) {
  const variableName = text(name);
  const bindingKey = text(key);
  if (!ENV_NAME.test(variableName) || !/^[A-Z_][A-Z0-9_]{0,62}$/.test(bindingKey)) {
    throw new ApiError(400, "Choose a valid environment variable name and managed runtime binding key.");
  }
  return { name: variableName, valueFrom: { secretKeyRef: { name: "yscale-runtime-bindings", key: bindingKey } } };
}

function templateEnvErrors(value, nodeOnly, runtimeBindingKeys) {
  if (nodeOnly) return value?.length ? [{ name: "Node-only templates cannot carry environment references." }] : [];
  if (!Array.isArray(value)) return [{ name: "Environment references must be a list." }];
  if (value.length > MAX_TEMPLATE_ENV) return [{ name: `Add at most ${MAX_TEMPLATE_ENV} environment references.` }];
  const counts = new Map(value.map((entry) => [text(entry?.name), 0]));
  value.forEach((entry) => counts.set(text(entry?.name), (counts.get(text(entry?.name)) || 0) + 1));
  return value.map((entry) => {
    const error = {};
    const name = text(entry?.name);
    if (!ENV_NAME.test(name)) error.name = "Use letters, numbers, and underscores; start with a letter or underscore.";
    else if (counts.get(name) > 1) error.name = "Another reference already uses this variable name.";
    const source = entry?.valueFrom?.secretKeyRef || entry?.valueFrom?.configMapKeyRef || {};
    if (!OBJECT_NAME.test(text(source.name))) error.objectName = "Enter a bare Kubernetes object name.";
    if (!OBJECT_KEY.test(text(source.key))) error.key = "Enter a Kubernetes-compatible key.";
    if (entry?.valueFrom?.secretKeyRef?.name === "yscale-runtime-bindings" && runtimeBindingKeys && !runtimeBindingKeys.has(text(source.key))) {
      error.key = "Choose a managed runtime binding that still exists.";
    }
    return error;
  });
}

function canonicalDraftEnv(value) {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => {
    const sourceType = entry?.valueFrom?.secretKeyRef ? "secretKeyRef" : "configMapKeyRef";
    const source = entry?.valueFrom?.[sourceType] || {};
    return { name: text(entry?.name), valueFrom: { [sourceType]: { name: text(source.name), key: text(source.key) } } };
  });
}

// A revision is an opaque marker the console shows and compares, never orders,
// so a counter and an etag-like string are both acceptable answers.
function revisionOf(value) {
  if (typeof value === "string" && value.trim()) return value.trim().slice(0, 64);
  if (Number.isInteger(value) && value >= 0) return String(value);
  return "";
}

function normalizeTemplate(entry) {
  if (!entry || typeof entry !== "object") throw invalid("a template that is not an object");
  const id = text(entry.id);
  if (!TEMPLATE_ID.test(id)) throw invalid(`an invalid template id: ${id || "(empty)"}`);
  const version = entry.version;
  if (!Number.isInteger(version) || version < 1) throw invalid(`an invalid version for ${id}`);
  const title = text(entry.title);
  if (!title || title.length > MAX_TITLE) throw invalid(`an invalid title for ${id}`);
  const kind = text(entry.kind);
  if (!kind || kind.length > MAX_KIND) throw invalid(`an invalid kind for ${id}`);
  const description = text(entry.description);
  if (description.length > MAX_DESCRIPTION) throw invalid(`an overlong description for ${id}`);
  const defaults = entry.defaults;
  if (!defaults || typeof defaults !== "object") throw invalid(`no defaults for ${id}`);
  const nodeOnly = entry.nodeOnly === true;
  const name = text(defaults.name);
  if (!isWorkloadName(name)) throw invalid(`an invalid default workload name for ${id}`);
  const image = nodeOnly ? "" : text(defaults.image);
  if (image.length > MAX_IMAGE || /[\r\n]/.test(image)) throw invalid(`an invalid default image for ${id}`);
  if (!nodeOnly && !image) throw invalid(`no default image for ${id}`);
  const size = text(defaults.size);
  if (!SIZE_OPTIONS.includes(size)) throw invalid(`an unsupported default size for ${id}`);
  const mode = text(defaults.mode);
  if (!TEMPLATE_MODES.includes(mode)) throw invalid(`an unsupported default compute mode for ${id}`);
  let gpuKind = text(defaults.gpuKind);
  if (gpuKind && !GPU_KINDS.includes(gpuKind)) throw invalid(`an unsupported default GPU kind for ${id}`);
  if (mode === "gpu" && !gpuKind) gpuKind = "any";
  const command = normalizeRecipeTokens(defaults.command, id, "Default command");
  const args = normalizeRecipeTokens(defaults.args, id, "Default arguments");
  const env = normalizeTemplateEnv(defaults.env, id);
  if (nodeOnly && (command.length || args.length)) throw invalid(`an execution recipe on node-only template ${id}`);
  if (nodeOnly && env.length) throw invalid(`environment references on node-only template ${id}`);
  return {
    id,
    version,
    // The mark is the card's two-glyph badge. Central is not required to carry
    // one, so a catalog without it still renders rather than showing a hole.
    mark: text(entry.mark).slice(0, MAX_MARK) || id.slice(0, 2).toUpperCase(),
    title,
    kind,
    description,
    nodeOnly,
    disabled: entry.disabled === true,
    defaults: { name, image, size, mode, ...(gpuKind ? { gpuKind } : {}), command, args, env },
  };
}

export function normalizeTemplateCatalog(payload) {
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.templates)) {
    throw invalid("no template list");
  }
  if (payload.templates.length > MAX_TEMPLATES) throw invalid(`more than ${MAX_TEMPLATES} templates`);
  const tenantId = text(payload.tenant_id);
  if (!tenantId) throw invalid("no tenant");
  const catalogRevision = revisionOf(payload.catalog_revision);
  if (!catalogRevision) throw invalid("no catalog revision");
  const source = text(payload.source);
  if (!source) throw invalid("no catalog source");
  const templates = [];
  const seen = new Set();
  for (const entry of payload.templates) {
    const template = normalizeTemplate(entry);
    if (seen.has(template.id)) throw invalid(`a duplicate template id: ${template.id}`);
    seen.add(template.id);
    templates.push(template);
  }
  return {
    tenantId,
    catalogRevision,
    source,
    templates,
    role: text(payload.role),
    changed: payload.changed === true,
  };
}

// One draft row for the catalog manager. The editable fields are the normalized
// ones, so a loaded catalog is edited in place rather than translated into a
// second shape that has to be kept in step with the first.
export function templateDraftRow(template = null) {
  return {
    id: template?.id || "",
    version: template?.version || 1,
    mark: template?.mark || "",
    title: template?.title || "",
    kind: template?.kind || "Run-once job",
    description: template?.description || "",
    nodeOnly: template?.nodeOnly === true,
    disabled: template?.disabled === true,
    defaults: {
      name: template?.defaults?.name || "",
      image: template?.nodeOnly ? "" : template?.defaults?.image || "",
      size: template?.defaults?.size || "small",
      mode: template?.defaults?.mode || "cpu",
      gpuKind: template?.defaults?.gpuKind || "any",
      command: template?.nodeOnly ? [] : [...(template?.defaults?.command || [])],
      args: template?.nodeOnly ? [] : [...(template?.defaults?.args || [])],
      env: template?.nodeOnly ? [] : normalizeTemplateEnv(template?.defaults?.env || [], template?.id || "template"),
    },
  };
}

// Per-row, per-field messages rather than one thrown sentence: the manager edits
// several entries at once, and a save that reports only the first problem makes
// the reader submit the form once per mistake.
export function templateDraftErrors(rows, runtimeBindings) {
  const runtimeBindingKeys = runtimeBindings ? new Set(runtimeBindings.map((binding) => text(binding?.key))) : null;
  const errors = rows.map(() => ({}));
  const counts = new Map();
  rows.forEach((row) => {
    const id = text(row.id);
    if (id) counts.set(id, (counts.get(id) || 0) + 1);
  });
  rows.forEach((row, index) => {
    const at = errors[index];
    const id = text(row.id);
    if (!id) at.id = "Give this template an id.";
    else if (!TEMPLATE_ID.test(id)) at.id = "Use 1–63 lowercase letters, numbers, or hyphens.";
    else if (counts.get(id) > 1) at.id = "Another template already uses this id.";
    const title = text(row.title);
    if (!title) at.title = "Give this template a title.";
    else if (title.length > MAX_TITLE) at.title = `Use at most ${MAX_TITLE} characters.`;
    const kind = text(row.kind);
    if (!kind) at.kind = "Say what kind of request this template makes.";
    else if (kind.length > MAX_KIND) at.kind = `Use at most ${MAX_KIND} characters.`;
    if (text(row.description).length > MAX_DESCRIPTION) at.description = `Use at most ${MAX_DESCRIPTION} characters.`;
    if (text(row.mark).length > MAX_MARK) at.mark = `Use at most ${MAX_MARK} characters.`;
    const defaults = row.defaults || {};
    if (!isWorkloadName(text(defaults.name))) at.name = "Use 1–63 lowercase letters, numbers, dots, or hyphens.";
    const image = text(defaults.image);
    if (!row.nodeOnly && !image) at.image = "An OCI image is required unless this template is node-only.";
    else if (image.length > MAX_IMAGE || /[\r\n]/.test(image)) at.image = "The image must fit on one line.";
    if (!SIZE_OPTIONS.includes(text(defaults.size))) at.size = "Choose a supported size.";
    if (!TEMPLATE_MODES.includes(text(defaults.mode))) at.mode = "Choose CPU or GPU.";
    if (text(defaults.mode) === "gpu" && !GPU_KINDS.includes(text(defaults.gpuKind))) at.gpuKind = "Choose a GPU kind this platform places.";
    if (!row.nodeOnly) {
      const commandError = recipeTokensError(defaults.command, "Command");
      const argsError = recipeTokensError(defaults.args, "Arguments");
      if (commandError) at.command = commandError;
      if (argsError) at.args = argsError;
    }
    const env = templateEnvErrors(defaults.env || [], row.nodeOnly, runtimeBindingKeys);
    if (env.some((entry) => Object.keys(entry).length)) at.env = env;
  });
  return errors;
}

// One entry exactly as it crosses the wire: trimmed, and carrying a GPU kind
// only where the mode asks for one. Emitting and comparing through the same
// function is what keeps the two readings of "this entry" from drifting apart.
function canonicalTemplate(row) {
  const defaults = row?.defaults || {};
  const mode = text(defaults.mode);
  return {
    id: text(row?.id),
    version: Number.isInteger(row?.version) && row.version >= 1 ? row.version : 1,
    mark: text(row?.mark),
    title: text(row?.title),
    kind: text(row?.kind),
    description: text(row?.description),
    nodeOnly: row?.nodeOnly === true,
    disabled: row?.disabled === true,
    defaults: {
      name: text(defaults.name),
      image: row?.nodeOnly ? "" : text(defaults.image),
      size: text(defaults.size),
      mode,
      ...(mode === "gpu" ? { gpuKind: text(defaults.gpuKind) } : {}),
      ...(!row?.nodeOnly && defaults.command?.length ? { command: [...defaults.command] } : {}),
      ...(!row?.nodeOnly && defaults.args?.length ? { args: [...defaults.args] } : {}),
      ...(!row?.nodeOnly && defaults.env?.length ? { env: canonicalDraftEnv(defaults.env) } : {}),
    },
  };
}

// What a save would publish for this entry, with the version left out of the
// comparison. A trailing space in a title, or a GPU kind left behind by an
// entry that is CPU-only again, emits the document already on record — neither
// is a change a version may move for.
function templateContentKey(row) {
  return JSON.stringify({ ...canonicalTemplate(row), version: 0 });
}

// An entry the reader changed is a different template from the one records were
// launched under, so it earns the next version. Untouched entries keep theirs,
// which is what makes a version on a record mean something later.
export function versionedTemplateDraft(rows, loaded = []) {
  const previous = new Map(loaded.map((template) => [template.id, template]));
  return rows.map((row) => {
    const trimmed = { ...row, id: text(row.id) };
    const before = previous.get(trimmed.id);
    const next = templateDraftRow(trimmed);
    if (!before) return { ...next, version: 1 };
    const moved = templateContentKey(before) !== templateContentKey(trimmed);
    return { ...next, version: moved ? before.version + 1 : before.version };
  });
}

// Whether the editor holds anything a save would actually change. Compared by
// position against what was loaded, so an addition, a removal, and a reorder
// count as much as an edited field — and a retyped-identical value does not.
export function templateDraftChanged(rows, loaded = []) {
  if (!Array.isArray(rows)) return false;
  const before = Array.isArray(loaded) ? loaded : [];
  if (rows.length !== before.length) return true;
  return rows.some((row, index) => templateContentKey(row) !== templateContentKey(before[index]));
}

// The wire shape. The catalog is replaced whole: a PUT that carried only the
// edited entries would delete every entry the manager did not touch. The
// revision the rows were read at travels with them, so a write composed against
// a catalog someone else has already replaced is refused rather than applied.
export function templateCatalogBody(rows, catalogRevision = "") {
  if (!Array.isArray(rows)) throw new Error("A catalog must be a list of templates.");
  if (rows.length > MAX_TEMPLATES) throw new Error(`A catalog holds at most ${MAX_TEMPLATES} templates.`);
  const revision = revisionOf(catalogRevision);
  if (!revision) throw new Error("A catalog write must carry the revision it was read at.");
  const errors = templateDraftErrors(rows);
  const firstBad = errors.findIndex((row) => Object.keys(row).length > 0);
  if (firstBad >= 0) throw new Error(`Template ${firstBad + 1} is incomplete: ${Object.values(errors[firstBad])[0]}`);
  return { catalog_revision: revision, templates: rows.map(canonicalTemplate) };
}

export function catalogTemplateById(catalog, id) {
  const templates = Array.isArray(catalog) ? catalog : catalog?.templates;
  if (!Array.isArray(templates)) return null;
  return templates.find((template) => template.id === id) || null;
}

// Whether the entry a reader is holding may still be launched. Every answer but
// the empty string is a dead end on purpose: a submit against a template the
// catalog no longer carries is a run central records provenance it never
// served, and a submit at a version the catalog has moved past is a run
// launched from a document nobody is looking at.
export function templateSelectionError(catalog, id, version = null) {
  if (!catalog) return "The template catalog has not been read yet, so there is nothing to launch from.";
  const template = catalogTemplateById(catalog, id);
  if (!template) return "This template is no longer in the tenant catalog. Choose a template that is.";
  if (template.disabled) return "This template is disabled in the tenant catalog and cannot be launched.";
  if (version !== null && template.version !== version) {
    return `This template changed to version ${template.version} while the form was open. Reload it before launching.`;
  }
  return "";
}

// What a record was launched from. Central records the template on the record
// itself, so a record that carries one is provenance; a record from before the
// contract has only its own shape, and reading a template back off that shape is
// a guess this console must label as one.
export function templateReceipt(workload, catalog = null) {
  const recorded = workload?.template;
  const id = text(recorded?.id);
  if (recorded && typeof recorded === "object" && TEMPLATE_ID.test(id)) {
    const known = catalogTemplateById(catalog, id);
    const version = Number.isInteger(recorded.version) && recorded.version >= 1 ? recorded.version : null;
    return {
      exact: true,
      id,
      version,
      catalogRevision: revisionOf(recorded.catalog_revision),
      title: known?.version === version ? known.title : id,
      note: "Recorded by central on this submission.",
    };
  }
  const inferred = templateForWorkload(workload);
  return {
    exact: false,
    id: inferred.id,
    version: null,
    catalogRevision: "",
    title: inferred.title,
    note: "Inferred from the request shape; this record carries no template provenance.",
  };
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
  if (!response.ok) {
    const detail = typeof payload?.error === "string" ? payload.error : "";
    throw new ApiError(response.status, MESSAGES[response.status] || detail || `Request failed (HTTP ${response.status}).`, {
      code: detail,
    });
  }
  return normalizeTemplateCatalog(payload);
}

function base(tenantId) {
  return `/api/tenants/${encodeURIComponent(tenantId)}/templates`;
}

export function fetchTemplateCatalog({ token, tenantId, signal }) {
  if (isDevPreviewToken(token)) return previewTemplateCatalog(tenantId).then(normalizeTemplateCatalog);
  return call(base(tenantId), { token, signal });
}

export function putTemplateCatalog({ token, tenantId, templates, catalogRevision, signal }) {
  const body = templateCatalogBody(templates, catalogRevision);
  if (isDevPreviewToken(token)) return previewPutTemplateCatalog(tenantId, body).then(normalizeTemplateCatalog);
  return call(base(tenantId), { token, method: "PUT", body, signal });
}
