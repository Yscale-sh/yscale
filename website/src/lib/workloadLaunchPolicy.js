const CATALOG_BUILDER_ROLES = new Set(["owner", "admin"]);
const GPU_LABELS = {
  any: "best available GPU",
  rtx4000ada: "RTX 4000 Ada",
  rtx4000: "RTX 4000",
  rtx6000: "RTX 6000",
  l4: "NVIDIA L4",
  l40s: "NVIDIA L40S",
  a100: "NVIDIA A100",
  h100: "NVIDIA H100",
  h200: "NVIDIA H200",
};

export function canOverrideTemplateRuntime(role) {
  return CATALOG_BUILDER_ROLES.has(role);
}

export function canInspectRawWorkload(role) {
  return CATALOG_BUILDER_ROLES.has(role);
}

// Runtime is catalog-owned for members and viewers. Reapplying it at the
// composition seam means a hidden field, stale component state, or future UI
// refactor cannot turn a non-builder launch into an unreviewed image override.
export function templateOwnedLaunchValues(role, template, values) {
  if (!values || !template || canOverrideTemplateRuntime(role)) return values;
  return {
    ...values,
    image: template.nodeOnly ? "" : template.defaults.image,
    command: template.nodeOnly ? [] : [...(template.defaults.command || [])],
    args: template.nodeOnly ? [] : [...(template.defaults.args || [])],
    env: template.nodeOnly ? [] : structuredClone(template.defaults.env || []),
  };
}

export function templateCatalogSummary(template) {
  const runtime = template.nodeOnly ? "Node capacity" : "Container job";
  const compute = template.defaults.mode === "gpu"
    ? `${template.defaults.size} · ${GPU_LABELS[template.defaults.gpuKind || "any"] || template.defaults.gpuKind}`
    : `${template.defaults.size} · CPU`;
  const referenceCount = template.nodeOnly ? 0 : (template.defaults.env || []).length;
  const references = `${referenceCount} environment reference${referenceCount === 1 ? "" : "s"}`;
  return { runtime, compute, references };
}
