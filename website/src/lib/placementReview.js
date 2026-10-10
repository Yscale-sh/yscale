import { formatUSD, statusLabel } from "./format.js";

const MAX_PLACEMENT_ALTERNATIVES = 8;

function tokenSummary(tokens, limit = 180) {
  if (!Array.isArray(tokens) || !tokens.length) return "Image default";
  const rendered = tokens.map((token) => JSON.stringify(token)).join(" · ");
  const bounded = rendered.length > limit ? `${rendered.slice(0, limit - 1)}…` : rendered;
  return `${tokens.length} token${tokens.length === 1 ? "" : "s"} · ${bounded}`;
}

export function executionRecipeRows(spec = {}) {
  const command = Array.isArray(spec.command) ? spec.command : [];
  const args = Array.isArray(spec.args) ? spec.args : [];
  const execution = spec?.execution;
  if (!command.length && !args.length && execution?.mode === "custom") {
    const commandTokens = Number.isInteger(execution.command_tokens) && execution.command_tokens >= 0 ? execution.command_tokens : 0;
    const argumentTokens = Number.isInteger(execution.argument_tokens) && execution.argument_tokens >= 0 ? execution.argument_tokens : 0;
    return [
      { label: "Program", value: `Custom entrypoint · ${commandTokens} token${commandTokens === 1 ? "" : "s"}` },
      { label: "Arguments", value: `${argumentTokens} token${argumentTokens === 1 ? "" : "s"} · values hidden after submission` },
    ];
  }
  if (!command.length && !args.length) {
    return [
      { label: "Program", value: "Image default" },
      { label: "Arguments", value: "Image default" },
    ];
  }
  return [
    { label: "Program", value: tokenSummary(command, 120) },
    { label: "Arguments", value: tokenSummary(args, 120) },
  ];
}

export function placementReviewShape({ template, values, composedVersion, selectedCluster }) {
  const backend = values?.backend === "auto" || !values?.backend ? "automatic" : values.backend;
  const backendLabel = values?.region ? `${backend} / ${values.region}` : backend;
  const compute = values?.mode === "gpu" ? `${values.gpuKind} x ${values.gpuCount} GPU` : `${values?.size || "default"} CPU`;
  const clusterNote = selectedCluster?.source === "hosted" ? ` · reserved on ${selectedCluster.name}` : "";
  return {
    template: `${template.id} v${composedVersion}`,
    namespace: `${values.namespace}${clusterNote}`,
    backend: backendLabel,
    compute,
  };
}

export const PLACEMENT_RE_REVIEW_CODES = new Set([
  "placement_changed",
  "placement_preview_expired",
  "placement_digest_invalid",
]);

export function isPlacementReReviewError(error) {
  return PLACEMENT_RE_REVIEW_CODES.has(error?.code);
}

export function normalizePreviewPlacement(preview) {
  if (!preview?.placement || (preview.status !== "ok" && preview.status !== "rejected")) return null;
  const p = preview.placement || {};
  const selected = p.selected || {};
  const constraints = p.constraints || {};
  const candidates = Array.isArray(p.candidates) ? p.candidates : [];
  const dollars = (microUSD) => Number.isFinite(Number(microUSD)) ? Number(microUSD) / 1_000_000 : null;
  return {
    grantedClusterId: p.granted_cluster_id || "",
    clusterMode: p.cluster_mode || "",
    provider: selected.provider || "",
    region: selected.region || "",
    sku: selected.sku || "",
    gpuKind: selected.gpu_kind || "",
    gpuCount: Number(selected.gpu_count) || 0,
    hourlyRate: dollars(selected.hourly_micro_usd),
    maximumDurationSeconds: Number(selected.maximum_duration_seconds || constraints.deadline_seconds) || 0,
    maximumChargeUSD: dollars(selected.maximum_charge_micro_usd || constraints.max_charge_micro_usd),
    confidence: p.availability_confidence || "",
    pricingVersion: p.pricing_version || "",
    issuedAt: p.issued_at || "",
    expiresAt: p.expires_at || "",
    quoteId: p.quote_id || "",
    digest: p.digest || "",
    candidates: candidates.filter((c) => !c.selected).slice(0, MAX_PLACEMENT_ALTERNATIVES).map((c) => ({
      provider: c.provider || "",
      region: c.region || "",
      sku: c.sku || "",
      gpuKind: c.gpu_kind || "",
      gpuCount: Number(c.gpu_count) || 0,
      hourlyRate: dollars(c.hourly_micro_usd),
      reason: c.reason || "",
    })),
  };
}

function formatDurationSeconds(seconds) {
  if (!seconds) return "Not declared";
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

export function previewPlacementRows(normalized) {
  if (!normalized) return [];
  const rows = [
    ...(normalized.gpuKind ? [{ label: "GPU", value: `${normalized.gpuKind} × ${normalized.gpuCount}` }] : []),
    { label: "SKU", value: normalized.sku || "—" },
    { label: "Provider", value: normalized.provider || "—" },
    { label: "Region", value: normalized.region || "—" },
    { label: "Granted cluster", value: normalized.grantedClusterId || "—" },
  ];
  if (normalized.hourlyRate != null) {
    rows.push({ label: "Hourly rate", value: formatUSD(normalized.hourlyRate) });
  }
  rows.push({ label: "Maximum duration", value: formatDurationSeconds(normalized.maximumDurationSeconds) });
  rows.push({ label: "Maximum charge", value: normalized.maximumChargeUSD == null ? "Not declared" : formatUSD(normalized.maximumChargeUSD) });
  rows.push({ label: "Confidence", value: normalized.confidence ? statusLabel(normalized.confidence) : "—" });
  return rows;
}

export function placementReviewGuardrail({ values, selectedCluster }) {
  if (!selectedCluster) {
    return {
      limit: `${formatUSD(values.maxUSD)} / ${values.deadline}`,
      note: "Enforcement follows the cluster central grants; the stored placement receipt is authoritative.",
    };
  }
  const centralOwned = selectedCluster?.source === "hosted";
  return {
    limit: `${formatUSD(values.maxUSD)} / ${values.deadline}`,
    note: centralOwned
      ? "Central-owned capacity may be stopped at budget or deadline."
      : "Customer-owned cluster budgets are recorded, not centrally enforced.",
  };
}

// The plain-language rows the launch review leads with. The generated YAML
// stays disclosed behind them, so these carry everything a submitter needs.
export function launchReviewRows({ values, template, composedVersion, clusterId, allowAutomatic, hostedTarget }) {
  return [
    { label: "Name", value: values.name },
    { label: "Namespace", value: `${values.namespace}${hostedTarget ? ` · reserved on ${hostedTarget.name}` : ""}` },
    { label: "Target", value: clusterId || (allowAutomatic ? "Automatic" : "no cluster selected") },
    { label: "Template", value: `${template.title} · ${template.id} v${composedVersion}` },
    { label: "Compute", value: values.mode === "gpu" ? `${values.gpuKind} × ${values.gpuCount}` : `${values.size} CPU` },
    { label: "Backend", value: `${values.backend === "auto" ? "automatic" : values.backend}${values.region ? ` / ${values.region}` : ""}` },
    ...executionRecipeRows(values),
    { label: "Input", value: values.dataEnabled ? `Pull ${values.dataProvider === "r2" ? "R2" : "S3"} into ${values.dataTarget}` : "None" },
    { label: "Output", value: values.outputEnabled ? `Push ${values.outputTarget} to ${values.outputProvider === "r2" ? "R2" : "S3"}` : "None" },
    { label: "Budget", value: `${formatUSD(values.maxUSD)} / ${values.deadline}` },
  ];
}

export function placementReceiptRows(workload) {
  const placement = workload?.placement || {};
  const mode = placement.mode === "auto"
    ? "ordered automatic"
    : placement.mode === "pinned"
      ? "explicit target"
      : "not recorded";
  return [
    { label: "Granted cluster", value: placement.granted_cluster_id || workload?.cluster_id || "not recorded" },
    { label: "Stored mode", value: mode },
    { label: "Requested cluster", value: placement.requested_cluster_id || "none recorded" },
  ];
}
