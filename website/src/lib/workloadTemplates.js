export const WORKLOAD_TEMPLATES_VERSION = 3;

export const RECIPE_MAX_TOKENS = 16;
export const RECIPE_MAX_TOKEN_BYTES = 256;
export const RECIPE_MAX_BYTES = 2048;

const RECIPE_CONTROL = /[\u0000-\u001f\u007f]/;
const byteLength = (value) => new TextEncoder().encode(value).length;

export function recipeTokensFromText(value) {
  return value === "" ? [] : String(value).split("\n");
}

export function recipeTextFromTokens(tokens) {
  return Array.isArray(tokens) ? tokens.join("\n") : "";
}

export function recipeTokensError(tokens, label) {
  if (!Array.isArray(tokens)) return `${label} must be a list of tokens.`;
  if (tokens.length > RECIPE_MAX_TOKENS) return `${label} accepts at most ${RECIPE_MAX_TOKENS} lines.`;
  let total = 0;
  for (const token of tokens) {
    if (typeof token !== "string") return `${label} tokens must be text.`;
    if (!token.length) return `${label} cannot contain an empty line.`;
    if (RECIPE_CONTROL.test(token)) return `${label} tokens cannot contain control characters.`;
    const bytes = byteLength(token);
    if (bytes > RECIPE_MAX_TOKEN_BYTES) return `Each ${label.toLowerCase()} token must be at most ${RECIPE_MAX_TOKEN_BYTES} bytes.`;
    total += bytes;
    if (total > RECIPE_MAX_BYTES) return `${label} must be at most ${RECIPE_MAX_BYTES.toLocaleString()} bytes in total.`;
  }
  return "";
}

export const WORKLOAD_TEMPLATES = [
  {
    id: "container-job",
    version: WORKLOAD_TEMPLATES_VERSION,
    mark: "01",
    title: "Generic container job",
    kind: "Run-once job",
    description: "Run an OCI image to completion on fresh CPU or GPU capacity.",
    defaults: {
      name: "container-job",
      image: "docker.io/library/busybox:1.36",
      size: "small",
      mode: "cpu",
    },
  },
  {
    id: "pytorch-training",
    version: WORKLOAD_TEMPLATES_VERSION,
    mark: "PT",
    title: "PyTorch training job",
    kind: "Run-once GPU job",
    description: "Launch a PyTorch image with an explicit GPU shape and budget ceiling.",
    defaults: {
      name: "pytorch-train",
      image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
      size: "large",
      mode: "gpu",
      gpuKind: "rtx4000ada",
      command: ["python"],
      args: ["-c", "import torch; d=\"cuda\"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec(\"for _ in range(3):\\n e=w*x+b-y\\n w-=.1*(e*x).mean()\\n b-=.1*e.mean()\"); print(float(w),float(b))"],
    },
  },
  {
    id: "node-capacity",
    version: WORKLOAD_TEMPLATES_VERSION,
    mark: "N+",
    title: "Node-only capacity",
    kind: "Capacity request",
    description: "Provision a burst node for a controller that already owns the workload.",
    nodeOnly: true,
    defaults: {
      name: "node-capacity",
      image: "",
      size: "medium",
      mode: "cpu",
    },
  },
];

export const SIZE_OPTIONS = ["nano", "small", "medium", "large", "xlarge", "2xlarge"];

// Central refuses spot and any before it acts on a submission, so on-demand
// capacity is the only mode a launch can carry. It is a fixed property of the
// request rather than a choice, and the label is what the form states.
export const LAUNCH_RELIABILITY = "reliable";
export const LAUNCH_RELIABILITY_LABEL = "Reliable · on-demand";

// The namespace a launch lands in comes from the tenant summary, never from
// this module — see workloadNamespaces in consoleData.js. This is only the
// fallback that summary falls back to, so a central that predates the field
// still launches where every tenant has always been able to.
export const DEFAULT_WORKLOAD_NAMESPACE = "default";

export const DATA_PROVIDER_OPTIONS = [
  { value: "r2", label: "Cloudflare R2" },
  { value: "s3", label: "Amazon S3" },
];

// The GPU catalog mirrors each backend's MapGPUType seam: a kind maps to the
// exact GPU counts that provider has a plan for, and nothing else. Central
// resolves the plan before a node is created, so a kind or count missing here
// is a launch that dies at admission — the console must never offer it. "any"
// is the provider's cheapest-GPU default (rtx4000ada on Linode, l4 on AWS) and
// so carries that family's counts.
const LINODE_GPU_COUNTS = {
  any: [1, 2, 4],
  rtx4000ada: [1, 2, 4],
  rtx4000: [1, 2, 4], // alias Linode accepts for rtx4000ada
  rtx6000: [1, 2, 3, 4],
};

const AWS_GPU_COUNTS = {
  any: [1, 4, 8],
  l4: [1, 4, 8],
  l40s: [1, 4, 8],
  a100: [8],
  h100: [8],
  h200: [8],
};

const GPU_COUNTS_BY_BACKEND = { linode: LINODE_GPU_COUNTS, aws: AWS_GPU_COUNTS };

// The backend choices that can carry a GPU at all: fly.io, GCP, and Azure are
// CPU-only and the router rejects a GPU request against them outright.
const GPU_CAPABLE_BACKENDS = ["auto", "linode", "aws"];

// Kinds offered in the launch form, cheapest first. Aliases stay accepted by
// validation but are not offered as a second way to say the same card.
export const GPU_KINDS = ["any", "rtx4000ada", "rtx6000", "l4", "l40s", "a100", "h100", "h200"];

// Every spelling the two backends accept, for readers that parse a kind back
// out of submitted YAML rather than off the form.
export const ACCEPTED_GPU_KINDS = [
  ...new Set([...Object.keys(LINODE_GPU_COUNTS), ...Object.keys(AWS_GPU_COUNTS)]),
];

// Auto-routing mirrors the router: the RTX cards and the cheapest-GPU default
// land on Linode, every datacenter card lands on AWS. Null means the pinned
// backend places no GPUs at all.
export function gpuBackendFor(backend, gpuKind) {
  if (backend === "linode" || backend === "aws") return backend;
  if (backend !== "auto") return null;
  return Object.hasOwn(LINODE_GPU_COUNTS, gpuKind) ? "linode" : "aws";
}

// The GPU counts the backend this request actually routes to can place for
// this kind, or null when that backend has no plan for the kind.
export function gpuCountsFor(backend, gpuKind) {
  const resolved = gpuBackendFor(backend, gpuKind);
  const counts = resolved ? GPU_COUNTS_BY_BACKEND[resolved] : null;
  return counts && Object.hasOwn(counts, gpuKind) ? counts[gpuKind] : null;
}

// The kinds selectable under a backend: all of them while placement is
// automatic, only that provider's catalog once a backend is pinned.
export function gpuKindsFor(backend) {
  if (!GPU_CAPABLE_BACKENDS.includes(backend)) return [];
  if (backend === "auto") return GPU_KINDS;
  return GPU_KINDS.filter((kind) => Object.hasOwn(GPU_COUNTS_BY_BACKEND[backend], kind));
}

// Counts are ascending, so the closest placeable count at or below the request
// is the last one that fits; a request under the family's floor (a100 x1) is
// raised to the floor rather than silently dropped.
function nearestCount(counts, requested) {
  const fits = counts.filter((count) => count <= requested);
  return fits.length ? fits[fits.length - 1] : counts[0];
}

// Mode, backend, GPU kind, and count constrain each other, so every edit snaps
// the dependent fields back onto a shape the routed backend can place. Without
// this the form happily shows a combination — a100 x1, or an RTX card pinned
// to AWS — that is only rejected after submit, once the user has reviewed YAML
// they believed was accepted.
export function reconcileWorkloadForm(values) {
  const next = { ...values };
  if (next.dataProvider === "r2") next.dataRegion = "auto";
  if (next.dataProvider === "s3") next.dataEndpoint = "";
  if (next.outputProvider === "r2") next.outputRegion = "auto";
  if (next.outputProvider === "s3") next.outputEndpoint = "";
  if (next.mode === "gpu" && !GPU_CAPABLE_BACKENDS.includes(next.backend)) next.backend = "auto";
  if (next.backend !== "linode") next.region = "";
  if (next.mode !== "gpu") return next;
  const kinds = gpuKindsFor(next.backend);
  if (!kinds.includes(next.gpuKind)) next.gpuKind = kinds[0];
  const counts = gpuCountsFor(next.backend, next.gpuKind);
  const requested = Number(next.gpuCount);
  if (!counts.includes(requested)) next.gpuCount = String(nearestCount(counts, requested));
  // Central cannot enforce a customer GPU price ceiling on AWS until its
  // catalog is authoritative for the launch region. Keep the reviewed YAML on
  // the accepted path when an edit changes automatic placement from Linode to
  // an AWS-only card.
  if (gpuBackendFor(next.backend, next.gpuKind) === "aws") next.maxHourlyUSD = "";
  return next;
}

export function templateById(id) {
  return WORKLOAD_TEMPLATES.find((template) => template.id === id) || null;
}

// The template a record was launched from is not stored on the record, so it is
// read back off the shape central kept: a node-only request, a GPU request, or
// the plain container job everything else is. The lookup is by template id —
// the order of WORKLOAD_TEMPLATES is a display order and must not be
// load-bearing, or reordering the launch page silently relabels every record.
const SHAPE_TEMPLATE_IDS = {
  nodeOnly: "node-capacity",
  gpu: "pytorch-training",
  container: "container-job",
};

export function templateForWorkload(workload) {
  const spec = workload?.spec?.spec;
  if (spec?.nodeOnly) return templateById(SHAPE_TEMPLATE_IDS.nodeOnly);
  if (spec?.gpu) return templateById(SHAPE_TEMPLATE_IDS.gpu);
  return templateById(SHAPE_TEMPLATE_IDS.container);
}

// Central decides an unqualified submission against the first namespace the
// tenant is authorized for, so the form opens on that same entry: what the
// reader sees selected is what central would have picked for them anyway.
export function initialWorkloadForm(template, namespaces = [DEFAULT_WORKLOAD_NAMESPACE]) {
  return reconcileWorkloadForm({
    name: template.defaults.name,
    namespace: namespaces[0],
    image: template.defaults.image,
    command: template.nodeOnly ? [] : [...(template.defaults.command || [])],
    args: template.nodeOnly ? [] : [...(template.defaults.args || [])],
    env: template.nodeOnly ? [] : structuredClone(template.defaults.env || []),
    size: template.defaults.size,
    mode: template.defaults.mode,
    backend: "auto",
    region: "",
    gpuKind: template.defaults.gpuKind || "any",
    gpuCount: "1",
    reliability: LAUNCH_RELIABILITY,
    maxHourlyUSD: "",
    maxUSD: "1.00",
    deadline: "1h",
    dataEnabled: false,
    dataProvider: "r2",
    dataName: "input-data",
    dataBucket: "",
    dataPrefix: "",
    dataEndpoint: "",
    dataRegion: "auto",
    dataCredentialsSecret: "yscale-data",
    dataTarget: "/data/input",
    dataRetention: "ephemeral",
    dataSizeHintGB: "100",
    outputEnabled: false,
    outputProvider: "r2",
    outputName: "results",
    outputBucket: "",
    outputPrefix: "",
    outputEndpoint: "",
    outputRegion: "auto",
    outputCredentialsSecret: "yscale-data",
    outputTarget: "/outputs",
    outputMaxFiles: "100",
    outputMaxSizeGB: "10",
  });
}

const YAML_SAFE_NAME = /^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$/;

// The one place a workload name is decided. The launch form and the template
// catalog both answer to it, so a default the catalog accepts can never be a
// name the form then refuses.
export function isWorkloadName(value) {
  return typeof value === "string" && YAML_SAFE_NAME.test(value);
}

const K8S_SECRET_NAME = /^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$/;
const BUCKET_NAME = /^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])$/;
const AWS_REGION = /^[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]*$/;
const DURATION = /^(?:[1-9][0-9]*)(?:s|m|h)$/;

function isSafeAbsolutePath(value) {
  if (typeof value !== "string" || !value.startsWith("/") || value === "/" || /[\r\n]/.test(value)) return false;
  const segments = value.slice(1).split("/");
  return !segments.some((segment) => segment === "" || segment === "." || segment === "..");
}

function isR2Endpoint(value) {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "https:"
      && parsed.hostname.endsWith(".r2.cloudflarestorage.com")
      && parsed.hostname !== ".r2.cloudflarestorage.com"
      && !parsed.username
      && !parsed.password
      && !parsed.search
      && !parsed.hash
      && (parsed.pathname === "/" || parsed.pathname === "");
  } catch {
    return false;
  }
}

export function validateWorkloadForm(template, values, namespaces = [DEFAULT_WORKLOAD_NAMESPACE]) {
  const errors = {};
  if (!YAML_SAFE_NAME.test(values.name)) errors.name = "Use 1–63 lowercase letters, numbers, dots, or hyphens.";
  // The select only offers this tenant's authorized namespaces, but a state
  // reached any other way — a tenant whose authorization was narrowed while the
  // form sat open — has to be caught here rather than by central's admission
  // decision, after the reader believed the YAML was accepted.
  if (!namespaces.includes(values.namespace)) errors.namespace = "Choose a namespace this tenant is authorized to use.";
  if (!template.nodeOnly && !values.image.trim()) errors.image = "An OCI image is required.";
  if (/\r|\n/.test(values.image)) errors.image = "The image must fit on one line.";
  if (!template.nodeOnly) {
    const commandError = recipeTokensError(values.command, "Command");
    const argsError = recipeTokensError(values.args, "Arguments");
    if (commandError) errors.command = commandError;
    if (argsError) errors.args = argsError;
  }
  if (!SIZE_OPTIONS.includes(values.size)) errors.size = "Choose a supported size.";
  if (!DURATION.test(values.deadline)) errors.deadline = "Use a duration such as 30m, 1h, or 3600s.";
  if (!(Number(values.maxUSD) > 0)) errors.maxUSD = "Set a budget above $0.";
  if (values.region && values.backend !== "linode") errors.region = "A region request requires the Linode backend.";
  if (values.dataEnabled) {
    if (template.nodeOnly) errors.dataEnabled = "Node-only capacity does not own a Job data volume.";
    if (!YAML_SAFE_NAME.test(values.dataName)) errors.dataName = "Use a lowercase attachment name up to 63 characters.";
    if (!BUCKET_NAME.test(values.dataBucket) || values.dataBucket.includes("..")) errors.dataBucket = "Enter a valid S3 or R2 bucket name.";
    if (typeof values.dataPrefix !== "string" || values.dataPrefix.length > 1024 || /[\r\n]/.test(values.dataPrefix)) errors.dataPrefix = "Use an object prefix of at most 1,024 characters on one line.";
    if (!K8S_SECRET_NAME.test(values.dataCredentialsSecret)) errors.dataCredentialsSecret = "Enter the bare Kubernetes Secret name from this workload namespace.";
    if (!isSafeAbsolutePath(values.dataTarget)) errors.dataTarget = "Use one canonical absolute container path, such as /data/input.";
    if (values.dataRetention !== "ephemeral") errors.dataRetention = "Web launches use a Job-local input deleted with the run.";
    const sizeHint = Number(values.dataSizeHintGB);
    if (!Number.isInteger(sizeHint) || sizeHint < 1 || sizeHint > 65536) errors.dataSizeHintGB = "Set an integer from 1 to 65,536 GiB.";
    if (values.dataProvider === "r2") {
      if (!isR2Endpoint(values.dataEndpoint)) errors.dataEndpoint = "Use the HTTPS R2 account endpoint ending in .r2.cloudflarestorage.com.";
    } else if (values.dataProvider === "s3") {
      if (!AWS_REGION.test(values.dataRegion)) errors.dataRegion = "Use an AWS region such as us-east-1.";
    } else {
      errors.dataProvider = "Choose Cloudflare R2 or Amazon S3.";
    }
  }
  if (values.outputEnabled) {
    if (template.nodeOnly) errors.outputEnabled = "Node-only capacity does not own a Job output volume.";
    if (!YAML_SAFE_NAME.test(values.outputName)) errors.outputName = "Use a lowercase output name up to 63 characters.";
    if (!BUCKET_NAME.test(values.outputBucket) || values.outputBucket.includes("..")) errors.outputBucket = "Enter a valid S3 or R2 bucket name.";
    if (typeof values.outputPrefix !== "string" || values.outputPrefix.length > 1024 || /[\r\n]/.test(values.outputPrefix)) errors.outputPrefix = "Use an object prefix of at most 1,024 characters on one line.";
    if (!K8S_SECRET_NAME.test(values.outputCredentialsSecret)) errors.outputCredentialsSecret = "Enter the bare Kubernetes Secret name from this workload namespace.";
    if (!isSafeAbsolutePath(values.outputTarget)) errors.outputTarget = "Use one canonical absolute container path, such as /outputs.";
    if (values.dataEnabled && (values.outputTarget === values.dataTarget || values.outputTarget.startsWith(`${values.dataTarget}/`) || values.dataTarget.startsWith(`${values.outputTarget}/`))) {
      errors.outputTarget = "Use an output path that does not overlap the input mount.";
    }
    const maxFiles = Number(values.outputMaxFiles);
    if (!Number.isInteger(maxFiles) || maxFiles < 1 || maxFiles > 1000) errors.outputMaxFiles = "Set an integer from 1 to 1,000 files.";
    const maxSize = Number(values.outputMaxSizeGB);
    if (!Number.isInteger(maxSize) || maxSize < 1 || maxSize > 1024) errors.outputMaxSizeGB = "Set an integer from 1 to 1,024 GiB.";
    if (values.outputProvider === "r2") {
      if (!isR2Endpoint(values.outputEndpoint)) errors.outputEndpoint = "Use the HTTPS R2 account endpoint ending in .r2.cloudflarestorage.com.";
    } else if (values.outputProvider === "s3") {
      if (!AWS_REGION.test(values.outputRegion)) errors.outputRegion = "Use an AWS region such as us-east-1.";
    } else {
      errors.outputProvider = "Choose Cloudflare R2 or Amazon S3.";
    }
  }
  if (values.mode === "gpu") {
    // The form reconciles these as they change, but a state built any other
    // way — a restored draft, a hand-edited value — still has to be caught
    // here rather than at the provider, after the request is accepted.
    const resolved = gpuBackendFor(values.backend, values.gpuKind);
    const counts = gpuCountsFor(values.backend, values.gpuKind);
    if (!resolved) errors.backend = `The ${values.backend} backend does not place GPU workloads.`;
    else if (!counts) errors.gpuKind = `${resolved} does not offer ${values.gpuKind} GPUs.`;
    if (!Number.isInteger(Number(values.gpuCount)) || Number(values.gpuCount) < 1) errors.gpuCount = "GPU count must be at least 1.";
    else if (counts && !counts.includes(Number(values.gpuCount))) errors.gpuCount = `${resolved} places ${values.gpuKind} in groups of ${counts.join(", ")}.`;
    // The form states this rather than offering it, so only a state built some
    // other way can carry an unsupported mode. It is refused here rather than
    // by central, which rejects the submission outright.
    if (values.reliability !== LAUNCH_RELIABILITY) errors.reliability = "Only reliable on-demand capacity can be launched.";
    if (values.maxHourlyUSD !== "" && resolved === "aws") errors.maxHourlyUSD = "AWS GPU price caps are unavailable until region-aware pricing is authoritative.";
    else if (values.maxHourlyUSD !== "" && !(Number(values.maxHourlyUSD) > 0)) errors.maxHourlyUSD = "Set a positive hourly cap or leave it blank.";
  }
  return errors;
}

function yamlString(value) {
  return JSON.stringify(String(value));
}

export function workloadYAML(template, values) {
  const lines = [
    "apiVersion: yscale.sh/v1",
    "kind: Workload",
    "metadata:",
    `  name: ${yamlString(values.name)}`,
    `  namespace: ${yamlString(values.namespace)}`,
    "spec:",
  ];
  if (!template.nodeOnly) lines.push(`  image: ${yamlString(values.image.trim())}`);
  if (template.nodeOnly) lines.push("  nodeOnly: true");
  if (!template.nodeOnly && values.command.length) lines.push(`  command: ${JSON.stringify(values.command)}`);
  if (!template.nodeOnly && values.args.length) lines.push(`  args: ${JSON.stringify(values.args)}`);
  if (!template.nodeOnly && (values.env || []).length) {
    lines.push("  env:");
    for (const entry of values.env) {
      const sourceType = entry.valueFrom.secretKeyRef ? "secretKeyRef" : "configMapKeyRef";
      const source = entry.valueFrom[sourceType];
      lines.push(`    - name: ${yamlString(entry.name)}`);
      lines.push("      valueFrom:");
      lines.push(`        ${sourceType}:`);
      lines.push(`          name: ${yamlString(source.name)}`);
      lines.push(`          key: ${yamlString(source.key)}`);
    }
  }
  lines.push(`  size: ${values.size}`);
  if (values.backend !== "auto") lines.push(`  backend: ${values.backend}`);
  if (values.region) lines.push(`  region: ${yamlString(values.region)}`);
  if (values.mode === "gpu") {
    lines.push("  gpu:");
    lines.push(`    kind: ${values.gpuKind}`);
    lines.push(`    count: ${Number(values.gpuCount)}`);
    lines.push(`    reliability: ${LAUNCH_RELIABILITY}`);
    if (values.maxHourlyUSD !== "") lines.push(`    maxHourlyUSD: ${Number(values.maxHourlyUSD)}`);
  }
  if (!template.nodeOnly) lines.push("  retries: 0");
  if (!template.nodeOnly && (values.dataEnabled || values.outputEnabled)) {
    lines.push("  storage:");
  }
  if (!template.nodeOnly && values.dataEnabled) {
    lines.push("    cache:");
    lines.push(`      - name: ${yamlString(values.dataName)}`);
    lines.push("        source:");
    lines.push(`          bucket: ${yamlString(values.dataBucket)}`);
    if (values.dataPrefix) lines.push(`          prefix: ${yamlString(values.dataPrefix)}`);
    if (values.dataProvider === "r2") lines.push(`          endpoint: ${yamlString(values.dataEndpoint)}`);
    lines.push(`          region: ${yamlString(values.dataProvider === "r2" ? "auto" : values.dataRegion)}`);
    lines.push(`          credentialsSecret: ${yamlString(values.dataCredentialsSecret)}`);
    lines.push(`        target: ${yamlString(values.dataTarget)}`);
    lines.push(`        retention: ${yamlString(values.dataRetention)}`);
    lines.push(`        sizeHintGB: ${Number(values.dataSizeHintGB)}`);
  }
  if (!template.nodeOnly && values.outputEnabled) {
    lines.push("    artifacts:");
    lines.push(`      - name: ${yamlString(values.outputName)}`);
    lines.push(`        target: ${yamlString(values.outputTarget)}`);
    lines.push("        to:");
    lines.push(`          bucket: ${yamlString(values.outputBucket)}`);
    if (values.outputPrefix) lines.push(`          prefix: ${yamlString(values.outputPrefix)}`);
    if (values.outputProvider === "r2") lines.push(`          endpoint: ${yamlString(values.outputEndpoint)}`);
    lines.push(`          region: ${yamlString(values.outputProvider === "r2" ? "auto" : values.outputRegion)}`);
    lines.push(`          credentialsSecret: ${yamlString(values.outputCredentialsSecret)}`);
    lines.push(`        maxFiles: ${Number(values.outputMaxFiles)}`);
    lines.push(`        maxSizeGB: ${Number(values.outputMaxSizeGB)}`);
  }
  lines.push("  budget:");
  lines.push(`    maxUSD: ${Number(values.maxUSD)}`);
  lines.push(`    deadline: ${values.deadline}`);
  return `${lines.join("\n")}\n`;
}
