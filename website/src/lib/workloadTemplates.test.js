import test from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_WORKLOAD_NAMESPACE,
  GPU_KINDS,
  WORKLOAD_TEMPLATES,
  WORKLOAD_TEMPLATES_VERSION,
  gpuBackendFor,
  gpuCountsFor,
  gpuKindsFor,
  initialWorkloadForm,
  reconcileWorkloadForm,
  recipeTextFromTokens,
  recipeTokensError,
  recipeTokensFromText,
  templateForWorkload,
  validateWorkloadForm,
  workloadYAML,
} from "./workloadTemplates.js";

const GPU_TEMPLATE = WORKLOAD_TEMPLATES.find((template) => template.defaults.mode === "gpu");

test("every approved template emits the current Workload envelope", () => {
  for (const template of WORKLOAD_TEMPLATES) {
    const values = initialWorkloadForm(template);
    assert.deepEqual(validateWorkloadForm(template, values), {});
    const yaml = workloadYAML(template, values);
    assert.match(yaml, /^apiVersion: yscale\.sh\/v1\nkind: Workload\n/);
    assert.match(yaml, /\nmetadata:\n  name: "[a-z0-9.-]+"\n  namespace: "default"\n/);
    assert.match(yaml, /\nspec:\n/);
    assert.match(yaml, /\n  size: (?:nano|small|medium|large|xlarge|2xlarge)\n/);
    assert.match(yaml, /\n  budget:\n    maxUSD: 1\n    deadline: 1h\n$/);
    if (template.nodeOnly) {
      assert.match(yaml, /\n  nodeOnly: true\n/);
      assert.doesNotMatch(yaml, /\n  image:/);
      assert.doesNotMatch(yaml, /\n  (?:command|args):/);
    } else {
      assert.match(yaml, /\n  image: ".+"\n/);
      assert.match(yaml, /\n  retries: 0\n/);
    }
  }
});

test("the v3 PyTorch template seeds and emits the working recipe exactly", () => {
  assert.equal(WORKLOAD_TEMPLATES_VERSION, 3);
  const values = initialWorkloadForm(GPU_TEMPLATE);
  assert.deepEqual(values.command, ["python"]);
  assert.deepEqual(values.args, ["-c", "import torch; d=\"cuda\"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec(\"for _ in range(3):\\n e=w*x+b-y\\n w-=.1*(e*x).mean()\\n b-=.1*e.mean()\"); print(float(w),float(b))"]);
  assert.match(workloadYAML(GPU_TEMPLATE, values), /  command: \["python"\]\n  args: \["-c",/);
});

test("line-per-token controls preserve spaces and validate exact byte bounds", () => {
  assert.deepEqual(recipeTokensFromText("python\n-c\necho one two"), ["python", "-c", "echo one two"]);
  assert.equal(recipeTextFromTokens(["python", "-c", "echo one two"]), "python\n-c\necho one two");
  assert.match(recipeTokensError(["ok", ""], "Arguments"), /empty line/);
  assert.match(recipeTokensError(["x".repeat(257)], "Command"), /256 bytes/);
  assert.match(recipeTokensError(["bad\u007ftoken"], "Command"), /control/);
  assert.match(recipeTokensError(Array.from({ length: 17 }, () => "x"), "Arguments"), /at most 16/);
});

// Central decides an unqualified submission against the first namespace the
// tenant is authorized for. The form has to open on that same entry, or the
// reader is shown one namespace and central uses another.
test("the launch form opens on the tenant's first authorized namespace", () => {
  for (const template of WORKLOAD_TEMPLATES) {
    const values = initialWorkloadForm(template, ["ml-team-a", "default"]);
    assert.equal(values.namespace, "ml-team-a");
    assert.deepEqual(validateWorkloadForm(template, values, ["ml-team-a", "default"]), {});
    assert.match(workloadYAML(template, values), /\n  namespace: "ml-team-a"\n/);
    // A tenant whose only namespace is not "default" still opens on its own.
    assert.equal(initialWorkloadForm(template, ["platform-lab"]).namespace, "platform-lab");
  }
  // No list at all is the mixed-version case: the namespace every tenant has.
  assert.equal(initialWorkloadForm(WORKLOAD_TEMPLATES[0]).namespace, DEFAULT_WORKLOAD_NAMESPACE);
});

// The select cannot produce this, but a form left open while the tenant's
// authorization is narrowed can. It has to be refused by the console rather
// than by central, after the reader has reviewed YAML they believed was good.
test("a namespace outside the tenant's list is refused before review", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const namespaces = ["default", "ml-team-a"];
  const values = initialWorkloadForm(template, namespaces);
  assert.deepEqual(validateWorkloadForm(template, { ...values, namespace: "ml-team-a" }, namespaces), {});

  for (const namespace of ["kube-system", "ml-team-b", "", undefined, "Default", " default"]) {
    const errors = validateWorkloadForm(template, { ...values, namespace }, namespaces);
    assert.ok(errors.namespace, `${JSON.stringify(namespace)} was accepted`);
  }
  // Narrowing the tenant's list invalidates a selection that was fine before.
  assert.ok(validateWorkloadForm(template, { ...values, namespace: "ml-team-a" }, ["default"]).namespace);
});

// The reviewed YAML is the exact request that crosses the tenant API, so the
// namespace in it is the selected one and nothing else.
test("the reviewed YAML carries the selected namespace verbatim", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const values = { ...initialWorkloadForm(template, ["ml-team-a"]), name: "catalog-refresh" };
  assert.equal(workloadYAML(template, values), `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: "catalog-refresh"
  namespace: "ml-team-a"
spec:
  image: "docker.io/library/busybox:1.36"
  size: small
  retries: 0
  budget:
    maxUSD: 1
    deadline: 1h
`);
  // Editing any other field leaves the namespace where the reader put it.
  const edited = reconcileWorkloadForm({ ...values, mode: "gpu", backend: "aws" });
  assert.equal(edited.namespace, "ml-team-a");
  assert.match(workloadYAML(template, edited), /\n  namespace: "ml-team-a"\n/);
});

test("template environment references are deep-copied and emitted exactly without literal values", () => {
  const template = {
    ...WORKLOAD_TEMPLATES[0],
    defaults: {
      ...WORKLOAD_TEMPLATES[0].defaults,
      env: [
        { name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: "app-runtime", key: "database-url" } } },
        { name: "LOG_LEVEL", valueFrom: { configMapKeyRef: { name: "app-settings", key: "log.level" } } },
      ],
    },
  };
  const values = initialWorkloadForm(template);
  template.defaults.env[0].valueFrom.secretKeyRef.key = "changed-after-compose";
  assert.equal(values.env[0].valueFrom.secretKeyRef.key, "database-url");
  assert.match(workloadYAML(template, values), /  env:\n    - name: "DATABASE_URL"\n      valueFrom:\n        secretKeyRef:\n          name: "app-runtime"\n          key: "database-url"\n    - name: "LOG_LEVEL"\n      valueFrom:\n        configMapKeyRef:\n          name: "app-settings"\n          key: "log.level"\n/);
  assert.doesNotMatch(workloadYAML(template, values), /\n\s+value:/);
});

test("R2 input data is validated and emitted as an exact pull-before-run cache", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const values = {
    ...initialWorkloadForm(template, ["ml-team-a"]),
    dataEnabled: true,
    dataProvider: "r2",
    dataName: "training-data",
    dataBucket: "acme-training-data",
    dataPrefix: "fraud/v4/",
    dataEndpoint: "https://account123.r2.cloudflarestorage.com",
    dataRegion: "auto",
    dataCredentialsSecret: "r2-training-read",
    dataTarget: "/data/input",
    dataRetention: "ephemeral",
    dataSizeHintGB: "64",
  };
  assert.deepEqual(validateWorkloadForm(template, values, ["ml-team-a"]), {});
  const yaml = workloadYAML(template, values);
  assert.match(yaml, /\n  storage:\n    cache:\n      - name: "training-data"\n/);
  assert.match(yaml, /          bucket: "acme-training-data"\n          prefix: "fraud\/v4\/"\n          endpoint: "https:\/\/account123\.r2\.cloudflarestorage\.com"\n          region: "auto"\n          credentialsSecret: "r2-training-read"\n/);
  assert.match(yaml, /        target: "\/data\/input"\n        retention: "ephemeral"\n        sizeHintGB: 64\n/);
});

test("S3 input data clears endpoint state and requires a concrete AWS region", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const base = {
    ...initialWorkloadForm(template),
    dataEnabled: true,
    dataProvider: "s3",
    dataBucket: "acme-training-data",
    dataEndpoint: "https://stale.r2.cloudflarestorage.com",
    dataRegion: "us-east-1",
  };
  const values = reconcileWorkloadForm(base);
  assert.equal(values.dataEndpoint, "");
  assert.deepEqual(validateWorkloadForm(template, values), {});
  const yaml = workloadYAML(template, values);
  assert.doesNotMatch(yaml, /endpoint:/);
  assert.match(yaml, /          region: "us-east-1"\n/);
  assert.ok(validateWorkloadForm(template, { ...values, dataRegion: "east" }).dataRegion);
});

test("unsafe or incomplete object inputs are refused before review", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const base = { ...initialWorkloadForm(template), dataEnabled: true };
  const cases = [
    ["dataName", { dataName: "Bad Name" }],
    ["dataBucket", { dataBucket: "bad..bucket" }],
    ["dataEndpoint", { dataEndpoint: "http://169.254.169.254/latest" }],
    ["dataEndpoint", { dataEndpoint: "https://account.r2.cloudflarestorage.com/path" }],
    ["dataCredentialsSecret", { dataCredentialsSecret: "other/secret" }],
    ["dataTarget", { dataTarget: "../data" }],
    ["dataTarget", { dataTarget: "/data/../secret" }],
    ["dataRetention", { dataRetention: "ttl=24h" }],
    ["dataSizeHintGB", { dataSizeHintGB: "0" }],
    ["dataSizeHintGB", { dataSizeHintGB: "1.5" }],
  ];
  for (const [field, edit] of cases) {
    const errors = validateWorkloadForm(template, { ...base, ...edit });
    assert.ok(errors[field], `${JSON.stringify(edit)} was accepted`);
  }
  const nodeOnly = WORKLOAD_TEMPLATES.find((candidate) => candidate.nodeOnly);
  assert.ok(validateWorkloadForm(nodeOnly, { ...initialWorkloadForm(nodeOnly), dataEnabled: true }).dataEnabled);
});

test("R2 outputs are validated and emitted as bounded push-after-run artifacts", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const values = {
    ...initialWorkloadForm(template, ["ml-team-a"]),
    outputEnabled: true,
    outputProvider: "r2",
    outputName: "checkpoints",
    outputBucket: "acme-training-results",
    outputPrefix: "nightly/",
    outputEndpoint: "https://account123.r2.cloudflarestorage.com",
    outputCredentialsSecret: "r2-training-write",
    outputTarget: "/outputs",
    outputMaxFiles: "250",
    outputMaxSizeGB: "12",
  };
  assert.deepEqual(validateWorkloadForm(template, values, ["ml-team-a"]), {});
  const yaml = workloadYAML(template, values);
  assert.match(yaml, /\n  storage:\n    artifacts:\n      - name: "checkpoints"\n/);
  assert.match(yaml, /        target: "\/outputs"\n        to:\n          bucket: "acme-training-results"\n          prefix: "nightly\/"\n          endpoint: "https:\/\/account123\.r2\.cloudflarestorage\.com"\n          region: "auto"\n          credentialsSecret: "r2-training-write"\n        maxFiles: 250\n        maxSizeGB: 12\n/);
});

test("artifact outputs are bounded and cannot overlap an input mount", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const base = {
    ...initialWorkloadForm(template),
    outputEnabled: true,
    outputBucket: "training-results",
    outputEndpoint: "https://account.r2.cloudflarestorage.com",
  };
  const cases = [
    ["outputName", { outputName: "Bad Name" }],
    ["outputBucket", { outputBucket: "bad..bucket" }],
    ["outputEndpoint", { outputEndpoint: "http://169.254.169.254/latest" }],
    ["outputCredentialsSecret", { outputCredentialsSecret: "other/secret" }],
    ["outputTarget", { outputTarget: "/outputs/../secret" }],
    ["outputTarget", { outputTarget: "/outputs//nested" }],
    ["outputMaxFiles", { outputMaxFiles: "0" }],
    ["outputMaxFiles", { outputMaxFiles: "1001" }],
    ["outputMaxSizeGB", { outputMaxSizeGB: "1025" }],
  ];
  for (const [field, edit] of cases) {
    const errors = validateWorkloadForm(template, { ...base, ...edit });
    assert.ok(errors[field], `${JSON.stringify(edit)} was accepted`);
  }
  const overlap = validateWorkloadForm(template, { ...base, dataEnabled: true, dataTarget: "/outputs/input" });
  assert.ok(overlap.outputTarget);
  const nodeOnly = WORKLOAD_TEMPLATES.find((candidate) => candidate.nodeOnly);
  assert.ok(validateWorkloadForm(nodeOnly, { ...initialWorkloadForm(nodeOnly), outputEnabled: true }).outputEnabled);
});

test("GPU review YAML contains the exact selected placement guardrails", () => {
  const template = GPU_TEMPLATE;
  const values = {
    ...initialWorkloadForm(template),
    name: "nightly-train",
    backend: "linode",
    region: "us-east",
    gpuKind: "rtx6000",
    gpuCount: "2",
    maxHourlyUSD: "3.5",
    maxUSD: "8.25",
    deadline: "4h",
  };
  assert.deepEqual(validateWorkloadForm(template, values), {});
  assert.equal(workloadYAML(template, values), `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: "nightly-train"
  namespace: "default"
spec:
  image: "docker.io/pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime"
  command: ["python"]
  args: ["-c","import torch; d=\\"cuda\\"; w=torch.tensor(0.,device=d); b=torch.tensor(0.,device=d); x=torch.arange(4.,device=d); y=2*x+1; exec(\\"for _ in range(3):\\\\n e=w*x+b-y\\\\n w-=.1*(e*x).mean()\\\\n b-=.1*e.mean()\\"); print(float(w),float(b))"]
  size: large
  backend: linode
  region: "us-east"
  gpu:
    kind: rtx6000
    count: 2
    reliability: reliable
    maxHourlyUSD: 3.5
  retries: 0
  budget:
    maxUSD: 8.25
    deadline: 4h
`);
});

// No provider launch path holds preemptible capacity, so central refuses spot
// and any before it acts. The console has to be the same shape: one mode in
// the reviewed YAML, and anything else refused before review.
test("only reliable on-demand capacity reaches the reviewed YAML", () => {
  const values = initialWorkloadForm(GPU_TEMPLATE);
  assert.equal(values.reliability, "reliable");
  assert.deepEqual(validateWorkloadForm(GPU_TEMPLATE, values), {});
  assert.match(workloadYAML(GPU_TEMPLATE, values), /\n    reliability: reliable\n/);

  for (const reliability of ["spot", "any", "Reliable", "on-demand", "", undefined]) {
    const errors = validateWorkloadForm(GPU_TEMPLATE, { ...values, reliability });
    assert.ok(errors.reliability, `${JSON.stringify(reliability)} was accepted`);
    // Refused, and never rendered either: a tampered state cannot put a
    // preemptible request in the document that crosses the tenant API.
    assert.match(workloadYAML(GPU_TEMPLATE, { ...values, reliability }), /\n    reliability: reliable\n/);
  }

  // A CPU workload asks for no capacity mode at all, so it names none and this
  // validator has nothing to say about one.
  const cpuTemplate = WORKLOAD_TEMPLATES[0];
  const cpuValues = { ...initialWorkloadForm(cpuTemplate), reliability: "spot" };
  assert.deepEqual(validateWorkloadForm(cpuTemplate, cpuValues), {});
  assert.doesNotMatch(workloadYAML(cpuTemplate, cpuValues), /reliability/);
});

// An H100 is only sold as a full 8-GPU node, and only by AWS. The review step
// has to show that shape exactly, not the count the user first asked for.
test("an AWS datacenter GPU reviews as the whole 8-GPU node", () => {
  const values = reconcileWorkloadForm({
    ...initialWorkloadForm(GPU_TEMPLATE),
    name: "h100-run",
    gpuKind: "h100",
    maxUSD: "400.00",
    deadline: "2h",
  });
  assert.equal(values.gpuCount, "8");
  assert.equal(gpuBackendFor(values.backend, values.gpuKind), "aws");
  assert.deepEqual(validateWorkloadForm(GPU_TEMPLATE, values), {});
  assert.match(workloadYAML(GPU_TEMPLATE, values), /\n  gpu:\n    kind: h100\n    count: 8\n/);
});

test("an AWS-routed edit clears the unsupported hourly price cap", () => {
  const values = reconcileWorkloadForm({
    ...initialWorkloadForm(GPU_TEMPLATE),
    gpuKind: "a100",
    maxHourlyUSD: "2.00",
  });
  assert.equal(gpuBackendFor(values.backend, values.gpuKind), "aws");
  assert.equal(values.maxHourlyUSD, "");
  assert.deepEqual(validateWorkloadForm(GPU_TEMPLATE, values), {});

  const direct = { ...values, maxHourlyUSD: "2.00" };
  assert.match(validateWorkloadForm(GPU_TEMPLATE, direct).maxHourlyUSD, /AWS GPU price caps are unavailable/);
});

// Every console page that labels a record — usage, history, detail — reads the
// template back through this, so it has to resolve to a real template by id
// rather than by where the template happens to sit in the launch list.
test("a record is labelled by the template shape it was launched from", () => {
  const shapes = [
    { record: { spec: { spec: { nodeOnly: true } } }, id: "node-capacity" },
    { record: { spec: { spec: { gpu: { kind: "a100", count: 1 } } } }, id: "pytorch-training" },
    { record: { spec: { spec: { image: "docker.io/library/busybox:1.36" } } }, id: "container-job" },
    // A record with nothing readable is still the plain container job, not a crash.
    { record: undefined, id: "container-job" },
  ];
  for (const { record, id } of shapes) {
    const template = templateForWorkload(record);
    assert.ok(template, `no template for ${id}`);
    assert.equal(template.id, id);
    assert.ok(WORKLOAD_TEMPLATES.includes(template), `${id} is not one of the approved templates`);
    assert.ok(template.title && template.kind, `${id} has no label to render`);
  }
  // node-only wins over gpu: a capacity request is not a training job.
  assert.equal(templateForWorkload({ spec: { spec: { nodeOnly: true, gpu: { kind: "a100" } } } }).id, "node-capacity");
});

// The GPU default is the one shape a new user launches without editing
// anything, so it has to be both placeable and cheap: one RTX card, which
// automatic placement routes to Linode.
test("the GPU template defaults to a placeable one-GPU shape", () => {
  const values = initialWorkloadForm(GPU_TEMPLATE);
  assert.equal(values.backend, "auto");
  assert.equal(values.gpuCount, "1");
  assert.equal(gpuBackendFor(values.backend, values.gpuKind), "linode");
  assert.deepEqual(validateWorkloadForm(GPU_TEMPLATE, values), {});
  assert.match(workloadYAML(GPU_TEMPLATE, values), /\n  gpu:\n    kind: rtx4000ada\n    count: 1\n/);
});

// Mirrors the router: RTX cards and the cheapest-GPU default are Linode's,
// every datacenter card is AWS's. Every offered kind must reach a backend.
test("automatic placement routes every offered GPU kind to a backend that has it", () => {
  const routes = {
    any: "linode",
    rtx4000ada: "linode",
    rtx6000: "linode",
    l4: "aws",
    l40s: "aws",
    a100: "aws",
    h100: "aws",
    h200: "aws",
  };
  assert.deepEqual(GPU_KINDS, Object.keys(routes));
  for (const [kind, backend] of Object.entries(routes)) {
    assert.equal(gpuBackendFor("auto", kind), backend, `${kind} routes to the wrong backend`);
    assert.ok(gpuCountsFor("auto", kind)?.length, `${kind} has no placeable count`);
  }
});

test("each backend only offers the GPU kinds and counts it has plans for", () => {
  assert.deepEqual(gpuKindsFor("linode"), ["any", "rtx4000ada", "rtx6000"]);
  assert.deepEqual(gpuKindsFor("aws"), ["any", "l4", "l40s", "a100", "h100", "h200"]);
  assert.deepEqual(gpuCountsFor("linode", "rtx4000ada"), [1, 2, 4]);
  assert.deepEqual(gpuCountsFor("linode", "rtx6000"), [1, 2, 3, 4]);
  assert.deepEqual(gpuCountsFor("aws", "l4"), [1, 4, 8]);
  for (const kind of ["a100", "h100", "h200"]) {
    assert.deepEqual(gpuCountsFor("aws", kind), [8], `${kind} is not a full-node-only card`);
  }
  // "any" is the provider's cheapest card, so it inherits that family's counts.
  assert.deepEqual(gpuCountsFor("linode", "any"), [1, 2, 4]);
  assert.deepEqual(gpuCountsFor("aws", "any"), [1, 4, 8]);
  // A card the other provider sells is not selectable once a backend is pinned.
  assert.equal(gpuCountsFor("linode", "h100"), null);
  assert.equal(gpuCountsFor("aws", "rtx6000"), null);
  // CPU-only backends place no GPU at all.
  for (const backend of ["flyio", "gcp", "azure"]) {
    assert.equal(gpuBackendFor(backend, "any"), null);
    assert.deepEqual(gpuKindsFor(backend), []);
  }
});

test("editing backend, kind, or count leaves a combination the backend can place", () => {
  const base = initialWorkloadForm(GPU_TEMPLATE);
  const cases = [
    // An RTX card pinned to AWS falls back to the cheapest card AWS does have.
    { edit: { backend: "aws" }, expect: { gpuKind: "any", gpuCount: "1" } },
    // A card sold only as a full node raises the count to that node.
    { edit: { gpuKind: "a100" }, expect: { gpuKind: "a100", gpuCount: "8" } },
    // Coming back down snaps to the closest count Linode actually sells.
    { edit: { gpuKind: "a100" }, then: { backend: "linode" }, expect: { gpuKind: "any", gpuCount: "4" } },
    // Three RTX 4000s is not a plan; two is.
    { edit: { backend: "linode", gpuKind: "rtx4000ada", gpuCount: "3" }, expect: { gpuCount: "2" } },
    // rtx6000 does sell three.
    { edit: { backend: "linode", gpuKind: "rtx6000", gpuCount: "3" }, expect: { gpuCount: "3" } },
    // A CPU-only backend cannot hold a GPU request; placement goes back to auto.
    { edit: { backend: "flyio" }, expect: { backend: "auto" } },
  ];
  for (const { edit, then, expect } of cases) {
    let values = reconcileWorkloadForm({ ...base, ...edit });
    if (then) values = reconcileWorkloadForm({ ...values, ...then });
    for (const [key, want] of Object.entries(expect)) {
      assert.equal(values[key], want, `${JSON.stringify(edit)} left ${key}=${values[key]}`);
    }
    assert.deepEqual(validateWorkloadForm(GPU_TEMPLATE, values), {}, `${JSON.stringify(edit)} left an invalid form`);
  }
});

// The form reconciles as it goes, but a state assembled any other way still
// has to be stopped here rather than at the provider, after it is accepted.
test("impossible GPU combinations are rejected even when set directly", () => {
  const base = initialWorkloadForm(GPU_TEMPLATE);
  const impossible = [
    { field: "gpuCount", values: { gpuKind: "a100", gpuCount: "1" } },
    { field: "gpuCount", values: { backend: "aws", gpuKind: "l4", gpuCount: "2" } },
    { field: "gpuCount", values: { backend: "linode", gpuKind: "rtx4000ada", gpuCount: "8" } },
    { field: "gpuCount", values: { backend: "linode", gpuKind: "rtx6000", gpuCount: "5" } },
    { field: "gpuKind", values: { backend: "aws", gpuKind: "rtx6000" } },
    { field: "gpuKind", values: { backend: "linode", gpuKind: "h100", gpuCount: "8" } },
    { field: "gpuKind", values: { gpuKind: "rtx4090" } },
    { field: "backend", values: { backend: "flyio" } },
    { field: "backend", values: { backend: "gcp" } },
  ];
  for (const { field, values } of impossible) {
    const errors = validateWorkloadForm(GPU_TEMPLATE, { ...base, ...values });
    assert.ok(errors[field], `${JSON.stringify(values)} was accepted`);
  }
  // The same shapes on a CPU workload are none of this validator's business.
  assert.deepEqual(validateWorkloadForm(WORKLOAD_TEMPLATES[0], { ...initialWorkloadForm(WORKLOAD_TEMPLATES[0]), backend: "flyio" }), {});
});

test("invalid names, deadlines, budgets, and region requests are rejected", () => {
  const template = WORKLOAD_TEMPLATES[0];
  const values = {
    ...initialWorkloadForm(template),
    name: "Bad Name",
    deadline: "tomorrow",
    maxUSD: "0",
    region: "us-east",
  };
  const errors = validateWorkloadForm(template, values);
  assert.ok(errors.name);
  assert.ok(errors.deadline);
  assert.ok(errors.maxUSD);
  assert.ok(errors.region);
});
