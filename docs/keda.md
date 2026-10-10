# yscale + KEDA + autoscaling

Three patterns for getting yscale to provision capacity in response to load:

1. **Workload CRD** — one-shot job, one user-submitted YAML, kubectl apply (see `architecture.md`).
2. **KEDA / Deployment / HPA** — annotate a pod template; yscale provisions when pods go Pending. **This doc.**
3. **Argo Workflows / Tekton / Kubeflow Pipelines** — pattern (2) applied to step-by-step pipeline tools.

---

## The bridge: PendingPodWatcher

yscale-agent runs a goroutine that watches Pending pods cluster-wide. Two conditions to trigger:

1. `pod.spec.nodeSelector` contains `yscale.sh/burst-node: "true"`
2. `pod.metadata.annotations` contains `yscale.sh/burst-template: <YAML>`

When a Pending pod matches both, the watcher:

1. Parses the annotation as a Workload spec fragment (gpu / cpu / memory / storage / budget / reliability)
2. Hashes (namespace, template) → group key
3. If we've submitted for this group in the last 30s, skip (burst is still warming)
4. If we've already submitted for this specific pod UID, skip (no double-submit)
5. POSTs a `nodeOnly: true` Workload to central
6. Central provisions the burst; standard K8s scheduling binds the pod to it once the kubelet joins

`nodeOnly: true` tells central "provision the burst node but don't create a Job" — the customer's controller (KEDA / Deployment / Argo) already created the pod.

---

## `yscale inject` — supported-tooling seam

The two boilerplate fields — `nodeSelector: {yscale.sh/burst-node: "true"}` and
the matching NoSchedule toleration — are mechanical. Author the
`yscale.sh/burst-template` annotation on your Pod template, pipe the manifest
through `yscale inject`, and the tool supplies the selector and toleration
into place. It is a pure text transform: nothing touches the cluster.

Supported kinds: `Pod`; `Deployment` / `StatefulSet` / `DaemonSet` /
`ReplicaSet`; `Job` / `CronJob`; KEDA `ScaledJob` at
`spec.jobTargetRef.template`; and Argo `Workflow` for each
`spec.templates[]`. Anything else passes through unchanged.

```yaml
# controller.yaml — you write this
apiVersion: apps/v1
kind: Deployment
metadata: { name: inference }
spec:
  selector: { matchLabels: { app: inference } }
  template:
    metadata:
      labels: { app: inference }
      annotations:
        yscale.sh/burst-template: |
          gpu: { kind: a100, count: 1 }
          budget: { maxUSD: 5, deadline: 1h }
    spec:
      containers:
        - name: server
          image: ghcr.io/your-org/inference:latest
          resources:
            limits: { nvidia.com/gpu: 1 }   # your normal GPU request
```

```sh
yscale inject -f controller.yaml | kubectl apply -f -
```

The output adds:

```yaml
      nodeSelector:
        yscale.sh/burst-node: "true"
      tolerations:
        - { key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }
```

Behaviour worth knowing:

- **Never invents the annotation.** A template without
  `yscale.sh/burst-template` is left alone.
- **Fail-closed on conflict.** A pre-existing
  `nodeSelector.yscale.sh/burst-node` set to anything other than `"true"` is
  refused rather than overwritten.
- **Compatible existing fields count.** An `Exists` or `Equal true`
  toleration with a matching effect is respected; no duplicate is added.
- **Idempotent.** Running the tool twice produces the same output as
  running it once.
- **Validates the template.** The annotation body is checked against the
  same nodeOnly workload contract central enforces at submit time, so a
  malformed template fails locally instead of at apply time.

Stdin works too: `cat controller.yaml | yscale inject -f - | kubectl apply -f -`.

---

## KEDA — full walkthrough

KEDA scales a Deployment from 0 → N based on any trigger (queue depth, Prometheus, Kafka lag, cron, etc.). Yscale bursts get provisioned to fit the new pods.

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inference
  namespace: ml-team-a
spec:
  replicas: 0                                  # KEDA owns this
  selector: { matchLabels: { app: inference } }
  template:
    metadata:
      labels: { app: inference }
      annotations:
        # ─── The bit that tells yscale "this pod needs a burst" ───
        yscale.sh/burst-template: |
          gpu: { kind: a100, count: 1, reliability: reliable, maxHourlyUSD: 2.00 }
          budget: { maxUSD: 5, deadline: 1h }
          storage:
            cache:
              - name: weights
                source: { bucket: my-models, prefix: llama-7b/, credentialsSecret: r2-creds }
                target: /models
                retention: keep
    spec:
      # ─── Required: tells K8s scheduler this pod wants a burst-node ───
      nodeSelector:
        yscale.sh/burst-node: "true"
      tolerations:
        - { key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }
      containers:
        - name: server
          image: ghcr.io/your-org/inference:latest
          resources:
            requests: { cpu: 2, memory: 8Gi }
            limits:   { nvidia.com/gpu: 1 }
          ports: [{ containerPort: 8080 }]
```

### KEDA ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: inference-autoscale
  namespace: ml-team-a
spec:
  scaleTargetRef:
    name: inference
  minReplicaCount: 0
  maxReplicaCount: 20
  pollingInterval: 30
  cooldownPeriod: 300
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(rate(inference_requests_total[1m]))
        threshold: "10"
```

### What happens

```
Request rate ramps up
   │
   ▼
KEDA queries Prometheus, sees rate=42, threshold=10 → desiredReplicas=4
   │
   ▼
KEDA HPA patches Deployment.spec.replicas = 4
   │
   ▼
K8s creates 4 pods. All Pending. (No nodes match yscale.sh/burst-node selector.)
   │
   ▼
yscale-agent.PendingPodWatcher observes pods:
   ├─ Each has the annotation + selector → eligible
   ├─ Dedup by UID + 30s group cooldown
   └─ Submits 1-N nodeOnly Workloads to central (one per unique pod UID,
      throttled to one per 30s per template hash)
   │
   ▼
Central provisions bursts on Linode / AWS / Fly / etc.
Burst kubelets join the cluster as Nodes.
   │
   ▼
K8s scheduler binds the Pending pods to the new burst Nodes.
   │
   ▼
Inference serves requests. Queue drains.
   │
   ▼
KEDA scales Deployment back to 0.
Pods deleted. Burst nodes are now empty.
   │
   ▼
yscale-agent.IdleNodeWatcher sees each burst node carrying no schedulable pods.
   ├─ Starts a grace window the first pass it observes the node idle
   ├─ Resets it the moment the node takes work again, disappears, or the
   │  connector can't read the pod list
   └─ After 10m continuously idle, reports Idle for that burst to central
   │
   ▼
Central checks the burst is this tenant's, in this cluster, with this node name,
and nodeOnly — then asks the connector to cordon the node and confirm it is STILL
empty. Only on that confirmation does it tear the burst down and mark the
workload cancelled.
```

---

## Picking the right shape

### Inference services (Deployment + KEDA)

What you want: bursts up when demand is up, gone when it's not. Pattern above.

Sizing tips:
- `reliability` is `reliable` (or omitted) today; `spot` and `any` are rejected at submit time because every provider launch path is on-demand, so accepting them would bill reliable capacity under a spot label.
- Use `replicas` (multi-pod-per-burst) when each request is small but you want >1 worker per node. For 1-vCPU-per-pod workloads on a 32-vCPU GPU box, set `replicas: 32` on the Workload and KEDA's HPA at 1; one burst hosts all.
- Don't set `deadline` too short (pods get killed mid-request when burst expires). Match it to your KEDA `cooldownPeriod` + buffer.
- Idle teardown, not `deadline`, is what normally ends these bursts. Keep the idle grace comfortably longer than your KEDA `cooldownPeriod`, or the burst is torn down during a lull the autoscaler was about to end anyway and the next request pays a cold start.

---

## Idle tear-down

nodeOnly bursts back pods yscale never created, so nothing reports them finished when KEDA scales to zero. The connector closes that gap.

**What it does.** Every minute it takes one list of burst nodes and one cluster-wide list of pods, and builds occupancy per node from them — one pair of calls per sweep, whatever the fleet size. A node is *busy* if it carries an assigned Pending or Running pod that isn't being deleted. Once a node has been continuously observed idle for `idleTeardown.grace` (default **10m**), the connector asks central to tear that burst down.

**What doesn't count as work:**

| Ignored | Why |
|---|---|
| DaemonSet pods | On every node by definition — counting them means no node is ever idle |
| Static / mirror pods (`kubernetes.io/config.mirror`) | Kubelet-owned infrastructure, no controller to reschedule |
| `Succeeded` / `Failed` pods | The run is over |
| Pods with a deletion timestamp | Already draining; whatever deleted them has moved on |

**What resets the window.** The node taking work, the node disappearing, the node going NotReady, a pod list the connector couldn't read, and a connector restart. Unknown is never treated as idle, so a throttled or partitioned API server delays a teardown rather than causing one. A failed cluster-wide pod list blinds the whole sweep, so every window restarts and no Idle is sent that pass. The cost of that is a burst billing for another grace period; the backstop for the pathological case is central's own silence ceiling (below).

**The preflight — why an idle report is not enough on its own.** The report describes what the connector saw on its last sweep. The scheduler is free to bind a pod into the gap between that sweep and central acting on it, and nothing central holds can see that pod. So after the record checks pass and *before* it claims anything, central pushes the connector a `prepare_idle_teardown` command: cordon the named node, then list pods cluster-wide for it and apply the same occupancy rules as the table above. Any occupying pod, an unreadable API, a refusal, a dropped socket, a timeout, or a connector too old to know the command means **no claim, no provider teardown, no workload finish, and no acknowledgement** — the request stays with the connector. On success the node is left **cordoned**, so nothing new can land while central reaps it. A managed Job burst's idle report is still an acknowledged no-op and is never cordoned.

**Holding the cordon across retries.** That cordon outlives the preflight. If inline cleanup fails, central restores the burst with `ReapPending`, and the ordinary watchdog retries it independently of workload budgets or lifetime ceilings. The cordon stays while that durable retry is outstanding: giving it back would let the scheduler place work onto a node central is about to delete. Operators should repair the provider, receipt, or settlement dependency and let the retry finish; uncordon only after cancelling the pending teardown or proving the provider resource is meant to remain.

**Withdrawal is explicit.** The connector holds an unacknowledged idle request and re-sends it until central answers, so one made during a central outage is still pending when central returns. Only `IdleNodeWatcher` — after observing the node busy or unknown — withdraws it. An ordinary health report does not, which matters because the connector re-states every burst node's phase on a timer (below); a report that also revoked pending requests would let an unrelated tick cancel a teardown. `Removed` still supersedes `Idle`.

**Scope limitation — read this before running multiple connectors on one cluster.** Idle tear-down needs *cluster-wide* pod visibility, because "idle" is a claim about every pod on the node. The connector **refuses to run the watcher at all** unless **both** hold, and logs which one failed at startup:

1. **No `workloadNamespace`.** A connector started with `-workload-namespace` (Helm `workloadNamespace`) can only see one namespace's pods, so a burst node busy serving another team would look empty to it.

```
idle burst teardown disabled: this connector is scoped to one namespace, so it
cannot tell an idle burst node from one busy with another namespace's pods
```

2. **`rbac.scope: cluster`.** Being unscoped says nobody restricted the connector; it does not say the connector's Role can list pods cluster-wide. The chart's default scope is `namespaced`, so the shipped install passes `-authoritative-pod-visibility=false` and the watcher stays off. The chart computes that flag from the RBAC it renders — the connector never guesses.

```
idle burst teardown disabled: this connector has not been granted authoritative
cluster-wide pod visibility, so it cannot tell an idle burst node from one it
simply cannot see
```

There is no partial version of this — either the pod list is authoritative or the signal is wrong. Do **not** widen the namespaced Role with a cluster-wide pod `list` to switch the watcher on; that grant reads every team's workloads and is the exposure the namespaced scope exists to avoid. Those installs use `budget.deadline` / `budget.maxUSD` instead — see below.

**Central's backstop — observation silence, not age.** `YSCALE_NODE_ONLY_MAX_LIFETIME` (default **6h**) is the longest a nodeOnly burst may go **unobserved**, not the longest it may run. The observation is a specific claim: the idle watcher read the **whole cluster's pods** on that sweep, and so could have reported the node idle. It re-states that at most every `occupancyObserveInterval` (default **5m**) per burst, central stamps it, and the backstop measures from the latest stamp.

**It applies only where an observation was owed.** The connector states in its `hello` whether it has the cluster-wide pod visibility at all, and central records that promise on the burst when it admits it. A burst whose connector **promised** and has sent nothing measures from its creation time — that is a connector which died between admission and its first sweep, and it is the one case where "never observed" really is silence. A burst whose connector promised **nothing** is not silent, it is unwatched, and this ceiling never touches it: aging those out from creation destroyed active KEDA-backed nodes at 6h for a feature the default namespaced install never had. Those are bounded at admission instead — see below.

**Node health is not an observation.** A connector reports every burst node's phase whether or not it can see the cluster's pods, so a health report proves the connector is alive, not that the idle signal works. Only the pod-list claim counts. A failed pod list sends nothing.

That distinction is the whole point for KEDA-shaped capacity: **a node that is up and serving under a cluster-scoped connector is observed continuously and is never reaped for being old** — a burst can run for days. What gets reaped is a burst that *was* being observed and has gone quiet, because the connector that was supposed to end it is now presumed dead. It ignores a *longer* declared deadline (`deadline: 24h` does not buy more silence), a *shorter* declared deadline still wins, and it runs through the same claim/receipt/finish path as every other teardown. Set it to `0` to let an abandoned nodeOnly burst run indefinitely.

**Where that leaves you if the connector cannot see pods.** Two installs claim nothing in their `hello`: `workloadNamespace` set, and `rbac.scope: namespaced` (the **shipped default**). Nothing in them can ever report a node idle, and the silence ceiling has no observation to measure from — so the bound is put on at **admission** instead:

- A nodeOnly submission that **declares** `budget.deadline` or `budget.maxUSD` is admitted exactly as written. Your budget is your budget; central does not shorten it, lengthen it, or add a second bound beside it.
- A nodeOnly submission that declares **neither** is given a `budget.deadline` equal to `YSCALE_NODE_ONLY_MAX_LIFETIME` (default **6h**) before it is stored. It is written onto the spec, so `yscale get` and the API show the real limit rather than a policy applied invisibly later. No caller change is needed — this is what bounds the connector's own KEDA scale-up submissions.
- If an operator sets `YSCALE_NODE_ONLY_MAX_LIFETIME=0`, that submission is **refused** (`400`) rather than admitted unbounded, and the message asks for a budget.

`idleTeardown.grace: 0` and `occupancyObserveInterval: 0` land in the same place, and for the same reason. The watcher is the only sender of an observation — a zero grace never starts it, and a zero interval refuses every observation it would have sent — so a connector with either set claims **nothing** in its `hello`. A claim central measures silence against but never receives would hold the ceiling open forever, which is worse than no claim at all. Those bursts get the admission deadline above, exactly as a namespaced install's do.

Keep `occupancyObserveInterval` well under the ceiling — the shipped 5m against 6h leaves a wide margin for a flaky API server or a connector restart.

**Tuning:**

```sh
helm upgrade yscale-agent ... --set idleTeardown.grace=30m            # longer lull tolerated
helm upgrade yscale-agent ... --set idleTeardown.grace=0              # disable; the claim goes with it, so the admission deadline (or your declared budget) is what ends the burst
helm upgrade yscale-agent ... --set occupancyObserveInterval=2m       # tighter liveness margin
```

### Batch / queue workers (ScaledJob)

What you want: one pod per queue message, no double-processing, no lingering capacity. Use `ScaledJob`, not `ScaledObject`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata: { name: process-batch, namespace: ml-team-a }
spec:
  jobTargetRef:
    template:
      metadata:
        annotations:
          yscale.sh/burst-template: |
            gpu: { kind: l4, count: 1 }
            budget: { maxUSD: 2, deadline: 30m }
      spec:
        nodeSelector: { yscale.sh/burst-node: "true" }
        tolerations: [{ key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }]
        containers:
          - name: worker
            image: ghcr.io/your-org/batch-worker:latest
            resources: { limits: { nvidia.com/gpu: 1 } }
  triggers:
    - type: aws-sqs-queue
      metadata:
        queueURL: https://sqs.../queue
        queueLength: "5"
        awsRegion: us-east-1
      authenticationRef: { name: sqs-creds }
```

KEDA's `ScaledJob` creates one K8s Job per N messages. yscale provisions one burst per Job (or batches by template hash within 30s).

### Training (one-shot, no autoscaler)

Just use the Workload CRD directly. No KEDA needed:

```yaml
apiVersion: yscale.sh/v1
kind: Workload
metadata: { name: train, namespace: ml-team-a }
spec:
  image: ghcr.io/your-org/trainer:latest
  gpu: { kind: a100, count: 1, reliability: reliable }
  budget: { maxUSD: 50, deadline: 8h }
  storage:
    persistent:
      - { name: ckpt, sizeGB: 200, target: /workspace, snapshot: { to: { bucket: m, prefix: t/, credentialsSecret: r2-creds }, interval: 5m } }
```

KEDA isn't the right pattern for one-shot training — there's nothing to autoscale on. Submit the Workload, watch logs, let it finish.

### Argo Workflows

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: { name: train-and-evaluate }
spec:
  entrypoint: train-then-evaluate
  templates:
    - name: train-then-evaluate
      dag:
        tasks:
          - { name: train, template: train }
          - { name: evaluate, dependencies: [train], template: evaluate }
    - name: train
      metadata:
        annotations:
          yscale.sh/burst-template: |
            gpu: { kind: a100, reliability: reliable }
            budget: { maxUSD: 30, deadline: 4h }
      nodeSelector: { yscale.sh/burst-node: "true" }
      tolerations: [{ key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }]
      container:
        image: ghcr.io/your-org/trainer:latest
    - name: evaluate
      ...
```

Argo's `template.metadata.annotations` propagates to the underlying pod. PendingPodWatcher sees them; provisions accordingly.

---

## Dedup, cooldowns, and double-submit prevention

| Mechanism | What it prevents |
|---|---|
| UID-level dedup | Same pod observed multiple times (informer re-list, watch reconnect) submits once |
| 30s group cooldown | 50 Pending pods with the same template don't spawn 50 redundant submissions while bursts are still warming |
| Per-pod-UID forget on Delete | A pod deleted and recreated (KEDA recreation, manual restart) re-triggers submit |
| Failure doesn't mark seen | Network blip on submit retries on next event for the same UID |

Tunable via `PendingPodWatcher.SubmitCooldown` (default 30s). Set to `0` in tests; keep at 30s in production.

---

## What this DOES NOT do (yet)

- **Cross-Workload burst sharing** — different pod templates → different bursts. Two pods with the same template within the 30s cooldown share one burst (via multi-replica), but two with different annotation hashes each get their own burst even if the underlying SKU is the same. Real bin-packing is the next chunk.
- **Idle tear-down on namespace-scoped connectors** — needs cluster-wide pod visibility, so it is off whenever `workloadNamespace` is set. See the scope limitation above.
- **Sub-grace scale-to-zero** — the burst survives the whole idle grace before it is torn down, so a workload that idles for two minutes between requests keeps its node (which is the point). Scaling capacity down faster than the grace means lowering the grace and paying more cold starts.

---

## Troubleshooting

### Pods stay Pending forever, no burst appears

1. Check whether Yscale rejected an incompatible hard scheduling constraint:
   ```sh
   kubectl get pod <name> -o jsonpath='{.metadata.annotations.yscale\.sh/burst-rejection}{"\n"}'
   ```
   A rejection is recorded before any central capacity request:

   | Code | Meaning |
   |---|---|
   | `missing_burst_toleration` | The pod cannot tolerate the burst-node `NoSchedule` taint. |
   | `unsupported_node_name` | A newly provisioned node cannot satisfy a pre-bound `nodeName`. |
   | `unsupported_required_affinity` | Required node/pod affinity is not carried into placement. |
   | `unsupported_node_selector` | The selector is not part of the supported burst-node contract. |
   | `node_selector_conflicts_with_template` | A known provider/region/GPU selector disagrees with the burst template. |

   Supported hard selectors are `yscale.sh/burst-node=true`, matching
   Yscale provider/region/GPU labels, `nvidia.com/gpu.present=true` for GPU
   templates, and Linux/amd64 stable node labels. Preferred affinity remains
   advisory and does not block admission.

2. Check the agent sees them:
   ```sh
   kubectl -n yscale-system logs -l app.kubernetes.io/name=yscale-agent -c agent --since=10m | grep pending
   ```
   Should see `pending-pod watcher starting` at startup, then `submit burst for pending pod` events.

3. Verify both markers on the pod:
   ```sh
   kubectl get pod <name> -o yaml | grep -A2 'nodeSelector:\|burst-template'
   ```
   Missing either = watcher skips.

4. Check the namespace is in `rbac.allowedNamespaces`:
   ```sh
   kubectl get rolebinding -n <ns> | grep yscale-agent-workloads
   ```

### Burst appears but pod still Pending

1. Check the pod's toleration:
   ```sh
   kubectl get pod <name> -o jsonpath='{.spec.tolerations}'
   ```
   Must include `{key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule}`.

2. Check the burst node's taint matches:
   ```sh
   kubectl get node ys-burst-<id> -o jsonpath='{.spec.taints}'
   ```

3. Check resource requests aren't exceeding the burst's capacity:
   ```sh
   kubectl describe node ys-burst-<id>
   ```

### KEDA scales but yscale doesn't

The KEDA-created pods don't have the annotation. KEDA copies the Deployment's `spec.template.metadata.annotations` automatically, so verify it's there:

```sh
kubectl get deploy inference -o jsonpath='{.spec.template.metadata.annotations}'
```

If empty, you put the annotation on the Deployment's metadata instead of `spec.template.metadata`. The annotation must be on the **pod template**, not the Deployment itself.
