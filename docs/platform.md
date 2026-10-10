# yscale for platform teams

You run the cluster. Your users submit ML / CI / batch workloads. This doc is what you need to roll yscale out behind your existing tooling.

For the packet path see [networking](networking.md). For KEDA + autoscaling see [`keda.md`](keda.md). For storage see [`storage.md`](storage.md).

---

## The contract

1. You install one Helm chart into a `yscale-system` namespace.
2. You declare which namespaces your users' workloads can live in via `rbac.allowedNamespaces`.
3. You optionally create shared K8s Secrets in those namespaces for bucket credentials.
4. Your users submit Workloads via `kubectl apply`, `yscale apply`, or annotated Deployments (KEDA-friendly).
5. yscale provisions burst capacity behind the scenes; pods run as normal K8s pods on real K8s nodes; your Grafana / Loki / kubectl-based observability keeps working.

You configure once. You never re-configure per user, per project, or per workload.

---

## Install

```sh
# From the repository or release-archive root; no hosted chart registry is used.
helm install yscale-agent ./deploy/helm/yscale-agent \
  --namespace yscale-system --create-namespace \
  --set endpoint=https://central.example.invalid \
  --set token="$YSCALE_TOKEN" \
  --set rbac.scope=namespaced \
  --set rbac.allowedNamespaces='{ml-team-a,ml-team-b,ci-bursts}'
```

What this puts in the cluster:

| Resource | Purpose |
|---|---|
| `Deployment <release>-yscale-agent` | Cluster Connector plus its mesh sidecar, joined as `tag:yscale` |
| `Deployment <release>-yscale-agent-gateway` | Mesh gateway, joined as `tag:yscale-gateway`, enabled by default; advertises cluster routes and SNATs what it forwards to its own pod IP |
| `Secret yscale-agent-token` | Holds `YSCALE_TOKEN` |
| `ConfigMap yscale-gateway-routes` | Auto-detected pod, Service, and node CIDRs advertised by the gateway (or an explicit `gateway.advertiseRoutes` override) |
| `CustomResourceDefinition workloads.yscale.sh` | `kubectl apply -f` shape |
| `ClusterRole + ClusterRoleBinding` | Read nodes, evict pods, approve CSRs, watch Workload CRs |
| `Role + RoleBinding (kube-system)` | Create bootstrap-token Secrets (scoped to `kube-system` only) |
| `Role + RoleBinding (per allowedNamespace)` | Jobs/pods CRUD in each allowed namespace |
| `Role + RoleBinding (per allowedNamespace, storage-creds)` | `get` on Secrets in each allowed namespace, for a workload's `spec.storage` credentials |

### Verify

```sh
# Pod up and connected
kubectl -n yscale-system get pods
# yscale-agent-7c5d-xyz   2/2   Running

kubectl -n yscale-system logs -l app.kubernetes.io/name=yscale-agent -c agent --tail=20
# {"msg":"connected to central","cluster":"cl_580df066..."}

# CRD installed
kubectl get crd workloads.yscale.sh

# Gateway is installed and has its route configuration
kubectl -n yscale-system get deploy yscale-agent-yscale-agent-gateway
kubectl -n yscale-system get configmap yscale-gateway-routes
```

---

## Networking model

No public WireGuard endpoint, UDP/51820 Service, or inbound firewall rule is
required. Central mints an ephemeral mesh auth key for each burst; the burst
uses `tailscale up` to join its tenant's self-hosted coordination server in
the default managed-mesh installation. For the default `full` tier, the gateway advertises
the cluster's auto-detected pod, Service, and node routes from
`yscale-gateway-routes`; bursts accept those routes and reach the cluster through
the gateway. The gateway SNATs what it forwards to its own pod IP, which is what
makes the return path work: the cluster has no route for the burst pod supernet
`10.244.0.0/16`, so a reply addressed to the burst pod's real IP would be
dropped, while a reply addressed to the gateway is ordinary pod-to-pod traffic
the customer CNI already routes. Conntrack on the gateway undoes the rewrite and
sends the reply back over the mesh, where the burst's own advertised pod CIDR
makes it routable. The cost is that in-cluster workloads see the gateway's
address rather than the burst pod's, so NetworkPolicy cannot distinguish one
burst from another.

---

## Burst lifecycle — what ends a burst

Two shapes, with different endings. Getting this wrong is the difference between a bill you expected and one you didn't.

**Managed Job bursts** (Workload CRD, `yscale apply`). Central created the Job, so the connector reports the Job's terminal state and central reaps the burst. `spec.budget.deadline` / `maxUSD` bound the failure case where that report never arrives.

**nodeOnly bursts** (KEDA, Deployments, Argo — anything that goes through a Pending pod). Central never created the pods, so there is no completion to report. Three things can end one:

| Signal | Who sends it | When |
|---|---|---|
| Node removed | Connector (`NodeWatcher`) | The burst's Node object left the cluster |
| Node idle | Connector (`IdleNodeWatcher`) | The node carried no schedulable pods for `idleTeardown.grace`, default 10m |
| Observation silence | Central (reaper watchdog) | The connector promised cluster-wide pod visibility and has not proved it can still see whether the node is idle for `YSCALE_NODE_ONLY_MAX_LIFETIME`, default 6h |
| Admission deadline | Central (submit path) | The connector promised no such visibility and the submission declared no budget: it is stored with a `budget.deadline` of the same duration |

The first two are the normal path and both come from the customer's cluster. An idle report is not acted on by itself: central puts a **preflight** back to the reporting connector first — cordon the node, then re-check it cluster-wide — and a pod that landed after the connector's last sweep means no claim, no teardown, and no acknowledgement.

A confirmed node stays **cordoned** so nothing lands on it while central reaps. If inline cleanup fails, central restores the burst with `ReapPending`; the watchdog retries it on its next pass even when the workload has no budget and every lifetime ceiling is disabled. The cordon stays throughout that retry window so the scheduler cannot put work onto a node already scheduled for deletion.

Both are authorised against central's own burst record: the reporting cluster must be the one the burst was booked in, and the node name must be the one central assigned. Bursts written before central stored the cluster have no cluster to match — nothing backfills one onto them, and nothing guesses. Both reports are refused on one of those, on the same rule: whichever clusters a tenant happens to have connected now is not evidence about where central booked capacity earlier. A missing cluster, a cluster that does not match, or a node name that does not match exactly is a no-op with no acknowledgement, and the connector keeps retrying. What ends a burst nothing can vouch for is the reaper watchdog, not a report central cannot authorise.

The third exists because the first two do: a connector that dies or loses its RBAC stops sending either, and a nodeOnly burst with no signal has nothing else to stop it. It measures **silence, not age** — the idle watcher re-states every `occupancyObserveInterval` (default 5m) that it could still read the whole cluster's pods, so a node that is up and serving under a cluster-scoped connector is never reaped for being old, however long it runs. Node health does **not** count: a connector with no pod visibility reports Ready forever while being unable to ever ask for the teardown. It still ignores a *longer* declared deadline (`deadline: 24h` does not buy more silence); a shorter one still wins.

It applies only where an observation was **owed**. The connector states its pod-visibility capability in its `hello`, and central records that promise on the burst it admits. A burst whose connector promised and has sent nothing measures from its creation time — a connector that died before its first sweep. A burst whose connector promised nothing is not silent, it is unwatched, and the ceiling never touches it; reaping those destroyed live nodes for a feature they never had.

Idle tear-down requires **cluster-wide pod visibility**, and the connector needs it granted explicitly rather than inferred. It runs only when `workloadNamespace` is empty **and** `rbac.scope: cluster` — the chart computes `-authoritative-pod-visibility` from the RBAC it renders, so the **shipped namespaced default keeps it off**. It logs `idle burst teardown disabled` at startup with the reason.

The capability the connector *claims* is stricter still: it also needs a positive `idleTeardown.grace` and a positive `occupancyObserveInterval`. Either set to `0` means no observation is ever sent, and a claim central would measure silence against but never receive is worse than no claim at all — it holds the ceiling open forever. Those installs are bounded at admission instead, exactly as a namespaced one is.

Those installs claim nothing, so the silence ceiling never applies to their nodeOnly bursts and nothing can report those nodes idle. The bound moves to **admission** instead: a nodeOnly submission that declares `budget.deadline` or `budget.maxUSD` is stored exactly as written, and one that declares neither is stored with a `budget.deadline` of `YSCALE_NODE_ONLY_MAX_LIFETIME` — visible on the workload, not applied invisibly. With that duration set to `0` the submission is refused (`400`) rather than admitted unbounded. Do not widen the namespaced Role with a cluster-wide pod `list` to turn the watcher on; that grant reads every team's workloads. Full detail, including what counts as "busy", is in [`keda.md`](keda.md).

---

## RBAC scope — namespaced vs cluster

Default: `namespaced` with an explicit `allowedNamespaces` list. The chart fails install if you pick `namespaced` and forget the list (validation block in `_helpers.tpl`).

```yaml
rbac:
  scope: namespaced               # namespaced | cluster
  allowedNamespaces:              # required when namespaced
    - ml-team-a
    - ml-team-b
    - ci-bursts
```

What `namespaced` gives the agent:

- **Cluster-wide read**: nodes, namespaces, Workload CRs, CSRs
- **Cluster-wide write**: cordon/uncordon nodes, evict pods, approve CSRs (scoped to specific signers + groups)
- **Per-namespace write**: Jobs, pods, pods/log, events in each allowed namespace
- **kube-system write**: `create` on Secrets only — the bootstrap tokens (one Role limited to that namespace; no read, list or delete)
- **Per-namespace Secret read**: `get` only, in each allowed namespace, for a workload's `spec.storage` credentials Secret. Never cluster-wide, including under `scope: cluster`

What `cluster` gives instead: a single cluster-wide Workload Role granting Jobs/pods CRUD across all namespaces. Useful if you want users to submit to any namespace. Most platforms should keep `namespaced`.

---

## Per-team / per-user namespace setup

Each `allowedNamespace` is independent. Standard prep:

```sh
# 1. Create the namespace
kubectl create ns ml-team-a

# 2. Drop a bucket-credentials Secret (per team or per user)
kubectl -n ml-team-a create secret generic r2-creds \
  --from-literal=AWS_ACCESS_KEY_ID=$R2_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=$R2_SECRET \
  --from-literal=AWS_ENDPOINT_URL=https://abc123.r2.cloudflarestorage.com

# 3. (Optional) standard K8s RBAC: only users in your team can read this secret
kubectl -n ml-team-a apply -f team-rbac.yaml
```

Users in this namespace reference `credentialsSecret: r2-creds` in their Workload spec. The yscale agent uses its in-cluster ServiceAccount to read the Secret and sign short-lived presigned URLs — your team's R2 creds never leave the cluster.

---

## TenantLimits — declarative governance (designed; not yet enforced)

Commit this to your platform infra repo:

```yaml
# .yscale/limits.yaml
apiVersion: yscale.sh/v1
kind: TenantLimits
spec:
  budget:
    monthlyUSD: 25000
    hardCap: true
    alertAtPct: [50, 80, 95]
  concurrency:
    maxBursts: 50
    maxConcurrentGPUs: 16
  defaults:
    backend: auto
    gpu: { kind: l4, reliability: reliable, maxHourlyUSD: 0.99 }
  routing:
    allowedRegions: [us-east, us-west, eu-fra]
  reliability:
    autoFailover: true
    snapshotInterval: 5m
  pricing:
    quoteLockHours: 24
    maxRenewalIncrease: 0.30
  storage:
    cache:
      defaultPolicy: ttl=7d
      maxCacheGB: 2000
      keepBudgetGB: 500
```

When TenantLimits enforcement ships (next chunk), central reads this on every submit and applies the limits. ArgoCD / Flux apply the manifest the same way as any other K8s object.

---

## Observability — what to scrape and what to display

### Prometheus / kube-state-metrics

Standard `kube-state-metrics` picks up:

- **Burst nodes** as `Node` objects with label `yscale.sh/burst-node=true`
- **Burst-node pods** scheduled with the burst-node taint toleration
- **Workload CRs** if `kube-state-metrics` is configured to watch CRDs (custom resource state metrics)

The yscale agent itself exposes Prometheus metrics on `:9090/metrics` (matches Helm chart default port). Add a `ServiceMonitor`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: { name: yscale-agent, namespace: monitoring }
spec:
  selector: { matchLabels: { app.kubernetes.io/name: yscale-agent } }
  namespaceSelector: { matchNames: [yscale-system] }
  endpoints: [{ port: metrics, interval: 30s }]
```

### Grafana dashboards

We ship a starter dashboard at `deploy/helm/yscale-agent/dashboards/yscale.json` (TODO — pending real metrics implementation). Useful panels:

| Panel | Query |
|---|---|
| Spend over time per team | `sum(rate(yscale_spend_usd_total[1h])) * 3600 by (namespace, backend)` |
| Active bursts by backend | `yscale_bursts_active{state="running"}` |
| Cache hit rate | `yscale_cache_hits_total / (yscale_cache_hits_total + yscale_cache_misses_total)` |
| Cold-start latency p95 | `histogram_quantile(0.95, yscale_burst_ready_seconds_bucket)` |
| Per-team GPU-hours / day | `sum(rate(yscale_gpu_seconds_total[24h])) * 24 by (namespace)` |

### Logs

Standard `kubectl logs` pulls pod logs from burst nodes via the customer's apiserver → kubelet on the burst → the WG tunnel. Works because the burst is a real Kubernetes node; everything else is just `kubectl logs`.

Cluster-wide log shipping (Loki / Fluent Bit / Vector / your existing stack) works the same way as for any other pod — burst-node pods have stdout/stderr like normal pods.

### Errors / alerts

- **Agent disconnected**: `yscale_agent_connected == 0` for >5min — page on-call.
- **Burst create failure rate elevated**: `rate(yscale_burst_create_failures_total[5m]) > 0.25 * rate(yscale_burst_create_attempts_total[5m])` — wakes engineer if a provider is degraded.
- **Budget approaching**: `yscale_spend_usd_total / yscale_budget_usd_total > 0.95` — email finance.

---

## User onboarding — what to send your ML users

The two-paragraph email:

> Hey team, we've installed yscale. To run a GPU job, write a YAML like this and `kubectl apply` it:
>
> ```yaml
> apiVersion: yscale.sh/v1
> kind: Workload
> metadata: { name: my-job, namespace: ml-team-a }
> spec:
>   image: ghcr.io/your-org/your-image:tag
>   gpu: { kind: a100, count: 1, reliability: reliable, maxHourlyUSD: 2.00 }
>   budget: { maxUSD: 50, deadline: 8h }
> ```
>
> `kubectl get workloads` shows status; `kubectl logs -l yscale.sh/workload-id=<id>` shows output. `yscale skus list` shows what GPUs are available. The team's monthly budget is $25k — see grafana.../d/yscale-spend. Questions: <your internal #yscale channel>.

Three-paragraph email if your users will also use KEDA:

> Add this to your Deployment if you want it to autoscale onto yscale bursts:
> ```yaml
> spec.template.metadata.annotations:
>   yscale.sh/burst-template: |
>     gpu: { kind: a100, count: 1, reliability: reliable }
>     budget: { maxUSD: 5, deadline: 1h }
> spec.template.spec.nodeSelector: { yscale.sh/burst-node: "true" }
> spec.template.spec.tolerations: [{ key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }]
> ```
> Then a standard `kind: ScaledObject` from KEDA. We provision bursts when pods go Pending; kill them when KEDA scales down. See `docs/keda.md`.

---

## Argo Workflows / Tekton / Kubeflow integration

These tools submit Jobs / Pods through standard K8s APIs. They work with yscale unchanged:

- Argo Workflows: a step's container template with the burst-node nodeSelector + toleration + burst-template annotation. Argo creates the pod → PendingPodWatcher provisions burst → pod runs → Argo sees the result.
- Tekton: same shape via `taskRun.podTemplate.metadata.annotations` + `taskRun.podTemplate.nodeSelector`.
- Kubeflow Pipelines: pipeline step's `container.resources` + node-target same way.

No yscale-specific integration code. Same kubectl, same Argo CLI, same `tkn` — burst capacity is a property of the pod template, not a separate platform concept.

---

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| Workload sits `Provisioning` forever | Mesh auth or central connectivity failed | Check the agent and gateway logs, then confirm the burst can join the selected mesh |
| Burst node appears but pod stays `Pending` | Toleration missing on pod | Add `{ key: yscale.sh/burst-node, operator: Exists, effect: NoSchedule }` |
| Helm install fails with "rbac.scope=namespaced requires at least one entry" | You set `scope: namespaced` (or it defaulted) with empty list | Add `--set rbac.allowedNamespaces='{your-ns}'` |
| 403 on bootstrap token endpoint logs | Burst auth key or bootstrap credential is stale | Recreate the Workload; if it persists, inspect the agent's bootstrap logs and central minting logs |
| CSR Pending forever | CSR approver RBAC missing | The chart wires it; verify `ClusterRole yscale-agent-cluster` has `certificatesigningrequests/approval` |
| nodeOnly (KEDA/Deployment) bursts never tear down | Idle tear-down is disabled: `workloadNamespace` is set, or `rbac.scope` is the default `namespaced` | Check the connector log for `idle burst teardown disabled` and which reason it names. Either install with `rbac.scope=cluster` and no `workloadNamespace`, or accept the admission deadline central stamps on them (default 6h, visible on the stored workload) and set your own `spec.budget.deadline` if you want a different one. See `keda.md` |
| Idle burst reaped while still wanted | Idle grace is shorter than the traffic lull | Raise `idleTeardown.grace` above your KEDA `cooldownPeriod` |
| Burst node remains cordoned after a failed teardown | The reap is marked pending and the watchdog is retrying it; keeping the cordon prevents new work from landing on a node scheduled for deletion | Check central logs for `burst teardown failed; re-queued for retry` and subsequent watchdog attempts. Repair provider access or the failing receipt/settlement dependency; uncordon only after cancelling the pending teardown or proving the provider resource is meant to remain |
| Live nodeOnly burst reaped at 6h despite steady traffic | It had been observed and the connector then stopped: disconnected, lost its cluster-wide pod `list`, or `occupancyObserveInterval` raised near the ceiling | The backstop measures OCCUPANCY-observation silence, and node health does not count. Restore the connector's pod visibility, keep `occupancyObserveInterval` well under the ceiling, or raise `YSCALE_NODE_ONLY_MAX_LIFETIME` centrally. A burst whose connector never claimed pod visibility is not affected by this ceiling at all — it carries an admission deadline instead |
| `kubectl get workloads` shows blank columns | CRD installed but agent not running | Check `kubectl -n yscale-system get pods` |

---

## Multi-cluster

One Helm release per cluster. Each cluster gets its own `cluster-id` (persisted in PVC) so central tracks them independently. The bearer token in `YSCALE_TOKEN` ties them all to the same customer / billing entity.

Don't run two agents in one namespace — `cluster-id` collision causes central to alternately accept commands from both. Use distinct release names + namespaces if you really need multiple agents in one cluster (no good reason though).

---

## Uninstall

```sh
helm uninstall yscale-agent -n yscale-system
kubectl delete ns yscale-system
# Optionally:
kubectl delete crd workloads.yscale.sh
```

`helm uninstall` cleans up the Deployment, Service, RBAC, etc. The CRD is left behind by default (Helm's standard behavior) — delete it explicitly if you want a clean tear-down.

In-flight workloads continue running on their bursts until their deadline. Central tears down bursts when their agent reconnects from a new cluster_id (or via manual cleanup endpoint — TODO).
