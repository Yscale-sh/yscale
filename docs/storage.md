# Workload storage

Yscale supports two workload-input storage paths:

- `spec.modelVolume` attaches a pre-seeded Linode Block Storage volume. It is the only supported model-cache path that persists across bursts.
- `spec.storage.cache` downloads an S3-compatible prefix into a pod-local `emptyDir` before each workload starts. The workload receives read-only access. The copy is ephemeral; no provider volume is provisioned or reattached.

Generic durable caches and persistent-volume provisioning are not supported. Admission rejects durable cache retention. You own the source bucket or Linode volume; Yscale does not retain bucket credentials.

## Pre-seeded Linode volumes

`spec.modelVolume` identifies a volume by label. Central attaches it, and the burst mounts it read-only at `/models`, with `HF_HOME=/models/hf` and `HF_HUB_OFFLINE=1`. This path supports Linode only. Volumes are single-attach (RWO), so workloads sharing a volume run serially. See [Preparing model weights](model-volumes.md).

For the cluster-to-burst packet path, see [networking](networking.md).

---

## Current `storage.cache` data path

```
   Your R2 / S3 bucket           ◄──── source of truth (you own it)
            │
            │ agent signs short-lived GET URLs
            ▼
   cache init container (`curl`)
            │
            ▼
   pod-local `emptyDir`          ◄──── discarded with the Pod
            │ read-only mount
            ▼
   workload container
```

---

## Credentials — set up once per namespace

```sh
kubectl -n ml-team-a create secret generic r2-creds \
  --from-literal=AWS_ACCESS_KEY_ID=$R2_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=$R2_SECRET \
  --from-literal=AWS_ENDPOINT_URL=https://abc123.r2.cloudflarestorage.com
```

That's it. yscale never reads the keys directly — the agent (running in your cluster as a normal Pod with normal RBAC) reads the Secret and signs short-lived pre-signed URLs the burst can use. Burst nodes (running on untrusted neocloud VMs) never see your access keys.

`credentialsSecret` is a **bare Secret name**, and the Secret must live in the workload's own namespace. It is not a `namespace/name` reference: the agent reads it only from the namespace central authorized for that workload, so one team's burst can never reach another team's bucket credentials.

Two things have to name that namespace for signing to work, and they are checked independently:

- Your account's authorized workload namespaces in central. A `metadata.namespace` outside that set is refused at submit time with a 403; an account with none configured can only submit into `default`.
- The connector's `rbac.allowedNamespaces`. That list is the only thing that grants the connector `get` on Secrets, and it is not widened by `rbac.scope: cluster`.

Keep them identical. The install command a provisioned account is handed sets both from one list, and adding a namespace later is an operator call to `PUT /v1/admin/tenants/{id}/workload-namespaces` — which changes central's half and returns the `helm upgrade` that changes the connector's.

R2 is recommended over S3 because Cloudflare's zero-egress pricing means cache miss penalty is just bandwidth time, not bandwidth cost. S3 / B2 / Backblaze / MinIO all work — egress fees apply per their pricing.

---

## Cache — read-heavy mirror

Use for: model weights, datasets, container scratch caches, anything where the bucket is the source-of-truth and the pod only reads.

```yaml
spec:
  storage:
    cache:
      - name: llama-70b-weights
        source:
          bucket: my-models
          prefix: llama-70b/
          credentialsSecret: r2-creds
        target: /models/llama-70b      # mount path inside your pod
        retention: ephemeral           # optional; this is the only supported policy
        sizeHintGB: 200                # optional emptyDir capacity limit
```

### What happens for every workload

```
  Customer submits workload
       ↓
  central authorizes the exact bucket/prefix for this burst
       ↓
  cache init calls agent's /storage/sign-urls endpoint with mode=read
       ↓
  agent: reads r2-creds Secret, lists bucket prefix, signs GET URL per object (1h TTL)
       ↓
  cache init downloads each signed HTTP(S) URL into a pod-local emptyDir
       ↓
  Kubernetes mounts that emptyDir read-only at /models/llama-70b
       ↓
  the emptyDir is discarded when the Pod is removed
```

The init container validates the complete signer response before downloads.
Invalid JSON, unsafe object keys, non-200 responses, and incomplete transfers
fail initialization, preventing the workload container from starting. Each
successful download atomically replaces its destination; failed transfers do
not leave partial cache files. Presigned URLs and upstream error bodies are not
printed in init logs. An explicitly empty object list is a valid empty prefix.

The source prefix is a directory boundary: `weights` and `weights/` both list
`weights/`, matching the signer's object-key construction. An omitted prefix
lists the bucket root. Safe zero-byte S3 folder markers are ignored, but empty
files are downloaded. Unsafe/out-of-prefix keys, duplicates, missing pagination
status, and missing or cycling continuation tokens fail the listing; partial
results are never signed. Each request is bounded to 1,000 files and 1,000 list
pages. Nonempty objects whose names end in `/` are rejected rather than silently
discarded.

The init image is digest-pinned; it currently installs `curl` and `jq` from
Alpine repositories at startup. This still requires package-repository access
and does not provide a pre-baked toolchain or durable cross-workload reuse.

### Retention

```yaml
retention: ephemeral             # explicit current behavior
# or omit retention; omission is also ephemeral
```

Admission rejects `keep`, `ttl=...`, and `until=...` because no provider-backed
cache volume exists to honor those contracts. Use `spec.modelVolume` on Linode
when weights must survive across bursts.

---

## Persistent — design target, not shipped

Use for: training checkpoints, model registry, anything where the pod writes and you want the state to persist between bursts (or between workload re-submissions).

```yaml
spec:
  storage:
    persistent:
      - name: training-state          # unique per tenant; same name across workloads = same volume
        sizeGB: 500
        target: /workspace
        retention: keep
        snapshot:
          to:
            bucket: my-models
            prefix: snapshots/llama-train/
            credentialsSecret: r2-creds
          interval: 5m
```

### What happens

```
  First workload referencing "training-state":
    central allocates a new 500GB volume on the chosen backend
    backend attaches it to the burst at /local-persist/training-state/
    burst bind-mounts → /workspace
    your training writes checkpoints
    every 5 min: burst's snapshot loop scans /workspace for changed files
    burst calls agent's /storage/sign-urls with mode=write, list of changed paths
    agent signs PUT URLs (1h TTL, scoped to snapshots/llama-train/ prefix only)
    burst HTTP PUTs each file to your bucket

  Second workload (same tenant, same volume name):
    central finds the existing volume → pins burst routing to that DC
    burst boots, volume re-attaches, training resumes from latest checkpoint

  Spot preemption mid-training:
    burst dies; backend volume persists
    next workload restarts from the volume; loses at most snapshotInterval of state
    if the entire backend region vanishes:
      next workload submitted with same name → new volume on alternate backend
      burst init fetches snapshot from your bucket → reconstructs state
```

### Snapshot diff awareness

The snapshot loop walks `/workspace`, computes `modtime > last_snapshot_time` for each file, and only signs+uploads changed ones. For typical training workloads (1-5 GB changed per snapshot, even if the total volume is hundreds of GB), this means:

- R2 egress cost (if using R2): $0 forever
- S3 egress cost (if using S3): roughly $0.09/GB × diff size = pennies per snapshot
- Wall-clock: 1-5 seconds typical, non-blocking to the pod

---

## Mixing cache and persistent in one workload

```yaml
spec:
  image: ghcr.io/me/trainer:latest
  gpu: { kind: a100, count: 1, reliability: reliable }
  budget: { maxUSD: 50, deadline: 8h }
  storage:
    cache:
      - name: base-weights
        source: { bucket: my-models, prefix: llama-7b-base/, credentialsSecret: r2-creds }
        target: /models/base
        retention: keep
      - name: training-dataset
        source: { bucket: my-data, prefix: instruct/, credentialsSecret: r2-creds }
        target: /data
        retention: ttl=30d
    persistent:
      - name: ckpt-llama-instruct-v3
        sizeGB: 200
        target: /workspace
        retention: keep
        snapshot:
          to: { bucket: my-models, prefix: ckpt-runs/v3/, credentialsSecret: r2-creds }
          interval: 5m
```

What you get:

- `/models/base` — frozen base model weights, fetched once per cluster, shared by every workload referencing this bucket+prefix
- `/data` — training corpus, similar deal but only kept 30 days after last use (saves storage if you swap datasets)
- `/workspace` — your training writes checkpoints here; survives across bursts; gets snapshotted to your bucket every 5 min

If your training takes 8 hours and gets spot-preempted at hour 6, the next submission resumes at ~hour 5:55 (last successful snapshot). You pay for compute from there, not from scratch.

---

## Egress warnings — planned, not emitted today

The following is a target UX example, not current CLI output:

```sh
$ yscale apply -f workload.yaml
wl_7c4a91 provisioning on reliable (NVIDIA A100 80GB, est $3.58/hr)
  cache fetch: base-weights (140GB) — first-time pull
  ⚠ egress estimate: ~$12.60 (S3 us-east-1 → burst-node DC us-east)
     downgrade to R2 to reduce to $0; current cache fetches once per workload
  est total cost (1h): $16.18
```

Current admission does not estimate or cap bucket egress. Because the cache is
pod-local, a paid-egress source can incur egress on every workload.

Check the current pricing of the selected object-store provider before launch;
yscale does not presently retain a price quote for this transfer.

---

## Budget controls (TenantLimits — designed, not yet enforced)

```yaml
spec:
  storage:
    cache:
      defaultPolicy: ttl=7d
      maxCacheGB: 2000              # tenant's total cache budget
      keepBudgetGB: 500             # within that, max "retention: keep" volumes
      egressBudgetUSDPerWorkload: 5 # reject workload if estimated egress > $5
      requireR2: false              # set true to ban paid-egress sources entirely
```

These fields are design notes, not accepted workload fields. Current cache
admission allows only omitted or `ephemeral` retention.

---

## Garbage collection — design target

The state layer contains cache-retention and GC scaffolding, but it does not
provision or delete provider cache volumes. The intended policy is:

| Tier | Trigger | Action |
|---|---|---|
| 1 — explicit retention | `ephemeral` end-of-workload; `ttl=X` deadline; `until=<time>` | Soft-delete (6h grace) → hard-delete |
| 2 — source freshness | (on workload submit) ETag mismatch | Mark stale; refetch on next reference |
| 3 — tenant budget | (designed, not yet wired) over `maxCacheGB` | LRU evict, drop ephemeral first, then ttl, then keep last |
| 4 — cost-aware | (designed, not yet wired) `storage > 1.5 × refetch cost` | Suggest evict |

This table must not be treated as a support claim until provider volumes and
their deletion inventory are wired and exercised end to end.

---

## Provider portability — design target

Backend volumes can vanish at any moment (provider outage, account suspension, regional capacity exhaustion). The R2 snapshot is the durability layer:

```
  A backend volume vanishes
       ↓
  central marks volume state="deleted" on next reconcile
       ↓
  next workload referencing the volume:
       central sees no existing volume → allocates new one on alternate backend
       burst init: no manifest, no local data, but snapshot config is present
       burst init reads "to" prefix → finds latest snapshot in customer's bucket
       burst init: s5cmd cp s3://snapshots/run/* /local-persist/training-state/
       resumes from last successful snapshot
```

This is what makes the multi-provider arbitrage model defensible: any single provider is replaceable; the customer's data is theirs and always reachable via their own bucket.

---

## Performance — measure the actual workload

The figures below are planning estimates, not retained Yscale acceptance
evidence:

| Backend | Burst egress | s5cmd throughput | 100 GB pull time |
|---|---|---|---|
| Fly Performance | ~2.5 Gbps | 250-350 MB/s | 5-7 min |
| Linode dedicated | ~5 Gbps | 400-600 MB/s | 3-4 min |
| Hetzner bare-metal | ~10 Gbps | 1000+ MB/s | <90s |

Current `storage.cache` fetches once per workload. Do not budget around a warm
reattach time. Use a pre-seeded Linode `modelVolume` when eliminating repeated
model downloads is required.

---

## Common patterns

The persistent-volume examples below describe the target API. Only the
ephemeral cache form and Linode `modelVolume` are shipped today.

### Pattern 1: shared base model + per-user fine-tunes

```yaml
storage:
  cache:
    - name: llama-base
      source: { bucket: shared-models, prefix: llama-7b-base/, credentialsSecret: r2-creds }
      target: /models/base
      retention: ephemeral                     # current cache refetches per workload
  persistent:
    - name: user-finetune-${USER}              # tenant-namespaced
      sizeGB: 50
      target: /workspace
      snapshot:
        to: { bucket: user-models, prefix: ${USER}/, credentialsSecret: r2-creds }
        interval: 5m
```

### Pattern 2: ephemeral inference cache

```yaml
storage:
  cache:
    - name: model-weights
      source: { bucket: m, prefix: serving/, credentialsSecret: r2-creds }
      target: /models
      retention: ttl=24h           # if no inference for 24h, evict and save storage cost
```

### Pattern 3: training that absolutely must not lose progress

```yaml
storage:
  persistent:
    - name: critical-training-state
      sizeGB: 500
      target: /workspace
      retention: keep
      snapshot:
        to: { bucket: critical, prefix: train/, credentialsSecret: r2-creds }
        interval: 60s              # 1 min granularity = worst-case 60s of lost work
```

Snapshot interval has no minimum; at 60s you'll do ~1440 uploads/day. Diff sizes are small (only changed files), but if your training writes a 200GB checkpoint file every step, set the interval longer or restructure your checkpoint format to be diff-friendly (sharded files, append-only logs).

---

## What's not yet built

- **TenantLimits enforcement** — `maxCacheGB`, `keepBudgetGB`, `egressBudgetUSDPerWorkload` are advisory today. Will become hard caps next chunk.
- **K8s-native PVC backed by yscale-managed storage** — today you reference storage via `spec.storage` in the Workload. A proper CSI driver that exposes yscale volumes as standard K8s `PersistentVolumeClaim` resources is on the roadmap (3-4 weeks of work).
- **Cross-tenant cache sharing** — never. Caches are strictly per-tenant for security.
- **Network volumes with K8s `accessMode: ReadWriteMany`** — most backends don't support this; we'd need to ship a shared-FS layer (CephFS-on-WG or similar). Not on the near-term roadmap.
