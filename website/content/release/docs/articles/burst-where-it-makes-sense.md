# Run Kubernetes jobs on temporary cloud workers

*How Yscale provisions burst capacity, connects workers to your cluster, and supports application-specific job routing.*

Yscale is a self-hosted Kubernetes burst controller. It provisions temporary
CPU or supported GPU machines through your cloud accounts, joins them as nodes,
runs workloads, and coordinates cleanup. You continue submitting work through
Kubernetes.

Use burst capacity for jobs that need more compute than your existing cluster
has available. Developers keep their current tools and Git workflows; operators
configure the infrastructure, networking and provider access.

## Choose jobs that can run remotely

The strongest candidates are separable jobs whose outputs can outlive the worker.

| Use case | What leaves the local cluster | What can stay local |
| --- | --- | --- |
| Batch processing | Containerized imports, image transformations, report generation | Scheduling, application APIs, source-of-truth databases |
| Builds and tests | Selected build jobs or isolated test suites | Git, CI orchestration, artifact ownership |
| AI workloads | Supported GPU inference batches or CPU embedding/preprocessing jobs | Application endpoints and job queues |
| Media processing | Compatible transcode workers and their input data | Library management, playback coordination, user accounts |

These are workload patterns, not four bundled applications. Your image still
needs its dependencies, input access, output destination, and retry behavior.
An interactive notebook can submit work too, but Yscale is not a notebook host.

Bursts are less attractive when a job spends most of its time making tiny,
latency-sensitive requests back home, or finishes faster than a node can boot.
Moving a worker does not move its database—or make the WAN behave like a LAN.

## Install and operate the burst infrastructure

Operators run central, the fabric factory, Cluster Connector, gateway and
databases. They configure TLS, secrets, provider credentials, networking and
node images. There is no Yscale-hosted service or compulsory signup. You
currently build your own images.

The factory creates persistent Headscale/DERP coordination boxes on Linode
regardless of worker provider. Tenant and operations coordination use self-hosted
Headscale without a Tailscale account or subscription. Infrastructure nodes use
Tailscale clients; workload authors need no laptop VPN app.

Configure a separate operations coordinator and private callback path.
`FACTORY_OPS_LOGIN_SERVER` is mandatory, with no hosted fallback. A fresh
real-provider operations join has not yet been qualified. Verify the installation
in your environment before admitting workloads.

The [factory environment reference](../../deploy/runbooks/factory.md#environment-variables)
lists required variables, safe placeholders, and the separate ops-mesh setup.

After that setup, a developer with permission in an authorized namespace can
submit a workload using `kubectl` or their team's reviewed Git workflow. They do
not each need provider keys or the central admin token.

Here is a small first test. Save it as `first-burst.yaml` after the operator has
confirmed Linode capacity, the region, namespace access, and the cost estimate:

```yaml
apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: first-linode-burst
  namespace: default
spec:
  image: docker.io/library/busybox:1.36
  backend: linode
  region: us-ord
  cpu: "500m"
  memory: "2Gi"
  networking:
    tier: full
  command: ["/bin/sh", "-c"]
  args:
    - |
      echo "Burst worker: $(hostname)"
      dd if=/dev/zero bs=1M count=64 2>/dev/null | sha256sum
      echo "Burst finished"
  retries: 0
  budget:
    maxUSD: 0.10
    deadline: 20m
```

The budget numbers are illustrative configuration, not a quote or guaranteed
provider spending cap. Review rounded billing periods, boot and teardown time,
storage, transfer, and persistent mesh costs separately. A CPU request is a pod
resource request, not a promise of a matching fractional-size provider VM.

From a source checkout, build the CLI and inspect the local translation:

```sh
GOFLAGS=-p=2 make build
./bin/yscale apply --dry-run -f first-burst.yaml
```

That dry run neither provisions capacity nor proves provider admission. Once
the installation and paid test are approved, submit the **Workload resource**:

```sh
kubectl config current-context
kubectl get crd workloads.yscale.sh
kubectl apply -f first-burst.yaml
kubectl -n default get workloads.yscale.sh first-linode-burst -w
```

In another terminal:

```sh
kubectl -n default get jobs,pods -l yscale.sh/workload=first-linode-burst -o wide
kubectl -n default logs -l yscale.sh/workload=first-linode-burst --all-containers=true
```

Use `kubectl apply` for submission: `yscale apply` without `--dry-run` creates a
Job directly and is a different path. Replace the tiny checksum task with your
real container once node joining, execution, connectivity, and cleanup work.

To cancel or remove the completed record:

```sh
kubectl -n default delete workloads.yscale.sh first-linode-burst
```

Have the operator confirm the provider resource is gone; deletion is asynchronous.
Keep the connector running through cleanup. Use a new name for a new run, and if
GitOps owns the object, remove its desired state before cancelling so it does not
recreate billable work. Persistent tenant mesh infrastructure remains separate.

## Route media work to compatible workers

`yscale-media` is a separate integration example. Its sharded-transcoding path
converts viewer capabilities and requested quality into a rendition plan, then
queues chunks containing playback segments. Workers write segments to object
storage for delivery to the player.

```text
Viewer capabilities + requested quality
                 ↓
        Rendition and chunk planning
                 ↓
    Queue ordered by playback urgency
                 ↓
     Compatible worker claims a chunk
                 ↓
       Segment store → delivery → player
```

Worker claims follow these rules:

- **Prioritize playback.** Chunks nearest the playhead rank first. At equal
  distance, requested renditions take priority, with a bitrate-based cost tiebreak.
- **Rank eligible workers.** A bounded holdoff gives higher-ranked live encoder
  classes first refusal on new video work. `YSCALE_ENCODER_CLASS` overrides pool
  defaults. Ranking is declared, not continuously benchmarked.
- **Preserve encoder-family affinity.** The first video claim establishes pool
  affinity for later chunks. Audio and text claims are handled separately;
  arbitrary midstream GPU-family failover is not supported.
- **Protect claims.** Atomic leases and fencing coordinate ownership. Startup
  encoder probes can demote unhealthy video workers to audio-only.

The media repository includes a Linode NVIDIA burst-worker template with GPU
scheduling, namespace Secrets, pre-seeded source media on a volume and burst-side
MinIO.

Operators must integrate node selection, GPU runtime, source paths, output-store
visibility, scaling triggers and safe draining. Worker routing, KEDA manifests
and the template do not establish a turnkey viewer-triggered cloud autoscaler.
Pod scaling and cloud-node provisioning remain separate responsibilities.

## Application, capacity and packet routing

Application routing assigns work to workers. Kubernetes places pods using
resource requests, selectors and taints. Yscale provisions configured external
capacity.

Yscale honors an explicit backend. Its current `auto` policy sends CPU work to
Fly, unspecified/RTX-class GPU requests to Linode, and other supported GPU kinds
to AWS. Azure and GCP are explicit CPU-only options in the launch scope. These
are policy rules; they do not compare live prices or availability. Qualify
quotas, images, shapes and the selected configuration.

Full networking connects burst-side host-mode Cilium through an encrypted mesh
and in-cluster gateway to the existing CNI. Verify CNI-specific configuration,
CIDRs, MTU, firewalls, policy and return routes. Gateway source NAT makes
destination workloads see the gateway address. See the [packet path](../networking.md).

## Choose how workers receive their inputs

For model inference or media processing, input delivery can matter as much as the
processor. Yscale currently offers two different read-heavy paths:

- `spec.modelVolume`: a pre-seeded Linode volume, mounted read-only at `/models`.
  It survives bursts but is single-attach; jobs sharing it must be serialized.
- `spec.storage.cache`: an S3-compatible prefix, including R2, downloaded into
  pod-local storage before each workload starts. It is read-only to the workload
  and disappears with the pod—not a persistent cross-burst cache.

The [storage guide](../storage.md) covers both. Neither is a generic writable
durable-volume service. Plan a separate durable destination for job outputs.

We measured the difference on September 24 using one Chicago Linode CPU burst,
one attached volume, and a private R2 bucket. The same pod ran every test:

| Access pattern | Median read / GET | Median write / PUT |
| --- | ---: | ---: |
| Linode volume, direct I/O, queue depth 16 | 266.9 MiB/s | 167.6 MiB/s |
| R2, one 2 GiB stream | 48.9 MiB/s | 36.1 MiB/s |
| R2, eight parallel 128 MiB objects | 180.2 MiB/s | 127.6 MiB/s |

These are different access patterns, not equivalent storage semantics. In a
separate whole-file test, downloading 2 GiB from R2 to root disk, hashing it, and
syncing it took **35.6 seconds**. A buffered volume scan and hash after file-scoped
cache advice took **63.5 seconds**, then **31.6 seconds** on repetition. The file
exceeded the pod's memory limit, so the repeat was not a fully RAM-cached read.

Choose storage around input reuse and access patterns. Persistent
`spec.modelVolume` avoids repeated downloads but is single-attach. Ephemeral
`spec.storage.cache` downloads inputs for each workload and disappears with the
pod. These synthetic I/O tests do not measure inference, framework loading or
end-to-end cache startup. See the [method and results](../benchmarks/storage-2026-09-24.md).
One VM was reused throughout, then removed.

## Install Yscale and test a representative job

Start with the [installation guide](../getting-started.md), then test a
representative job. Verify connectivity, outputs and provider-resource deletion.
Charges continue until resources are deleted; budget settings are not billing caps.

For worker pools, define scaling triggers, claim shutdown and draining before
enabling automatic provisioning. Include persistent coordination, storage and
transfer in cost estimates. Account for provider billing granularity when
deciding how long to keep a worker running.
