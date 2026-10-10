# Yscale Cluster Connector deployment examples

This directory contains minimal Helm overlays for installing the **Yscale
Cluster Connector** into an existing Kubernetes cluster. The chart and release
remain named `yscale-agent` as compatibility artifacts. The Connector opens an
outbound authenticated connection to the **Yscale Control Plane**, coordinates
burst capacity, and joins burst workers to the customer cluster over an
encrypted mesh. It stores no cloud-provider credentials and uses constrained
Kubernetes permissions for burst operations; see
[the canonical naming and trust boundary](../../docs/cluster-connector.md).

These are **source-cluster installation examples**, not a claim that every
source/target combination has completed live qualification. Install your own
central and factory first using [managed-mesh setup](../../docs/self-hosted-mesh.md),
then use these overlays to connect a cluster. No Yscale-operated signup is offered.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Safe Secret Creation](#safe-secret-creation)
- [Profile to cloudProvider Mapping](#profile-to-cloudprovider-mapping)
- [Helm Values Overlays](#helm-values-overlays)
- [Deployment via Helm CLI](#deployment-via-helm-cli)
- [Deployment via GitOps](#deployment-via-gitops)
- [Verifying the Installation](#verifying-the-installation)
- [Burst-Target Surface & Providers](#burst-target-surface--providers)
- [Hard Architectural Boundary (DR/HA)](#hard-architectural-boundary-drha)
- [Managed-Cluster Survival Test](#managed-cluster-survival-test)

---

## Prerequisites

Before installing the `yscale-agent`, ensure you have:

1. A running Kubernetes cluster belonging to one of the profiles below.
2. A tenant API token issued by your own configured central instance.
3. Helm 3 and `kubectl` configured for the target cluster.
4. An API-server address that burst workers can reach and whose TLS certificate
   covers the configured hostname.
5. Network access from the cluster to `ghcr.io`, where each release publishes
   the Connector image the chart defaults to, or your own image and its pull
   secret.

Your central instance mints tenant-scoped mesh credentials when the pod starts.
Workload authors do not manage those keys; the operator configures the factory,
its operations mesh, and tenant Headscale as documented in the setup guide.

---

## Safe Secret Creation

Never commit the customer token to a values file. Create a Kubernetes Secret
out-of-band in the release namespace and reference it with `existingSecret`.

Create the Secret using the following command:

```bash
# Create the namespace first if it does not exist.
kubectl create namespace yscale-agent --dry-run=client -o yaml | kubectl apply -f -

# Read without echoing or putting the token in shell history.
read -rsp "yscale token: " YSCALE_TOKEN; echo
printf %s "$YSCALE_TOKEN" | kubectl -n yscale-agent create secret generic \
  yscale-agent-secrets --from-file=YSCALE_TOKEN=/dev/stdin \
  --dry-run=client -o yaml | kubectl apply -f -
unset YSCALE_TOKEN
```

The Helm overlays in this folder are preconfigured to reference this Secret via:
```yaml
existingSecret: "yscale-agent-secrets"
```

---

## Profile to cloudProvider Mapping

The chart uses `cloudProvider` to choose initial DNS and provider-ID defaults.
Always inspect the real cluster and override `cluster.dnsIP` when its service
network differs from the chart default.

Today, the Connector uses `cloudProvider` for local route detection and logs the DNS
and provider-ID values, but those cluster facts are not yet carried in the
Connector's `Hello` message to the Control Plane. LKE → Linode survives because
the Linode
backend registers the real instance provider ID. Treat other managed-cluster
provider-ID combinations as qualification inputs—not as completed support—until
the central bootstrap plumbing and live survival test both pass.

| Source Cluster Profile | `cloudProvider` Value | Default DNS IP | Chart providerID default | Description |
| :--- | :--- | :--- | :--- | :--- |
| **k3s (Bare Metal / Homelab)** | `k3s` | `10.43.0.10` | *None* | No cloud-controller-manager (CCM) integration to satisfy. |
| **k0s** | `self-managed` | `10.96.0.10` | *None* | Self-managed control plane. |
| **kubeadm** | `self-managed` | `10.96.0.10` | *None* | Standard upstream self-managed cluster. |
| **Rancher-managed k3s** | `k3s` | `10.43.0.10` | *None* | Rancher-deployed k3s cluster. |
| **Rancher-managed RKE2** | `self-managed` | `10.96.0.10` | *None* | Rancher RKE2 deployment. |
| **LKE (Linode)** | `linode` | `10.96.0.10` | `linode://yscale-burst-{BURST_ID}` | Managed-cluster profile; LKE → Linode is live-validated. |
| **EKS (AWS)** | `aws` | `172.20.0.10` | `aws://yscale-burst-{BURST_ID}` | Managed-cluster profile; live qualification is in progress. |
| **GKE (GCP)** | `gcp` | `10.0.0.10` | `gce://yscale-burst-{BURST_ID}` | Experimental until the survival test passes. |
| **AKS (Azure)** | `azure` | `10.0.0.10` | `azure://yscale-burst-{BURST_ID}` | Experimental until the survival test passes. |

> [!IMPORTANT]
> DNS IPs are chart defaults and API endpoints use the reserved `.invalid`
> domain. Replace them after inspecting CoreDNS, the API endpoint, network
> reachability, and certificate SANs in the real cluster.

---

## Helm Values Overlays

Individual, minimal overlay files are provided in this directory for each profile:

- **k3s on bare metal/homelab**: [values-k3s.yaml](values-k3s.yaml)
- **k0s**: [values-k0s.yaml](values-k0s.yaml)
- **kubeadm**: [values-kubeadm.yaml](values-kubeadm.yaml)
- **Rancher k3s**: [values-rancher-k3s.yaml](values-rancher-k3s.yaml)
- **Rancher RKE2**: [values-rancher-rke2.yaml](values-rancher-rke2.yaml)
- **Linode LKE**: [values-lke.yaml](values-lke.yaml)
- **AWS EKS**: [values-eks.yaml](values-eks.yaml)
- **Google GKE**: [values-gke.yaml](values-gke.yaml)
- **Azure AKS**: [values-aks.yaml](values-aks.yaml)

Each overlay keeps namespaced workload RBAC as the customer-facing default and
allows only `default`. Add explicit namespaces or deliberately select cluster
scope when the operating model requires it.

---

## Deployment via Helm CLI

To install the Cluster Connector onto a cluster, run:

```bash
helm upgrade --install yscale-agent deploy/helm/yscale-agent \
  --namespace yscale-agent \
  --create-namespace \
  -f examples/deploy/values-<profile>.yaml
```

Replace `<profile>` with the appropriate name, such as `eks`, `k3s`, or
`lke`. Run this from the repository root. For production, use a reviewed,
immutable chart revision rather than an unpinned checkout.

---

## Deployment via GitOps

Flux and Argo CD can render the same chart. Keep `yscale-agent-secrets`
out-of-band or manage it with SOPS, External Secrets, Sealed Secrets, or an
equivalent secret controller. Do not commit a plaintext Kubernetes Secret.

### Flux CD Example

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: yscale
  namespace: flux-system
spec:
  interval: 1m
  url: https://github.com/yscale-sh/yscale.git
  ref:
    commit: REPLACE_WITH_REVIEWED_COMMIT
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: yscale-agent
  namespace: flux-system
spec:
  interval: 5m
  targetNamespace: yscale-agent
  install:
    createNamespace: true
  chart:
    spec:
      chart: ./deploy/helm/yscale-agent
      sourceRef:
        kind: GitRepository
        name: yscale
        namespace: flux-system
      interval: 5m
  values:
    endpoint: wss://api.yscale.sh
    existingSecret: yscale-agent-secrets
    cloudProvider: aws
    rbac:
      scope: namespaced
      allowedNamespaces: [default]
    bootstrap:
      apiserverURL: https://eks-api.example.invalid:443
```

### Argo CD Example

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: yscale-agent
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/yscale-sh/yscale.git
    targetRevision: REPLACE_WITH_REVIEWED_COMMIT
    path: deploy/helm/yscale-agent
    helm:
      valuesObject:
        endpoint: wss://api.yscale.sh
        existingSecret: yscale-agent-secrets
        cloudProvider: aws
        rbac:
          scope: namespaced
          allowedNamespaces: [default]
        bootstrap:
          apiserverURL: https://eks-api.example.invalid:443
  destination:
    server: https://kubernetes.default.svc
    namespace: yscale-agent
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

---

## Verifying the Installation

To verify that the Cluster Connector and sidecar are healthy and connected:

1. Check the Pods:
   ```bash
   kubectl get pods -n yscale-agent
   ```
2. Stream logs from the Connector container:
   ```bash
   kubectl logs -f -n yscale-agent -l app.kubernetes.io/name=yscale-agent -c agent
   ```
3. Inspect Tailscale sidecar status to ensure it has successfully joined your tailnet:
   ```bash
   kubectl logs -f -n yscale-agent -l app.kubernetes.io/name=yscale-agent -c tailscale
   ```

---

## Burst-Target Surface & Providers

The managed-mesh control plane manages bursting to remote infrastructure providers:

- **Implemented workload targets today:** `flyio`, `linode`, `aws`, and `gcp`.
  `gcp` is **CPU-only** — GPU is a deliberate stub (`pkg/backends/gcp`'s
  `mapGPUType` always errors; the decider's `route` rejects a GPU request for
  `backend=gcp` before admission). GCP also carries no auto-routing target:
  it is reachable only via an explicit `backend: gcp` in the Workload spec,
  never selected by `auto`/omitted `backend` regardless of GPU/CPU.
- **Planned Target Adapter**: `azure`.
  > [!NOTE]
  > Azure is a planned roadmap item. Do not attempt to use Azure in
  > executable configurations today.
- **Dynamic selection:** omit `backend` or use `backend: auto` to let the
  decider select from `flyio`/`linode`/`aws` using workload resources and
  current routing policy — `gcp` never participates in `auto` selection (see
  above).
- **Fly warm-capacity note:** `prewarmPool` belongs to the legacy OSS controller.
  The managed-mesh decider currently provisions each workload and does not consume that
  pool, so these examples make no warm-start promise.

Existing executable examples live in
`deploy/smoketest`. GCP and Azure in a source-cluster
overlay mean “the customer cluster is GKE/AKS”; a source-cluster overlay of
either does **not** by itself enable that provider as a burst target — `gcp`
as a burst target requires the explicit `backend: gcp` above, and `azure` as
a burst target isn't available at all yet.

---

## Hard Architectural Boundary (DR/HA)

A key architectural boundary exists in the yscale design:

- **Worker-capacity loss:** yscale can add remote workers while the original API
  server remains reachable.
- **Control-plane loss:** new workers cannot register and `kubectl` is
  unavailable when the original API server/etcd is down. That scenario requires
  a standby cluster or separate multi-cluster disaster-recovery design.

For k3s, transparent `kubectl logs`, `exec`, and `attach` to stock burst
kubelets additionally requires the chart's opt-in
`gateway.kubeletProxyRouting.enabled` path and the documented k3s egress-selector
prerequisite. Qualify that route on a disposable cluster before enabling it in
production; do not blindly add host routes to an existing control plane.

---

## Managed-Cluster Survival Test

Certain cloud provider control planes run aggressive Node reconciliation loops that delete nodes not recognized as local VMs (e.g., the Linode CCM reaps burst nodes ~75-90s after registration).

A cluster-agnostic survival check is available at
`deploy/eks-test/survive-test.sh` to
verify compatibility.

### Running the Test

1. Point your kubeconfig at your target cluster:
   ```bash
   export KUBECONFIG=~/.kube/config
   ```
2. Verify the `yscale-agent` is installed and running in the cluster.
3. Execute the survival check:
   ```bash
   # Test node persistence using Linode providerID formatting
   BACKEND=linode ./deploy/eks-test/survive-test.sh

   # Test node persistence using AWS providerID formatting
   BACKEND=aws ./deploy/eks-test/survive-test.sh
   ```

The script:
- Submits a minimal `nodeOnly` Workload.
- Waits up to 6 minutes for the burst node to boot, establish its Tailscale tunnel, and join the cluster.
- Monitors the node for 3 minutes.
- **PASS**: The node persists. The CCM did not delete it.
- **FAIL**: The node is deleted. The CCM reaped the external node.
