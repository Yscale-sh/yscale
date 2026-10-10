# Yscale Cluster Connector

The Yscale Cluster Connector runs in your existing Kubernetes cluster. Installed as a Helm release, it opens an outbound, authenticated WebSocket connection to the control plane you operate.

The Connector reconciles control-plane instructions and enrolls burst nodes using a defined Kubernetes role. Scheduling and spending decisions belong to the control plane, not the Connector.

## Installation identifiers

Commands, manifests and configuration retain identifiers such as `yscale-agent`. The chart name, binary, image references, Kubernetes labels, API routes and MagicDNS hostname remain unchanged. See [Compatibility identifiers](#compatibility-seam).

## Components

The following table describes the control and execution planes, followed by the three roles within the Connector's Helm release.

| Component | Where it runs | Responsibility |
|---|---|---|
| **Yscale Control Plane** | Your operator-owned central endpoint | Tenant management, provider orchestration, placement and lifecycle tracking. Holds the cloud-provider credentials. Yscale does not operate this service. |
| **Yscale Cluster Connector** | Your cluster (one Helm release) | The customer-installed package and its outbound control channel to the control plane. |
| ↳ Workload Controller | Your cluster | Reconciles `Workload` resources and creates the Jobs that run on burst nodes. |
| ↳ Node Enrollment Service | Your cluster | Mints short-lived kubelet bootstrap tokens and approves the narrowly scoped CSRs for burst nodes only. |
| ↳ Network Gateway | Your cluster | Private connectivity/routing to burst nodes over the mesh overlay. |

## Permissions and trust boundary

The Connector has Kubernetes privileges. Review the shipped RBAC in `deploy/helm/yscale-agent/templates/rbac.yaml` before installation.

- **Cloud credentials stay in the control plane.** The Connector does not hold Fly, Linode or AWS keys.
- **Kubernetes permissions include sensitive namespace access.** Its ServiceAccount can watch and reconcile `Workload` resources, create Jobs, approve the two burst-kubelet CSR signers and manage Yscale burst Nodes. For kubelet bootstrap tokens, it can create, get, list and delete Secrets in `kube-system`. Kubernetes RBAC cannot restrict those verbs by Secret type; this access extends beyond bootstrap-token Secrets.
- **The control plane is trusted.** The Connector creates Jobs and manages burst Nodes and enrollment on its instructions. Treat control-plane access as authority to act through the Connector's Kubernetes permissions.

## Compatibility seam

The technical identifiers listed below remain unchanged. Existing installations require no selector, resource, configuration or API changes for the Cluster Connector name. Keep `yscale-agent` wherever it appears in commands or manifests.

| Kind | Identifier | Status |
|---|---|---|
| Helm chart / directory | `yscale-agent`, `helm install yscale-agent …` | unchanged |
| Release / resource / ServiceAccount / container names | `yscale-agent*` | unchanged |
| Selector label | `app.kubernetes.io/name: yscale-agent` | unchanged (immutable on live workloads) |
| Component label (additive) | `app.kubernetes.io/component: cluster-connector` | **added** to common labels; not a selector |
| Binary | `yscale-agent` | unchanged |
| Container image | `ghcr.io/yscale-sh/yscale-cluster-agent` | public, published by each release; the default tag is the chart's `appVersion` |
| API routes | `/v1/agent/*` | unchanged |
| Config / env / JSON fields, metrics, dashboards | `YSCALE_*`, `yscale_agent_*`, etc. | unchanged |
| MagicDNS hostname | `yscale-agent-<cluster-id>` | unchanged |
| Go package / directory / symbols | `agent/…` | unchanged |

If you are scripting against the install, keep discovering resources by their
existing labels and names. The new `app.kubernetes.io/component:
cluster-connector` label is purely additive and safe to select on going
forward, but nothing requires it.
