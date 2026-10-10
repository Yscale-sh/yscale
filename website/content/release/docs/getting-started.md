# Getting started with Yscale

Yscale is a self-hosted Kubernetes burst controller. It adds temporary CPU or supported GPU nodes through your cloud accounts and connects them to your existing cluster over an encrypted mesh. Workloads remain submitted through Kubernetes.

An operator installs central, the factory and the Cluster Connector. Workload authors continue using `kubectl`, Git or existing Flux/Argo workflows; they do not install a separate control plane or laptop VPN application.

Yscale is experimental, source-available software, not a hosted service or a one-command production installer. Operators currently build their own images and configure networking for their cluster. Begin with [operator setup](self-hosted-mesh.md) for infrastructure and mesh requirements.

## 1. Build from source

Use a pinned revision of the complete source snapshot provided for your installation. Building the CLI alone does not install Yscale.

From the source root, with Make and the Go version specified in [go.mod](../go.mod) installed:

```sh
GOFLAGS=-p=2 make build
./bin/yscale help
```

This builds the CLI, central, connector, factory and migration tools into `bin/`.
The CLI by itself is not the installed system. Do not run bare `yscale` while
exploring: without a subcommand it starts the older standalone controller.

**Native bundles skip Go compilation.** Once a tagged release is published,
download its archive and matching `.sha256` file for your OS/architecture from
that release. Do not download only the CLI. For a Linux amd64 archive, set
`YSCALE_RELEASE` to the exact version you downloaded, then run in that directory:

```sh
: "${YSCALE_RELEASE:?Set the exact downloaded release version}"
sha256sum -c "yscale-${YSCALE_RELEASE}-linux-amd64.tar.gz.sha256"
tar -xzf "yscale-${YSCALE_RELEASE}-linux-amd64.tar.gz"
cd "yscale-${YSCALE_RELEASE}-linux-amd64"
./bin/yscale help
```

These commands are for Linux amd64; choose the matching asset on other hosts.
The checksum checks download integrity, not publisher identity. The bundle
includes `bin/`, charts, provisioning assets, examples and docs, but is not a
source checkout or a set of published container/VM images. Keep its directory
structure: the factory reads templates relative to the bundle root.

## 2. Try the control-plane flow without a cloud account

Prerequisites: matching native binaries built or extracted above, and Node.js
20 or newer. Run from the source or bundle root:

```sh
node examples/local-managed-mesh-smoke.mjs .
```

The script starts central and a **fake** factory on loopback, creates a test
tenant, checks the central-to-factory request and authentication boundary,
checks rejection of legacy OAuth configuration, then stops its processes.
It generates temporary credentials in memory and does not inherit your cloud
credentials, database URLs or kubeconfig. It does not create cloud resources.

Expected final output:

```text
PASS: local managed-mesh API flow. Fake provisioning only; no cloud resources.
```

This is a wiring check, not a running Kubernetes installation. It does **not**
create real Headscale, DERP or burst nodes, exercise persistence, prove provider
cleanup, or leave a console running. A real tenant still needs the deployment
below. If it fails, check that both binaries match the host architecture and
that local TCP listeners are allowed.

## 3. Decide whether to operate a real installation

The operator supplies:

- An existing Kubernetes cluster, administrative access, Helm and `kubectl`.
- Central and factory services, separate Postgres state, TLS, encryption keys,
  backups and a private network for factory callbacks.
- Cloud-provider accounts, quotas and narrowly scoped credentials.
- Operator-built connector and burst images. Private maintainer image defaults
  are not pullable by new users. Native bundles do not remove this requirement.
- Linode for the current factory's persistent Headscale/DERP boxes, even if
  workers use a different provider; a separate self-hosted Headscale coordinator
  for the private ops mesh. No Tailscale account or subscription is required.
  Yscale's mesh components still use the Tailscale client software; workload
  authors do not install it on their laptops. See the ops setup in the factory
  guide below.
- A provider cost estimate and a cleanup plan, including coordination VMs,
  worker VMs, disks and networking. Software budget fields are not hard caps
  on the provider bill.

For a Linux amd64 connector image, this existing Make target accepts your own
registry instead of the private default. From a **source checkout**, with
Docker Buildx and registry authentication already configured:

```sh
: "${YSCALE_CONNECTOR_IMAGE:?Set your registry/repository:version}"
make agent-image-build FLY_AGENT_IMAGE="$YSCALE_CONNECTOR_IMAGE"
docker push "$YSCALE_CONNECTOR_IMAGE"
```

Resolve and pin the pushed digest in the chart. The variable's historical
`FLY_` name does not require a Fly registry. Other service images and each
provider's burst image/AMI still need their own build and configuration.

Follow the [managed-mesh deployment guide](self-hosted-mesh.md) in order:
state and secrets → factory and private ops network → central → tenant →
connector and gateway. Its [tenant/connector recipe](self-hosted-mesh.md#operator-tenant-and-connector-recipe)
shows the handoff. Creating a tenant with a real factory starts a billable
coordination VM; it is not a free signup action.

Keep the default `managedMesh: true` and full-network gateway. A tenant can
simply represent your own team. No Yscale-operated identity service is required
for the operator-token installation path. The web console is separate
configuration, not a prerequisite for the first workload.

## 4. Hand the cluster to workload authors

Once the operator has verified the connector, mesh and provider configuration,
give the team Kubernetes access to its allowed namespace and the
[first-workload guide](first-workload.md). Keep the central admin token,
factory bearer and provider credentials with the operator.

Workload authors keep their existing editor and Git repository. Flux or Argo
can remain the manifest-delivery layer; Yscale handles the requested burst
capacity. This does not add a Jupyter hosting service or a VS Code extension.

## Troubleshooting the handoff

| Symptom | Check first |
|---|---|
| `ImagePullBackOff` | The default image is public; check the node can reach `ghcr.io`. A custom `agent.image.repository` needs its registry's pull secret in `imagePullSecrets`. |
| Connector cannot reach central | Use your reachable HTTPS/WSS endpoint, valid TLS and the correct credential; an example `.invalid` hostname is deliberately unusable. |
| Init container reports missing login server | Wait for real fabric readiness; confirm central can reach its factory. Do not disable managed-mesh validation to hide it. |
| Fabric stays pending | Inspect factory provisioning and private callback connectivity; `FACTORY_DEV=1` cannot create a real mesh. |
| Node joins but logs, exec or Services fail | Check gateway routes, pod/service CIDRs, apiserver reachability and the cluster-specific network requirements. |
| Workload ends but cloud resources remain | Inspect cleanup status and provider inventory. Keep central/factory/connector running while cleanup is unresolved. |

Ready to run a real job? Continue with [Your first workload](first-workload.md).
