# Getting started with Yscale

Yscale is a self-hosted Kubernetes burst controller. It adds temporary CPU or supported GPU nodes through your cloud accounts and connects them to your existing cluster over an encrypted mesh. Workloads remain submitted through Kubernetes.

An operator installs central, the factory and the Cluster Connector. Workload authors continue using `kubectl`, Git or existing Flux/Argo workflows; they do not install a separate control plane or laptop VPN application.

Yscale is experimental, source-available software, not a hosted service or a one-command production installer. Begin with the [Quickstart](quickstart.md): clone the repo and host your own images using your fork's GitHub Actions workflow, or build locally and push to your registry. The project's own `ghcr.io/yscale-sh` packages are private and cannot be pulled by other users. See [operator setup](self-hosted-mesh.md) for infrastructure and mesh requirements.

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
- Your own connector, central and factory container images, published by your
  fork's workflow or built locally, plus provider-specific burst images. Private
  maintainer image defaults are not pullable by new users. Native bundles do
  not remove this requirement.
- Linode for the current factory's persistent Headscale/DERP boxes, even if
  workers use a different provider; a separate self-hosted Headscale coordinator
  for the private ops mesh. No Tailscale account or subscription is required.
  Yscale's mesh components still use the Tailscale client software; workload
  authors do not install it on their laptops. See the ops setup in the factory
  guide below.
- A provider cost estimate and a cleanup plan, including coordination VMs,
  worker VMs, disks and networking. Software budget fields are not hard caps
  on the provider bill.

### Build and publish your images

From a **source checkout**, install Docker Buildx and authenticate to a registry
you control (`docker login ghcr.io` for the example below). Replace `example`
with your own user or organization. Publish all five images with one target:

```sh
export IMAGE_REGISTRY=ghcr.io/example
export TAG=v0.0.2-preview
export PLATFORM=linux/amd64
make all-images-push IMAGE_REGISTRY="$IMAGE_REGISTRY" TAG="$TAG" PLATFORM="$PLATFORM"
```

`IMAGE_REGISTRY` is the registry plus repository prefix, without a trailing
slash. `TAG` applies to every image. Individual overrides are `FLY_AGENT_IMAGE`,
`FLY_CLOUD_IMAGE`, `FLY_FACTORY_IMAGE`, `FLY_BURST_BASE_IMAGE` and
`FLY_BURST_IMAGE`; their historical names do not require a Fly registry.
Make passes `FLY_BURST_BASE_IMAGE` as the kubelet Dockerfile's `BASE_IMAGE` and
publishes the base before building the kubelet overlay, including with `make -j`.

The equivalent direct commands below all use the **source root** as context.
The connector, cloud and factory BuildKit Dockerfiles compile from source;
the cloud image also includes `yscale-lifecycle-migrate`, and the factory
includes its coordination-box provisioning templates.

```sh
docker buildx build --platform "$PLATFORM" \
  -f agent/build/Dockerfile.agent.buildkit --build-arg VERSION="$TAG" \
  --push -t "$IMAGE_REGISTRY/yscale-cluster-agent:$TAG" .
docker buildx build --platform "$PLATFORM" \
  -f central/build/Dockerfile.cloud.buildkit \
  --push -t "$IMAGE_REGISTRY/yscale-cloud:$TAG" .
docker buildx build --platform "$PLATFORM" \
  -f factory/build/Dockerfile.factory.buildkit \
  --push -t "$IMAGE_REGISTRY/yscale-factory:$TAG" .

# Fly burst node: publish the base first, then the kubelet overlay.
docker buildx build --platform linux/amd64 \
  -f burst/image/Dockerfile.kubelet.base \
  --push -t "$IMAGE_REGISTRY/yscale-burst-base:$TAG" .
docker buildx build --platform linux/amd64 \
  -f burst/image/Dockerfile.kubelet \
  --build-arg BASE_IMAGE="$IMAGE_REGISTRY/yscale-burst-base:$TAG" \
  --push -t "$IMAGE_REGISTRY/yscale-burst-image:$TAG" .
```

The three service images support `linux/amd64` or `linux/arm64`; for both, use
`PLATFORM=linux/amd64,linux/arm64` with the direct `--push` commands. Make's
`--load` targets accept one platform. The burst Dockerfiles are **amd64-only**:
the base downloads hardcode amd64 and would need to become `TARGETARCH`-aware
before an arm64 burst build is valid. The aggregate Make target therefore
requires `PLATFORM=linux/amd64`; build services separately for arm64 clusters.

Use a registry your burst provider can pull from. For a private Fly registry,
create the burst/base apps in your own Fly organization, run `flyctl auth docker`,
and set `FLY_BURST_BASE_IMAGE` and `FLY_BURST_IMAGE` to their
`registry.fly.io/<your-app>:$TAG` references. Kubernetes `imagePullSecrets` only
authenticate pod pulls; they do not grant a Fly Machine access to a private GHCR
package. Other providers still require their own account-owned VM images/AMIs;
these container builds do not create them. See the [Quickstart](quickstart.md)
for the Linode image bake and the [operator guide](self-hosted-mesh.md) for
provider setup.

### Pin the images you published

Inspect each pushed image; copy its top-level `Digest: sha256:...` value, rather
than a platform-specific child digest when deploying a multi-platform image:

```sh
for image in yscale-cluster-agent yscale-cloud yscale-factory yscale-burst-base yscale-burst-image; do
  docker buildx imagetools inspect "$IMAGE_REGISTRY/$image:$TAG"
done
```

Set these values in your connector Helm values file, replacing the example
repository and digest with your own. `agent.image.digest` takes precedence over
the tag, and `requireDigest` rejects an unpinned install:

```yaml
agent:
  image:
    repository: ghcr.io/example/yscale-cluster-agent
    digest: sha256:<connector-digest>
    requireDigest: true
# For a private package, create this Secret in the connector's namespace.
# imagePullSecrets:
#   - name: your-registry-pull-secret
```

Before applying `deploy/cloud/cloud.yaml` and `deploy/cloud/factory.yaml`,
replace each container's `image:` line with its own digest reference:

```yaml
# deploy/cloud/cloud.yaml
image: ghcr.io/example/yscale-cloud@sha256:<cloud-digest>
# deploy/cloud/factory.yaml
image: ghcr.io/example/yscale-factory@sha256:<factory-digest>
```

Use that same cloud digest for any lifecycle migration Job. For private images,
create a registry pull secret in each Deployment's namespace and uncomment its
`spec.template.spec.imagePullSecrets` example. The pinned maintainer image
lines are release placeholders; they are private and must be replaced.

On **central**, set `YSCALE_FLYIO_BURST_IMAGE` to your published kubelet image
reference (for example `registry.fly.io/<your-burst-app>@sha256:<burst-digest>`).
`YSCALE_BURST_IMAGE` is the shared runtime OCI default; set it to your own image
where applicable. Neither variable changes Make's build tags. For a reproducible
kubelet rebuild, `BASE_IMAGE` can also be an immutable
`<your-registry>/yscale-burst-base@sha256:<base-digest>` reference.

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
| `ImagePullBackOff` | The `ghcr.io/yscale-sh` defaults are private. Set `agent.image.repository` to your own published image and set its tag or digest; make your package public or reference its registry pull secret in top-level `imagePullSecrets`. |
| Connector cannot reach central | Use your reachable HTTPS/WSS endpoint, valid TLS and the correct credential; an example `.invalid` hostname is deliberately unusable. |
| Init container reports missing login server | Wait for real fabric readiness; confirm central can reach its factory. Do not disable managed-mesh validation to hide it. |
| Fabric stays pending | Inspect factory provisioning and private callback connectivity; `FACTORY_DEV=1` cannot create a real mesh. |
| Node joins but logs, exec or Services fail | Check gateway routes, pod/service CIDRs, apiserver reachability and the cluster-specific network requirements. |
| Workload ends but cloud resources remain | Inspect cleanup status and provider inventory. Keep central/factory/connector running while cleanup is unresolved. |

Ready to run a real job? Continue with [Your first workload](first-workload.md).
