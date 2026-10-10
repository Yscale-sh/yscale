# Yscale

**A flexible Kubernetes burst tool.**

Yscale adds temporary compute capacity to an existing Kubernetes cluster.
Submit a workload, provision a node through a cloud backend, run ordinary
Kubernetes pods, and release the capacity when the workload finishes.

Run it yourself with your own infrastructure and provider accounts. This is a
full-source project, not a hosted-service launch or a limited-feature edition.
Yscale is distributed as self-hosted software; we do not offer a managed
Yscale hosting service. Paid resale/hosting licenses authorize other operators
to provide their own offerings, not access to a service operated by us.
The control plane, cluster connector, provider adapters, fabric factory and
tenant-management implementations are included in this repository.

**The default release is the full managed-mesh version, including self-hosted
installs.** You run central and the factory, which manages your tenants'
Headscale/DERP coordination boxes. Start with [Getting started](docs/getting-started.md)
for downloads, a no-cloud local check and the operator/user handoff. Then use
the [self-hosted managed-mesh guide](docs/self-hosted-mesh.md) to deploy it.

[Documentation](docs/README.md) · [Your first workload](docs/first-workload.md) ·
[Introducing Yscale](docs/articles/introducing-yscale.md)

> **Early release.** Provider adapters and test harnesses are not a promise that
> every provider, Kubernetes distribution, region or instance type is qualified.
> Start with an isolated test environment. Cloud resources can incur charges
> until deletion succeeds; application budgets are not provider-enforced caps.

## What it does

- Extends an existing cluster with temporary CPU or GPU capacity through
  provider-specific adapters.
- Represents burst workloads as Kubernetes resources, with a control plane
  and an agent connecting the workload to its provisioned node.
- Includes lifecycle tracking, cost estimates, cancellation and cleanup paths.
- Uses full pod networking over a mesh; the former `lite` tier is not supported.
- Works alongside your deployment workflow. It does not replace Flux, Argo,
  an editor or your existing Kubernetes cluster.

## Scope and status

| Provider | Included implementation | Release scope |
|---|---|---|
| Fly.io | CPU backend | Experimental; verify the exact configuration |
| Linode | CPU and GPU backend paths | Existing CPU/GPU scope retained; verify the selected shape |
| AWS EC2 | CPU and GPU backend paths | Existing CPU/GPU scope retained; verify the selected shape |
| Azure | CPU backend | CPU-only; GPU support deferred |
| Google Compute Engine | CPU backend | CPU-only; GPU support deferred |

AKS, GKE and EKS are **cluster targets**, separate from the compute provider.
Evidence for one provider/target combination does not qualify another.
The [configuration record](docs/release/supported-configurations.md) starts
unqualified for this source snapshot. Private operator receipts are not public
release proof. The operational readiness checker retains its complete checks,
but this source release does not activate a hosted service.

## Build and explore

Install the Go version required by [go.mod](go.mod), plus `curl` and `jq` for
the local regression tests. No cloud account is needed for the unit suite.

```sh
git clone https://github.com/yscale-sh/yscale.git
cd Yscale
GOFLAGS=-p=2 make build
./bin/yscale help
go test -p=2 ./...
```

The CLI includes workload submission, Helm values generation, template catalog
commands and manifest injection. Running it without a subcommand starts the
legacy controller; use `help` when exploring without provisioning anything.

The default build and tagged release archives include central, agent, factory,
CLI and the lifecycle migration tool. No enterprise tag or export step
is needed. `make run` starts central; configure its environment first.
To build individual services during development:

```sh
go build -p=2 -o bin/yscale-cloud ./central/cmd/yscale-cloud
go build -p=2 -o bin/yscale-agent ./agent/cmd/yscale-agent
go build -p=2 -o bin/yscale-factory ./factory/cmd/yscale-factory
```

## Before provisioning

You need a Kubernetes cluster you control, narrowly scoped provider credentials,
appropriate node images, and the mesh/network configuration for your deployment.
You are responsible for provider charges, quota, cleanup and ongoing operation.

- Read [the networking overview](docs/networking.md) and
  [cluster connector guide](docs/cluster-connector.md).
- Review the [agent chart](deploy/helm/yscale-agent) and
  [deployment examples](examples/deploy). Configure your own endpoints and
  image references; do not use historical operator endpoints.
- Follow the [managed-mesh guide](docs/self-hosted-mesh.md) for central, factory,
  private ops networking and connector setup. The older
  Tailscale-only mode is not the default release path.
- Use an isolated account/project and a small, explicitly estimated test.
  Verify provider-side deletion after both successful and failed workloads.

This source release does not promise one-command production installation,
managed hosting, an SLA, or guaranteed cloud-cost ceilings.

## Repository map

- `cmd/yscale/` — CLI and legacy controller
- `central/` — control plane, placement, lifecycle and account APIs
- `agent/` — Kubernetes cluster connector
- `burst/` — burst-node images and bootstrap components
- `pkg/backends/` — compute provider adapters
- `factory/` — coordination-fabric provisioning and management
- `pricefeed/` — provider pricing collection
- `deploy/`, `examples/` — charts, manifests and test harnesses
- `website/` — website and console source, included directly
- `test/` — regression and integration checks

Private planning documents and operator records are excluded from the export.
Retained legacy implementation paths are not offers of a managed service.
Legacy publishing/deployment workflows are disabled in the public snapshot.
The active CI runs on GitHub-hosted runners and does not provision cloud capacity.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks and
[SECURITY.md](SECURITY.md) for vulnerability reporting. Do not include credentials,
customer data or private cluster configuration in issues or pull requests.

## License

Yscale is **source-available, not OSI open source**:

- Personal use and internal self-hosting are free of license fees, regardless
  of company revenue or size, including internal use to run your own apps.
- Reselling Yscale or providing its functionality or brokered compute capacity
  to third parties as a hosted/managed service requires a separate paid license.
- All application features are in this source distribution; no separate
  proprietary edition is required for internal use. Third-party licenses remain
  unchanged. Historical proprietary-source markers belong to the old export
  tooling, not a separate feature license for this full-source release.

See [LICENSE](LICENSE) and [COMMERCIAL.md](COMMERCIAL.md).
This summary does not create a license grant or override the license text.
