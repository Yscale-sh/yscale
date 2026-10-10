# Introducing Yscale: temporary compute for your Kubernetes cluster

Yscale is a self-hosted, source-available Kubernetes burst controller. It adds temporary CPU or supported GPU nodes through your cloud accounts when a job needs capacity beyond your existing on-premises or cloud cluster. Workloads remain submitted through Kubernetes, and Yscale coordinates resource cleanup when the work finishes or is cancelled.

For operators evaluating Yscale, the [installation guide](../getting-started.md) is the starting point. This release is experimental; plan to qualify a specific provider and cluster configuration before relying on it.

## Where jobs run

In the explicit Workload path, your team submits a `Workload` resource. The Cluster Connector forwards it to the control plane, which selects a configured backend and provisions a machine. Yscale coordinates its enrollment as a Kubernetes node, where the workload runs as a pod.

Workload authors keep using `kubectl`, Git and their editor. Flux or Argo can continue delivering manifests; Yscale does not replace them. It does not provide Jupyter hosting or a VS Code extension.

The included provider implementations cover Fly.io CPU, Linode and AWS CPU and GPU, and Azure and Google Compute Engine CPU. Azure and GCP GPU support is outside this release’s scope. An implementation in the source does not establish qualification for every region, machine shape or cluster.

## How networking connects the nodes

Full networking connects burst-side Cilium to your existing cluster CNI through an encrypted mesh and gateway routes. Operators must configure and verify networking for their CNI. The [networking guide](../networking.md) explains that part of deployment.

Tenant coordination and the separate operations network can both use self-hosted Headscale. No Tailscale account or subscription is required, although Tailscale clients run on infrastructure nodes. Workload authors do not need a laptop VPN app.

The current real factory uses Linode for persistent coordination boxes even when another provider supplies the workers. Operators also configure a separate operations coordinator and private callback path. `FACTORY_OPS_LOGIN_SERVER` is mandatory, with no hosted fallback. A fresh real-provider operations join has not yet been qualified.

## What you operate

There is no Yscale-hosted service, compulsory signup or subscription. You run central and the factory, maintain the databases, and manage TLS, mesh routing, secrets and cloud accounts.

The full source includes the connector, gateway and cloud adapters alongside central and the factory. It also includes tenant and account management, cloud cost estimates, tools and the console.

Installation begins with the local no-cloud smoke test. It exercises control-plane provisioning with a fake worker; it does not create a running mesh or demonstrate node enrollment.

For a real deployment, build your application and burst-node images and replace the legacy private registry defaults. Prepare persistent state, secrets, TLS and private networking, then start the factory and central. Create a tenant and install the connector and gateway into your cluster. Follow the [first-workload guide](../first-workload.md) with a small CPU job, checking connectivity and resource deletion as part of the evaluation.

## Inputs and workload examples

Workloads can use R2 or S3-compatible object storage and pre-seeded Linode model volumes for inputs. The [storage guide](../storage.md) covers these options.

The separate `yscale-media` integration example ranks workers, prioritizes chunks near the playhead and uses encoder-family affinity. Its templates are starting points for an integration, not a complete turnkey autoscaler.

## Costs and terms

You pay your cloud providers for resources. Persistent coordination boxes incur charges between jobs, and other resources remain billable until deleted. Budget settings are not provider billing caps. Verify cleanup against provider inventory, including after cancellation or failure.

Personal and internal organizational use is free of license fees. Resale, third-party hosting or brokered compute requires a separate paid written license. Yscale is source-available, not OSI open source; consult the [license](../../LICENSE) and [commercial terms](../../COMMERCIAL.md) for the permissions and conditions. Provider charges are separate.

Start with the [installation guide](../getting-started.md), then evaluate one small workload on your chosen configuration. The [configuration record](../release/supported-configurations.md) identifies qualification by revision, provider and cluster target. Local smoke tests do not establish real-cloud readiness; your evaluation needs to cover mesh connectivity, workload execution and confirmed cleanup.
