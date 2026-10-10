# Yscale documentation

Yscale adds temporary CPU or supported GPU nodes to your existing Kubernetes cluster through your cloud accounts. You operate the control plane and networking; developers submit workloads through Kubernetes. There is no Yscale-hosted service or required signup.

## Start here

The [Quickstart](quickstart.md) is the shortest path from nothing to one burst node running a job on your cluster.

Begin with [Getting started](getting-started.md) for operator prerequisites and the local no-cloud smoke test. The smoke test uses fake provisioning, not a running mesh.

Continue with [Self-hosted managed mesh](self-hosted-mesh.md) to build your images and deploy the system. The current factory requires Linode coordination boxes regardless of worker provider. Then follow [Your first workload](first-workload.md).

## Plan your deployment

This release is experimental. Check the [configuration record](release/supported-configurations.md) for qualification scope and [Full networking](networking.md) for CNI-specific requirements.

Use [Storage and model volumes](storage.md) and [Operations and recovery](operations.md) when planning workloads. Provider charges continue until resources are deleted. Read the [license](../LICENSE) and [commercial terms](../COMMERCIAL.md) for permitted use.
