# Networking across the burst boundary

Yscale connects temporary cloud nodes to your existing Kubernetes cluster. Burst-side Cilium works with the cluster’s existing CNI through an encrypted mesh and gateway routes. This experimental networking path requires CNI-specific configuration and verification.

## Packet path

1. Host-mode Cilium on the burst node resolves a Kubernetes Service to a backend pod.
2. An encrypted mesh carries traffic toward the in-cluster gateway.
3. The gateway forwards traffic into your cluster’s pod network.
4. Gateway source NAT and return routes carry the response back to the burst node.

The tenant workload mesh uses Tailscale clients with an operator-managed Headscale coordinator. Direct WireGuard transport is used when available; DERP provides a relay path when needed. Relayed traffic remains subject to latency and bandwidth constraints.

## Operator requirements

Verify the networking path for your cluster and provider:

- Pod, Service and node CIDRs, route approval, and possible address overlap.
- Provider images with the required Cilium and node-bootstrap components.
- Firewalls, MTU, API-server reachability, DNS, and network policies.
- Workload execution, private-service connectivity, and resource teardown.

Gateway source NAT means destination workloads see traffic from the gateway, rather than a preserved burst-pod source identity. CNI-specific identity and policy behavior may not carry across the boundary unchanged. Networking tier `full` is the only supported tier.

## Tenant and operations networks

The tenant mesh carries workload traffic. A separate private operations mesh lets newly provisioned coordination boxes return credentials to the factory.

Both meshes can use self-hosted Headscale. A separate self-hosted operations coordinator is mandatory: set `FACTORY_OPS_LOGIN_SERVER` to its HTTPS origin. There is no hosted fallback, and no Tailscale account or subscription is required. Tailscale clients remain installed on infrastructure nodes; workload authors do not need a laptop VPN application.

Keep the operations mesh and its private callback path isolated from workload tenants and the public internet. A fresh operations-mesh join on a real provider has not yet been qualified.

See [operator setup](self-hosted-mesh.md) and the [factory environment reference](../deploy/runbooks/factory.md#environment-variables).

Measure your application’s network and input-data path. A working mesh provides connectivity, but remote services still have network latency.
