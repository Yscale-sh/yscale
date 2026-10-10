# Operating your own Yscale installation

Yscale is experimental software that you operate alongside your Kubernetes cluster. There is no Yscale-hosted service. Start with [operator setup](self-hosted-mesh.md).

You run central, the factory, databases and container images, and manage TLS, secrets, mesh routing and cloud accounts. The source also includes account management, tenancy and estimates of your cloud costs.

Configure a separate self-hosted operations coordinator and private callback path. `FACTORY_OPS_LOGIN_SERVER` is mandatory, with no hosted fallback. Infrastructure nodes still use Tailscale clients, but you do not need a Tailscale account or subscription.

Cloud resources are billed through your provider accounts until deleted. Yscale budget settings are not provider billing caps.

## State and recovery

Inventory the PostgreSQL databases used by central, lifecycle and the
factory. Keep their schemas, encrypted credentials, the factory KEK, and the
tenant coordination servers' Headscale state recoverable together. Losing the
KEK can make stored credentials unusable even when the database backup exists.
Use your database and storage operators' supported backup mechanisms, encryption,
access controls and retention policy. Do not put backups or secrets in Git.

Test restoration into a new, isolated target with separate credentials. Verify
schema versions and representative tenants, workload records and encrypted
credentials before relying on recovery. Do not overwrite a serving database or
reuse its credentials for a rehearsal. Record the actual recovery time and data
loss window; this repository does not supply a verified RTO or RPO.

## Upgrade and rollback

Record exact source revisions, image digests, rendered manifests, configuration
and database migration versions before an upgrade. Back up the affected state
and establish whether each migration is backward-compatible before replacing
binaries. Rolling a binary back is not a database rollback. Rehearse on an
isolated installation and keep a known-working version available.

## Qualify one configuration

Choose a provider, cluster target, region, instance shape and lifecycle mode.
Check provider quotas, node images, routes, network policies and current costs.
Get an approved spend estimate before provisioning. Reuse an hourly-billed test
node across checks where possible, then delete it once and verify provider-side
absence, including separately billed disks and other attached resources.

Verify provisioning, node join, pod execution, logs, exec, port-forward, tenant
isolation, cancellation and cleanup on that exact configuration. Local tests and
source inclusion do not qualify another provider, region, SKU or cluster.
See the [configuration record](release/supported-configurations.md).

## Check operational readiness

Use the readiness validator to assess the evidence recorded for your installation. Its manifest currently has unset identity fields and pending operational checks, so **NOT READY** is expected until those checks have independently verifiable evidence.

A fresh operations-mesh join on a real provider remains unqualified. Local no-cloud smoke tests use fake provisioning and do not establish a working mesh. Verify the complete lifecycle on your chosen provider and cluster before relying on it for workloads.

Readiness applies to the configuration tested. Source availability and passing local checks do not establish production readiness.

```sh
go run ./cmd/yscale-launch-readiness readiness -manifest docs/release/manifest.json
```
