# Self-hosted mesh deployment

Yscale uses a factory-managed mesh to connect your cluster and burst infrastructure. You operate the control plane, factory, mesh coordination and supporting services. Yscale does not provide a hosted service or require a Yscale account or subscription.

Start with [Getting started](getting-started.md) for build instructions and a no-cloud evaluation. This guide covers operator-managed deployment, not a one-command installation. After setup, workload authors can follow [Your first workload](first-workload.md).

Fresh real-provider operations-mesh joins remain experimental and are not yet qualified. Local build and configuration tests do not verify a running mesh or a complete cloud installation. See the [configuration record](release/supported-configurations.md).

## Components and ownership

```text
your CLI / cluster connector -> your central (yscale-cloud)
                                |
                                +-> your private factory (yscale-factory)
                                      |
                                      +-> tenant Headscale + DERP boxes
your cluster gateway <---------- managed mesh ----------> burst nodes
```

- Central schedules work and coordinates lifecycle and mesh policy.
- Factory owns the coordination-box credentials and encryption key. It
  provisions a dedicated Headscale/DERP box for each tenant. A tenant can be
  your own organization; this does not require selling a service.
- Agents and gateways use Tailscale **clients**, but discover their
  operator-managed login server from central. They do not need a shared
  Tailscale SaaS coordination account for the workload mesh.
- A separate private **ops mesh** connects boxes to the factory's key-handoff
  listener. You must supply and secure that network; it is not the tenant mesh.
  Run a separate self-hosted Headscale coordinator and set
  `FACTORY_OPS_LOGIN_SERVER` to its HTTPS origin. Boxes join that coordinator
  with a Headscale-issued `FACTORY_OPS_AUTHKEY`; missing or invalid coordinator
  configuration is rejected before any cloud provisioning. There is no default
  fallback to Tailscale SaaS. See the [ops setup recipe](../deploy/runbooks/factory.md#self-hosted-ops-setup).

**No Tailscale account or subscription is needed**, for operators or workload
authors. The Tailscale client binaries remain part of the mesh implementation
on infrastructure nodes; this is not a replacement of that client software.
Workload authors use Kubernetes access, without a Tailscale app on their laptop.

**Current factory dependency:** production box provisioning uses Linode, even
when burst workers use AWS, Fly or another provider. Coordination boxes and
burst nodes both incur provider charges. Removing a workload does not imply
that its tenant's persistent coordination box is deleted.

**Third-party services used at runtime (none operated by Yscale):** each box
downloads Headscale from GitHub releases and Tailscale from `tailscale.com`,
discovers its public IP via `api.ipify.org`/`ifconfig.me`, uses Linode's
`*.ip.linodeusercontent.com` hostname and obtains a Let's Encrypt certificate.
Boxes need outbound internet; the factory's Linode firewall opens inbound TCP
80/443 and UDP 3478/41641 on each box.

**Images:** no published images or chart registry are part of this release.
Chart defaults for the connector image and the compiled-in burst-node image
point at the original maintainer's private registry and will not pull. Build
and push your own agent, factory, central and burst images, then set
`agent.image.repository`/`imagePullSecrets` on the chart and
`YSCALE_BURST_IMAGE` on central.

## 1. Build the full stack

From the repository root:

```sh
make build
./bin/yscale help
```

`make build` includes `yscale`, `yscale-cloud`, `yscale-agent`,
`yscale-factory` and `yscale-lifecycle-migrate`.
The CLI alone is not the control plane. `make run` starts central; the older
standalone Fly controller is explicitly available as `make run-legacy-controller`.

Tagged release archives contain the same binaries plus provisioning templates,
charts and these docs. Run the factory with the extracted archive root as its
working directory: it reads `deploy/headscale/cloud-init.sh` and `acls.hujson`.
The factory container includes those assets under `/app`.

To prepare an archive locally without publishing:

```sh
GOOS=linux GOARCH=amd64 bash scripts/package-release.sh v0.0.0-preview dist
```

## 2. Prepare isolated state, secrets and networking

Use your own infrastructure and secret manager. Do not source factory and
central environment files into the same process: both use `DATABASE_URL` and
`LINODE_TOKEN`, with different permissions and purposes.

| Process | Required operator configuration |
|---|---|
| Central | Own TLS endpoint; central Postgres; credential-encryption key; operator API token; factory URL and bearer; selected burst-provider credentials and images |
| Factory | Separate Postgres database/role; KEK; matching factory bearer; box-scoped Linode token; private ops auth key, listener address and callback URL |
| Connector | Your central endpoint; tenant token; gateway and routing settings for your cluster |

Start from [.env.cloud.example](../.env.cloud.example),
[.env.factory.example](../.env.factory.example) and
[.env.agent.example](../.env.agent.example). Empty secrets and `.invalid`
hostnames are intentional placeholders. Generate independent random secrets;
both encryption keys use base64-encoded 32-byte keys. Back up keys separately
from the encrypted databases. Never commit populated `.env.*` files.

For non-disposable workloads, configure the durable lifecycle database and
run the included lifecycle migration with its migration-role credentials
before starting central with the least-privilege runtime DSN. The migrator
requires `LIFECYCLE_MIGRATION_DATABASE_URL` and `LIFECYCLE_RUNTIME_ROLE`; run
`./bin/yscale-lifecycle-migrate` in that separate migration environment.
See the [operations and recovery guide](operations.md);
do not enable flags against an unmigrated database or reuse a privileged DSN
for a normal service. Central state and factory state also require backups.

Do **not** set `TS_OAUTH_CLIENT_ID` or `TS_OAUTH_CLIENT_SECRET` on central.
The full release rejects them rather than falling back to shared coordination.

## 3. Start your factory, then central

Complete the [factory runbook](../deploy/runbooks/factory.md), including the
private ops-network connection. Production mode is the default. `FACTORY_DEV=1`
uses a fake worker and cannot create a usable workload mesh.

Configure the factory's ops client and new boxes against the **same separate
ops Headscale**, not a tenant's coordinator. The factory process requires
`FACTORY_OPS_LOGIN_SERVER`; the deployment template does not install an ops
coordinator or connect its stubbed sidecar automatically.

The factory's `:8080` fabric API is private to central and bearer-authenticated.
Its `:8081` handoff listener must be reachable only by coordination boxes over
the ops mesh. Set `FACTORY_OPS_LISTEN` to that private interface and
`FACTORY_OPS_URL` to the address the boxes can reach; the example's loopback
binding deliberately prevents a copy/paste from exposing it publicly.
Never publish the handoff listener through an internet-facing ingress.

Run each binary from the repository/archive root with its separate environment:

```sh
# In the factory process environment:
./bin/yscale-factory
# In the central process environment:
./bin/yscale-cloud -listen=:8443
```

Central's listener is plain HTTP; terminate TLS at your own ingress or proxy
before allowing remote agents or users. Set `YSCALE_CENTRAL_ENDPOINT` to your
reachable endpoint so generated install instructions point to your deployment.
Keep both central and factory at one replica until their cache/read-source
constraints are addressed. Legacy `deploy/cloud` examples need adaptation to
your secrets, registries, database and private-network wiring; do not apply
them unchanged as a production installer.

## 4. Provision your tenant and install the connector

The operator-authenticated `POST /v1/admin/tenants` endpoint creates a tenant,
returns its token/install instructions, and requests its mesh from the factory.
**This starts a billable coordination VM with a real factory.** Choose your
region, spending limits and cleanup plan before invoking it. Keep the response
private: it contains a credential. No Yscale identity provider is required for
the operator-token path; human-account/console login is separate configuration.

Wait for the tenant's fabric to be ready before installing the connector.
Use [the agent chart](../deploy/helm/yscale-agent), not the legacy
`deploy/helm/yscale` controller chart. Set its `endpoint` to your central and
use an `existingSecret` carrying the tenant's `YSCALE_TOKEN`. Retain the
default full-network gateway. Configure your cluster CIDRs, apiserver reachability
and immutable operator-built images; gateway requirements differ by cluster.

With automatic key minting, the agent and gateway init containers obtain
`login_server` with their keys from central and join the tenant's managed mesh.
The blank `tsLoginServer` values are discovery defaults, not instructions to
use Tailscale SaaS. `managedMesh: true` is the chart default: both clients stop
if no login server is available instead of silently joining shared coordination.
If using pre-minted keys instead, explicitly set both
`tailscale.tsLoginServer` and `gateway.tsLoginServer` to the tenant's Headscale
URL. Only an intentional legacy install should set `managedMesh: false`.

### Operator tenant and connector recipe

Run these from a trusted operator machine, after completing steps 1–3 and
reviewing provider costs. You need `curl`, `jq`, Helm and `kubectl`. Set
`YSCALE_CENTRAL_ENDPOINT`, `YSCALE_ADMIN_TOKEN` and a positive
`YSCALE_TEST_MAX_HOURLY_USD` from your cost estimate. Use a new tenant ID;
the example uses `cust_first_team`. Do not enable shell tracing or share the
response: it contains a tenant credential.

**Billable step:** the POST asks your real factory to create a coordination VM.
The hourly limit below controls burst admission, not that persistent VM's bill.

```sh
: "${YSCALE_CENTRAL_ENDPOINT:?Set your central HTTPS endpoint}"
: "${YSCALE_ADMIN_TOKEN:?Load your operator token securely}"
: "${YSCALE_TEST_MAX_HOURLY_USD:?Set a positive limit from your cost estimate}"
umask 077
tenant_response=$(mktemp)
jq -n --argjson hourly "$YSCALE_TEST_MAX_HOURLY_USD" \
  'if ($hourly | type) == "number" and $hourly > 0 then {id:"cust_first_team", MaxConcurrentBursts:1,
    MaxHourlyUSD:$hourly, workload_namespaces:["default"]}
   else error("hourly limit must be positive") end' | \
  curl --fail-with-body --silent --show-error \
    -H "Authorization: Bearer $YSCALE_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' \
    --data-binary @- --output "$tenant_response" \
    "$YSCALE_CENTRAL_ENDPOINT/v1/admin/tenants"
jq -e '.customer_id == "cust_first_team" and (.token | length > 0)' "$tenant_response"
```

Stop on an error; a successful POST requests provisioning but does not prove
the box is ready. From a machine with access to the private factory API,
inspect only the non-secret status fields (the full response contains secrets):

```sh
: "${FACTORY_URL:?Set your private fabric API URL}"
: "${FACTORY_BEARER_TOKEN:?Load your factory API token securely}"
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $FACTORY_BEARER_TOKEN" \
  "$FACTORY_URL/v1/tenants/cust_first_team/fabric" | \
  jq '{status, login_server}'
```

Wait for `status: "ready"` and a real `login_server`. Confirm central has
attached the fabric too; its polling interval means readiness is not instant.
Do not substitute a `FACTORY_DEV=1` result for a working coordination box.

Store the token in a Kubernetes Secret without putting its value in a Helm
command or values file. Run against the **intended cluster** and ensure the
POST and JSON checks above succeeded first:

```sh
kubectl config current-context
kubectl create namespace yscale-system --dry-run=client -o yaml | kubectl apply -f -
token_file=$(mktemp)
jq -er '.token' "$tenant_response" | tr -d '\n' > "$token_file"
kubectl -n yscale-system create secret generic yscale-agent-credentials \
  --from-file=YSCALE_TOKEN="$token_file" --dry-run=client -o yaml | kubectl apply -f -
rm -f "$token_file" "$tenant_response"
```

Use an approved secret manager for ongoing storage and rotation. Do not print
or commit that Secret. The temporary response is removed after handoff.

Create `connector-values.yaml` using your cluster-specific
[deployment example](../examples/deploy/README.md) and the chart's
[values reference](../deploy/helm/yscale-agent/values.yaml). Override the old
SaaS endpoint, private registry/pull secret and every placeholder. At minimum:

```yaml
endpoint: https://central.example.invalid
existingSecret: yscale-agent-credentials
managedMesh: true
rbac:
  scope: namespaced
  allowedNamespaces: [default]
agent:
  image:
    repository: registry.example.invalid/team/yscale-cluster-agent
    digest: sha256:REPLACE_WITH_THE_64_HEX_DIGEST_YOU_PUSHED
    requireDigest: true
imagePullSecrets:
  - name: your-registry-pull-secret
bootstrap:
  apiserverURL: https://your-burst-reachable-apiserver.example.invalid:6443
gateway:
  enabled: true
```

This is an edit-required skeleton, not runnable values. Add the correct
`cloudProvider` for your cluster, storage class if required, and validate
gateway pod/service/node routes and apiserver reachability. Set
`imagePullSecrets: []` only when your image is actually public; otherwise create
the referenced pull secret in `yscale-system`. Keep login-server discovery
unless you intentionally supply pre-minted keys. Review the rendered RBAC.

```sh
helm template yscale-agent ./deploy/helm/yscale-agent \
  --namespace yscale-system -f connector-values.yaml > connector-rendered.yaml
helm upgrade --install yscale-agent ./deploy/helm/yscale-agent \
  --namespace yscale-system -f connector-values.yaml
kubectl -n yscale-system get pods
kubectl -n yscale-system rollout status deployment/yscale-agent-yscale-agent --timeout=180s
```

Pod readiness is only the connector checkpoint. Verify mesh routes and the
real workload lifecycle next; then hand the team the
[first-workload guide](first-workload.md).

## 5. Verify before trusting a workload

Check the fabric status, agent connection and gateway routes. Then, with an
explicit provider budget, run one small full-network workload and verify node
join, pod execution, logs/exec, cancellation and provider-side deletion. A
healthy HTTP endpoint alone does not prove any of those steps.

Check for remaining instances, disks, network interfaces, firewalls and mesh
nodes. Offboarding a tenant is separate from completing a workload and must
also remove its coordination box. Retain the evidence for the exact revision,
provider, region, image and cluster target you tested. Do not infer Azure/GCP
or another GPU SKU works from a successful Linode run.

The historical Tailscale-only mode and older split-edition
export scripts are historical alternatives, **not the default release path**.
