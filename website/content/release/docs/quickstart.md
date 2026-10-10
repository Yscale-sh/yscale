# Quickstart

Go from nothing to one temporary Linode node joining your Kubernetes cluster,
running a job and being removed. You run every component yourself; there is no
Yscale account, hosted service or Tailscale account. Expect an hour or two the
first time.

Yscale is experimental, source-available software. Internal use is free at any
company size under the [Yscale Community License](../LICENSE). This page is the
shortest working path; each step links to the full guide.

## What you need

- A Kubernetes cluster you administer, with `kubectl` and Helm 3. Burst nodes
  reach its API server through the connector's gateway, so pick an API server
  address inside a node CIDR you can advertise (for example a node IP).
- A Linode account and API token. Each tenant's coordination box runs on
  Linode, and this quickstart bursts onto Linode too. Both bill by the hour.
- A Linux host for the factory and central, plus a PostgreSQL database for the
  factory.
- A small VM with a public DNS name for the operations Headscale server.
- `curl`, `jq`, `openssl` and the open-source Tailscale client. No Tailscale
  sign-up is involved.

## 1. Get the release

Download the bundle for the host that will run the factory and central, verify
it, and unpack it:

```sh
VERSION=v0.0.1-preview
TARGET=linux-amd64
base=https://github.com/yscale-sh/yscale/releases/download/$VERSION
curl -fLO "$base/yscale-$VERSION-$TARGET.tar.gz"
curl -fLO "$base/yscale-$VERSION-$TARGET.tar.gz.sha256"
sha256sum -c "yscale-$VERSION-$TARGET.tar.gz.sha256"
tar -xzf "yscale-$VERSION-$TARGET.tar.gz" && cd "yscale-$VERSION-$TARGET"
node examples/local-managed-mesh-smoke.mjs .   # optional no-cloud check
```

Clone the same release of the source as well; the connector chart and the
burst-image script come from it:

```sh
git clone --branch "$VERSION" https://github.com/yscale-sh/yscale.git
```

- **Connector image.** Published with every release as
  `ghcr.io/yscale-sh/yscale-cluster-agent:$VERSION` (linux/amd64 and
  linux/arm64), and the chart defaults to it. The release's `images.txt` lists
  its digest. To build your own instead, use
  `agent/build/Dockerfile.agent.buildkit` with the source root as context.
- **Linode burst image.** Run
  `LINODE_TOKEN=... ./pkg/backends/linode/bake-cpu-image.sh`. It prints a
  `private/<id>` image. Linode images belong to the account that baked them,
  so you must bake your own.

## 2. Run the operations Headscale

The factory and the per-tenant coordination boxes talk over a small private
operations mesh that you host. Install Headscale 0.26.1 on the VM with a valid
HTTPS certificate (Headscale's built-in Let's Encrypt support is enough), then:

```sh
headscale users create yscale-ops-factory
headscale users create yscale-ops-boxes
headscale users list   # note both numeric IDs
headscale preauthkeys create --user <FACTORY_USER_ID> --expiration 2h
headscale preauthkeys create --user <BOX_USER_ID> --reusable --ephemeral --expiration 24h
```

Give it a policy that only lets coordination boxes reach the factory's handoff
port:

```json
{
  "groups": {
    "group:factory": ["yscale-ops-factory@"],
    "group:boxes": ["yscale-ops-boxes@"]
  },
  "acls": [
    {"action": "accept", "src": ["group:boxes"], "dst": ["group:factory:8081"]}
  ]
}
```

Details and hardening: [factory environment](../deploy/runbooks/factory.md).

## 3. Start the factory

Join the factory host to the operations mesh with the first key, and note its
mesh address:

```sh
tailscale up --login-server=https://ops.example.com --auth-key=<FACTORY_KEY> --accept-dns=false
tailscale ip -4
```

Put the factory's settings in a file only it can read (`chmod 600`), then run
it from the bundle root, where it finds its provisioning templates:

```sh
FACTORY_LISTEN=127.0.0.1:8080
FACTORY_OPS_LISTEN=<FACTORY_MESH_IP>:8081
FACTORY_OPS_URL=http://<FACTORY_MESH_IP>:8081
FACTORY_OPS_LOGIN_SERVER=https://ops.example.com
FACTORY_OPS_AUTHKEY=<BOX_KEY>
FACTORY_BEARER_TOKEN=<openssl rand -hex 32>
FACTORY_KEK=<openssl rand -base64 32>
DATABASE_URL=postgres://factory:<password>@db.example.com:5432/factory
LINODE_TOKEN=<token with Linodes and Cloud Firewalls read/write>
```

```sh
set -a; . ./factory.env; set +a; ./bin/yscale-factory
```

It logs `yscale-factory PROD`. Keep the fabric API (`:8080`) private to
central and never expose the handoff listener publicly.

## 4. Start central

```sh
FACTORY_URL=http://127.0.0.1:8080
FACTORY_BEARER_TOKEN=<same value as the factory>
YSCALE_ADMIN_TOKEN=<openssl rand -hex 32>
YSCALE_CENTRAL_ENDPOINT=https://central.example.com
LINODE_TOKEN=<token for burst nodes>
LINODE_REGION=us-ord
YSCALE_LINODE_IMAGE_CPU=private/<id from bake-cpu-image.sh>
```

```sh
set -a; . ./central.env; set +a; ./bin/yscale-cloud -listen=:8443
```

Central speaks plain HTTP; put your own TLS proxy in front of it. Do not set
`TS_OAUTH_*`. Without `DATABASE_URL` central keeps state in memory, which is
fine for this trial; configure its databases before relying on it
([operations and recovery](operations.md)).

## 5. Create a tenant

A tenant can simply be your own team. Creating one starts its coordination box
on Linode:

```sh
curl -fsS -X POST "$YSCALE_CENTRAL_ENDPOINT/v1/admin/tenants" \
  -H "Authorization: Bearer $YSCALE_ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"team_a","MaxConcurrentBursts":1,"MaxHourlyUSD":0.10,"workload_namespaces":["yscale-apps"]}' \
  > tenant.json && chmod 600 tenant.json
```

Wait about two minutes, until the factory reports the box ready:

```sh
curl -sS -H "Authorization: Bearer $FACTORY_BEARER_TOKEN" \
  http://127.0.0.1:8080/v1/tenants/team_a/fabric | jq '{status, login_server}'
```

Central attaches the box on its next poll. The connector and every burst node
get their join keys through central and the factory; central never holds the
box's admin key.

## 6. Install the connector

```sh
kubectl create namespace yscale-system
kubectl create namespace yscale-apps
jq -r .token tenant.json | tr -d '\n' > token
kubectl -n yscale-system create secret generic yscale-agent-credentials --from-file=YSCALE_TOKEN=token
rm token
```

`connector-values.yaml` (replace every placeholder):

```yaml
endpoint: https://central.example.com
existingSecret: yscale-agent-credentials
clusterID: my-cluster
cloudProvider: k3s
rbac:
  scope: namespaced
  allowedNamespaces: [yscale-apps]
agent:
  image:
    digest: sha256:<yscale-cluster-agent digest from the release's images.txt>
    requireDigest: true
bootstrap:
  apiserverURL: https://10.0.0.10:6443
gateway:
  enabled: true
  advertiseRoutes: ["10.42.0.0/16", "10.43.0.0/16", "10.0.0.0/24"]
```

Use your own pod, service and node CIDRs in `advertiseRoutes`. Then install the
chart from the source checkout:

```sh
helm upgrade --install yscale-agent ./deploy/helm/yscale-agent \
  -n yscale-system -f connector-values.yaml
kubectl -n yscale-system rollout status deployment/yscale-agent-yscale-agent
```

Central logs `agent connected` and `gateway node routes approved`. More options:
[Cluster Connector](cluster-connector.md) and [operator setup](self-hosted-mesh.md).

## 7. Run your first burst

```yaml
# first-burst.yaml
apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: first-burst
  namespace: yscale-apps
spec:
  image: docker.io/library/busybox:1.36
  backend: linode
  size: small
  command: ["/bin/sh", "-c"]
  args: ["echo running on $(hostname); sleep 60"]
  budget:
    maxUSD: 0.10
    deadline: 15m
```

```sh
kubectl apply -f first-burst.yaml
kubectl get nodes -w          # a ys-burst-* node joins after a few minutes
kubectl -n yscale-apps get pods -o wide
```

When you are done, delete the Workload. That removes the burst node, its
Linode instance and its mesh device:

```sh
kubectl -n yscale-apps delete workloads.yscale.sh first-burst
```

Deletion is asynchronous. Keep the connector and central running until the node
is gone, and check your Linode account afterwards; this release is a preview,
so confirm cleanup rather than assuming it. More: [your first workload](first-workload.md).

## 8. Clean up the trial

Offboarding the tenant deletes its coordination box:

```sh
curl -fsS -X DELETE "$YSCALE_CENTRAL_ENDPOINT/v1/admin/tenants/team_a" \
  -H "Authorization: Bearer $YSCALE_ADMIN_TOKEN"
helm -n yscale-system uninstall yscale-agent
```

The operations Headscale VM is yours to keep or delete.
