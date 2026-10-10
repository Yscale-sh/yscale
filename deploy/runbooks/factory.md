# Factory deployment

The factory provisions and removes a dedicated Headscale coordination box per tenant. It alone holds each box's admin key and the key-encryption key (KEK). It runs alongside central (`yscale-cloud`), with its own namespace, Secret and Postgres database.

You operate the factory. The real factory currently requires Linode for persistent coordination boxes, regardless of worker provider. `FACTORY_OPS_LOGIN_SERVER` is mandatory; there is no hosted fallback.

Start with the [self-hosted mesh guide](../../docs/self-hosted-mesh.md). Build images for your registry and configure your secrets and database before deployment; the examples below are not production-ready configuration.

## Trust boundaries (why two listeners)

| Listener | Port | Reachable from | Auth |
|---|---|---|---|
| Fabric API | 8080 | central's pods only (ClusterIP + NetworkPolicy) | bearer `FACTORY_BEARER_TOKEN` |
| Ops handoff | 8081 | boxes, over the **ops tailnet** only | single-use handoff token + ops-tailnet membership |

The ops listener receives a box's freshly-generated Headscale admin key. It is
NOT fronted by a Service and must never be reachable from central, bursts, or a
customer tailnet — only over the private ops tailnet.

## Prerequisites (one-time)

1. **Image** — cross-compile + push:
   ```
   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/yscale-factory-linux-amd64 ./factory/cmd/yscale-factory
   docker build -f factory/build/Dockerfile.factory -t <your-registry>/yscale-factory:<version> .
   docker push <your-registry>/yscale-factory:<version>
   ```
2. **Postgres database** — create a database SEPARATE from central's on the
   dev-datastores instance (`deploy/cloud/dev-datastores.yaml`); the factory owns
   its `boxes`/`jobs`/`policy_state`/`audit` schema (auto-created on boot).
3. **Box-scoped Linode token** — a Linode API token scoped to **Linodes +
   Cloud Firewalls RW only** (a blast-radius reduction vs central's burst token).
4. **KEK** — a base64-encoded 32-byte key: `head -c 32 /dev/urandom | base64`.
   Losing it makes every box admin key unrecoverable — back it up out-of-band.
5. **Ops Headscale** — run a dedicated self-hosted coordinator (separate from
   every customer mesh) and mint an **ephemeral** pre-auth key for boxes. Set
   `FACTORY_OPS_LOGIN_SERVER` to its HTTPS origin. Join the factory pod to the
   same coordinator via an ops client (stubbed in `deploy/cloud/factory.yaml`).
   Neither operators nor workload authors need a Tailscale account.

### Self-hosted ops setup

This is a one-time operator task, not something each workload author installs.
Do it **before** enabling real factory provisioning:

1. Install a separate Headscale server with durable state and valid HTTPS,
   reachable by your factory client and new coordination boxes. Use the
   [Headscale installation guide](https://headscale.net/stable/setup/install/)
   and configuration matching your chosen version; the tenant bootstrap in
   this repository pins 0.26.1. Do not reuse a tenant's server, database or keys.
   Its public coordination endpoint is not the factory's private handoff API.
2. Create separate Headscale users for the factory client and coordination
   boxes. On Headscale 0.26.1, `headscale users create yscale-ops-factory` and
   `headscale users create yscale-ops-boxes` return numeric user IDs. Mint a
   one-use, non-ephemeral factory key and a short-lived box enrollment key.
   For the latter, use `headscale preauthkeys create --user <BOX_USER_ID>
   --reusable --ephemeral --expiration 24h` (one command). Store the resulting
   key directly in your secret manager as `FACTORY_OPS_AUTHKEY`; never commit
   it or paste it into logs. Rotate it before expiry, or use a fresh one-use key
   for each provisioning. Ephemeral membership is a **key setting**, not a
   `tailscale up --ephemeral` flag.
3. Join the factory's private ops client with `tailscale up
   --login-server="$FACTORY_OPS_LOGIN_SERVER" --auth-key="$FACTORY_CLIENT_KEY"
   --accept-dns=false` (one command). The factory's client key is separate from
   the boxes' key; it is not another credential required by `yscale-factory`.
   Supply it privately to your chosen sidecar/host client, then remove the
   enrollment value from the shell environment. The deployment sidecar remains
   an operator integration, not a turnkey installer.
4. Configure ops ACLs to allow only coordination boxes to reach the factory's
   ops-mesh address on TCP 8081. Deny box-to-box access and do not join workload
   nodes, central, or personal machines to this mesh. Configure your own DERP
   relay if direct peer connectivity is unavailable; do not rely on a hosted
   Tailscale relay map. Do not copy the tenant full-network ACL into this mesh.
5. Set `FACTORY_OPS_LISTEN` to the local private ops interface (or loopback
   behind a private proxy), and `FACTORY_OPS_URL` to its box-reachable address.
   Verify both permitted and denied paths before creating a tenant. The factory
   now requires `FACTORY_OPS_LOGIN_SERVER` on every production startup; existing
   installations must configure and verify their ops network before upgrading.
   This setting does not migrate already-running boxes to another coordinator.

The box bootstrap passes `--login-server` explicitly and never silently selects
Tailscale's hosted coordination. Tailscale **client software** still runs on
these infrastructure nodes; no Tailscale signup, subscription, OAuth client or
app on a workload author's laptop is required. A fresh real-provider join still
needs qualification; local regression tests do not prove your network routing.

## Environment variables

These settings belong to the **factory process**, not a user's workload. They
describe the real managed-mesh path; `FACTORY_DEV=1` is only a fake, in-memory
test worker. Start from [the empty factory environment example](../../.env.factory.example).

### Required for the real factory

Supply these through your secret manager or the factory's dedicated
`yscale-factory-secrets` Secret. All seven must be nonempty; the KEK also has
format validation.

| Variable | Purpose and source | Secret? |
|---|---|---|
| `FACTORY_BEARER_TOKEN` | Generate a dedicated random token for central-to-factory requests. Set the identical value on central; do not reuse an admin or tenant token. | Yes |
| `FACTORY_KEK` | Base64-encoded **32-byte** encryption key for stored box admin keys. Generate once and back it up separately from the database; do not regenerate at every boot. | Yes |
| `DATABASE_URL` | Factory's own PostgreSQL connection string and role, separate from central's database. The factory creates its schema on startup. | Yes |
| `LINODE_TOKEN` | Operator's box-scoped Linode API token with Linodes and Cloud Firewalls read/write permissions. Separate from central's burst token. | Yes |
| `FACTORY_OPS_AUTHKEY` | Pre-auth key from the operator's **self-hosted ops Headscale**, used to enroll newly provisioned coordination boxes. Keep it valid for each provisioning; replace a consumed single-use key before another box is created. | Yes |
| `FACTORY_OPS_LOGIN_SERVER` | HTTPS origin of the separate ops Headscale, e.g. `https://ops-headscale.example.invalid`. Replace the placeholder with your reachable, TLS-valid server. Required; no hosted-coordinator default. Not the tenant Headscale URL. | No |
| `FACTORY_OPS_URL` | Private base URL where boxes reach the factory's ops listener, e.g. `http://factory-ops.example.invalid:8081`. Bootstrap appends `/ops/register`; do not include that suffix. Replace the example hostname with your real ops-mesh address. | No |

For keys, generate independent random values on a trusted operator machine,
then save them directly into the secret manager. Never paste populated
environment files, provider user-data, or key-handoff payloads into logs or
support reports. Do not commit populated `.env` files.

### Listener settings and test-only mode

| Variable | Code default | Operator action |
|---|---|---|
| `FACTORY_LISTEN` | `:8080` | Bind the fabric API on the private central-facing network; protect it with network policy/firewalls as well as the bearer. |
| `FACTORY_OPS_LISTEN` | `:8081` | Override this wildcard default with the **local private ops-interface address** and port. The example uses `127.0.0.1:8081` to fail closed until you wire the private path. |
| `FACTORY_DEV` | Unset: real factory | Leave unset for real provisioning. The exact value `1` selects the fake worker; it is not an account-free production mode. |

`FACTORY_OPS_LISTEN` is a local bind address, **without** an HTTP scheme.
`FACTORY_OPS_URL` is the reachable callback URL from a newly created box.
Loopback on the factory is not reachable from a box unless an explicitly
configured private proxy forwards to it. These variables do not create an
interface, join a tailnet, configure DNS, or install that proxy for you.

The ops sidecar in [the deployment template](../cloud/factory.yaml) is a stub.
Provision its private connectivity and client credentials separately.
`FACTORY_OPS_AUTHKEY` enrolls the **boxes**; merely setting it on the factory
does not connect the factory pod to the ops network. Never expose the ops
listener through a public ingress or to workload tenants.

### Matching settings on central

Central's managed-mesh integration needs:

| Variable | Value |
|---|---|
| `FACTORY_URL` | Private **fabric API** URL, e.g. `http://yscale-factory.yscale-factory:8080`, not the ops callback URL. |
| `FACTORY_BEARER_TOKEN` | Same dedicated bearer as the factory. |

This is only the central-to-factory pairing, not central's complete environment.
Keep the two processes' environments separate: both use `DATABASE_URL` and
`LINODE_TOKEN`. Do not reuse central's database credentials or broader
burst-provider token for the factory. Central does not need `FACTORY_KEK` or
`FACTORY_OPS_AUTHKEY`.

### Tailscale account boundary

The workload mesh uses each tenant's self-hosted Headscale. The factory's ops
mesh uses the separate coordinator selected by `FACTORY_OPS_LOGIN_SERVER`.
Neither needs a Tailscale account. [Box bootstrap](../headscale/cloud-init.sh)
always supplies the ops `--login-server` explicitly. The connector's tenant
login-server settings do not configure this separate ops client.

Factory-generated bootstrap variables are not additional operator settings:

| Generated variable | Source |
|---|---|
| `HS_OPS_AUTHKEY` | Copy of `FACTORY_OPS_AUTHKEY` injected into box user-data. |
| `HS_OPS_LOGIN_SERVER` | Copy of `FACTORY_OPS_LOGIN_SERVER` injected into box user-data. |
| `HS_OPS_FACTORY_URL` | Copy of `FACTORY_OPS_URL` injected into box user-data. |
| `HS_OPS_TOKEN` | Per-registration, single-use handoff token generated by the factory. Not the shared fabric bearer. |

See [factory startup](../../factory/cmd/yscale-factory/main.go),
[bootstrap environment assembly](../../factory/internal/boxes/linode.go), and
[KEK validation](../../factory/internal/store/store.go) for the configuration
contract. Documentation does not remove the need to qualify your private
network and provider configuration.

## Deploy

Manifests are reconciled by Flux like everything else — commit
`deploy/cloud/factory.yaml`; do **not** `kubectl apply`. Central reaches the
fabric API at `http://yscale-factory.yscale-factory:8080` (set that as central's
`FACTORY_URL` + share `FACTORY_BEARER_TOKEN`).

## Verify

- Pod `Running`, logs show `yscale-factory PROD — durable store + Linode worker`.
  If it logs `FACTORY_DEV=1 to run the in-memory dev mode` or `... is required`,
  a Secret key is missing — the factory fails closed on partial config by design.
- `GET /v1/tenants/<id>/fabric` via central returns a status; no `api_key` ever appears.
- After a restart the boot log shows durable state re-hydrated (no re-provision).

## Operations

- **Onboard**: central `Provision` → async `POST fabric`; the box provisions
  (~3-5 min), joins the ops tailnet, hands its key back, and reports `ready`.
  Central returns a typed `503 fabric_provisioning` + `Retry-After` until then.
- **Offboard**: central `Offboard` → `DELETE fabric` (unconditional, idempotent);
  the box drains then is tag-verified-deleted. Never manual `deprovision.sh` now.
- **Lost KEK / lost box key**: not recoverable by rebuild — the fix is box
  replacement (provision new → repoint tenant → decommission old), not recovery.
- **Compromise posture**: factory down = no new fabrics/mints, running bursts
  unaffected. Factory compromised = box keys + KEK + box Linode token exposed
  (deliberate crown-jewel concentration off the internet-facing surface); mitigate
  with token scoping + rotation + the audit table.
