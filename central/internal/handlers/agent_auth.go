package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/yscale-sh/yscale/central/internal/cost"
	"github.com/yscale-sh/yscale/central/internal/mesh"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/central/internal/tailscale"
)

// tsKeyExpiry is the TTL on auth keys central hands out. Keys are
// short-lived because `tailscale up` consumes them in seconds —
// anything beyond a few minutes is just leak surface.
const tsKeyExpiry = 30 * time.Minute

// clusterIDHeader is how a caller names WHICH of its clusters a request is
// about. A cluster token authenticates a tenant, not a cluster, so on a tenant
// running more than one connector every route that resolves "the customer's
// cluster" needs this to be told rather than guessed. Absent is still valid on
// a single-cluster tenant — which is every deployed one today.
const clusterIDHeader = "X-Cluster-ID"

const (
	AgentTailnetTag   = "tag:yscale"
	GatewayTailnetTag = "tag:yscale-gateway"
)

// deviceKindSpec maps a device "kind" to the hostname it should
// register as and the tailnet tag it claims. Kinds are an explicit
// allowlist — anything else returns an error from the handler with
// a 400. Add new kinds here when introducing new tailnet device
// roles (today: agent + gateway, both of which the customer install
// can deploy via the yscale-agent Helm chart).
//
// Why per-kind hostnames AND per-kind tags:
//   - Hostname: the stale-device delete only matches the exact hostname,
//     so calling mint for kind=gateway never disturbs the agent's
//     device. This bug bit us 2026-05-25 when the legacy mint hardcoded
//     `yscale-agent-${clusterID}` and a gateway stopgap call deleted
//     the running agent's tailnet identity.
//   - Tag: the tailnet ACL keys autoApprovers (routes) on tag. The
//     gateway needs tag:yscale-gateway so its --advertise-routes for
//     the customer pod CIDR get auto-approved without operator
//     intervention. Agent keeps tag:yscale for backcompat with the
//     existing tagOwners ACL.
//
// Adding a new kind requires (a) a row in this map, (b) tagOwners
// entry on the tailnet ACL, (c) any autoApprover entries if it
// advertises routes. See deploy/runbooks/tailnet-acl-gateway.md for
// the gateway case.
func deviceKindSpec(kind, clusterID string) (hostname, tag string, err error) {
	switch kind {
	case "agent":
		return fmt.Sprintf("yscale-agent-%s", clusterID), AgentTailnetTag, nil
	case "gateway":
		return GatewayHostname(clusterID), GatewayTailnetTag, nil
	default:
		return "", "", fmt.Errorf("unknown device kind %q (want agent or gateway)", kind)
	}
}

// GatewayHostname is the name a cluster's gateway registers under on the
// coordination server. Exported because the route reconciler has to resolve
// that exact node to converge its approved routes, and a second copy of the
// format string is a mismatch waiting to happen: the mint path here already
// learned once (2026-05-25) that an off-by-one hostname mutates the wrong
// device. deviceKindSpec builds the gateway's name from this function.
func GatewayHostname(clusterID string) string {
	return fmt.Sprintf("yscale-gateway-%s", clusterID)
}

// AgentAuth handles tailnet-device auth flows for components the
// customer install runs in their cluster: the agent itself, and the
// optional gateway sidecar that carries cross-cluster pod-to-pod
// traffic (see docs/architecture/cilium-gateway-sidecar.md).
//
// Why mint-per-boot rather than persist node state:
//   - One trust root (central's TS_OAUTH_*), one customer secret
//     (YSCALE_TOKEN). No tskey-auth-* in the customer cluster.
//   - Pod restart -> fresh key. Old key bounded by 30-min expiry,
//     blast radius bounded.
//   - Old device with same hostname is deleted before the new one
//     registers, so MagicDNS never resolves a `-2` suffix.
type AgentAuth struct {
	Store *state.Store
	TS    *tailscale.Client
	Log   *slog.Logger
	Cost  *cost.Meter // optional; nil = no metrics

	// BoxProvider constructs a provider for a customer's self-hosted mesh
	// endpoint. nil in the OSS build (Tailscale-only); wired by an alternative
	// build so providerFor can route box customers to their own coordination server.
	BoxProvider mesh.BoxProviderFunc
	// FabricProvider routes factory-provisioned tenants (no box key in
	// central) through the factory. Nil when no factory is configured.
	FabricProvider func(customerID, loginServer string) mesh.Provider

	// clusterSeen records the first customer to mint for a given clusterID so a
	// second tenant can't claim it during the pre-WS window — before that
	// customer's agent connects and Store.CustomerForClusterID becomes
	// authoritative. In-memory, best-effort; the store is the source of truth
	// once an agent is connected. clusterID -> customerID.
	clusterSeen sync.Map
}

// clusterOwner reports which customer owns clusterID, if known. A connected
// agent is authoritative; the in-memory first-seen map covers the pre-WS window.
func (h *AgentAuth) clusterOwner(clusterID string) (string, bool) {
	if owner, ok := h.Store.CustomerForClusterID(clusterID); ok {
		return owner, true
	}
	if v, ok := h.clusterSeen.Load(clusterID); ok {
		return v.(string), true
	}
	return "", false
}

// providerFor returns the coordination-server provider to mint against
// for a customer. The DEFAULT is the shared Tailscale SaaS client (h.TS),
// so any customer without a self-hosted mesh endpoint keeps the exact same
// behavior. A customer that has opted into their own coordination box gets a
// provider pointed at it — but only when a box-provider constructor is wired
// (h.BoxProvider); the OSS build leaves it unset (Tailscale-only).
//
// Returns a true interface nil (not a typed-nil *tailscale.Client) when
// the default client is unconfigured, so the caller's `prov == nil` guard
// is reliable.
func (h *AgentAuth) providerFor(cust *state.Customer) mesh.Provider {
	if cust != nil && cust.Mesh != nil && cust.Mesh.Provider == state.MeshProviderFactory {
		if h.FabricProvider == nil {
			return nil
		}
		return h.FabricProvider(cust.ID, cust.Mesh.LoginServer)
	}
	if cust != nil && cust.Mesh != nil && h.BoxProvider != nil {
		return h.BoxProvider(cust.Mesh.LoginServer, cust.Mesh.APIKey, cust.Mesh.User)
	}
	if h.TS == nil {
		return nil
	}
	return h.TS
}

// MintAuthKeyRequest is the optional JSON body. Empty body / missing
// fields are tolerated — `kind` defaults to "agent" for backcompat
// with the existing agent init container which posts an empty body.
type MintAuthKeyRequest struct {
	// Kind selects which device shape the key authorizes. One of
	// "agent" (default) or "gateway". See deviceKindSpec for the
	// hostname/tag derived from each.
	Kind string `json:"kind,omitempty"`
}

// MintAuthKeyResponse is the JSON body returned. `AuthKey` is
// `tskey-auth-...`; the caller's TS sidecar consumes it via
// `--auth-key=file:...` and discards.
type MintAuthKeyResponse struct {
	AuthKey   string `json:"auth_key"`
	Hostname  string `json:"hostname"`   // what the device will register as
	Tag       string `json:"tag"`        // what tag the key authorizes (lets the caller verify)
	ExpiresIn int64  `json:"expires_in"` // seconds; caller can log for debug
	// LoginServer is the coordination-server URL the device must point at
	// via --login-server. Empty for the default Tailscale SaaS path (so
	// the response is byte-for-byte unchanged for SaaS customers); the
	// self-hosted box URL when that customer has their own coordination box.
	LoginServer string `json:"login_server,omitempty"`
}

// MintKey handles POST /v1/agent/ts-auth-key.
//
// Flow:
//  1. YSCALE_TOKEN auth (middleware did this).
//  2. Parse optional JSON body for `kind` (defaults to "agent").
//  3. Resolve clusterID from X-Cluster-ID header or the connected agent.
//  4. Compute hostname + tag via deviceKindSpec.
//  5. Best-effort delete any existing tailnet device with that exact
//     hostname (so MagicDNS resolves THIS device, not a `-2` suffix).
//  6. Mint a fresh auth key tagged for this kind.
//  7. Return JSON.
func (h *AgentAuth) MintKey(w http.ResponseWriter, r *http.Request) {
	cust, err := CustomerFromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Select the mesh provider for this customer: their own coordination box
	// if configured, else the shared Tailscale SaaS client. Default path
	// is unchanged for any customer with Mesh==nil.
	primary := h.providerFor(cust)
	if primary == nil {
		// Misconfiguration on central, not the customer's fault.
		http.Error(w, "mesh coordination server not configured on central", http.StatusServiceUnavailable)
		return
	}
	// Optional body. The legacy agent init container posts with no body
	// or an empty `{}`; either is fine and defaults kind=agent.
	// json.Decoder returns io.EOF on a truly empty body — treat that as
	// "default everything." Any other decode error is a real 400.
	var req MintAuthKeyRequest
	if r.Body != nil {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, fmt.Sprintf("invalid JSON body: %v", err), http.StatusBadRequest)
			return
		}
	}
	if req.Kind == "" {
		req.Kind = "agent"
	}

	// clusterID: prefer the header (init container can send it before
	// the agent's WS is up); fall back to the connected agent record.
	//
	// The fallback only holds while the tenant has ONE cluster. With two
	// connected, "the customer's cluster" picks a hostname and a stale-device
	// sweep for the wrong one — this route deletes the tailnet device named
	// yscale-<kind>-<clusterID>, so a guess here unregisters a running cluster's
	// MagicDNS name. Naming the cluster is the caller's job then; the ownership
	// rules for a caller who DOES name one are unchanged below.
	clusterID := r.Header.Get(clusterIDHeader)
	// A cluster-scoped connector credential is bound to exactly one cluster,
	// and ConnectorAuth already refused a header naming any other — so the
	// binding is the answer, header or no header, and the single-connector
	// guesswork below is only ever run for the legacy tenant token.
	bound := AgentClusterFromContext(r.Context())
	if bound != "" {
		clusterID = bound
	}
	if clusterID == "" {
		agent, err := h.Store.AgentForCustomer(cust.ID)
		switch {
		case err == nil:
			clusterID = agent.ClusterID
		case errors.Is(err, state.ErrAmbiguousCluster):
			http.Error(w, "X-Cluster-ID header required: this account has connected agents for more than one cluster", http.StatusBadRequest)
			return
		}
	}
	if clusterID == "" {
		http.Error(w, "X-Cluster-ID header required when no agent is connected yet", http.StatusBadRequest)
		return
	}

	// Bind the (caller-supplied) clusterID to the authenticated customer. Without
	// this, tenant A could pass tenant B's clusterID and — on the shared SaaS
	// tailnet — delete B's agent/gateway device (the mint's stale-device sweep
	// keys on hostname yscale-<kind>-<clusterID>) and re-register B's MagicDNS
	// name onto attacker hardware, so B's bursts fetch bootstrap from the
	// attacker. Reject a clusterID owned by another tenant; remember first use to
	// cover the pre-WS window (deviceKindSpec's hostname is per-clusterID).
	if owner, known := h.clusterOwner(clusterID); known && owner != cust.ID {
		h.Log.Warn("rejected cross-tenant cluster id in ts-auth-key mint",
			"customer", cust.ID, "cluster", clusterID, "owner", owner, "kind", req.Kind)
		http.Error(w, "cluster id is registered to another tenant", http.StatusForbidden)
		return
	}
	if bound == "" && h.Store.HostedClusterRequiresScopedCredential(cust.ID, clusterID) {
		h.Log.Warn("rejected tenant token for platform-managed hosted cluster mint",
			"customer", cust.ID, "cluster", clusterID, "kind", req.Kind)
		http.Error(w, "platform-managed hosted cluster requires its connector credential", http.StatusForbidden)
		return
	}
	h.clusterSeen.LoadOrStore(clusterID, cust.ID)

	hostname, tag, err := deviceKindSpec(req.Kind, clusterID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	// Both kinds this endpoint mints — agent and gateway — are LONG-LIVED
	// in-cluster Deployments, NOT per-workload throwaways. They run inside
	// pod-network where DERP-over-UDP is intermittently blocked (observed
	// 2026-05-25 on homelab). The coordination server GCs ephemeral devices
	// that go offline for >5min; on a pod restart (or a brief DERP blip) the old
	// ephemeral node is deleted and the NEW pod's tailscaled gets stuck
	// polling it with "PollNetMap: 404: node not found", never re-registering.
	// This bit the gateway 2026-05-25 and the AGENT against a self-hosted
	// coordination box 2026-05-31 (the agent's ephemeral node was GC'd, so
	// bursts could no longer reach its bootstrap endpoint and never joined).
	// Non-ephemeral keeps the device entry alive across restarts/blips; the
	// stale-device delete below handles cleanup on the next pod start (bounded
	// by the exact-hostname match, so it never disturbs the other kind).
	//
	// Bursts are NOT minted here — they get keys via the decider and STAY
	// ephemeral (created+destroyed per workload; stale entries would otherwise
	// accumulate). So every key this handler mints is non-ephemeral.
	const ephemeral = false

	// mintOn runs the full mint against ONE provider: a best-effort delete of
	// any stale device with this EXACT hostname (so MagicDNS resolves THIS
	// device, not a `-2` suffix), then the key mint. Bounded scope: only
	// matches this kind's hostname, never another kind's — so kind=gateway can
	// never disturb the agent's device, and vice versa. Running it inside the
	// mint closure means a box→Tailscale fallback cleans up and mints on
	// the provider it actually lands on.
	mintOn := func(p mesh.Provider) (string, error) {
		if dev, err := p.FindDeviceByHostname(ctx, hostname); err == nil && dev != "" {
			if err := p.DeleteDevice(ctx, dev); err != nil {
				h.Log.Warn("stale tailnet device delete failed (continuing)",
					"hostname", hostname, "device", dev, "error", err)
			} else {
				h.Log.Info("deleted stale tailnet device before re-mint",
					"hostname", hostname, "device", dev)
			}
		}
		return p.MintAuthKeyEphemeral(ctx, []string{tag}, tsKeyExpiry, ephemeral)
	}

	// Fail closed (#58): mint ONLY on the tenant's own mesh; no shared-tailnet
	// fallback on a box-mint failure. The mint is bounded by the request timeout
	// above, and the customer's mesh ref is left intact (teardown keys device
	// deletion on it; dead-box demotion is owned by startup box-validation).
	key, err := mintOn(primary)
	if err != nil {
		h.Cost.RecordMeshMint("unknown", "fail")
		h.Log.Error("mint ts auth key", "customer", cust.ID, "cluster", clusterID,
			"kind", req.Kind, "error", err)
		http.Error(w, "mint failed", http.StatusBadGateway)
		return
	}
	usedProv := primary
	mintProvider := "tailscale"
	if usedProv.LoginServer() != "" {
		mintProvider = "box"
	}
	h.Cost.RecordMeshMint(mintProvider, "ok")

	h.Log.Info("minted ts auth key",
		"customer", cust.ID, "cluster", clusterID, "kind", req.Kind,
		"hostname", hostname, "tag", tag,
		"expires_in_sec", int64(tsKeyExpiry.Seconds()))

	writeJSON(w, http.StatusOK, MintAuthKeyResponse{
		AuthKey:     key,
		Hostname:    hostname,
		Tag:         tag,
		ExpiresIn:   int64(tsKeyExpiry.Seconds()),
		LoginServer: usedProv.LoginServer(),
	})
}
