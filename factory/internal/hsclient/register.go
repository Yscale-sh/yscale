package hsclient

import (
	"context"
	"fmt"
	"strings"
)

// BoxInfo is the set of facts read back off a freshly-provisioned
// Headscale box (deploy/headscale/provision.sh + cloud-init.sh). It is
// the input to RegisterHeadscale — the write-side of the factory bridge.
//
//   - Hostname:  the box FQDN, e.g. "172-237-1-2.ip.linodeusercontent.com"
//     (cloud-init writes it to /var/lib/headscale/server-hostname)
//   - APIKey:    a Headscale API key with admin rights on that box
//     (cloud-init writes it to /var/lib/headscale/api-key); central uses
//     it as the bearer token for the v1 HTTP API
//   - User:      the customer's Headscale user/namespace (== HS_USER)
//   - BackendID: the provider VM id (Linode id), retained for teardown
//
// LoginServer is derived as "https://<Hostname>" — the value every burst/
// agent/gateway passes to `tailscale up --login-server`.
type BoxInfo struct {
	Hostname  string
	APIKey    string
	User      string
	BackendID string
}

// loginServerFor builds the https login-server URL from a box hostname,
// tolerating a hostname that already carries a scheme.
func loginServerFor(hostname string) string {
	h := strings.TrimSpace(hostname)
	if strings.HasPrefix(h, "http://") || strings.HasPrefix(h, "https://") {
		return strings.TrimRight(h, "/")
	}
	return "https://" + strings.TrimRight(h, "/")
}

// RegisterHeadscale wires a provisioned Headscale box to a customer: it
// validates the box is reachable + the API key works (EnsureUser, which is
// idempotent), then returns the BoxInfo turned into the {provider,
// loginServer, apiKey, user, backendID} tuple the caller persists via
// state.SetCustomerMesh. Splitting "verify" from "persist" keeps this
// package free of a state import; the caller (central) does the store
// write with the returned values.
//
// This is the ONLY new behavior the factory adds on top of the shipped
// full-mode networking: it does not touch how a burst joins, advertises
// routes, or runs cilium — it only records WHICH coordination server that
// customer's nodes point at.
func RegisterHeadscale(ctx context.Context, box BoxInfo) (loginServer, apiKey, user, backendID string, err error) {
	if box.Hostname == "" || box.APIKey == "" || box.User == "" {
		return "", "", "", "", fmt.Errorf("mesh: RegisterHeadscale requires Hostname, APIKey, and User")
	}
	login := loginServerFor(box.Hostname)
	hs := NewHeadscale(login, box.APIKey, box.User)

	// Idempotently ensure the customer's user/namespace exists. This also
	// proves the box is reachable over HTTPS and the API key is valid
	// BEFORE we persist the endpoint — so a bad box never gets recorded as
	// a customer's live mesh.
	if err := hs.EnsureUser(ctx); err != nil {
		return "", "", "", "", fmt.Errorf("mesh: validating headscale box %s: %w", login, err)
	}
	return login, box.APIKey, box.User, box.BackendID, nil
}
