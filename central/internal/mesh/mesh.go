// Package mesh defines a provider-agnostic abstraction over the coordination
// server that hands out node auth keys and tracks devices for the burst mesh.
//
// The default (and only built-in) provider is Tailscale SaaS: yscale operates a
// single tailnet and mints ephemeral, tag-scoped auth keys against the Tailscale
// API. Nodes joining this mesh use Tailscale's hosted coordination server, so
// they need no --login-server override. *tailscale.Client implements Provider and
// its LoginServer() returns "" to signal "use the default Tailscale server".
//
// The Provider interface is deliberately provider-agnostic so an alternative
// coordination server can be plugged in behind it — one that returns a non-empty
// LoginServer() for joining nodes to point at. Central treats every provider
// identically: the same MintAuthKey / FindDeviceByHostname / DeleteDevice calls
// work against any of them, and the only behavioural difference a caller must
// honour is LoginServer() — empty means Tailscale SaaS, non-empty means pass it
// through to the node as the coordination-server URL.
package mesh

import (
	"context"
	"time"
)

// BoxProviderFunc builds a coordination-server Provider from a customer's
// self-hosted mesh endpoint (login-server URL, API key, user). It is the seam
// for plugging in an alternative, self-hosted coordination server: the default
// (OSS) build leaves it unset — every customer uses Tailscale SaaS — while an
// alternative build can register one so a customer with a mesh endpoint gets a
// provider pointed at their own server.
type BoxProviderFunc func(loginServer, apiKey, user string) Provider

// Provider is the coordination-server abstraction central uses to manage mesh
// membership. It is satisfied by *tailscale.Client (Tailscale SaaS) and by any
// alternative coordination-server client plugged in behind it.
type Provider interface {
	// MintAuthKey returns a fresh node auth/pre-auth key carrying the given ACL
	// tags and expiring after expiry. Implementations default to ephemeral keys
	// (see MintAuthKeyEphemeral) so that nodes self-remove from the mesh when
	// they disconnect.
	MintAuthKey(ctx context.Context, tags []string, expiry time.Duration) (string, error)

	// MintAuthKeyEphemeral returns a fresh node auth/pre-auth key carrying the
	// given ACL tags and expiring after expiry. When ephemeral is true the
	// resulting node is removed from the coordination server shortly after it
	// goes offline; when false the node persists until explicitly deleted.
	MintAuthKeyEphemeral(ctx context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error)

	// FindDeviceByHostname returns the coordination-server device ID for the node
	// registered under the given hostname, or "" if no such device exists. The
	// match is on the node's hostname/given-name as the coordination server knows
	// it (which may differ from the OS hostname after name munging).
	FindDeviceByHostname(ctx context.Context, hostname string) (string, error)

	// DeleteDevice removes the device with the given coordination-server device ID
	// from the mesh. It is idempotent: deleting an already-absent device is not an
	// error.
	DeleteDevice(ctx context.Context, deviceID string) error

	// LoginServer reports the coordination-server URL that joining nodes must be
	// pointed at. It returns "" for Tailscale SaaS (the default hosted server, so
	// no --login-server flag is needed) and a non-empty https URL for a self-hosted
	// coordination server.
	LoginServer() string
}
