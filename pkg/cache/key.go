// Package cache holds the shared primitives for yscale's storage
// caching layer: stable cache_key derivation, manifest format used by
// burst nodes to mark a cache populated, and retention parsing.
//
// Cross-binary by design: central derives keys, agent reads/writes
// manifests via burst, GC daemon parses retention strings. Keeping
// the contract here prevents drift between sites.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Key uniquely identifies a (tenant, source) cache. The same source
// referenced by two workloads of the same tenant resolves to the
// same Key — that's the whole point: shared caching across the
// tenant's workloads.
type Key string

// String for tenant-scoped logging without exposing the raw hash.
func (k Key) String() string { return string(k) }

// DeriveKey produces a deterministic, opaque key for a (tenant,
// bucket+prefix) pair. We hash to keep the key short and to avoid
// exposing the customer's bucket structure in volume names on the
// backend (Linode volumes etc. show up in their dashboards as our
// volume names — let's not leak customer paths).
//
// Resulting key is 16 hex chars from SHA-256 — collisions are
// statistically irrelevant at our scale and the prefix collision
// surface is per-tenant anyway.
func DeriveKey(tenantID, sourceURI string) Key {
	h := sha256.Sum256([]byte(tenantID + "::" + strings.TrimRight(sourceURI, "/")))
	return Key("ck_" + hex.EncodeToString(h[:8]))
}
