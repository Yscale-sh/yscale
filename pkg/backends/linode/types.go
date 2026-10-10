package linode

import (
	"fmt"
	"strings"
	"time"
)

// linodeTime parses Linode API timestamps, which are RFC3339-ish but
// omit the timezone (e.g. "2026-05-15T13:38:42") and are implicitly UTC.
type linodeTime struct{ time.Time }

func (t *linodeTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05", time.RFC3339} {
		if parsed, err := time.Parse(layout, s); err == nil {
			t.Time = parsed
			return nil
		}
	}
	return fmt.Errorf("linode: unrecognized time %q", s)
}

// Field names track the Linode API v4 (techdocs.akamai.com/linode-api/reference).

// Instance is a Linode compute instance.
type Instance struct {
	ID     int    `json:"id"`
	Label  string `json:"label"`
	Region string `json:"region"`
	Type   string `json:"type"`
	Image  string `json:"image,omitempty"`
	// Status: provisioning, booting, running, offline, shutting_down,
	// rebooting, deleting, migrating, ...
	Status  string     `json:"status"`
	IPv4    []string   `json:"ipv4,omitempty"`
	Created linodeTime `json:"created"`
	Tags    []string   `json:"tags,omitempty"`
}

// Volume is a Block Storage volume row from /volumes. yscale uses it to
// resolve a pre-seeded model volume's id from its label before attaching.
type Volume struct {
	ID       int    `json:"id"`
	Label    string `json:"label"`
	Region   string `json:"region"`
	Status   string `json:"status"`
	LinodeID int    `json:"linode_id,omitempty"`
}

// InstanceMetadata carries cloud-init user-data. Linode's Metadata
// service runs it on first boot of images that support cloud-init.
type InstanceMetadata struct {
	UserData string `json:"user_data"` // base64-encoded cloud-init user-data
}

// CreateInstanceRequest provisions a new instance. Metadata.UserData
// carries the base64 cloud-init script the burst bootstraps from.
type CreateInstanceRequest struct {
	Label    string   `json:"label"`
	Region   string   `json:"region"`
	Type     string   `json:"type"`
	Image    string   `json:"image"`
	RootPass string   `json:"root_pass"`
	Booted   bool     `json:"booted"`
	Tags     []string `json:"tags,omitempty"`
	// AuthorizedKeys are SSH public keys added to root for burst
	// operability/debugging (the bootstrap log lives on the instance).
	AuthorizedKeys []string          `json:"authorized_keys,omitempty"`
	Metadata       *InstanceMetadata `json:"metadata,omitempty"`
	// FirewallID attaches a Linode Cloud Firewall at creation so the
	// instance is protected from first boot (no race window). 0 = none.
	FirewallID int `json:"firewall_id,omitempty"`
}

// Firewall is a Linode Cloud Firewall (network-edge stateful firewall,
// applied independent of the host so a compromised guest can't disable it).
type Firewall struct {
	ID    int           `json:"id"`
	Label string        `json:"label"`
	Rules FirewallRules `json:"rules"`
}

// FirewallRules is the inbound/outbound ruleset plus default policies.
// Policies are "ACCEPT" or "DROP" and apply to anything not matched.
type FirewallRules struct {
	Inbound        []FirewallRule `json:"inbound"`
	InboundPolicy  string         `json:"inbound_policy"`
	Outbound       []FirewallRule `json:"outbound"`
	OutboundPolicy string         `json:"outbound_policy"`
}

// FirewallRule is one allow/deny rule. Ports is a string like "22" or
// "80,443"; Addresses scopes the rule to source/dest CIDRs.
type FirewallRule struct {
	Label     string        `json:"label"`
	Action    string        `json:"action"`   // ACCEPT | DROP
	Protocol  string        `json:"protocol"` // TCP | UDP | ICMP
	Ports     string        `json:"ports,omitempty"`
	Addresses FirewallAddrs `json:"addresses"`
}

// FirewallAddrs scopes a rule. Empty == nowhere; use 0.0.0.0/0 + ::/0 for any.
type FirewallAddrs struct {
	IPv4 []string `json:"ipv4,omitempty"`
	IPv6 []string `json:"ipv6,omitempty"`
}

// CreateFirewallRequest creates a Cloud Firewall with rules (and optionally
// pre-attached devices, which we skip — we attach via firewall_id at instance
// create instead so there's no unprotected window).
type CreateFirewallRequest struct {
	Label string        `json:"label"`
	Rules FirewallRules `json:"rules"`
	Tags  []string      `json:"tags,omitempty"`
}

// Region is a Linode datacenter. Capabilities lists the features the
// region supports — "Metadata" must be present for cloud-init user-data
// (and therefore the burst bootstrap) to run.
type Region struct {
	ID           string   `json:"id"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type Account struct {
	UUID string `json:"uuid"`
}

// RegionAvailability is one row of /regions/availability: whether a
// given plan type can currently be deployed in a given region.
type RegionAvailability struct {
	Region    string `json:"region"`
	Plan      string `json:"plan"`
	Available bool   `json:"available"`
}

// InstanceConfig is a Linode boot config profile. yscale only needs the
// id (to PUT the kernel onto it); Kernel is carried for completeness.
type InstanceConfig struct {
	ID     int    `json:"id"`
	Kernel string `json:"kernel,omitempty"`
}

// image is one row of /images. yscale only reads the two fields it needs to
// verify a configured burst image is deploy-ready before the paid create:
// the exact id (guards against a mismatched or truncated response) and the
// status ("available" is the only deployable state; "pending_upload",
// "creating", "deleted" are not). The regions array is deliberately not
// consumed — Akamai's second-generation images deploy to any compatible
// region regardless of where a stored replica lives, so filtering on it
// would strand valid deployments.
type image struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// listResponse is the envelope Linode wraps paginated GET collections in.
type listResponse[T any] struct {
	Data    []T `json:"data"`
	Page    int `json:"page"`
	Pages   int `json:"pages"`
	Results int `json:"results"`
}
