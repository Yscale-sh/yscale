package routecidr

import (
	"fmt"
	"net/netip"
	"sort"
	"strings"
)

const (
	// BurstPodCIDRPool is reserved for central-authored burst routing.
	BurstPodCIDRPool = "10.244.0.0/16"

	// MaxReportedCIDRs bounds ONE cluster's reported set. It is not on its own
	// the bound on a tenant's coordination-server policy: that document is the
	// UNION of every cluster's stored set, so its ceiling is (clusters the
	// tenant holds) x MaxReportedCIDRs. What makes the first factor finite is
	// that a stored set may only belong to a cluster with a durable registry row
	// (state.MaxRegisteredClusters) or a live socket on this replica, and the
	// entries of clusters with neither are reaped at the registry cap, and — after
	// a grace period the reconnect cancels — at disconnect and at boot; see
	// state.ReapUnheldClusterGatewayRoutes.
	MaxReportedCIDRs = 32
)

var (
	privateCIDRAllowlist = []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.168.0.0/16"),
		netip.MustParsePrefix("100.64.0.0/10"),
		netip.MustParsePrefix("fc00::/7"),
	}
	reservedCIDRs = []netip.Prefix{
		netip.MustParsePrefix(BurstPodCIDRPool),
	}
)

// CleanPolicyCIDRs parses, masks, and deduplicates CIDR strings while
// preserving first-seen order for existing env-policy behavior.
func CleanPolicyCIDRs(values []string) (routes []string, invalid []string) {
	return canonicalizeCIDRs(values, false)
}

// CanonicalizeCIDRs parses, masks, deduplicates, and sorts CIDR strings.
func CanonicalizeCIDRs(values []string) (routes []string, invalid []string) {
	return canonicalizeCIDRs(values, true)
}

func canonicalizeCIDRs(values []string, sorted bool) (routes []string, invalid []string) {
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, cidr := range strings.Split(value, ",") {
			cidr = strings.TrimSpace(cidr)
			if cidr == "" {
				continue
			}
			prefix, err := netip.ParsePrefix(cidr)
			if err != nil {
				invalid = append(invalid, cidr)
				continue
			}
			canonical := prefix.Masked().String()
			if _, ok := seen[canonical]; ok {
				continue
			}
			seen[canonical] = struct{}{}
			routes = append(routes, canonical)
		}
	}
	if sorted {
		sort.Strings(routes)
	}
	return routes, invalid
}

// ValidateReportedCIDRs validates untrusted agent-reported gateway CIDRs.
func ValidateReportedCIDRs(in []string) (masked []string, err error) {
	return validateReportedCIDRs(in)
}

func validateReportedCIDRs(in []string) (masked []string, err error) {
	seen := map[string]struct{}{}
	for _, value := range in {
		for _, cidr := range strings.Split(value, ",") {
			cidr = strings.TrimSpace(cidr)
			if cidr == "" {
				continue
			}
			prefix, err := netip.ParsePrefix(cidr)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR %q: %w", cidr, err)
			}
			prefix = prefix.Masked()
			if prefix.Bits() == 0 {
				return nil, fmt.Errorf("default route %q is not allowed", prefix.String())
			}
			if prefix.Addr().Is4() && prefix.Bits() < 8 {
				return nil, fmt.Errorf("CIDR %q is broader than /8", prefix.String())
			}
			if !withinPrivateCIDR(prefix) {
				return nil, fmt.Errorf("CIDR %q is outside allowed private space", prefix.String())
			}
			for _, reserved := range reservedCIDRs {
				if prefix.Overlaps(reserved) {
					return nil, fmt.Errorf("CIDR %q overlaps reserved route %q", prefix.String(), reserved.String())
				}
			}
			canonical := prefix.String()
			if _, ok := seen[canonical]; ok {
				continue
			}
			seen[canonical] = struct{}{}
			masked = append(masked, canonical)
			if len(masked) > MaxReportedCIDRs {
				return nil, fmt.Errorf("reported CIDR count %d exceeds limit %d", len(masked), MaxReportedCIDRs)
			}
		}
	}
	sort.Strings(masked)
	return masked, nil
}

func withinPrivateCIDR(prefix netip.Prefix) bool {
	for _, allowed := range privateCIDRAllowlist {
		if prefix.Addr().Is4() != allowed.Addr().Is4() {
			continue
		}
		if prefix.Bits() < allowed.Bits() {
			continue
		}
		if allowed.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}
