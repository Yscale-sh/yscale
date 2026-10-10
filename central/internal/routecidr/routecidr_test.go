package routecidr_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/routecidr"
	"github.com/yscale-sh/yscale/central/internal/state"
)

func TestValidateReportedCIDRsRejectsWholeReport(t *testing.T) {
	overCap := make([]string, 0, routecidr.MaxReportedCIDRs+1)
	for i := 0; i <= routecidr.MaxReportedCIDRs; i++ {
		overCap = append(overCap, fmt.Sprintf("10.200.%d.0/24", i))
	}

	tests := []struct {
		name   string
		routes []string
	}{
		{name: "ipv4 default", routes: []string{"0.0.0.0/0"}},
		{name: "ipv6 default", routes: []string{"::/0"}},
		{name: "public", routes: []string{"8.8.8.0/24"}},
		{name: "broader than v4 minimum", routes: []string{"10.0.0.0/7"}},
		{name: "burst pool overlap", routes: []string{"10.244.1.0/24"}},
		{name: "over cap", routes: overCap},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := routecidr.ValidateReportedCIDRs(tt.routes); err == nil {
				t.Fatalf("ValidateReportedCIDRs(%v) = %v, want error", tt.routes, got)
			}
		})
	}
}

func TestValidateReportedCIDRsAcceptsPrivateMaskedSet(t *testing.T) {
	got, err := routecidr.ValidateReportedCIDRs([]string{
		"10.96.0.10/12",
		"10.42.1.1/16",
		"192.168.10.99/24",
		"100.64.1.2/24",
		"fc00::1/64",
		"10.42.0.0/16",
	})
	if err != nil {
		t.Fatalf("ValidateReportedCIDRs: %v", err)
	}
	want := []string{
		"10.42.0.0/16",
		"10.96.0.0/12",
		"100.64.1.0/24",
		"192.168.10.0/24",
		"fc00::/64",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("masked = %v, want %v", got, want)
	}
}

func TestRejectedReportLeavesStoredGatewayRoutesIntact(t *testing.T) {
	s := state.New()
	// The agent stream claims its cluster before it will accept a report, and
	// the store requires that claim to still stand.
	if _, _, err := s.ClaimAgentCluster("cust_test", "prod-1", false, time.Now().UTC()); err != nil {
		t.Fatalf("ClaimAgentCluster: %v", err)
	}
	changed, err := s.SetCustomerGatewayRoutes("cust_test", "prod-1", []string{"10.42.1.1/16"})
	if err != nil || !changed {
		t.Fatalf("SetCustomerGatewayRoutes changed=%v err=%v", changed, err)
	}

	if _, err := routecidr.ValidateReportedCIDRs([]string{"8.8.8.0/24"}); err == nil {
		t.Fatal("expected public route to be rejected")
	}

	c, err := s.CustomerByID("cust_test")
	if err != nil {
		t.Fatalf("CustomerByID: %v", err)
	}
	want := []string{"10.42.0.0/16"}
	if !reflect.DeepEqual(c.GatewayRoutes, want) {
		t.Fatalf("GatewayRoutes = %v, want %v", c.GatewayRoutes, want)
	}
}
