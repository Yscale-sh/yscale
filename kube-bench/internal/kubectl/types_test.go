package kubectl

import "testing"

func TestEndpointsReadyAddressCount(t *testing.T) {
	var endpoints Endpoints
	endpoints.Subsets = make([]struct {
		Addresses []struct {
			IP string `json:"ip"`
		} `json:"addresses"`
	}, 2)
	endpoints.Subsets[0].Addresses = make([]struct {
		IP string `json:"ip"`
	}, 2)
	endpoints.Subsets[1].Addresses = make([]struct {
		IP string `json:"ip"`
	}, 1)
	if got := endpoints.ReadyAddressCount(); got != 3 {
		t.Fatalf("ReadyAddressCount() = %d, want 3", got)
	}
}
