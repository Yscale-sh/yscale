package lifecycle

import (
	"context"
	"testing"
)

// TestLiveProvisioningOpsNilStore covers the nil-receiver contract.
func TestLiveProvisioningOpsNilStore(t *testing.T) {
	var s *Store
	_, err := s.LiveProvisioningOps(context.Background())
	if err == nil {
		t.Fatal("nil store must return an error")
	}
}

// TestLiveProvisioningOpsNoPool covers a Store with no pool wired.
func TestLiveProvisioningOpsNoPool(t *testing.T) {
	s := &Store{}
	_, err := s.LiveProvisioningOps(context.Background())
	if err == nil {
		t.Fatal("store with nil pool must return an error")
	}
}
