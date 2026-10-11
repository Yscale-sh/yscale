package state

import (
	"errors"
	"reflect"
	"testing"
)

type retryCustomerPersister struct {
	persister
	err     error
	calls   int
	written Customer
}

func (p *retryCustomerPersister) upsertCustomer(c *Customer) error {
	p.calls++
	if p.err != nil {
		return p.err
	}
	p.written = *c
	return nil
}

func TestGatewayRoutePersistenceFailureRetries(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Store) error
	}{
		{"desired", func(t *testing.T, s *Store) error {
			changed, err := s.SetCustomerGatewayRoutes("cust_test", "cl-live", []string{"10.2.0.0/16"})
			if changed != (err == nil) {
				t.Errorf("changed = %v, err = %v", changed, err)
			}
			return err
		}},
		{"pushed union", func(t *testing.T, s *Store) error {
			return s.SetCustomerPushedGatewayRoutes("cust_test", []string{"10.2.0.0/16"})
		}},
		{"pushed cluster", func(t *testing.T, s *Store) error {
			return s.SetCustomerPushedClusterGatewayRoutes("cust_test", "cl-live", []string{"10.2.0.0/16"})
		}},
		{"clear tombstone", func(t *testing.T, s *Store) error {
			return s.ClearCustomerPushedClusterGatewayRoutes("cust_test", "cl-gone")
		}},
		{"reap unheld", func(t *testing.T, s *Store) error {
			pruned, err := s.ReapUnheldClusterGatewayRoutes("cust_test", []string{"cl-gone"})
			if err == nil && !reflect.DeepEqual(pruned, []string{"cl-gone"}) {
				t.Errorf("pruned = %v, want cl-gone", pruned)
			}
			if err != nil && len(pruned) != 0 {
				t.Errorf("failed reap returned pruned clusters: %v", pruned)
			}
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, failure := range []string{"backend", "conflict", "tombstone"} {
				t.Run(failure, func(t *testing.T) {
					s := New()
					claimCluster(t, s, "cust_test", "cl-live")
					c := s.customers["cust_test"]
					c.GatewayRoutesByCluster = map[string][]string{"cl-live": {"10.0.0.0/16"}, "cl-gone": {"10.1.0.0/16"}}
					c.GatewayRoutes = []string{"10.0.0.0/16", "10.1.0.0/16"}
					c.PushedGatewayRoutesByCluster = copyRouteSets(c.GatewayRoutesByCluster)
					c.PushedGatewayRoutes = append([]string(nil), c.GatewayRoutes...)
					before := *c
					fresh := &Customer{ID: c.ID, GatewayRoutes: []string{"10.9.0.0/16"}}
					p := &retryCustomerPersister{err: errors.New("write failed")}
					if failure == "conflict" {
						p.err = &customerWriteConflict{current: fresh}
					} else if failure == "tombstone" {
						p.err = &customerWriteConflict{tombstoned: true}
					}
					s.persist = p
					if err := tt.run(t, s); !errors.Is(err, p.err) {
						t.Fatalf("write = %v, want %v", err, p.err)
					}
					if failure == "conflict" {
						if s.customers[c.ID] != fresh || !reflect.DeepEqual(fresh.GatewayRoutes, []string{"10.9.0.0/16"}) {
							t.Fatal("rollback overwrote refreshed customer")
						}
						return
					}
					if failure == "tombstone" {
						if s.customers[c.ID] != nil {
							t.Fatal("rollback resurrected customer")
						}
						return
					}
					if !reflect.DeepEqual(*s.customers[c.ID], before) {
						t.Error("failed write changed in-memory customer")
					}
					p.err = nil
					if err := tt.run(t, s); err != nil {
						t.Fatalf("retry: %v", err)
					}
					if p.calls != 2 || !reflect.DeepEqual(p.written, *s.customers[c.ID]) {
						t.Fatal("identical retry did not persist current state")
					}
				})
			}
		})
	}
}
