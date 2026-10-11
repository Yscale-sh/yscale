package state

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestClaimAgentClusterPersistenceFailureRestoresRegistry(t *testing.T) {
	for _, mode := range []string{"new", "reconnect", "prune"} {
		for _, failure := range []string{"backend", "conflict", "tombstone"} {
			t.Run(mode+"/"+failure, func(t *testing.T) {
				s := New()
				claimCluster(t, s, "cust_test", "cl-old")
				c := s.customers["cust_test"]
				c.GatewayRoutesByCluster = map[string][]string{"cl-old": {"10.0.0.0/16"}}
				c.GatewayRoutes = []string{"10.0.0.0/16"}
				c.PushedGatewayRoutesByCluster = copyRouteSets(c.GatewayRoutesByCluster)
				if mode == "prune" {
					for i := 1; i < MaxRegisteredClusters; i++ {
						id := fmt.Sprintf("cl-fill-%02d", i)
						c.RegisteredClusters = append(c.RegisteredClusters, &RegisteredCluster{ClusterID: id, Source: ClusterSourceRegistered})
						s.clusterOwners[id] = c.ID
					}
				}
				before := *c
				owners := make(map[string]string, len(s.clusterOwners))
				for id, owner := range s.clusterOwners {
					owners[id] = owner
				}
				fresh := &Customer{ID: c.ID, RegisteredClusters: []*RegisteredCluster{{ClusterID: "cl-fresh"}}}
				p := &retryCustomerPersister{err: errors.New("write failed")}
				if failure == "conflict" {
					p.err = &customerWriteConflict{current: fresh}
				} else if failure == "tombstone" {
					p.err = &customerWriteConflict{tombstoned: true}
				}
				s.persist = p
				id := "cl-new"
				if mode == "reconnect" {
					id = "cl-old"
				}
				at := time.Now().UTC().Add(time.Hour)
				claimed, pruned, err := s.ClaimAgentCluster(c.ID, id, false, at)
				if !errors.Is(err, p.err) || claimed || pruned != "" {
					t.Fatalf("failed claim = %v, %q, %v", claimed, pruned, err)
				}
				if failure != "backend" {
					want := map[string]string{}
					if failure == "conflict" {
						want["cl-fresh"] = c.ID
						if s.customers[c.ID] != fresh {
							t.Fatal("rollback overwrote refreshed customer")
						}
					} else if s.customers[c.ID] != nil {
						t.Fatal("rollback resurrected customer")
					}
					if !reflect.DeepEqual(s.clusterOwners, want) {
						t.Fatal("rollback overwrote refreshed ownership")
					}
					return
				}
				if !reflect.DeepEqual(*s.customers[c.ID], before) || !reflect.DeepEqual(s.clusterOwners, owners) {
					t.Error("failed claim changed registry, ownership, or routes")
				}
				p.err = nil
				claimed, pruned, err = s.ClaimAgentCluster(c.ID, id, false, at)
				wantPruned := ""
				if mode == "prune" {
					wantPruned = "cl-old"
				}
				if err != nil || claimed != (mode != "reconnect") || pruned != wantPruned {
					t.Fatalf("retry = %v, %q, %v", claimed, pruned, err)
				}
				if p.calls != 2 || !reflect.DeepEqual(p.written, *s.customers[c.ID]) {
					t.Fatal("retry did not persist the customer")
				}
				if s.clusterOwners[id] != c.ID || (mode == "prune" && s.clusterOwners["cl-old"] != "") {
					t.Fatal("retry did not update ownership")
				}
				if mode == "prune" && (len(c.GatewayRoutes) != 0 || len(c.GatewayRoutesByCluster) != 0 || len(c.PushedGatewayRoutesByCluster["cl-old"]) == 0) {
					t.Fatal("prune must remove desired routes and preserve withdrawal tombstone")
				}
			})
		}
	}
}
