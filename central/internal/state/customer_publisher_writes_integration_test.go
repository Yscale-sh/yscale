//go:build integration

// yscale:proprietary

package state

import (
	"errors"
	"testing"
	"time"
)

// These combined-principal scenarios retain the private publisher coverage.
// Public connector-only counterparts remain in customer_writes_integration_test.go.
func TestPostgresCustomerWriteCannotRestoreRemovedPrincipals(t *testing.T) {
	a, p, owner := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	if _, err := a.DeleteTenantCluster(customerWriteTenant, owner, "synthetic-cluster", HumanActor(owner, customerWriteTenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.DeleteCatalogPublisher(customerWriteTenant, owner, "synthetic-publisher"); err != nil {
		t.Fatal(err)
	}
	before := customerWriteAuditCount(t, p)
	b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
	// Once refreshed, a later disconnect is a no-op, not a re-creation.
	b.MarkClusterDisconnected(customerWriteTenant, "synthetic-cluster", time.Now(), time.Now())
	if _, _, err := b.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
		t.Errorf("removed connector = %v; want denied", err)
	}
	if _, _, err := b.AuthCatalogPublisher("synthetic-publisher-token"); !errors.Is(err, ErrNotFound) {
		t.Errorf("removed publisher = %v; want denied", err)
	}
	if len(b.clusterCreds) != 0 || len(b.catalogPublisherCreds) != 0 || len(b.clusterOwners) != 0 {
		t.Error("conflict refresh retained removed principal indexes")
	}
	if customerWriteAuditCount(t, p) != before {
		t.Error("disconnect added an audit event")
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, HumanActor(owner, customerWriteTenant)); err != nil {
		t.Fatalf("refresh lost the active owner's membership: %v", err)
	}
}

func TestPostgresCustomerWriteScopedCredentialRotation(t *testing.T) {
	a, p, owner := customerWriteFixture(t)
	b := customerWriteReplica(t, p)
	_, connectorToken, _, err := a.RotateTenantClusterCredential(customerWriteTenant, owner, "synthetic-cluster", HumanActor(owner, customerWriteTenant))
	if err != nil {
		t.Fatal(err)
	}
	_, publisherToken, _, err := a.RotateCatalogPublisherCredential(customerWriteTenant, owner, "synthetic-publisher")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("stale write = %v", err)
	}
	if _, _, err := b.AuthClusterCredential("synthetic-connector"); !errors.Is(err, ErrNotFound) {
		t.Error("old connector credential restored")
	}
	if _, _, err := b.AuthCatalogPublisher("synthetic-publisher-token"); !errors.Is(err, ErrNotFound) {
		t.Error("old publisher credential restored")
	}
	if _, _, err := b.AuthClusterCredential(connectorToken); err != nil {
		t.Error("new connector credential lost")
	}
	if _, _, err := b.AuthCatalogPublisher(publisherToken); err != nil {
		t.Error("new publisher credential lost")
	}
	if _, ok := b.clusterCreds[HashClusterCredential(connectorToken)]; !ok {
		t.Error("connector index not refreshed")
	}
	if _, ok := b.catalogPublisherCreds[HashCatalogPublisherCredential(publisherToken)]; !ok {
		t.Error("publisher index not refreshed")
	}
	if _, _, err := b.SetCustomerLimits(customerWriteTenant, 2, 6, OperatorActor()); err != nil {
		t.Fatalf("retry: %v", err)
	}
}
