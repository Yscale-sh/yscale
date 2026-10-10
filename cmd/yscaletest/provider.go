package main

import (
	"context"
	"errors"
)

const (
	// backendLinode is the value central stamps into
	// Workload.status.backend for a Linode burst. Duplicated here rather
	// than imported from pkg/backends because yscaletest is deliberately
	// an outside-in harness: it asserts against the values a customer's
	// cluster would actually see on the wire.
	backendLinode = "linode"

	// yscaleBurstTag is the ownership tag pkg/backends/linode stamps on
	// every instance CreateNode provisions (its unexported ownerTag).
	// CreateNode tags an instance with exactly two things: this, and the
	// full burst ID. Proving reap means proving no instance carries BOTH.
	//
	// It is a literal here on purpose. If the backend ever renames the
	// tag, the audit must fail loudly rather than silently re-point itself
	// at the new name and keep passing.
	yscaleBurstTag = "yscale-burst"
)

// Errors from the provider audit. Each is a distinct outcome under
// #93/#24: unconfigured and inconclusive both mean "we did not establish
// absence", residue means "we established presence". None may set
// Reaped=true.
var (
	// errProviderUnconfigured: nothing is wired up that could audit this
	// case's backend.
	errProviderUnconfigured = errors.New("provider inventory not configured")
	// errProviderInconclusive: the account could not be listed, or there
	// was nothing to match on.
	errProviderInconclusive = errors.New("provider inventory inconclusive")
	// errProviderResidue: instances carrying the burst's ownership tags
	// were still there at the deadline.
	errProviderResidue = errors.New("provider resources still present")
)

// OwnedInstances is the redacted result of an audit: how many instances
// still carry the full ownership tag set for one burst, and their provider
// IDs.
//
// IDs only — never labels, IPs, regions, or raw API payloads. This struct
// is what lands in a failing case's report line, so it must carry enough
// to go delete the leak by hand and nothing that describes the account.
type OwnedInstances struct {
	Count int
	IDs   []string
}

// ProviderResidue is the typed sub-count result of a granular provider
// audit. Each category is queried independently so tests can prove every
// resource type was checked. The aggregate ProviderResourcesRemaining is
// the sum.
type ProviderResidue struct {
	TaggedInstances int
	ExactIDInstance int
	// ExactIDTagged records whether the exact provider instance is already
	// included in TaggedInstances, so the retained aggregate counts resources
	// rather than observation methods.
	ExactIDTagged   bool
	AttachedVolumes int
	FirewallDevices int
}

func (r ProviderResidue) Total() int {
	return r.TaggedInstances + r.ExactIDInstance + r.AttachedVolumes + r.FirewallDevices
}

// UniqueTotal counts unique instance presence plus attachment counts.
func (r ProviderResidue) UniqueTotal() int {
	instances := r.TaggedInstances + r.ExactIDInstance
	if r.ExactIDTagged && r.TaggedInstances > 0 && r.ExactIDInstance > 0 {
		instances--
	}
	return instances + r.AttachedVolumes + r.FirewallDevices
}

// ProviderInventory answers exactly one question, exactly: how many
// instances in the account still carry BOTH ownership tags for this burst.
//
// It is intentionally read-only and intentionally narrow. yscaletest must
// be able to prove a burst is gone without holding any capability to
// create or delete one.
type ProviderInventory interface {
	// Name is the backend this inventory audits, matched against the
	// Workload's status.backend so a case can never be cleared by an audit
	// of the wrong provider.
	Name() string

	// CountBurstInstances returns the instances tagged with BOTH
	// yscaleBurstTag and the full burstID. A non-nil error means the
	// account could not be listed; callers must treat that as
	// inconclusive, never as absence.
	CountBurstInstances(ctx context.Context, burstID string) (OwnedInstances, error)

	// AuditBurstResidue performs a granular audit covering tagged
	// instances, exact provider instance ID, volumes attached to that
	// instance, and firewall device attachments. providerID is the
	// numeric Linode instance ID parsed from spec.providerID.
	AuditBurstResidue(ctx context.Context, burstID string, providerID int) (ProviderResidue, error)
}
