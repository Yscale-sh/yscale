package lifecycle

import "time"

const (
	ProviderDeleteQueued          = "queued"
	ProviderDeleteDeleting        = "deleting"
	ProviderDeleteRetrying        = "retrying"
	ProviderDeleteTerminated      = "terminated"
	ProviderDeleteManualAttention = "manual_attention"

	providerDeletePayloadV1 = 1

	// ProviderDeleteCleanupEventType is the outbox event that carries the
	// post-delete repair: releases, receipts, settlement and node cleanup for a
	// provider resource that is already CONFIRMED GONE.
	//
	// It is an outbox row rather than a second state machine because the outbox
	// already is the durable "this must still happen" contract — leased,
	// retried, dead-lettered. What it must never be is confused with a
	// publication: a publisher that claimed one of these would hold the lease the
	// delete worker needs, and a delete worker that claimed a publication would
	// try to settle a workload that was never torn down. Both claim paths filter
	// on this exact type, in opposite directions.
	ProviderDeleteCleanupEventType = "provider_delete.cleanup"
)

// ProviderDeleteRequest creates the authoritative record for removing one
// provider resource. Provider identity is read from the burst row under lock;
// callers cannot substitute a different account, SKU, region, or resource ID.
type ProviderDeleteRequest struct {
	CustomerID string
	ClusterID  string
	BurstID    string
	Reason     string
	// Payload is worker-only, bounded non-secret metadata needed to repeat the
	// provider call. Read/list methods intentionally redact it.
	Payload []byte
	Actor   string
	TraceID string
}

type ProviderDeleteResponse struct {
	DeleteID int64
	BurstID  string
	State    string
	Inserted bool
}

// ProviderDeleteRecord is the durable teardown operation. DeletedAt is the
// only timestamp that proves provider deletion; queued, retrying, and
// manual-attention records remain live-cost obligations.
type ProviderDeleteRecord struct {
	ID                 int64
	CustomerID         string
	ClusterID          string
	WorkloadID         string
	BurstID            string
	Provider           string
	Region             string
	CloudAccountID     string
	SKU                string
	ProviderResourceID string
	Reason             string
	PayloadVersion     int
	Payload            []byte
	State              string
	Generation         int
	Attempts           int
	LeaseToken         string
	LockedUntil        *time.Time
	NextAttemptAt      time.Time
	LastError          string
	RequestedAt        time.Time
	UpdatedAt          time.Time
	DeletedAt          *time.Time
}
