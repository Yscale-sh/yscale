package lifecycle

import "time"

const (
	WorkloadAdmitted  = "admitted"
	WorkloadSucceeded = "succeeded"
	WorkloadFailed    = "failed"
	WorkloadCancelled = "cancelled"

	BurstCreateRequested = "create_requested"
	BurstProviderCreated = "provider_created"
	BurstManualAttention = "manual_attention"
	BurstTerminated      = "terminated"

	OperationProviderCreate = "provider_create"

	OperationPending    = "pending"
	OperationProcessing = "processing"
	OperationSucceeded  = "succeeded"
	OperationFailed     = "failed"
	OperationDeadLetter = "dead_letter"

	OutboxPending      = "pending"
	OutboxProcessing   = "processing"
	OutboxAcknowledged = "acknowledged"
	OutboxFailed       = "failed"
	OutboxDeadLetter   = "dead_letter"
)

type AdmissionRequest struct {
	CustomerID       string
	ClusterID        string
	IdempotencyKey   string
	CanonicalVersion int
	WorkloadID       string
	BurstID          string

	// WorkloadSpec and BurstSpec are already validated canonical JSON objects.
	// They are bounded and stored, but never included in returned errors.
	WorkloadSpec []byte
	BurstSpec    []byte

	// Provider, Region, CloudAccountID and SKU are the provider identity the
	// burst is admitted under, and they are the identity the authoritative
	// provider-delete projection later re-checks against the stored burst row.
	// Region is canonicalized exactly as that projection canonicalizes it, so a
	// workload that pinned no region admits and deletes under one value.
	Provider              string
	Region                string
	CloudAccountID        string
	SKU                   string
	ProviderCreatePayload []byte

	// OutboxPayload is optional. When omitted, the store writes a minimal
	// versioned workload-admitted event payload containing canonical IDs.
	OutboxPayload []byte
	Actor         string
	TraceID       string
}

type AdmissionResponse struct {
	WorkloadID string
	BurstID    string
	Inserted   bool
}

type ProviderCreateOperation struct {
	ID             int64
	CustomerID     string
	ClusterID      string
	WorkloadID     string
	BurstID        string
	Provider       string
	Region         string
	CloudAccountID string
	SKU            string
	PayloadVersion int
	Payload        []byte
	State          string
	Attempts       int
	LeaseToken     string
	LockedUntil    time.Time
	NextAttemptAt  time.Time
	CreatedAt      time.Time

	// LeaseExpired reports a `processing` operation whose lease has run out. It
	// is NOT a reclaimable operation: the attempt that held the lease had
	// already been handed the request and may have reached the provider, so the
	// create it dispatched is ambiguous rather than known not to have happened.
	// The scoped claim sets it so a refusal can name which of the two kinds of
	// `processing` refused it. See ClaimProviderCreateForBurst.
	LeaseExpired bool
}

type OutboxEvent struct {
	ID             int64
	CustomerID     string
	ClusterID      string
	AggregateType  string
	AggregateID    string
	EventKey       string
	EventType      string
	PayloadVersion int
	Payload        []byte
	State          string
	Attempts       int
	LeaseToken     string
	LockedUntil    time.Time
	NextAttemptAt  time.Time
	CreatedAt      time.Time
}
