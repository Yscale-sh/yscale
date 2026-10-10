package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

const (
	// unassignedClusterID stands in for a booking whose durable record predates
	// the cluster being recorded on it. The lifecycle aggregate is tenant- and
	// cluster-keyed, so the substitute has to be stable: two reap attempts on one
	// burst must land on the same row, and a legacy burst must never collide with
	// a real cluster's namespace.
	unassignedClusterID = "cluster-unassigned"

	// unspecifiedProviderRegion stands in for a booking that records no region.
	// The durable burst booking names a backend, a backend resource ID and a SKU
	// but no region, and the provider call is replayed from Payload rather than
	// from these columns — they exist so an operator can read the queue, and the
	// schema requires them to be non-empty.
	unspecifiedProviderRegion = "region-unspecified"

	bookingCanonicalVersion = 1
)

// ProviderDeleteBooking projects an already-booked provider resource into the
// lifecycle workload/burst identity RequestProviderDelete reads under lock.
//
// It exists because the production booking path is the durable state store, not
// lifecycle admission: by the time a reap runs, the provider resource has been
// paid for and recorded there, and the authoritative delete machine still needs
// an aggregate to hang the operation off. Every identity field here must be
// read off that booking. A caller that passed request input instead would be
// choosing which cloud resource to destroy, which is the one decision this
// package exists to take away from callers.
type ProviderDeleteBooking struct {
	CustomerID string
	// ClusterID and WorkloadID may be empty on a legacy booking; both are then
	// derived deterministically from the burst so replays converge.
	ClusterID  string
	WorkloadID string
	BurstID    string

	// Provider, Region, CloudAccountID, SKU and ProviderResourceID are the booked provider
	// identity. CloudAccountID and Region select credential/routing context;
	// ProviderResourceID selects the resource within it. Success re-checks the
	// complete identity against the burst before it stamps a delete receipt.
	Provider           string
	Region             string
	CloudAccountID     string
	SKU                string
	ProviderResourceID string

	// Spec is the canonical JSON of the booking itself, stored once as the
	// aggregate's immutable record.
	Spec []byte

	// ProviderCreatedAt carries the truthful provider-creation timestamp from
	// a compatibility booking. Zero means managed lifecycle recorded the
	// timestamp directly; a non-zero value is persisted on insert (COALESCE
	// preserves the first write on replay).
	ProviderCreatedAt time.Time

	Reason string
	// Payload is the worker-only replay of the provider call. Read/list methods
	// redact it.
	Payload []byte
	Actor   string
	TraceID string
}

// RequestProviderDeleteForBooking projects the booking and records delete
// intent in ONE transaction. It is idempotent per burst in both halves: the
// projection keeps whatever identity was first recorded, and the delete-intent
// write returns the existing operation rather than creating a second one.
//
// The single transaction is the point. A projection that committed on its own
// would leave, for the width of a process death, a burst recorded as
// provider_created with no delete operation against it — a paid resource the
// authoritative machine believes exists and has been told nothing about. The
// intent write is shared with RequestProviderDelete rather than copied, so the
// two paths cannot drift on which identity a delete is allowed to act on.
//
// A replay that carries a DIFFERENT provider identity for a burst already
// projected is refused with ErrIdentityConflict. Silently keeping the stored row
// would be safe for the resource but would let a caller believe it had queued a
// delete for something else entirely.
func (s *Store) RequestProviderDeleteForBooking(ctx context.Context, booking ProviderDeleteBooking) (ProviderDeleteResponse, error) {
	if err := s.assertReady(); err != nil {
		return ProviderDeleteResponse{}, err
	}
	booking, workloadAsserted, err := normalizeProviderDeleteBooking(booking)
	if err != nil {
		return ProviderDeleteResponse{}, err
	}
	request, err := normalizeProviderDeleteRequest(ProviderDeleteRequest{
		CustomerID: booking.CustomerID,
		ClusterID:  booking.ClusterID,
		BurstID:    booking.BurstID,
		Reason:     booking.Reason,
		Payload:    booking.Payload,
		Actor:      booking.Actor,
		TraceID:    booking.TraceID,
	})
	if err != nil {
		return ProviderDeleteResponse{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProviderDeleteResponse{}, fmt.Errorf("lifecycle: begin provider-delete booking: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := projectBookingTx(ctx, tx, booking, workloadAsserted); err != nil {
		return ProviderDeleteResponse{}, err
	}
	response, err := requestProviderDeleteTx(ctx, tx, request)
	if err != nil {
		return ProviderDeleteResponse{}, err
	}
	if err := commit(ctx, tx, "provider-delete booking"); err != nil {
		return ProviderDeleteResponse{}, err
	}
	return response, nil
}

// projectBookingTx writes the workload and burst aggregate rows if they are
// absent, then re-reads the burst under lock and holds the caller to whatever
// identity is actually stored. The lock it takes is the same burst row the
// delete-intent write locks next, and holding both in one transaction is what
// makes the pair atomic.
//
// workloadAsserted says whether the caller named a workload or whether the
// booking's was derived from the burst. A derived value is a stand-in for "this
// caller does not know", and holding it against a stored one would refuse every
// reap of a burst that lifecycle ADMISSION created — those rows carry the real
// workload id, and the booking path is handed a *state.Burst, which records no
// workload at all. The rest of the identity is compared unconditionally: it all
// comes off the booking, so a disagreement there is a caller naming a different
// cloud resource.
func projectBookingTx(ctx context.Context, tx pgx.Tx, booking ProviderDeleteBooking, workloadAsserted bool) error {
	if err := guardProviderResourceOwnershipTx(ctx, tx, ProviderResourceRef{
		Provider: booking.Provider, CloudAccountID: booking.CloudAccountID, ProviderResourceID: booking.ProviderResourceID,
	}); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.workloads
		(id, customer_id, cluster_id, idempotency_key, canonical_version, payload_hash, burst_id, spec)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
		ON CONFLICT DO NOTHING`,
		booking.WorkloadID, booking.CustomerID, booking.ClusterID,
		bookingIdempotencyKey(booking.BurstID), bookingCanonicalVersion,
		bookingPayloadHash(booking.Spec), booking.BurstID, string(booking.Spec)); err != nil {
		return mapWriteError("project booked workload", err)
	}
	// state='provider_created' on insert, never by transition: the provider
	// resource already exists and was already paid for, so the projection must
	// not pretend a create is still pending.
	var providerCreatedAt any
	if !booking.ProviderCreatedAt.IsZero() {
		providerCreatedAt = booking.ProviderCreatedAt.UTC()
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO lifecycle.bursts
		(id, customer_id, cluster_id, workload_id, provider, region, cloud_account_id, sku, request,
		 state, provider_resource_id, provider_created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,'provider_created',$10,$11)
		ON CONFLICT DO NOTHING`,
		booking.BurstID, booking.CustomerID, booking.ClusterID, booking.WorkloadID,
		booking.Provider, booking.Region, booking.CloudAccountID, booking.SKU, string(booking.Spec),
		booking.ProviderResourceID, providerCreatedAt); err != nil {
		return mapWriteError("project booked burst", err)
	}

	var stored ProviderDeleteBooking
	if err := tx.QueryRow(ctx, `
		SELECT workload_id, provider, region, cloud_account_id, sku, provider_resource_id
		FROM lifecycle.bursts
		WHERE customer_id=$1 AND cluster_id=$2 AND id=$3
		FOR UPDATE`, booking.CustomerID, booking.ClusterID, booking.BurstID).
		Scan(&stored.WorkloadID, &stored.Provider, &stored.Region, &stored.CloudAccountID, &stored.SKU,
			&stored.ProviderResourceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: booked burst %s vanished during projection", ErrInvariantViolation, booking.BurstID)
		}
		return fmt.Errorf("lifecycle: read projected burst: %w", err)
	}
	if (workloadAsserted && stored.WorkloadID != booking.WorkloadID) ||
		stored.Provider != booking.Provider ||
		stored.Region != booking.Region ||
		stored.CloudAccountID != booking.CloudAccountID ||
		stored.SKU != booking.SKU ||
		stored.ProviderResourceID != booking.ProviderResourceID {
		return fmt.Errorf("%w: burst %s is already projected with a different provider identity",
			ErrIdentityConflict, booking.BurstID)
	}
	return nil
}

// NonterminalProviderResourceIDs is the provider-resource projection of every
// delete that has not been confirmed — queued, deleting, retrying, or waiting on
// an operator. An orphan sweep must union this into its tracked set: the durable
// burst record is retired only once the delete terminalizes, and between the two
// the resource belongs to the delete worker, not to a leak reaper.
func (s *Store) NonterminalProviderResourceIDs(ctx context.Context) (map[string]bool, error) {
	if err := s.assertReady(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT provider_resource_id
		FROM lifecycle.provider_deletes
		WHERE state <> 'terminated'`)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: list nonterminal provider deletes: %w", err)
	}
	defer rows.Close()

	tracked := make(map[string]bool)
	for rows.Next() {
		var resourceID string
		if err := rows.Scan(&resourceID); err != nil {
			return nil, fmt.Errorf("lifecycle: scan nonterminal provider delete: %w", err)
		}
		if resourceID != "" {
			tracked[resourceID] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lifecycle: list nonterminal provider deletes: %w", err)
	}
	return tracked, nil
}

// normalizeProviderDeleteBooking returns the canonical booking and whether the
// caller actually named a workload, which decides how strictly the projection
// may hold it to one. See projectBookingTx.
func normalizeProviderDeleteBooking(booking ProviderDeleteBooking) (ProviderDeleteBooking, bool, error) {
	booking.CustomerID = strings.TrimSpace(booking.CustomerID)
	booking.ClusterID = strings.TrimSpace(booking.ClusterID)
	booking.WorkloadID = strings.TrimSpace(booking.WorkloadID)
	booking.BurstID = strings.TrimSpace(booking.BurstID)
	booking.Provider = strings.TrimSpace(booking.Provider)
	booking.Region = CanonicalProviderDeleteRegion(booking.Region)
	booking.CloudAccountID = strings.TrimSpace(booking.CloudAccountID)
	booking.SKU = strings.TrimSpace(booking.SKU)
	booking.ProviderResourceID = strings.TrimSpace(booking.ProviderResourceID)

	if booking.ClusterID == "" {
		booking.ClusterID = unassignedClusterID
	}
	workloadAsserted := booking.WorkloadID != ""
	if !workloadAsserted {
		booking.WorkloadID = booking.BurstID
	}
	// The aggregate KEYS go through the package's identifier grammar, because
	// RequestProviderDelete validates the same three and the two must agree.
	for label, value := range map[string]string{
		"customer ID": booking.CustomerID,
		"cluster ID":  booking.ClusterID,
		"workload ID": booking.WorkloadID,
		"burst ID":    booking.BurstID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return ProviderDeleteBooking{}, false, err
		}
	}
	// The provider descriptors are DATA, not identifiers: a backend's SKU or
	// resource ID is whatever that cloud hands back. Holding them to the
	// identifier grammar would fail a delete closed over a punctuation character
	// and strand a paid resource, so they are bounded exactly as the schema
	// bounds them and no further.
	for label, value := range map[string]string{
		"provider":             booking.Provider,
		"region":               booking.Region,
		"SKU":                  booking.SKU,
		"provider resource ID": booking.ProviderResourceID,
	} {
		if err := validateBookingField(label, value); err != nil {
			return ProviderDeleteBooking{}, false, err
		}
	}
	if booking.CloudAccountID != "" {
		if err := validateBookingField("cloud account ID", booking.CloudAccountID); err != nil {
			return ProviderDeleteBooking{}, false, err
		}
	}
	if len(booking.Spec) == 0 {
		return ProviderDeleteBooking{}, false, fmt.Errorf("%w: booking spec is required", ErrInvalidArgument)
	}
	if err := validateJSONPayload("booking spec", booking.Spec); err != nil {
		return ProviderDeleteBooking{}, false, err
	}
	return booking, workloadAsserted, nil
}

// CanonicalProviderDeleteRegion preserves an explicitly booked routing region
// and gives legacy records a stable identity instead of an empty SQL value.
func CanonicalProviderDeleteRegion(region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		return unspecifiedProviderRegion
	}
	return region
}

// validateBookingField mirrors the schema's own bound for a provider
// descriptor: non-empty after trimming, at most 255 bytes, valid UTF-8, and
// free of NUL (which Postgres rejects in text at all).
func validateBookingField(label, value string) error {
	if strings.TrimSpace(value) == "" || len(value) > maxIdentifierBytes {
		return fmt.Errorf("%w: %s must be 1..255 bytes", ErrInvalidArgument, label)
	}
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: %s must be valid UTF-8 without NUL", ErrInvalidArgument, label)
	}
	return nil
}

func bookingIdempotencyKey(burstID string) string {
	return "booked_burst:" + burstID
}

func bookingPayloadHash(spec []byte) string {
	sum := sha256.Sum256(spec)
	return hex.EncodeToString(sum[:])
}
