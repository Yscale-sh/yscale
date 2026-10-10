package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type lockedProviderDelete struct {
	ID                 int64
	CustomerID         string
	ClusterID          string
	BurstID            string
	Provider           string
	Region             string
	CloudAccountID     string
	SKU                string
	ProviderResourceID string
	State              string
	Attempts           int

	// Payload is the immutable teardown record the operation was created with.
	// It is read here, inside the lock, so the cleanup repair a success writes
	// carries exactly what the delete acted on — never a caller's copy of it.
	Payload string
}

func lockProviderDelete(ctx context.Context, tx pgx.Tx, id int64, leaseToken string) (lockedProviderDelete, error) {
	var operation lockedProviderDelete
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, cluster_id, burst_id, provider, region,
		       cloud_account_id, sku, provider_resource_id, state,
		       attempts, payload::text
		FROM lifecycle.provider_deletes
		WHERE id=$1 AND state='deleting' AND lease_token=$2 AND locked_until > now()
		FOR UPDATE`, id, leaseToken).
		Scan(&operation.ID, &operation.CustomerID, &operation.ClusterID,
			&operation.BurstID, &operation.Provider, &operation.Region,
			&operation.CloudAccountID, &operation.SKU, &operation.ProviderResourceID,
			&operation.State, &operation.Attempts, &operation.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedProviderDelete{}, ErrLeaseLost
	}
	if err != nil {
		return lockedProviderDelete{}, fmt.Errorf("lifecycle: lock provider-delete operation: %w", err)
	}
	return operation, nil
}

const providerDeleteSelect = `
	SELECT id, customer_id, cluster_id, workload_id, burst_id,
	       provider, region, cloud_account_id, sku, provider_resource_id, reason,
	       payload_version, payload::text, state, generation, attempts,
	       lease_token, locked_until, next_attempt_at, last_error,
	       requested_at, updated_at, deleted_at
	FROM lifecycle.provider_deletes
`

type providerDeleteScanner interface {
	Scan(dest ...any) error
}

func scanProviderDelete(row providerDeleteScanner) (ProviderDeleteRecord, error) {
	var record ProviderDeleteRecord
	var payloadText string
	var leaseToken pgtype.Text
	var lockedUntil, deletedAt pgtype.Timestamptz
	if err := row.Scan(&record.ID, &record.CustomerID, &record.ClusterID,
		&record.WorkloadID, &record.BurstID, &record.Provider, &record.Region,
		&record.CloudAccountID, &record.SKU, &record.ProviderResourceID, &record.Reason,
		&record.PayloadVersion, &payloadText, &record.State, &record.Generation,
		&record.Attempts, &leaseToken, &lockedUntil, &record.NextAttemptAt,
		&record.LastError, &record.RequestedAt, &record.UpdatedAt, &deletedAt); err != nil {
		return ProviderDeleteRecord{}, err
	}
	hydrateProviderDeleteRecord(&record, payloadText, leaseToken, lockedUntil, deletedAt)
	return record, nil
}

func hydrateProviderDeleteRecord(record *ProviderDeleteRecord, payloadText string, leaseToken pgtype.Text, lockedUntil, deletedAt pgtype.Timestamptz) {
	record.Payload = []byte(payloadText)
	if leaseToken.Valid {
		record.LeaseToken = leaseToken.String
	}
	if lockedUntil.Valid {
		value := lockedUntil.Time
		record.LockedUntil = &value
	}
	if deletedAt.Valid {
		value := deletedAt.Time
		record.DeletedAt = &value
	}
}

func redactProviderDeleteForRead(record ProviderDeleteRecord) ProviderDeleteRecord {
	record.LeaseToken = ""
	record.Payload = nil
	return record
}

func validateStoredProviderDeletePayload(ctx context.Context, tx pgx.Tx, payload []byte) error {
	var storedBytes int
	if err := tx.QueryRow(ctx, `SELECT octet_length($1::jsonb::text)`, string(payload)).Scan(&storedBytes); err != nil {
		return fmt.Errorf("lifecycle: validate stored provider-delete payload size: %w", err)
	}
	if storedBytes > maxPayloadBytes {
		return fmt.Errorf("%w: provider-delete payload exceeds 1048576 bytes after JSON normalization", ErrInvalidArgument)
	}
	return nil
}

func normalizeProviderDeleteRequest(req ProviderDeleteRequest) (ProviderDeleteRequest, error) {
	for label, value := range map[string]string{
		"customer ID": req.CustomerID,
		"cluster ID":  req.ClusterID,
		"burst ID":    req.BurstID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return ProviderDeleteRequest{}, err
		}
	}
	if req.Actor != "" {
		if err := validateIdentifier("actor", req.Actor); err != nil {
			return ProviderDeleteRequest{}, err
		}
	}
	if req.TraceID != "" {
		if err := validateIdentifier("trace ID", req.TraceID); err != nil {
			return ProviderDeleteRequest{}, err
		}
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		req.Reason = "unspecified"
	}
	if !utf8.ValidString(req.Reason) || len(req.Reason) > maxSafeErrorBytes {
		return ProviderDeleteRequest{}, fmt.Errorf("%w: reason must be valid UTF-8 and at most 1024 bytes", ErrInvalidArgument)
	}
	if len(req.Payload) == 0 {
		req.Payload = []byte(`{}`)
	}
	if err := validateJSONPayload("provider-delete payload", req.Payload); err != nil {
		return ProviderDeleteRequest{}, err
	}
	return req, nil
}

func providerDeleteEventID(id int64) string {
	return fmt.Sprintf("provider_delete:%d", id)
}

// providerDeleteCleanupEventKey is unique per delete operation, and the outbox
// enforces that uniqueness per tenant. One confirmed provider deletion can
// therefore produce exactly one cleanup repair, however many times the success
// transition is attempted.
func providerDeleteCleanupEventKey(id int64) string {
	return fmt.Sprintf("provider_delete_cleanup:%d", id)
}
