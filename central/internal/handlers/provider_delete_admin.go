package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
)

const providerDeleteAdminBodyLimit = 64 << 10

// ProviderDeleteReconciler is the operator surface of the durable delete
// machine. It is deliberately separate from the worker interface: request
// handlers cannot claim or terminalize provider operations.
type ProviderDeleteReconciler interface {
	ListProviderDeleteManualAttention(ctx context.Context, limit int) ([]lifecycle.ProviderDeleteRecord, error)
	RetryProviderDelete(ctx context.Context, customerID, clusterID, burstID, actor, traceID string) (lifecycle.ProviderDeleteRecord, error)
}

type ProviderDeleteAdmin struct {
	Deletes ProviderDeleteReconciler
	Wake    chan<- struct{}
}

type providerDeleteAdminRecord struct {
	ID                 int64      `json:"id"`
	CustomerID         string     `json:"customer_id"`
	ClusterID          string     `json:"cluster_id"`
	WorkloadID         string     `json:"workload_id"`
	BurstID            string     `json:"burst_id"`
	Provider           string     `json:"provider"`
	Region             string     `json:"region"`
	CloudAccountID     string     `json:"cloud_account_id,omitempty"`
	SKU                string     `json:"sku"`
	ProviderResourceID string     `json:"provider_resource_id"`
	Reason             string     `json:"reason"`
	State              string     `json:"state"`
	Generation         int        `json:"generation"`
	Attempts           int        `json:"attempts"`
	NextAttemptAt      time.Time  `json:"next_attempt_at"`
	LastError          string     `json:"last_error"`
	RequestedAt        time.Time  `json:"requested_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	DeletedAt          *time.Time `json:"deleted_at,omitempty"`
}

func providerDeleteAdminView(record lifecycle.ProviderDeleteRecord) providerDeleteAdminRecord {
	return providerDeleteAdminRecord{
		ID: record.ID, CustomerID: record.CustomerID, ClusterID: record.ClusterID,
		WorkloadID: record.WorkloadID, BurstID: record.BurstID, Provider: record.Provider,
		Region: record.Region, CloudAccountID: record.CloudAccountID, SKU: record.SKU,
		ProviderResourceID: record.ProviderResourceID, Reason: record.Reason, State: record.State,
		Generation: record.Generation, Attempts: record.Attempts, NextAttemptAt: record.NextAttemptAt,
		LastError: record.LastError, RequestedAt: record.RequestedAt, UpdatedAt: record.UpdatedAt,
		DeletedAt: record.DeletedAt,
	}
}

// ListManualAttention returns the bounded, redacted operator queue. Worker
// payloads and lease tokens are omitted by construction rather than relying on
// an upstream store implementation to redact them correctly.
func (h *ProviderDeleteAdmin) ListManualAttention(w http.ResponseWriter, r *http.Request) {
	if h.Deletes == nil {
		http.Error(w, "provider delete lifecycle unavailable", http.StatusServiceUnavailable)
		return
	}
	limit, err := providerDeleteListLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	records, err := h.Deletes.ListProviderDeleteManualAttention(r.Context(), limit)
	if err != nil {
		http.Error(w, "provider delete reconciliation unavailable", http.StatusServiceUnavailable)
		return
	}
	views := make([]providerDeleteAdminRecord, 0, len(records))
	for _, record := range records {
		views = append(views, providerDeleteAdminView(record))
	}
	writeJSON(w, http.StatusOK, map[string]any{"deletes": views})
}

type providerDeleteRetryRequest struct {
	CustomerID string `json:"customer_id"`
	ClusterID  string `json:"cluster_id"`
	BurstID    string `json:"burst_id"`
	TraceID    string `json:"trace_id,omitempty"`
}

// RetryManualAttention starts a new attempt generation for one exact
// tenant/cluster/burst identity. The store re-checks that identity under lock.
func (h *ProviderDeleteAdmin) RetryManualAttention(w http.ResponseWriter, r *http.Request) {
	if h.Deletes == nil {
		http.Error(w, "provider delete lifecycle unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, providerDeleteAdminBodyLimit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var request providerDeleteRetryRequest
	if err := dec.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "invalid provider delete retry payload", http.StatusBadRequest)
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid provider delete retry payload", http.StatusBadRequest)
		return
	}
	record, err := h.Deletes.RetryProviderDelete(r.Context(), request.CustomerID, request.ClusterID,
		request.BurstID, "operator", request.TraceID)
	if err != nil {
		switch {
		case errors.Is(err, lifecycle.ErrInvalidArgument):
			http.Error(w, "invalid provider delete retry identity", http.StatusBadRequest)
		case errors.Is(err, lifecycle.ErrNotFound):
			http.Error(w, "provider delete not found", http.StatusNotFound)
		case errors.Is(err, lifecycle.ErrInvariantViolation):
			http.Error(w, "provider delete cannot be retried", http.StatusConflict)
		default:
			http.Error(w, "provider delete reconciliation unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	if h.Wake != nil {
		select {
		case h.Wake <- struct{}{}:
		default:
		}
	}
	writeJSON(w, http.StatusAccepted, providerDeleteAdminView(record))
}

func providerDeleteListLimit(r *http.Request) (int, error) {
	limit := 100
	for key, values := range r.URL.Query() {
		if key != "limit" {
			return 0, errors.New("unsupported query parameter; this route takes limit")
		}
		if len(values) != 1 {
			return 0, errors.New("limit must be specified once")
		}
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed < 1 || parsed > 500 {
			return 0, errors.New("limit must be an integer from 1 to 500")
		}
		limit = parsed
	}
	return limit, nil
}
