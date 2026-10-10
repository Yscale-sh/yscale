// yscale:proprietary

package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/yscale-sh/yscale/central/internal/billing"
)

const maxBillingWebhookBodyBytes = 1 << 20

type verifiedWebhookStore interface {
	RecordVerifiedWebhook(context.Context, billing.VerifiedWebhookEvent) (int64, bool, error)
}

type BillingWebhooks struct {
	Store   verifiedWebhookStore
	Gateway billing.CheckoutGateway
	Log     *slog.Logger
}

func (h *BillingWebhooks) HandleStripe(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.Store == nil || h.Gateway == nil {
		http.NotFound(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBillingWebhookBodyBytes)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "webhook body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid webhook body"})
		}
		return
	}
	event, err := h.Gateway.VerifyWebhook(payload, r.Header.Get("Stripe-Signature"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "webhook rejected"})
		return
	}
	_, inserted, err := h.Store.RecordVerifiedWebhook(r.Context(), event)
	if err != nil {
		h.Log.Error("billing: record verified webhook", "provider", event.Provider, "event_type", event.EventType, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "webhook inbox unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"received": true, "inserted": inserted})
}
