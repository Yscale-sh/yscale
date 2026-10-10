// yscale:proprietary

package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/state"
)

const maxServiceCreditBodyBytes = 4 << 10

const (
	maxCheckoutBodyBytes       = 4 << 10
	defaultCheckoutMinMicroUSD = 5_000_000
	defaultCheckoutMaxMicroUSD = 10_000_000_000
)

type TenantBillingSummaryResponse struct {
	TenantID          string                  `json:"tenant_id"`
	Currency          string                  `json:"currency"`
	BalanceMicroUSD   int64                   `json:"balance_micro_usd"`
	HeldMicroUSD      int64                   `json:"held_micro_usd"`
	SpendableMicroUSD int64                   `json:"spendable_micro_usd"`
	DebtMicroUSD      int64                   `json:"debt_micro_usd"`
	Frozen            bool                    `json:"frozen"`
	UpdatedAt         *time.Time              `json:"updated_at,omitempty"`
	OpenHolds         []TenantBillingOpenHold `json:"open_holds"`
}

type TenantBillingOpenHold struct {
	ID             int64     `json:"id"`
	WorkloadID     string    `json:"workload_id"`
	AmountMicroUSD int64     `json:"amount_micro_usd"`
	ExpiresAt      time.Time `json:"expires_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type TenantBillingStatementResponse struct {
	TenantID                 string                        `json:"tenant_id"`
	Currency                 string                        `json:"currency"`
	PeriodStart              time.Time                     `json:"period_start"`
	PeriodEnd                time.Time                     `json:"period_end"`
	GeneratedAt              time.Time                     `json:"generated_at"`
	LedgerWatermark          int64                         `json:"ledger_watermark"`
	OpeningNetCreditMicroUSD int64                         `json:"opening_net_credit_micro_usd"`
	OpeningHeldMicroUSD      int64                         `json:"opening_held_micro_usd"`
	ClosingNetCreditMicroUSD int64                         `json:"closing_net_credit_micro_usd"`
	ClosingHeldMicroUSD      int64                         `json:"closing_held_micro_usd"`
	Entries                  []TenantBillingStatementEntry `json:"entries"`
}

type TenantBillingStatementEntry struct {
	LedgerID             int64     `json:"ledger_id"`
	CreatedAt            time.Time `json:"created_at"`
	EntryType            string    `json:"entry_type"`
	AmountMicroUSD       int64     `json:"amount_micro_usd"`
	BalanceDeltaMicroUSD int64     `json:"balance_delta_micro_usd"`
	HeldDeltaMicroUSD    int64     `json:"held_delta_micro_usd"`
	NetCreditMicroUSD    int64     `json:"net_credit_micro_usd"`
	HeldMicroUSD         int64     `json:"held_micro_usd"`
	WorkloadID           string    `json:"workload_id,omitempty"`
	HoldID               *int64    `json:"hold_id,omitempty"`
}

type TenantCheckoutResponse struct {
	ID             string                `json:"id"`
	TenantID       string                `json:"tenant_id"`
	AmountMicroUSD int64                 `json:"amount_micro_usd"`
	Currency       string                `json:"currency"`
	State          billing.CheckoutState `json:"state"`
	URL            string                `json:"url,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

// TenantWorkloadBillingReceipt is the stable receipt shape future
// reserve/capture integration will attach to workload responses. It contains
// economic identifiers and amounts only, never provider/payment metadata.
type TenantWorkloadBillingReceipt struct {
	HoldID           int64             `json:"hold_id"`
	State            billing.HoldState `json:"state"`
	ReservedMicroUSD int64             `json:"reserved_micro_usd"`
	CapturedMicroUSD int64             `json:"captured_micro_usd"`
	Currency         string            `json:"currency"`
	QuoteID          string            `json:"quote_id"`
	PricingVersion   int               `json:"pricing_version"`
}

func (a *Accounts) tenantWorkloadBilling(ctx context.Context, record *state.Workload) *TenantWorkloadBillingReceipt {
	return workloadBillingReceipt(ctx, a.Billing, record)
}

func workloadBillingReceipt(ctx context.Context, ledger any, record *state.Workload) *TenantWorkloadBillingReceipt {
	if record == nil || record.Billing == nil || ledger == nil {
		return nil
	}
	reader, ok := ledger.(interface {
		GetHold(context.Context, string, int64) (billing.Hold, error)
	})
	if !ok {
		return nil
	}
	hold, err := reader.GetHold(ctx, record.CustomerID, record.Billing.HoldID)
	if err != nil {
		return nil
	}
	return &TenantWorkloadBillingReceipt{
		HoldID: hold.ID, State: hold.State, ReservedMicroUSD: hold.AmountMicroUSD,
		CapturedMicroUSD: hold.CapturedMicroUSD, Currency: record.Billing.Currency,
		QuoteID: record.Billing.QuoteID, PricingVersion: record.Billing.PricingVersion,
	}
}

func (a *Accounts) HandleGetTenantBilling(w http.ResponseWriter, r *http.Request) {
	if a.Billing == nil {
		http.NotFound(w, r)
		return
	}
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if _, err := a.Store.MembershipFor(caller.ID, tenantID); err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	account, err := a.Billing.GetAccount(r.Context(), tenantID)
	var updatedAt *time.Time
	if errors.Is(err, billing.ErrNotFound) {
		account = billing.Account{CustomerID: tenantID, Currency: "USD"}
	} else if err != nil {
		a.Log.Error("account: read billing summary", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing unavailable"})
		return
	} else {
		updated := account.UpdatedAt
		updatedAt = &updated
	}
	holds, err := a.Billing.ListOpenHolds(r.Context(), tenantID, 100)
	if err != nil {
		a.Log.Error("account: list billing holds", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing unavailable"})
		return
	}
	open := make([]TenantBillingOpenHold, 0, len(holds))
	for _, hold := range holds {
		open = append(open, TenantBillingOpenHold{ID: hold.ID, WorkloadID: hold.WorkloadRef, AmountMicroUSD: hold.AmountMicroUSD, ExpiresAt: hold.ExpiresAt, CreatedAt: hold.CreatedAt})
	}
	writeJSON(w, http.StatusOK, TenantBillingSummaryResponse{
		TenantID: tenantID, Currency: account.Currency, BalanceMicroUSD: account.BalanceMicroUSD,
		HeldMicroUSD: account.HeldMicroUSD, SpendableMicroUSD: account.SpendableMicroUSD(),
		DebtMicroUSD: account.DebtMicroUSD, Frozen: account.Frozen, UpdatedAt: updatedAt, OpenHolds: open,
	})
}

func parseStatementPeriod(rawQuery string) (time.Time, time.Time, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("invalid query string")
	}
	for key, entries := range values {
		if key != "from" && key != "to" {
			return time.Time{}, time.Time{}, errors.New("statement route takes only from and to")
		}
		if len(entries) != 1 || entries[0] == "" {
			return time.Time{}, time.Time{}, errors.New(key + " must be given exactly once with a value")
		}
	}
	if len(values["from"]) != 1 || len(values["to"]) != 1 {
		return time.Time{}, time.Time{}, errors.New("from and to are required")
	}
	from, err := time.Parse(time.RFC3339, values.Get("from"))
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("from must be RFC3339")
	}
	to, err := time.Parse(time.RFC3339, values.Get("to"))
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("to must be RFC3339")
	}
	if !from.Before(to) || to.Sub(from) > billing.MaxStatementPeriod {
		return time.Time{}, time.Time{}, errors.New("statement period must be positive and at most 366 days")
	}
	return from.UTC(), to.UTC(), nil
}

func (a *Accounts) readTenantStatement(w http.ResponseWriter, r *http.Request) (billing.Statement, bool) {
	if a.Billing == nil {
		http.NotFound(w, r)
		return billing.Statement{}, false
	}
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return billing.Statement{}, false
	}
	if _, err := a.Store.MembershipFor(caller.ID, tenantID); err != nil {
		a.writeMembershipReadError(w, err)
		return billing.Statement{}, false
	}
	from, to, err := parseStatementPeriod(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return billing.Statement{}, false
	}
	statement, err := a.Billing.GetStatement(r.Context(), tenantID, from, to)
	if errors.Is(err, billing.ErrNotFound) {
		return billing.Statement{
			CustomerID: tenantID, Currency: "USD", PeriodStart: from, PeriodEnd: to,
			GeneratedAt: time.Now().UTC(), Entries: []billing.StatementEntry{},
		}, true
	}
	if errors.Is(err, billing.ErrStatementTooLarge) {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "statement contains more than " + strconv.Itoa(billing.MaxStatementEntries) + " entries; narrow the date range"})
		return billing.Statement{}, false
	}
	if err != nil {
		a.Log.Error("account: read billing statement", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing statement unavailable"})
		return billing.Statement{}, false
	}
	return statement, true
}

func tenantStatementResponse(statement billing.Statement) TenantBillingStatementResponse {
	entries := make([]TenantBillingStatementEntry, 0, len(statement.Entries))
	for _, entry := range statement.Entries {
		entries = append(entries, TenantBillingStatementEntry{
			LedgerID: entry.LedgerID, CreatedAt: entry.CreatedAt, EntryType: entry.EntryType,
			AmountMicroUSD: entry.AmountMicroUSD, BalanceDeltaMicroUSD: entry.BalanceDeltaMicroUSD,
			HeldDeltaMicroUSD: entry.HeldDeltaMicroUSD, NetCreditMicroUSD: entry.NetCreditMicroUSD,
			HeldMicroUSD: entry.HeldMicroUSD, WorkloadID: entry.WorkloadID, HoldID: entry.HoldID,
		})
	}
	return TenantBillingStatementResponse{
		TenantID: statement.CustomerID, Currency: statement.Currency,
		PeriodStart: statement.PeriodStart, PeriodEnd: statement.PeriodEnd, GeneratedAt: statement.GeneratedAt,
		LedgerWatermark:          statement.LedgerWatermark,
		OpeningNetCreditMicroUSD: statement.OpeningNetCreditMicroUSD, OpeningHeldMicroUSD: statement.OpeningHeldMicroUSD,
		ClosingNetCreditMicroUSD: statement.ClosingNetCreditMicroUSD, ClosingHeldMicroUSD: statement.ClosingHeldMicroUSD,
		Entries: entries,
	}
}

func (a *Accounts) HandleGetTenantBillingStatement(w http.ResponseWriter, r *http.Request) {
	statement, ok := a.readTenantStatement(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, tenantStatementResponse(statement))
}

func spreadsheetSafe(value string) string {
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed == "" {
		return value
	}
	switch trimmed[0] {
	case '=', '+', '-', '@':
		return "'" + value
	default:
		return value
	}
}

func (a *Accounts) HandleGetTenantBillingStatementCSV(w http.ResponseWriter, r *http.Request) {
	statement, ok := a.readTenantStatement(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="yscale-billing-statement.csv"`)
	writer := csv.NewWriter(w)
	header := []string{
		"tenant_id", "currency", "period_start", "period_end", "generated_at", "ledger_watermark",
		"opening_net_credit_micro_usd", "opening_held_micro_usd", "closing_net_credit_micro_usd", "closing_held_micro_usd",
		"ledger_id", "created_at", "entry_type", "amount_micro_usd", "balance_delta_micro_usd", "held_delta_micro_usd",
		"net_credit_micro_usd", "held_micro_usd", "workload_id", "hold_id",
	}
	if err := writer.Write(header); err != nil {
		a.Log.Error("account: write billing statement CSV header", "tenant", statement.CustomerID, "error", err)
		return
	}
	writeEntry := func(entry *billing.StatementEntry) error {
		record := []string{
			spreadsheetSafe(statement.CustomerID), statement.Currency, statement.PeriodStart.Format(time.RFC3339Nano),
			statement.PeriodEnd.Format(time.RFC3339Nano), statement.GeneratedAt.Format(time.RFC3339Nano),
			strconv.FormatInt(statement.LedgerWatermark, 10), strconv.FormatInt(statement.OpeningNetCreditMicroUSD, 10),
			strconv.FormatInt(statement.OpeningHeldMicroUSD, 10), strconv.FormatInt(statement.ClosingNetCreditMicroUSD, 10),
			strconv.FormatInt(statement.ClosingHeldMicroUSD, 10), "", "", "", "", "", "", "", "", "", "",
		}
		if entry != nil {
			record[10] = strconv.FormatInt(entry.LedgerID, 10)
			record[11] = entry.CreatedAt.Format(time.RFC3339Nano)
			record[12] = entry.EntryType
			record[13] = strconv.FormatInt(entry.AmountMicroUSD, 10)
			record[14] = strconv.FormatInt(entry.BalanceDeltaMicroUSD, 10)
			record[15] = strconv.FormatInt(entry.HeldDeltaMicroUSD, 10)
			record[16] = strconv.FormatInt(entry.NetCreditMicroUSD, 10)
			record[17] = strconv.FormatInt(entry.HeldMicroUSD, 10)
			record[18] = spreadsheetSafe(entry.WorkloadID)
			if entry.HoldID != nil {
				record[19] = strconv.FormatInt(*entry.HoldID, 10)
			}
		}
		return writer.Write(record)
	}
	if len(statement.Entries) == 0 {
		if err := writeEntry(nil); err != nil {
			a.Log.Error("account: write empty billing statement CSV row", "tenant", statement.CustomerID, "error", err)
			return
		}
	} else {
		for i := range statement.Entries {
			if err := writeEntry(&statement.Entries[i]); err != nil {
				a.Log.Error("account: write billing statement CSV row", "tenant", statement.CustomerID, "error", err)
				return
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		a.Log.Error("account: flush billing statement CSV", "tenant", statement.CustomerID, "error", err)
	}
}

func (a *Accounts) HandleGrantServiceCredit(w http.ResponseWriter, r *http.Request) {
	if a.Billing == nil {
		http.NotFound(w, r)
		return
	}
	operatorID := OperatorAccountFromContext(r.Context())
	if operatorID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "verified operator required"})
		return
	}
	tenantID := r.PathValue("tenant_id")
	if _, err := a.Store.TenantSummaryByIDContext(r.Context(), tenantID); err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxServiceCreditBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		AmountMicroUSD int64  `json:"amount_micro_usd"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit JSON"})
		}
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit JSON"})
		return
	}
	if err := a.Billing.EnsureAccount(r.Context(), tenantID); err != nil {
		a.writeBillingMutationError(w, tenantID, err)
		return
	}
	err := a.Billing.GrantCredit(r.Context(), billing.CreditGrantRequest{
		CustomerID: tenantID, AmountMicroUSD: req.AmountMicroUSD, IdempotencyKey: req.IdempotencyKey,
		Provider: "yscale", ProviderAccountID: operatorID, PaymentObjectID: serviceCreditObjectID(req.IdempotencyKey), LiveMode: a.Billing.LiveMode(),
	})
	if err != nil {
		a.writeBillingMutationError(w, tenantID, err)
		return
	}
	// Billing and the tenant journal are separate durable boundaries. The
	// economic mutation is exactly idempotent; the safe journal is at-least-once
	// across a retry because there is intentionally no cross-database outbox in
	// this checkpoint. A retry can append another identical safe decision row,
	// but can never move money twice.
	if err := a.Store.AppendAudit(state.NewAuditEvent(state.AuditEvent{
		CustomerID: tenantID, Actor: state.HumanActor(operatorID, tenantID),
		Action: state.ActionBillingServiceCreditGrant, Outcome: state.OutcomeAccepted,
		TargetKind: state.TargetTenant, TargetID: tenantID,
		Detail: state.AuditDetail{Reason: state.ReasonBillingServiceCreditGranted, AmountMicroUSD: req.AmountMicroUSD, Currency: "USD"},
	})); err != nil {
		a.Log.Error("operator: audit service credit grant", "tenant", tenantID, "operator", operatorID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing audit unavailable; retry with the same idempotency key"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID, "amount_micro_usd": req.AmountMicroUSD, "currency": "USD",
		"idempotency_key": req.IdempotencyKey, "granted": true,
	})
}

func (a *Accounts) HandleCreateTenantCheckout(w http.ResponseWriter, r *http.Request) {
	if a.Billing == nil || a.Checkout == nil {
		http.NotFound(w, r)
		return
	}
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	membership, err := a.Store.MembershipFor(caller.ID, tenantID)
	if err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	if membership.Role != state.RoleOwner && membership.Role != state.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden: purchasing credit requires owner or admin"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCheckoutBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var request struct {
		AmountMicroUSD int64  `json:"amount_micro_usd"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := dec.Decode(&request); err != nil {
		writeCheckoutDecodeError(w, err)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid checkout JSON"})
		return
	}
	minimum, maximum := a.CheckoutMinMicroUSD, a.CheckoutMaxMicroUSD
	if minimum == 0 {
		minimum = defaultCheckoutMinMicroUSD
	}
	if maximum == 0 {
		maximum = defaultCheckoutMaxMicroUSD
	}
	if request.AmountMicroUSD < minimum || request.AmountMicroUSD > maximum || request.AmountMicroUSD%10_000 != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "checkout amount is outside configured whole-cent bounds"})
		return
	}
	if err := a.Billing.EnsureAccount(r.Context(), tenantID); err != nil {
		a.writeCheckoutError(w, tenantID, err)
		return
	}
	checkout, inserted, err := a.Billing.BeginCheckout(r.Context(), billing.BeginCheckoutRequest{
		CustomerID: tenantID, IdempotencyKey: request.IdempotencyKey, AmountMicroUSD: request.AmountMicroUSD,
		Provider: "stripe", ProviderAccountID: a.Checkout.ProviderAccountID(), LiveMode: a.Checkout.LiveMode(),
	})
	if err != nil {
		a.writeCheckoutError(w, tenantID, err)
		return
	}
	if checkout.State == billing.CheckoutPendingProvider {
		providerSession, err := a.Checkout.CreateCheckout(r.Context(), billing.ProviderCheckoutRequest{
			CheckoutID: checkout.ID, CustomerID: tenantID, AmountMicroUSD: checkout.AmountMicroUSD,
		})
		if err != nil {
			a.Log.Error("account: create checkout provider session", "tenant", tenantID, "checkout", checkout.ID, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "checkout provider unavailable; retry with the same idempotency key",
				"id":    checkout.ID,
			})
			return
		}
		checkout, err = a.Billing.AttachCheckout(r.Context(), tenantID, checkout.ID, providerSession)
		if err != nil {
			a.writeCheckoutError(w, tenantID, err)
			return
		}
	}
	status := http.StatusOK
	if inserted {
		status = http.StatusCreated
	}
	writeJSON(w, status, tenantCheckoutResponse(checkout))
}

func (a *Accounts) HandleGetTenantCheckout(w http.ResponseWriter, r *http.Request) {
	if a.Billing == nil || a.Checkout == nil {
		http.NotFound(w, r)
		return
	}
	tenantID := r.PathValue("tenant_id")
	caller, ok := a.callerAccount(w, r)
	if !ok {
		return
	}
	if _, err := a.Store.MembershipFor(caller.ID, tenantID); err != nil {
		a.writeMembershipReadError(w, err)
		return
	}
	checkout, err := a.Billing.GetCheckout(r.Context(), tenantID, r.PathValue("id"))
	if errors.Is(err, billing.ErrNotFound) {
		tenantNotFound(w)
		return
	}
	if err != nil {
		a.writeCheckoutError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusOK, tenantCheckoutResponse(checkout))
}

func tenantCheckoutResponse(checkout billing.Checkout) TenantCheckoutResponse {
	response := TenantCheckoutResponse{
		ID: checkout.ID, TenantID: checkout.CustomerID, AmountMicroUSD: checkout.AmountMicroUSD,
		Currency: checkout.Currency, State: checkout.State, CreatedAt: checkout.CreatedAt, UpdatedAt: checkout.UpdatedAt,
	}
	if checkout.State == billing.CheckoutOpen {
		response.URL = checkout.URL
	}
	return response
}

func writeCheckoutDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid checkout JSON"})
}

func (a *Accounts) writeCheckoutError(w http.ResponseWriter, tenantID string, err error) {
	switch {
	case errors.Is(err, billing.ErrInvalidArgument), errors.Is(err, billing.ErrInvalidAmount):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid checkout request"})
	case errors.Is(err, billing.ErrIdempotencyConflict), errors.Is(err, billing.ErrEconomicObjectConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "checkout idempotency conflict"})
	case errors.Is(err, billing.ErrNotFound):
		tenantNotFound(w)
	default:
		a.Log.Error("account: checkout mutation", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing unavailable"})
	}
}

func serviceCreditObjectID(idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))
	return "svc_" + hex.EncodeToString(sum[:16])
}

func (a *Accounts) writeBillingMutationError(w http.ResponseWriter, tenantID string, err error) {
	switch {
	case errors.Is(err, billing.ErrInvalidArgument), errors.Is(err, billing.ErrInvalidAmount):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit request"})
	case errors.Is(err, billing.ErrIdempotencyConflict), errors.Is(err, billing.ErrEconomicObjectConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "service credit idempotency conflict"})
	default:
		a.Log.Error("operator: grant service credit", "tenant", tenantID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing unavailable"})
	}
}
