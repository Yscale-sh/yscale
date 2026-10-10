package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CentralEvidenceClient reads the authenticated GET /v1/workloads/{id}
// endpoint and extracts the durable evidence fields added by issue #93.
type CentralEvidenceClient struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// WorkloadEvidence is the tenant-safe subset of the workload detail
// response needed to prove provider lifecycle and durable teardown.
type WorkloadEvidence struct {
	Status     string             `json:"status"`
	CreatedAt  *time.Time         `json:"created_at,omitempty"`
	StartedAt  *time.Time         `json:"started_at,omitempty"`
	FinishedAt *time.Time         `json:"finished_at,omitempty"`
	Placement  *PlacementEvidence `json:"placement,omitempty"`
	Cost       *CostEvidence      `json:"cost,omitempty"`
	Cleanup    *CleanupEvidence   `json:"cleanup,omitempty"`
	Outcome    *OutcomeEvidence   `json:"outcome,omitempty"`
}

// OutcomeEvidence is the tenant-safe projection of central's stored receipt: a
// bounded compute result and, when the workload configured an export, the
// artifact result plus its nullable counts. Central's response also carries
// free-form reason strings; declining to declare them here is how they stay
// out of the retained artifact by construction.
type OutcomeEvidence struct {
	Compute   ComputeOutcomeEvidence   `json:"compute"`
	Artifacts *ArtifactOutcomeEvidence `json:"artifacts,omitempty"`
}

// ComputeOutcomeEvidence retains ONLY the receipt's result. No Reason field is
// declared: any reason field on the wire is discarded by json.Unmarshal.
type ComputeOutcomeEvidence struct {
	Result string `json:"result"`
}

// ArtifactOutcomeEvidence retains the export result and its nullable counts.
// The counts stay pointers so nil (the agent did not count) and 0 (the agent
// counted zero) remain distinguishable through the whole write path.
type ArtifactOutcomeEvidence struct {
	Result          string `json:"result"`
	ObjectsUploaded *int64 `json:"objects_uploaded"`
	BytesUploaded   *int64 `json:"bytes_uploaded"`
}

// PlacementEvidence retains the tenant-safe routing fields from the
// placement receipt. Provider resource IDs and cloud account IDs are
// never present in the public receipt.
type PlacementEvidence struct {
	GPUProduct string                    `json:"gpu_product,omitempty"`
	Receipt    *PlacementReceiptEvidence `json:"receipt,omitempty"`
}

// PlacementReceiptEvidence captures the provider/region/SKU from the
// placement receipt's selected candidate, plus the quote identity.
//
// IssuedAt is the sealed quote-issuance timestamp — the moment the placement
// decision that admitted this workload was recorded — and is the authoritative
// admission-stage observation. Workload.CreatedAt is written AFTER central
// returns from the provider create call, so it can arrive slightly after
// provider_created_at on a fast path; issued_at is always strictly before
// both.
type PlacementReceiptEvidence struct {
	Selected       SelectedEvidence `json:"selected"`
	QuoteID        string           `json:"quote_id"`
	IssuedAt       time.Time        `json:"issued_at"`
	PricingVersion int              `json:"pricing_version"`
}

type SelectedEvidence struct {
	Provider              string `json:"provider"`
	Region                string `json:"region"`
	SKU                   string `json:"sku"`
	GPUKind               string `json:"gpu_kind"`
	GPUCount              int    `json:"gpu_count"`
	HourlyMicroUSD        int64  `json:"hourly_micro_usd"`
	MaximumChargeMicroUSD int64  `json:"maximum_charge_micro_usd"`
}

// CostEvidence retains the frozen terminal cost observation.
type CostEvidence struct {
	USD       float64 `json:"usd"`
	HourlyUSD float64 `json:"hourly_usd"`
	Basis     string  `json:"basis"`
}

// CleanupEvidence mirrors the CleanupResponse fields the evidence
// client cares about. Provider resource IDs, cloud account IDs,
// payloads, and lease tokens are absent by construction.
type CleanupEvidence struct {
	State              string     `json:"state"`
	RequestedAt        *time.Time `json:"requested_at,omitempty"`
	DeletedAt          *time.Time `json:"deleted_at,omitempty"`
	ProviderCreatedAt  *time.Time `json:"provider_created_at,omitempty"`
	DurableReapReceipt bool       `json:"durable_reap_receipt"`
}

// ProbeAuth proves the configured credential is accepted before yscaletest
// creates a Workload. The dedicated read-only endpoint returns 204 only after
// ConnectorAuth succeeds, so a proxy fallback or missing route cannot be
// mistaken for authentication. No provider mutation or workload creation is
// involved.
func (c *CentralEvidenceClient) ProbeAuth(ctx context.Context) error {
	base := strings.TrimRight(c.BaseURL, "/")
	reqURL := base + "/v1/agent/auth-check"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("build evidence auth probe: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("evidence auth probe: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %s", errEvidenceAuth, resp.Status)
	default:
		return fmt.Errorf("evidence auth probe returned %s", resp.Status)
	}
}

// GetWorkloadEvidence calls GET /v1/workloads/{id} with the scoped
// connector/tenant token and returns only the evidence-relevant fields.
func (c *CentralEvidenceClient) GetWorkloadEvidence(ctx context.Context, workloadID string) (*WorkloadEvidence, error) {
	base := strings.TrimRight(c.BaseURL, "/")
	reqURL := base + "/v1/workloads/" + url.PathEscape(workloadID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build evidence request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("evidence request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read evidence response: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: %s", errEvidenceAuth, resp.Status)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("evidence request returned %s", resp.Status)
	}

	var result WorkloadEvidence
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode evidence: %w", err)
	}
	return &result, nil
}
