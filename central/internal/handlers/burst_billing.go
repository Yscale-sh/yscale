package handlers

import (
	"context"
	"fmt"

	"github.com/yscale-sh/yscale/central/internal/billing"
	"github.com/yscale-sh/yscale/central/internal/state"
)

// settleBurstBilling charges only the trusted cost frozen by central at the
// provider-absence hinge. Stable keys make queue redelivery and reap retries
// economic no-ops.
func settleBurstBilling(ctx context.Context, ledger BurstBilling, b *state.Burst, observed *state.WorkloadCost) error {
	if b == nil || b.Billing == nil {
		return nil
	}
	if ledger == nil {
		return fmt.Errorf("prepaid billing store is unavailable")
	}
	if observed == nil || observed.BurstID != b.ID {
		return fmt.Errorf("trusted terminal cost is unavailable for burst %s", b.ID)
	}
	amount, err := billing.TrustedCaptureMicroUSD(observed.EstimatedUSD, b.Billing.ReservedMicroUSD)
	if err != nil {
		return err
	}
	if amount <= 0 {
		return ledger.ReleaseHold(ctx, b.CustomerID, b.Billing.HoldID,
			"burst-zero-release:"+b.ID, "terminal_zero_cost")
	}
	return ledger.CaptureHold(ctx, b.CustomerID, b.Billing.HoldID, amount,
		"burst-capture:"+b.ID, b.ID)
}
