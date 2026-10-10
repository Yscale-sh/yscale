package billing

import (
	"fmt"
	"math"
)

// TrustedCaptureMicroUSD converts a trusted USD usage receipt to the amount
// consumed from its prepaid hold. This is shared by settlement and read-only
// reconciliation so their rounding and reservation cap cannot drift.
func TrustedCaptureMicroUSD(estimatedUSD float64, reservedMicroUSD int64) (int64, error) {
	if math.IsNaN(estimatedUSD) || math.IsInf(estimatedUSD, 0) || estimatedUSD < 0 {
		return 0, fmt.Errorf("billing: invalid trusted usage amount")
	}
	if reservedMicroUSD <= 0 {
		return 0, fmt.Errorf("billing: invalid trusted usage reservation")
	}
	microUSD := estimatedUSD * 1_000_000
	if math.IsInf(microUSD, 0) || microUSD >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("billing: trusted usage amount overflows micro-USD")
	}
	amount := int64(math.Ceil(microUSD))
	if amount > reservedMicroUSD {
		amount = reservedMicroUSD
	}
	return amount, nil
}

// TrustedHourlyMicroUSD applies the quote-time conversion used by placement.
func TrustedHourlyMicroUSD(hourlyUSD float64) (int64, error) {
	if math.IsNaN(hourlyUSD) || math.IsInf(hourlyUSD, 0) || hourlyUSD < 0 {
		return 0, fmt.Errorf("billing: invalid trusted hourly rate")
	}
	microUSD := hourlyUSD * 1_000_000
	if math.IsInf(microUSD, 0) || microUSD >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("billing: trusted hourly rate overflows micro-USD")
	}
	return int64(math.Ceil(microUSD)), nil
}
