package billing

import (
	"math"
	"testing"
)

func TestTrustedCaptureMicroUSDRoundsAndCaps(t *testing.T) {
	amount, err := TrustedCaptureMicroUSD(0.0000001, 10)
	if err != nil || amount != 1 {
		t.Fatalf("rounded capture=%d err=%v", amount, err)
	}
	amount, err = TrustedCaptureMicroUSD(2, 50)
	if err != nil || amount != 50 {
		t.Fatalf("capped capture=%d err=%v", amount, err)
	}
	amount, err = TrustedCaptureMicroUSD(0, 50)
	if err != nil || amount != 0 {
		t.Fatalf("zero capture=%d err=%v", amount, err)
	}
}

func TestTrustedUsageMoneyRejectsInvalidFloats(t *testing.T) {
	for _, value := range []float64{-1, math.NaN(), math.Inf(1), float64(math.MaxInt64) / 1_000_000} {
		if _, err := TrustedCaptureMicroUSD(value, 1); err == nil {
			t.Fatalf("capture accepted %v", value)
		}
		if _, err := TrustedHourlyMicroUSD(value); err == nil {
			t.Fatalf("rate accepted %v", value)
		}
	}
	if _, err := TrustedCaptureMicroUSD(1, 0); err == nil {
		t.Fatal("capture accepted empty reservation")
	}
}
