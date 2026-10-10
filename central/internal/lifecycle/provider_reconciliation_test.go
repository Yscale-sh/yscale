package lifecycle

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestProviderResourceIdentityUsesBoundedUTF8Descriptors(t *testing.T) {
	if err := validateProviderResourceIdentity("provider/with.punctuation", "account:tenant/1", "vm/id with punctuation!"); err != nil {
		t.Fatalf("provider descriptors should accept bounded UTF-8 punctuation: %v", err)
	}
	for _, tc := range []struct {
		name       string
		provider   string
		account    string
		resourceID string
	}{
		{name: "empty provider", provider: "", resourceID: "resource"},
		{name: "blank provider", provider: " \t", resourceID: "resource"},
		{name: "empty resource", provider: "provider", resourceID: ""},
		{name: "oversized provider", provider: strings.Repeat("a", maxIdentifierBytes+1), resourceID: "resource"},
		{name: "nul resource", provider: "provider", resourceID: "resource\x00id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateProviderResourceIdentity(tc.provider, tc.account, tc.resourceID); err == nil {
				t.Fatal("expected invalid provider resource identity")
			}
		})
	}
}

func TestProviderResourceObservationPreflightValidation(t *testing.T) {
	valid := ProviderResourceObservation{Provider: "linode", CloudAccountID: "account",
		ProviderResourceID: "resource", ObservedAt: time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)}
	for _, tc := range []struct {
		name string
		edit func(*ProviderResourceObservation)
		bad  bool
	}{
		{"empty_optional_fields", func(o *ProviderResourceObservation) { o.CloudAccountID = "" }, false},
		{"unicode_name", func(o *ProviderResourceObservation) { o.ResourceName = "GPU – test" }, false},
		{"long_name_normalized_by_store", func(o *ProviderResourceObservation) { o.ResourceName = strings.Repeat("x", 300) }, false},
		{"missing_timestamp", func(o *ProviderResourceObservation) { o.ObservedAt = time.Time{} }, true},
		{"missing_id", func(o *ProviderResourceObservation) { o.ProviderResourceID = "" }, true},
		{"oversized_id", func(o *ProviderResourceObservation) { o.ProviderResourceID = strings.Repeat("x", maxIdentifierBytes+1) }, true},
		{"nul_id", func(o *ProviderResourceObservation) { o.ProviderResourceID = "bad\x00id" }, true},
		{"invalid_utf8_id", func(o *ProviderResourceObservation) { o.ProviderResourceID = "bad\xffid" }, true},
		{"nul_name", func(o *ProviderResourceObservation) { o.ResourceName = "bad\x00name" }, true},
		{"invalid_utf8_name", func(o *ProviderResourceObservation) { o.ResourceName = "bad\xffname" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := valid
			tc.edit(&obs)
			err := ValidateProviderResourceObservation(obs)
			if (tc.bad && !errors.Is(err, ErrInvalidArgument)) || (!tc.bad && err != nil) {
				t.Fatalf("bad=%v validation=%v", tc.bad, err)
			}
		})
	}
}
