// yscale:proprietary

package handlers

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

// The console list carries the submission path for a record that has one, and
// carries NOTHING for a record written before the field existed. Both cases in
// one response, because the console renders them side by side: "this burst was
// asked for by a controller" and "nobody recorded who asked" are different
// answers, and defaulting the second to the first would invent the one thing
// this field exists to state honestly.
func TestTenantWorkloadListExposesSubmissionOriginAndLeavesLegacyBlank(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{state.RoleOwner: state.RoleOwner})
	created := time.Now().UTC()
	for _, seed := range []struct {
		id     string
		origin protocol.SubmissionOrigin
	}{
		{"wl_origin_pod", protocol.OriginPendingPod},
		{"wl_origin_cr", protocol.OriginWorkloadCR},
		{"wl_origin_api", protocol.OriginAPI},
		{"wl_origin_legacy", ""},
	} {
		putTenantWorkload(store, seed.id, "cust_console", "burst_"+seed.id, created)
		record, err := store.GetWorkload(seed.id)
		if err != nil {
			t.Fatalf("seed %s: %v", seed.id, err)
		}
		record.SubmissionOrigin = seed.origin
		store.PutWorkload(record)
	}

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_"+state.RoleOwner, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	got := map[string]protocol.SubmissionOrigin{}
	for _, w := range decodeTenantWorkloads(t, rec).Workloads {
		got[w.ID] = w.SubmissionOrigin
	}
	if len(got) != 4 {
		t.Fatalf("listed %d workloads, want the 4 seeded: %v", len(got), got)
	}
	for id, want := range map[string]protocol.SubmissionOrigin{
		"wl_origin_pod":    protocol.OriginPendingPod,
		"wl_origin_cr":     protocol.OriginWorkloadCR,
		"wl_origin_api":    protocol.OriginAPI,
		"wl_origin_legacy": "",
	} {
		if got[id] != want {
			t.Errorf("%s submission_origin = %q, want %q", id, got[id], want)
		}
	}

	// Absent, not empty-string: a console reading the field back must be able to
	// tell "not recorded" from a value, and an omitted key is how every other
	// legacy field on this record says so.
	var raw struct {
		Workloads []map[string]json.RawMessage `json:"workloads"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw list: %v", err)
	}
	for _, w := range raw.Workloads {
		var id string
		if err := json.Unmarshal(w["id"], &id); err != nil {
			t.Fatalf("decode id: %v", err)
		}
		_, present := w["submission_origin"]
		if id == "wl_origin_legacy" && present {
			t.Errorf("legacy record carries submission_origin: %s", w["submission_origin"])
		}
		if id != "wl_origin_legacy" && !present {
			t.Errorf("%s dropped submission_origin", id)
		}
	}
}

// The single-workload read the console shares with the CLI answers the same way
// the list does, for a stored origin and for a legacy record that has none.
func TestTenantWorkloadGetExposesSubmissionOrigin(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{state.RoleOwner: state.RoleOwner})
	putTenantWorkload(store, "wl_get_origin", "cust_console", "burst_get_origin", time.Now().UTC())
	record, err := store.GetWorkload("wl_get_origin")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	record.SubmissionOrigin = protocol.OriginWorkloadCR
	store.PutWorkload(record)
	putTenantWorkload(store, "wl_get_legacy", "cust_console", "burst_get_legacy", time.Now().UTC())

	for _, tc := range []struct {
		id      string
		want    string
		present bool
	}{
		{id: "wl_get_origin", want: string(protocol.OriginWorkloadCR), present: true},
		{id: "wl_get_legacy"},
	} {
		rec := callTenantWorkload(accounts, http.MethodGet,
			"/v1/tenants/cust_console/workloads/"+tc.id, "human_"+state.RoleOwner, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s get status = %d, want 200 (body %q)", tc.id, rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s decode: %v", tc.id, err)
		}
		got, present := body["submission_origin"]
		if present != tc.present {
			t.Errorf("%s submission_origin present = %v, want %v", tc.id, present, tc.present)
			continue
		}
		if tc.present && got != tc.want {
			t.Errorf("%s submission_origin = %v, want %q", tc.id, got, tc.want)
		}
	}
}
