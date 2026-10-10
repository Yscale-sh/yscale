package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestPaidGateDefaultsToDisabled(t *testing.T) {
	g := NewPaidGate()
	if g.State() != PaidGateDisabled {
		t.Fatalf("default state = %v, want disabled", g.State())
	}
	if g.Allowed() {
		t.Fatal("disabled gate reports allowed")
	}
}

func TestPaidGateEnableMovesToNotReady(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	if g.State() != PaidGateNotReady {
		t.Fatalf("after enable state = %v, want not_ready", g.State())
	}
	if g.Allowed() {
		t.Fatal("not-ready gate reports allowed")
	}
}

func TestPaidGateAllPrereqsReady(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	if g.State() != PaidGateReady {
		t.Fatalf("all prereqs set: state = %v, want ready", g.State())
	}
	if !g.Allowed() {
		t.Fatal("ready gate reports not allowed")
	}
}

func TestPaidGateEachPrereqIndependentlyPreventsReady(t *testing.T) {
	for skip := 0; skip < prereqCount; skip++ {
		g := NewPaidGate()
		g.Enable()
		for i := 0; i < prereqCount; i++ {
			if i != skip {
				g.SetReady(PaidGatePrerequisite(i))
			}
		}
		if g.State() == PaidGateReady {
			t.Fatalf("missing prereq %d (%s): gate should not be ready",
				skip, PaidGatePrerequisite(skip))
		}
		st, _, missing := g.Status()
		if st == PaidGateReady {
			t.Fatalf("status reports ready with missing prereq %d", skip)
		}
		if len(missing) != 1 || missing[0] != PaidGatePrerequisite(skip).String() {
			t.Fatalf("missing prereqs = %v, want [%s]", missing, PaidGatePrerequisite(skip))
		}
	}
}

func TestPaidGateClearReadyReverts(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	if !g.Allowed() {
		t.Fatal("gate not ready after setting all prereqs")
	}
	g.ClearReady(PrereqOperatorKillSwitch)
	if g.Allowed() {
		t.Fatal("gate still allowed after clearing kill switch")
	}
	if g.State() != PaidGateNotReady {
		t.Fatalf("state = %v, want not_ready", g.State())
	}
}

func TestPaidGateDisableOverridesReady(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	if !g.Allowed() {
		t.Fatal("gate not ready after setting all prereqs")
	}
	g.Disable()
	if g.Allowed() {
		t.Fatal("gate still allowed after disable")
	}
	if g.State() != PaidGateDisabled {
		t.Fatalf("state = %v, want disabled", g.State())
	}
}

func TestPaidGateConcurrentAccess(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	errCh := make(chan string, 100)

	// Concurrent readers checking allowed
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					g.Allowed()
					g.State()
					g.Status()
				}
			}
		}()
	}

	// Writer that disables and re-enables
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			g.Disable()
			g.Enable()
			for j := 0; j < prereqCount; j++ {
				g.SetReady(PaidGatePrerequisite(j))
			}
		}
		close(done)
	}()

	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e)
	default:
	}
}

func TestPaidGateConcurrentClosurePreventsAllowedAfterClose(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	if !g.Allowed() {
		t.Fatal("gate not ready before closure test")
	}

	// Kill switch closure must be atomic with reads.
	g.ClearReady(PrereqOperatorKillSwitch)

	// After ClearReady returns, no concurrent reader should see Allowed() = true.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.Allowed() {
				t.Error("saw allowed after kill switch cleared")
			}
		}()
	}
	wg.Wait()
}

func TestPaidGateSetReadyOnDisabledIsNoOp(t *testing.T) {
	g := NewPaidGate()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	if g.State() != PaidGateDisabled {
		t.Fatalf("disabled gate changed state on SetReady: %v", g.State())
	}
	g.Enable()
	if g.State() != PaidGateNotReady {
		t.Fatalf("disabled gate retained pre-armed prerequisites: %v", g.State())
	}
}

func TestReadyzPaidHandlerDisabled(t *testing.T) {
	g := NewPaidGate()
	handler := ReadyzPaidHandler(g)

	rr := httptest.NewRecorder()
	handler(rr, httptest.NewRequest("GET", "/readyz/paid", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled: status = %d, want 503", rr.Code)
	}
	var resp struct {
		State   string   `json:"state"`
		Reason  string   `json:"reason"`
		Missing []string `json:"missing"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.State != "disabled" {
		t.Fatalf("state = %q, want disabled", resp.State)
	}
}

func TestReadyzPaidHandlerNotReady(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	g.SetReady(PrereqBillingDB)
	handler := ReadyzPaidHandler(g)

	rr := httptest.NewRecorder()
	handler(rr, httptest.NewRequest("GET", "/readyz/paid", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("not_ready: status = %d, want 503", rr.Code)
	}
	var resp struct {
		State   string   `json:"state"`
		Missing []string `json:"missing"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.State != "not_ready" {
		t.Fatalf("state = %q, want not_ready", resp.State)
	}
	if len(resp.Missing) != prereqCount-1 {
		t.Fatalf("missing = %v, want %d entries", resp.Missing, prereqCount-1)
	}
}

func TestReadyzPaidHandlerReady(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	for i := 0; i < prereqCount; i++ {
		g.SetReady(PaidGatePrerequisite(i))
	}
	handler := ReadyzPaidHandler(g)

	rr := httptest.NewRecorder()
	handler(rr, httptest.NewRequest("GET", "/readyz/paid", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("ready: status = %d, want 200", rr.Code)
	}
	var resp struct {
		State   string   `json:"state"`
		Missing []string `json:"missing"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.State != "ready" {
		t.Fatalf("state = %q, want ready", resp.State)
	}
	if len(resp.Missing) != 0 {
		t.Fatalf("missing = %v, want empty", resp.Missing)
	}
}

func TestReadyzPaidHandlerLeaksNoSecrets(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	handler := ReadyzPaidHandler(g)

	rr := httptest.NewRecorder()
	handler(rr, httptest.NewRequest("GET", "/readyz/paid", nil))

	body := rr.Body.String()
	for _, sensitive := range []string{"postgres", "dsn", "password", "token", "key", "secret"} {
		if containsCI(body, sensitive) {
			t.Fatalf("response contains sensitive word %q: %s", sensitive, body)
		}
	}
}

func containsCI(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			c := s[i+j]
			if c >= 'A' && c <= 'Z' {
				c += 32
			}
			sc := substr[j]
			if sc >= 'A' && sc <= 'Z' {
				sc += 32
			}
			if c != sc {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func TestPaidGateStateString(t *testing.T) {
	tests := []struct {
		state PaidGateState
		want  string
	}{
		{PaidGateDisabled, "disabled"},
		{PaidGateNotReady, "not_ready"},
		{PaidGateReady, "ready"},
		{PaidGateState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("PaidGateState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestPrereqString(t *testing.T) {
	if s := PrereqBillingDB.String(); s != "billing_db" {
		t.Fatalf("PrereqBillingDB.String() = %q", s)
	}
	if s := PaidGatePrerequisite(99).String(); s != "prereq_99" {
		t.Fatalf("out-of-range prereq string = %q", s)
	}
	if s := PaidGatePrerequisite(-1).String(); s != "prereq_-1" {
		t.Fatalf("negative prereq string = %q", s)
	}
}

func TestPaidGateNegativePrereqNoPanic(t *testing.T) {
	g := NewPaidGate()
	g.Enable()
	g.SetReady(PaidGatePrerequisite(-1))
	g.ClearReady(PaidGatePrerequisite(-1))
	g.SetReady(PaidGatePrerequisite(9999))
	g.ClearReady(PaidGatePrerequisite(9999))
	if g.State() != PaidGateNotReady {
		t.Fatalf("state = %v after invalid prereqs", g.State())
	}
}
