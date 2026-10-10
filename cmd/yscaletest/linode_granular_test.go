package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// granularLinodeStub serves canned responses for all Linode audit endpoints.
type granularLinodeStub struct {
	instancePages       map[string]string
	instanceByID        map[int]int // providerID → status code
	volumePages         map[string]string
	firewallPages       map[string]string
	firewallDevicePages map[string]string
	defaultStatus       int

	mu         sync.Mutex
	methods    []string
	paths      []string
	authTokens []string
	srv        *httptest.Server
}

func (s *granularLinodeStub) observe(method, path, auth string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, method)
	s.paths = append(s.paths, path)
	s.authTokens = append(s.authTokens, auth)
}

func (s *granularLinodeStub) seen() (methods, paths, tokens []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.methods...),
		append([]string(nil), s.paths...),
		append([]string(nil), s.authTokens...)
}

func newGranularLinodeStub(t *testing.T, s *granularLinodeStub) *linodeInventory {
	t.Helper()
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.observe(r.Method, r.URL.Path, r.Header.Get("Authorization"))
		page := r.URL.Query().Get("page")

		if s.defaultStatus != 0 {
			w.WriteHeader(s.defaultStatus)
			fmt.Fprintf(w, `{"errors":[{"reason":"bad: %s"}]}`, fakeLinodeToken)
			return
		}

		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/linode/instances/"):
			// Exact instance lookup
			for id, status := range s.instanceByID {
				if strings.HasSuffix(path, fmt.Sprintf("/%d", id)) {
					w.WriteHeader(status)
					if status == http.StatusOK {
						fmt.Fprintf(w, `{"id":%d}`, id)
					}
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
			return

		case path == "/linode/instances":
			body, ok := s.instancePages[page]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, body)
			return

		case path == "/volumes":
			body, ok := s.volumePages[page]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, body)
			return

		case path == "/networking/firewalls":
			body, ok := s.firewallPages[page]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, body)
			return

		case strings.Contains(path, "/devices"):
			body, ok := s.firewallDevicePages[page]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, body)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(s.srv.Close)

	inv := newLinodeInventory(fakeLinodeToken)
	inv.baseURL = s.srv.URL
	return inv
}

func TestCountExactInstancePresent(t *testing.T) {
	stub := &granularLinodeStub{
		instanceByID: map[int]int{12345: http.StatusOK},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountExactInstance(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountExactInstance: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1, got %d", count)
	}
}

func TestCountExactInstanceAbsent(t *testing.T) {
	stub := &granularLinodeStub{
		instanceByID: map[int]int{},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountExactInstance(context.Background(), 99999)
	if err != nil {
		t.Fatalf("CountExactInstance: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0, got %d", count)
	}
}

func TestCountExactInstanceAPIError(t *testing.T) {
	stub := &granularLinodeStub{
		instanceByID: map[int]int{12345: http.StatusInternalServerError},
	}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.CountExactInstance(context.Background(), 12345)
	if err == nil {
		t.Fatal("should fail on API error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error should include status code, got %q", err.Error())
	}
}

func TestCountExactInstanceInvalidID(t *testing.T) {
	stub := &granularLinodeStub{}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.CountExactInstance(context.Background(), 0)
	if err == nil {
		t.Fatal("should reject invalid provider ID")
	}
}

func TestCountAttachedVolumesFindsAttached(t *testing.T) {
	stub := &granularLinodeStub{
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":1,"linode_id":12345},
				{"id":2,"linode_id":null},
				{"id":3,"linode_id":99999},
				{"id":4,"linode_id":12345}
			]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountAttachedVolumes(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountAttachedVolumes: %v", err)
	}
	if count != 2 {
		t.Fatalf("want 2, got %d", count)
	}
}

func TestCountAttachedVolumesClean(t *testing.T) {
	stub := &granularLinodeStub{
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":1,"linode_id":99999}]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountAttachedVolumes(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountAttachedVolumes: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0, got %d", count)
	}
}

func TestCountAttachedVolumesPagination(t *testing.T) {
	stub := &granularLinodeStub{
		volumePages: map[string]string{
			"1": `{"page":1,"pages":2,"data":[{"id":1,"linode_id":12345}]}`,
			"2": `{"page":2,"pages":2,"data":[{"id":2,"linode_id":12345}]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountAttachedVolumes(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountAttachedVolumes: %v", err)
	}
	if count != 2 {
		t.Fatalf("want 2 across both pages, got %d", count)
	}
}

func TestCountAttachedVolumesTruncation(t *testing.T) {
	stub := &granularLinodeStub{
		volumePages: map[string]string{
			"1": `{"page":1,"pages":9999,"data":[]}`,
			"2": `{"page":2,"pages":9999,"data":[]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)
	inv.maxPages = 2

	_, err := inv.CountAttachedVolumes(context.Background(), 12345)
	if err == nil {
		t.Fatal("should reject truncated volume listing")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("error should mention truncated, got %q", err.Error())
	}
}

func TestCountFirewallDevicesFindsAttached(t *testing.T) {
	stub := &granularLinodeStub{
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":100,"label":"yscale-burst-fw"},
				{"id":200,"label":"other-fw"}
			]}`,
		},
		firewallDevicePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":1,"entity":{"id":12345,"type":"linode"}},
				{"id":2,"entity":{"id":99999,"type":"linode"}},
				{"id":3,"entity":{"id":12345,"type":"nodebalancer"}}
			]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountFirewallDevices(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountFirewallDevices: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 (only type=linode entity.id=12345), got %d", count)
	}
}

func TestCountFirewallDevicesNoFirewall(t *testing.T) {
	stub := &granularLinodeStub{
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":200,"label":"other-fw"}]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountFirewallDevices(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountFirewallDevices: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0 (no yscale-burst-fw), got %d", count)
	}
}

func TestCountFirewallDevicesClean(t *testing.T) {
	stub := &granularLinodeStub{
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":100,"label":"yscale-burst-fw"}]}`,
		},
		firewallDevicePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":1,"entity":{"id":99999,"type":"linode"}}
			]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	count, err := inv.CountFirewallDevices(context.Background(), 12345)
	if err != nil {
		t.Fatalf("CountFirewallDevices: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0, got %d", count)
	}
}

func TestAuditBurstResidueFullSuccess(t *testing.T) {
	stub := &granularLinodeStub{
		instancePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		instanceByID: map[int]int{},
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	residue, err := inv.AuditBurstResidue(context.Background(), testBurstID, 12345)
	if err != nil {
		t.Fatalf("AuditBurstResidue: %v", err)
	}
	if residue.Total() != 0 {
		t.Fatalf("want zero residue, got %+v", residue)
	}
}

func TestAuditBurstResidueWithResidue(t *testing.T) {
	stub := &granularLinodeStub{
		instancePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":12345,"tags":["yscale-burst","burst_abc123def456"]}
			]}`,
		},
		instanceByID: map[int]int{12345: http.StatusOK},
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":1,"linode_id":12345}]}`,
		},
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":100,"label":"yscale-burst-fw"}]}`,
		},
		firewallDevicePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[
				{"id":1,"entity":{"id":12345,"type":"linode"}}
			]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	residue, err := inv.AuditBurstResidue(context.Background(), testBurstID, 12345)
	if err != nil {
		t.Fatalf("AuditBurstResidue: %v", err)
	}
	if residue.TaggedInstances != 1 {
		t.Fatalf("tagged = %d, want 1", residue.TaggedInstances)
	}
	if residue.ExactIDInstance != 1 {
		t.Fatalf("exact ID = %d, want 1", residue.ExactIDInstance)
	}
	if !residue.ExactIDTagged {
		t.Fatal("exact provider ID should be recognized as the tagged instance")
	}
	if residue.AttachedVolumes != 1 {
		t.Fatalf("volumes = %d, want 1", residue.AttachedVolumes)
	}
	if residue.FirewallDevices != 1 {
		t.Fatalf("firewall = %d, want 1", residue.FirewallDevices)
	}
	if residue.Total() != 4 {
		t.Fatalf("total = %d, want 4", residue.Total())
	}
	if residue.UniqueTotal() != 3 {
		t.Fatalf("unique total = %d, want 3", residue.UniqueTotal())
	}
}

func TestAuditBurstResidueExactIDWithoutTags(t *testing.T) {
	stub := &granularLinodeStub{
		instancePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		instanceByID: map[int]int{12345: http.StatusOK},
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	residue, err := inv.AuditBurstResidue(context.Background(), testBurstID, 12345)
	if err != nil {
		t.Fatalf("AuditBurstResidue: %v", err)
	}
	if residue.TaggedInstances != 0 {
		t.Fatalf("tagged = %d, want 0", residue.TaggedInstances)
	}
	if residue.ExactIDInstance != 1 {
		t.Fatalf("exact ID = %d, want 1 (instance exists without tags)", residue.ExactIDInstance)
	}
}

func TestAuditBurstResidueUsesOnlyGETAndRedactsErrors(t *testing.T) {
	stub := &granularLinodeStub{
		instancePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		instanceByID: map[int]int{},
		volumePages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[]}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.AuditBurstResidue(context.Background(), testBurstID, 12345)
	if err != nil {
		t.Fatalf("AuditBurstResidue: %v", err)
	}

	methods, _, tokens := stub.seen()
	for _, m := range methods {
		if m != http.MethodGet {
			t.Fatalf("audit used %s method, want GET-only", m)
		}
	}
	for _, tok := range tokens {
		if tok != "Bearer "+fakeLinodeToken {
			t.Fatalf("token not sent as bearer, got %q", tok)
		}
	}
}

func TestCountAttachedVolumesMissingPaginationMetadata(t *testing.T) {
	stub := &granularLinodeStub{
		volumePages: map[string]string{
			"1": `{"data":[],"page":0,"pages":0}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.CountAttachedVolumes(context.Background(), 12345)
	if err == nil {
		t.Fatal("zero pagination metadata should fail")
	}
	if !strings.Contains(err.Error(), "invalid pagination") {
		t.Fatalf("error should mention invalid pagination, got %q", err.Error())
	}
}

func TestCountFirewallDevicesMissingPaginationMetadata(t *testing.T) {
	stub := &granularLinodeStub{
		firewallPages: map[string]string{
			"1": `{"page":1,"pages":1,"data":[{"id":100,"label":"yscale-burst-fw"}]}`,
		},
		firewallDevicePages: map[string]string{
			"1": `{"data":[],"page":0,"pages":0}`,
		},
	}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.CountFirewallDevices(context.Background(), 12345)
	if err == nil {
		t.Fatal("zero pagination metadata in devices should fail")
	}
	if !strings.Contains(err.Error(), "invalid pagination") {
		t.Fatalf("error should mention invalid pagination, got %q", err.Error())
	}
}

func TestAuditBurstResidueRedactsResponseBodies(t *testing.T) {
	stub := &granularLinodeStub{
		defaultStatus: http.StatusUnauthorized,
	}
	inv := newGranularLinodeStub(t, stub)

	_, err := inv.AuditBurstResidue(context.Background(), testBurstID, 12345)
	if err == nil {
		t.Fatal("should fail on API error")
	}
	msg := err.Error()
	if strings.Contains(msg, fakeLinodeToken) {
		t.Fatalf("error leaked token: %q", msg)
	}
}
