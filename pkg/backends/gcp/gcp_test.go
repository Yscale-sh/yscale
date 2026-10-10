package gcp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/proto"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// mapType picks the smallest E2 machine type that fits the workload's
// memory, mirroring gcp-plan.md §6's table. A wrong threshold either
// starves the burst (kubelet/containerd can't fit) or silently
// overbills the customer for a bigger machine than requested.
func TestMapType(t *testing.T) {
	cases := []struct {
		name     string
		memoryMB int64
		want     string
	}{
		{"floor", 1, "e2-small"},
		{"at 1024 boundary", 1024, "e2-small"},
		{"just over 1024", 1025, "e2-medium"},
		{"at 3072 boundary", 3072, "e2-medium"},
		{"just over 3072", 3073, "e2-standard-2"},
		{"at 6144 boundary", 6144, "e2-standard-2"},
		{"just over 6144", 6145, "e2-standard-4"},
		{"at 14336 boundary", 14336, "e2-standard-4"},
		{"just over 14336", 14337, "e2-standard-8"},
		{"large request", 65536, "e2-standard-8"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mapType(backends.ResourceRequirements{MemoryMB: c.memoryMB})
			if got != c.want {
				t.Errorf("mapType(%d) = %q, want %q", c.memoryMB, got, c.want)
			}
		})
	}
}

// GPU is a deliberately unimplemented seam (GCP GPU quota is unapproved —
// see gcp-plan.md §13): CreateNode must reject a GPU request loudly and
// before touching the network, never guess a sizing catalog entry that
// has never been exercised. A zero-value Backend (no client, no New())
// proves no API call happens first.
func TestCreateNode_GPURejectedFast(t *testing.T) {
	b := &Backend{}
	_, err := b.CreateNode(context.Background(), &backends.NodeSpec{
		Name:    "ys-burst-x",
		BurstID: "burst_x",
		Resources: backends.ResourceRequirements{
			GPU: &backends.GPUSpec{Kind: "h100", Count: 8},
		},
	})
	if err == nil {
		t.Fatal("expected CreateNode to reject a GPU request, got nil")
	}
	if !strings.Contains(err.Error(), "GPU not yet supported on gcp") {
		t.Errorf("error should name the gap plainly, got: %v", err)
	}
	if backends.CreateOutcomeAmbiguous(err) {
		t.Fatalf("pre-dispatch GPU validation must prove zero resource: %v", err)
	}
}

func TestGCPCreateRequestRefusedClassification(t *testing.T) {
	clientErr, _ := apierror.FromError(&googleapi.Error{Code: 400, Message: "bad request"})
	conflictErr, _ := apierror.FromError(&googleapi.Error{Code: 409, Message: "already exists"})
	serverErr, _ := apierror.FromError(&googleapi.Error{Code: 503, Message: "unavailable"})
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "client refusal", err: clientErr, want: true},
		{name: "conflict may already exist", err: conflictErr, want: false},
		{name: "server failure", err: serverErr, want: false},
		{name: "transport failure", err: context.DeadlineExceeded, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gcpCreateRequestRefused(tc.err); got != tc.want {
				t.Fatalf("gcpCreateRequestRefused = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMapGPUType_AlwaysErrors(t *testing.T) {
	_, err := mapGPUType(&backends.GPUSpec{Kind: "l4", Count: 1})
	if err == nil {
		t.Fatal("expected mapGPUType to always error, got nil")
	}
}

// mapStatus mirrors the table in gcp-plan.md §4. A preempted Spot VM and
// a deliberately stopped one both land in TERMINATED — both must map to
// NodeStopped; the caller reconciles against its own intent, not this
// mapping.
func TestMapStatus(t *testing.T) {
	cases := []struct {
		status string
		want   backends.NodePhase
	}{
		{"RUNNING", backends.NodeRunning},
		{"PROVISIONING", backends.NodeStarting},
		{"STAGING", backends.NodeStarting},
		{"STOPPING", backends.NodeStopped},
		{"SUSPENDING", backends.NodeStopped},
		{"SUSPENDED", backends.NodeStopped},
		{"TERMINATED", backends.NodeStopped},
		{"STOPPED", backends.NodeStopped},
		{"REPAIRING", backends.NodeUnknown},
		{"", backends.NodeUnknown},
		{"SOME_FUTURE_STATUS", backends.NodeUnknown},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			if got := mapStatus(c.status); got != c.want {
				t.Errorf("mapStatus(%q) = %q, want %q", c.status, got, c.want)
			}
		})
	}
}

// ownsInstance is the hard safety boundary CleanupOrphans/ListPooledNodes/
// findOwnedInstance all delegate to: it must claim yscale's own instances
// in the exact configured scope, and NOTHING else — a non-yscale instance
// sharing the project, or a yscale instance in a different controller's
// scope, must never be claimed (porting methodology §10).
func TestOwnsInstance(t *testing.T) {
	yscaleInstance := func(labels map[string]string) *computepb.Instance {
		return &computepb.Instance{Labels: labels}
	}

	cases := []struct {
		name   string
		inst   *computepb.Instance
		scope  string
		claims bool
	}{
		{
			name:   "yscale instance, matching empty scope",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: ownerLabelValue, scopeLabelKey: ""}),
			scope:  "",
			claims: true,
		},
		{
			name:   "yscale instance, matching non-empty scope",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: ownerLabelValue, scopeLabelKey: "abc123"}),
			scope:  "abc123",
			claims: true,
		},
		{
			name:   "non-yscale instance (no owner label) is never claimed",
			inst:   yscaleInstance(map[string]string{"env": "prod"}),
			scope:  "",
			claims: false,
		},
		{
			name:   "non-yscale instance with an unrelated owner value is never claimed",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: "someone-else"}),
			scope:  "",
			claims: false,
		},
		{
			name:   "wrong ScopeHash is never claimed by an empty-scope backend",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: ownerLabelValue, scopeLabelKey: "abc123"}),
			scope:  "",
			claims: false,
		},
		{
			name:   "wrong ScopeHash is never claimed by a differently-scoped backend",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: ownerLabelValue, scopeLabelKey: "abc123"}),
			scope:  "xyz789",
			claims: false,
		},
		{
			name:   "empty scope is not a wildcard over a scoped instance",
			inst:   yscaleInstance(map[string]string{ownerLabelKey: ownerLabelValue, scopeLabelKey: "abc123"}),
			scope:  "",
			claims: false,
		},
		{
			name:   "no labels at all is never claimed",
			inst:   &computepb.Instance{},
			scope:  "",
			claims: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ownsInstance(c.inst, c.scope); got != c.claims {
				t.Errorf("ownsInstance(scope=%q) = %v, want %v", c.scope, got, c.claims)
			}
		})
	}
}

func TestOwnedNodeBurstIDComesFromExactLabel(t *testing.T) {
	inst := &computepb.Instance{Labels: map[string]string{
		ownerLabelKey:   ownerLabelValue,
		scopeLabelKey:   "",
		burstIDLabelKey: "burst_exact",
	}}
	if !ownsInstance(inst, "") {
		t.Fatal("fixture must be an owned instance")
	}
	if got := inst.GetLabels()[burstIDLabelKey]; got != "burst_exact" {
		t.Fatalf("BurstID label = %q, want exact marker", got)
	}
	if got := inst.GetName(); got == "burst_exact" {
		t.Fatal("test must not infer BurstID from the resource name")
	}
}

// CleanupOrphans feeds parseGCPTimestamp's output straight into the create-race
// gate, so the parse failing has to mean "keep this instance", not "it has no
// age, delete it". parseGCPTimestamp returns the zero time on anything it
// cannot read, which backends.OrphanTooYoung treats as too young.
func TestCreationTimestampFeedsTheOrphanGate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		timestamp string
		tooYoung  bool
	}{
		{"absent timestamp", "", true},
		{"unparseable timestamp", "not-a-time", true},
		{"just created", now.Format(time.RFC3339), true},
		{"inside the grace period", now.Add(-(backends.OrphanGracePeriod - time.Minute)).Format(time.RFC3339), true},
		{"past the grace period", now.Add(-(backends.OrphanGracePeriod + time.Minute)).Format(time.RFC3339), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := backends.OrphanTooYoung(parseGCPTimestamp(c.timestamp), now)
			if got != c.tooYoung {
				t.Errorf("OrphanTooYoung(parseGCPTimestamp(%q)) = %v, want %v", c.timestamp, got, c.tooYoung)
			}
		})
	}
}

func TestResolveZoneRegion(t *testing.T) {
	cases := []struct {
		name       string
		zone       string
		region     string
		wantZone   string
		wantRegion string
	}{
		{"both empty default to defaultRegion's -a zone", "", "", "us-central1-a", "us-central1"},
		{"region only expands to its -a zone", "", "europe-west4", "europe-west4-a", "europe-west4"},
		{"zone only derives its region", "asia-southeast1-b", "", "asia-southeast1-b", "asia-southeast1"},
		{"both set are passed through unchanged", "us-east1-c", "us-east1", "us-east1-c", "us-east1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotZone, gotRegion := resolveZoneRegion(c.zone, c.region)
			if gotZone != c.wantZone || gotRegion != c.wantRegion {
				t.Errorf("resolveZoneRegion(%q,%q) = (%q,%q), want (%q,%q)",
					c.zone, c.region, gotZone, gotRegion, c.wantZone, c.wantRegion)
			}
		})
	}
}

func TestIsZone(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"us-central1-a", true},
		{"europe-west4-b", true},
		{"us-central1", false},
		{"europe-west4", false},
		{"", false},
		{"a", false},
	}
	for _, c := range cases {
		t.Run(c.s, func(t *testing.T) {
			if got := isZone(c.s); got != c.want {
				t.Errorf("isZone(%q) = %v, want %v", c.s, got, c.want)
			}
		})
	}
}

// fakeZoneLister stands in for the Compute zones API. It exists because
// zone discovery must run in ordinary CI — no credentials, no network —
// see zoneLister. calls counts lookups so the cache is assertable.
type fakeZoneLister struct {
	zones []*computepb.Zone
	err   error
	calls int
}

func (f *fakeZoneLister) listZones(context.Context, string) ([]*computepb.Zone, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.zones, nil
}

func zoneOf(name, status string) *computepb.Zone {
	return &computepb.Zone{Name: proto.String(name), Status: proto.String(status)}
}

// usCentral1Zones is us-central1's real zone set: a, b, c and f, with no
// d and no e. usEast1Zones is us-east1's: b, c, d, and NO a. Both are why
// appending {a,b,c} to a region name is a wrong guess — it misses live
// capacity (a burst was rejected while us-central1-f sat UP and untried)
// and names zones that do not exist.
func usCentral1Zones() []*computepb.Zone {
	return []*computepb.Zone{
		zoneOf("us-central1-a", "UP"),
		zoneOf("us-central1-b", "UP"),
		zoneOf("us-central1-c", "UP"),
		zoneOf("us-central1-f", "UP"),
		zoneOf("europe-west4-a", "UP"),
	}
}

func usEast1Zones() []*computepb.Zone {
	return []*computepb.Zone{
		zoneOf("us-east1-b", "UP"),
		zoneOf("us-east1-c", "UP"),
		zoneOf("us-east1-d", "UP"),
	}
}

// resolveZones is the resolveRegion analogue (gcp-plan.md §9): a pinned
// zone is a hard constraint with no fallback; a pinned region or nothing
// at all expands to the zones GCP reports UP for it, preferring the
// backend's configured zone when it falls in the same region.
func TestResolveZones(t *testing.T) {
	cases := []struct {
		name      string
		zone      string
		region    string
		lister    *fakeZoneLister
		pinned    string
		want      []string
		wantCalls int
		wantErr   bool
	}{
		{
			name:      "pinned zone is a hard constraint, and needs no lookup at all",
			zone:      "us-central1-f",
			region:    "us-central1",
			lister:    &fakeZoneLister{zones: usCentral1Zones()},
			pinned:    "europe-west4-b",
			want:      []string{"europe-west4-b"},
			wantCalls: 0,
		},
		{
			name:      "discovered zones are used, and a DOWN zone is excluded",
			region:    "us-central1",
			lister:    &fakeZoneLister{zones: append(usCentral1Zones(), zoneOf("us-central1-x", "DOWN"))},
			pinned:    "us-central1",
			want:      []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"},
			wantCalls: 1,
		},
		{
			name:      "the configured zone sorts first when it is in the region",
			zone:      "us-central1-f",
			region:    "us-central1",
			lister:    &fakeZoneLister{zones: usCentral1Zones()},
			pinned:    "us-central1",
			want:      []string{"us-central1-f", "us-central1-a", "us-central1-b", "us-central1-c"},
			wantCalls: 1,
		},
		{
			name:      "a configured zone outside the target region never leads",
			zone:      "us-central1-f",
			region:    "us-central1",
			lister:    &fakeZoneLister{zones: usCentral1Zones()},
			pinned:    "europe-west4",
			want:      []string{"europe-west4-a"},
			wantCalls: 1,
		},
		{
			name:      "empty pinned falls back to the backend's own region",
			zone:      "us-central1-a",
			region:    "us-central1",
			lister:    &fakeZoneLister{zones: usCentral1Zones()},
			want:      []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"},
			wantCalls: 1,
		},
		{
			name:      "a region whose zones are not a,b,c resolves to its real zones",
			region:    "us-east1",
			lister:    &fakeZoneLister{zones: usEast1Zones()},
			pinned:    "us-east1",
			want:      []string{"us-east1-b", "us-east1-c", "us-east1-d"},
			wantCalls: 1,
		},
		{
			name:      "a failed lookup falls back to a,b,c rather than failing the burst",
			zone:      "us-central1-f",
			region:    "us-central1",
			lister:    &fakeZoneLister{err: errors.New("compute.zones.list denied")},
			pinned:    "us-central1",
			want:      []string{"us-central1-f", "us-central1-a", "us-central1-b", "us-central1-c"},
			wantCalls: 1,
		},
		{
			name:   "no zones client at all falls back to a,b,c",
			region: "us-central1",
			want:   []string{"us-central1-a", "us-central1-b", "us-central1-c"},
		},
		{
			name:    "no region or zone configured at all errors",
			lister:  &fakeZoneLister{zones: usCentral1Zones()},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := &Backend{zone: c.zone, region: c.region}
			if c.lister != nil {
				b.zones = c.lister
			}
			got, err := b.resolveZones(context.Background(), c.pinned)
			if c.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !equalStrings(got, c.want) {
				t.Errorf("resolveZones(%q) = %v, want %v", c.pinned, got, c.want)
			}
			if c.lister != nil && c.lister.calls != c.wantCalls {
				t.Errorf("zone lookups = %d, want %d", c.lister.calls, c.wantCalls)
			}
		})
	}
}

// Zone discovery sits on the burst-create path, so the answer — which
// changes on the order of years — is looked up once per region per TTL,
// not once per burst.
func TestResolveZonesCachesPerRegion(t *testing.T) {
	t.Run("a second burst in the same region reuses the first lookup", func(t *testing.T) {
		lister := &fakeZoneLister{zones: usCentral1Zones()}
		b := &Backend{region: "us-central1", zones: lister}

		first, err := b.resolveZones(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		second, err := b.resolveZones(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !equalStrings(first, second) {
			t.Errorf("cached resolve = %v, want %v", second, first)
		}
		if lister.calls != 1 {
			t.Errorf("zone lookups = %d, want 1", lister.calls)
		}
	})

	t.Run("an expired entry is looked up again", func(t *testing.T) {
		lister := &fakeZoneLister{zones: usCentral1Zones()}
		b := &Backend{region: "us-central1", zones: lister}

		if _, err := b.resolveZones(context.Background(), ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		b.zoneMu.Lock()
		b.zoneCache["us-central1"] = zoneCacheEntry{zones: []string{"us-central1-a"}, expires: time.Now().Add(-time.Minute)}
		b.zoneMu.Unlock()

		got, err := b.resolveZones(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"}
		if !equalStrings(got, want) {
			t.Errorf("resolveZones after expiry = %v, want %v", got, want)
		}
		if lister.calls != 2 {
			t.Errorf("zone lookups = %d, want 2", lister.calls)
		}
	})

	// A permanently failing lookup (a service account without
	// compute.zones.list) must not cost every burst a doomed round trip,
	// so the fallback is cached too — just briefly enough to self-heal.
	t.Run("a failed lookup caches the fallback only briefly", func(t *testing.T) {
		lister := &fakeZoneLister{err: errors.New("compute.zones.list denied")}
		b := &Backend{region: "us-central1", zones: lister}

		if _, err := b.resolveZones(context.Background(), ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := b.resolveZones(context.Background(), ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if lister.calls != 1 {
			t.Errorf("zone lookups = %d, want 1", lister.calls)
		}
		b.zoneMu.Lock()
		expires := b.zoneCache["us-central1"].expires
		b.zoneMu.Unlock()
		if expires.After(time.Now().Add(zoneCacheFailureTTL)) {
			t.Errorf("a failed lookup was cached until %v, want at most %v away", expires, zoneCacheFailureTTL)
		}
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSanitizeLabelValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"already valid", "burst_abc123", "burst_abc123"},
		{"uppercase is lowered", "BURST_ABC123", "burst_abc123"},
		{"disallowed chars become hyphens", "burst.abc/123", "burst-abc-123"},
		{"empty stays empty", "", ""},
		{"over 63 chars is truncated", strings.Repeat("a", 100), strings.Repeat("a", 63)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sanitizeLabelValue(c.in); got != c.want {
				t.Errorf("sanitizeLabelValue(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// isZoneExhausted's substring fallback is what actually fires for a
// plain wrapped error (the structured Reason() path needs a real
// google.rpc.ErrorInfo detail, which only a live API response carries).
func TestIsZoneExhausted(t *testing.T) {
	if !isZoneExhausted(errors.New("rpc error: code = ResourceExhausted desc = ZONE_RESOURCE_POOL_EXHAUSTED: ...")) {
		t.Error("expected ZONE_RESOURCE_POOL_EXHAUSTED substring to be detected")
	}
	if isZoneExhausted(errors.New("some other failure")) {
		t.Error("expected an unrelated error to not be treated as zone exhaustion")
	}
}

func TestIsNotFoundErr(t *testing.T) {
	if !isNotFoundErr(errors.New("googleapi: Error 404: The resource was not found, notFound")) {
		t.Error("expected a notFound substring to be detected")
	}
	if isNotFoundErr(errors.New("googleapi: Error 403: forbidden")) {
		t.Error("expected a 403 to not be treated as not-found")
	}
	if isNotFoundErr(nil) {
		t.Error("expected a nil error to not be not-found")
	}
}

func TestBuildStartupScript(t *testing.T) {
	got := buildStartupScript(map[string]string{
		"B_VAR": "b",
		"A_VAR": "a",
		"EMPTY": "",
	}, "echo done\n")

	if !strings.HasPrefix(got, "#!/bin/bash\n") {
		t.Errorf("script should start with a shebang, got: %q", got)
	}
	if !strings.Contains(got, "export A_VAR='a'\nexport B_VAR='b'\n") {
		t.Errorf("expected sorted, quoted exports, got: %q", got)
	}
	if strings.Contains(got, "EMPTY") {
		t.Errorf("empty-valued vars must be skipped, got: %q", got)
	}
	if !strings.HasSuffix(got, "echo done\n") {
		t.Errorf("expected the bootstrap body appended verbatim, got: %q", got)
	}
}
