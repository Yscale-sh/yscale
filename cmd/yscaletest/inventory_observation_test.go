package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/internal/evidence"
)

type fakeMesh struct {
	provider string
	devices  map[string]int
	err      error
}

func (f *fakeMesh) ProviderName() string { return f.provider }

func (f *fakeMesh) FindDevice(_ context.Context, hostname string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.devices[hostname], nil
}

func completeRunner() *Runner {
	now := time.Now().UTC()
	provCreated := now.Add(-9 * time.Minute)
	reqAt := now.Add(-2 * time.Minute)
	delAt := now.Add(-1 * time.Minute)
	return &Runner{
		Provider:             &fakeProvider{name: backendLinode},
		Mesh:                 &fakeMesh{provider: evidence.MeshProviderTailscale, devices: map[string]int{}},
		providerInstanceID:   12345,
		kubernetesNodeAbsent: true,
		centralEvidence: &WorkloadEvidence{
			Cleanup: &CleanupEvidence{
				State:              "terminated",
				ProviderCreatedAt:  &provCreated,
				RequestedAt:        &reqAt,
				DeletedAt:          &delAt,
				DurableReapReceipt: true,
			},
		},
	}
}

func linodeResult() CaseResult {
	return CaseResult{
		Case:    Case{Name: "linode-gpu-test"},
		Phase:   "Succeeded",
		Backend: backendLinode,
		BurstID: testBurstID,
		Reaped:  true,
	}
}

func TestObserveInventoryFullSuccess(t *testing.T) {
	r := completeRunner()
	res := linodeResult()

	inv, err := r.observeInventory(context.Background(), res)
	if err != nil {
		t.Fatalf("observeInventory failed: %v", err)
	}
	if !inv.Complete {
		t.Fatal("inventory should be complete")
	}
	if inv.ProviderResourcesRemaining != 0 {
		t.Fatalf("provider resources = %d, want 0", inv.ProviderResourcesRemaining)
	}
	if inv.KubernetesNodesRemaining != 0 {
		t.Fatalf("kubernetes nodes = %d, want 0", inv.KubernetesNodesRemaining)
	}
	if inv.MeshDevicesRemaining != 0 {
		t.Fatalf("mesh devices = %d, want 0", inv.MeshDevicesRemaining)
	}
	if inv.PodCIDRReservationsRemaining != 0 {
		t.Fatalf("pod cidr = %d, want 0", inv.PodCIDRReservationsRemaining)
	}
	if inv.AccountLeasesRemaining != 0 {
		t.Fatalf("account leases = %d, want 0", inv.AccountLeasesRemaining)
	}
	if inv.MeshProvider != evidence.MeshProviderTailscale {
		t.Fatalf("mesh_provider = %q, want %q", inv.MeshProvider, evidence.MeshProviderTailscale)
	}
}

func TestObserveInventoryMissingProviderID(t *testing.T) {
	r := completeRunner()
	r.providerInstanceID = 0
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with missing provider ID")
	}
	if !strings.Contains(err.Error(), "provider instance ID") {
		t.Fatalf("error should mention provider instance ID, got %q", err.Error())
	}
}

func TestObserveInventoryProviderAuditFailure(t *testing.T) {
	r := completeRunner()
	r.Provider = &fakeProvider{
		name:       backendLinode,
		residueErr: errors.New("status 429 (body redacted)"),
	}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail on provider audit error")
	}
	if !strings.Contains(err.Error(), "provider residue audit") {
		t.Fatalf("error should mention provider residue, got %q", err.Error())
	}
}

func TestObserveInventoryTaggedInstanceResidue(t *testing.T) {
	r := completeRunner()
	r.Provider = &fakeProvider{
		name:    backendLinode,
		residue: &ProviderResidue{TaggedInstances: 1},
	}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with tagged instance residue")
	}
	if !strings.Contains(err.Error(), "tagged=1") {
		t.Fatalf("error should include tagged count, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "total=1") {
		t.Fatalf("error should include unique total, got %q", err.Error())
	}
}

func TestObserveInventoryExactIDResidue(t *testing.T) {
	r := completeRunner()
	r.Provider = &fakeProvider{
		name:    backendLinode,
		residue: &ProviderResidue{ExactIDInstance: 1},
	}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with exact ID residue")
	}
	if !strings.Contains(err.Error(), "exact_id=1") {
		t.Fatalf("error should include exact_id count, got %q", err.Error())
	}
}

func TestObserveInventoryVolumeResidue(t *testing.T) {
	r := completeRunner()
	r.Provider = &fakeProvider{
		name:    backendLinode,
		residue: &ProviderResidue{AttachedVolumes: 2},
	}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with volume residue")
	}
	if !strings.Contains(err.Error(), "volumes=2") {
		t.Fatalf("error should include volume count, got %q", err.Error())
	}
}

func TestObserveInventoryFirewallResidue(t *testing.T) {
	r := completeRunner()
	r.Provider = &fakeProvider{
		name:    backendLinode,
		residue: &ProviderResidue{FirewallDevices: 1},
	}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with firewall residue")
	}
	if !strings.Contains(err.Error(), "firewall=1") {
		t.Fatalf("error should include firewall count, got %q", err.Error())
	}
}

func TestObserveInventoryMeshDevicePresent(t *testing.T) {
	r := completeRunner()
	hostname := burstTailscaleHostname(testBurstID)
	r.Mesh = &fakeMesh{provider: evidence.MeshProviderTailscale, devices: map[string]int{hostname: 1}}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with mesh device present")
	}
	if !strings.Contains(err.Error(), "mesh devices remaining: 1") {
		t.Fatalf("error should mention mesh devices, got %q", err.Error())
	}
}

func TestObserveInventoryMeshAuditFailure(t *testing.T) {
	r := completeRunner()
	r.Mesh = &fakeMesh{provider: evidence.MeshProviderTailscale, err: errors.New("tailnet list failed")}
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail on mesh audit error")
	}
	if !strings.Contains(err.Error(), "mesh audit") {
		t.Fatalf("error should mention mesh audit, got %q", err.Error())
	}
}

func TestObserveInventoryMeshUnconfigured(t *testing.T) {
	r := completeRunner()
	r.Mesh = nil
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with unconfigured mesh")
	}
	if !strings.Contains(err.Error(), "mesh audit not configured") {
		t.Fatalf("error should mention mesh not configured, got %q", err.Error())
	}
}

func TestObserveInventoryMissingDurableReceipt(t *testing.T) {
	r := completeRunner()
	r.centralEvidence.Cleanup.DurableReapReceipt = false
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with missing durable receipt")
	}
	if !strings.Contains(err.Error(), "durable reap receipt") {
		t.Fatalf("error should mention durable receipt, got %q", err.Error())
	}
}

func TestObserveInventoryMissingCentralEvidence(t *testing.T) {
	r := completeRunner()
	r.centralEvidence = nil
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with missing central evidence")
	}
}

func TestObserveInventoryProviderUnconfigured(t *testing.T) {
	r := completeRunner()
	r.Provider = nil
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with unconfigured provider for linode case")
	}
	if !strings.Contains(err.Error(), "provider audit not configured") {
		t.Fatalf("error should mention provider not configured, got %q", err.Error())
	}
}

func TestObserveInventoryNonLinodeFailsClosed(t *testing.T) {
	r := completeRunner()
	r.Provider = nil
	res := CaseResult{
		Case:    Case{Name: "flyio-cpu-test"},
		Phase:   "Succeeded",
		Backend: "flyio",
		BurstID: testBurstID,
		Reaped:  true,
	}

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("non-linode provider should fail closed")
	}
	if !strings.Contains(err.Error(), "unsupported provider") {
		t.Fatalf("error should mention unsupported provider, got %q", err.Error())
	}
}

func TestObserveInventoryKubernetesAbsenceNotObserved(t *testing.T) {
	r := completeRunner()
	r.kubernetesNodeAbsent = false
	res := linodeResult()

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail when kubernetes node absence was not observed")
	}
	if !strings.Contains(err.Error(), "kubernetes node absence") {
		t.Fatalf("error should mention kubernetes node absence, got %q", err.Error())
	}
}

func TestObserveInventoryRejectsUnknownMeshProvider(t *testing.T) {
	r := completeRunner()
	res := linodeResult()
	for _, name := range []string{"", "auto", "netmaker", "TAILSCALE"} {
		t.Run(name, func(t *testing.T) {
			r.Mesh.(*fakeMesh).provider = name
			_, err := r.observeInventory(context.Background(), res)
			if err == nil {
				t.Fatalf("should fail for provider %q", name)
			}
			if !strings.Contains(err.Error(), "mesh provider") {
				t.Fatalf("error should mention mesh provider, got %q", err.Error())
			}
		})
	}
}

func TestObserveInventoryFabricStampsProvenance(t *testing.T) {
	r := completeRunner()
	r.Mesh.(*fakeMesh).provider = evidence.MeshProviderFabric
	res := linodeResult()
	inv, err := r.observeInventory(context.Background(), res)
	if err != nil {
		t.Fatalf("observeInventory: %v", err)
	}
	if inv.MeshProvider != evidence.MeshProviderFabric {
		t.Fatalf("mesh_provider = %q, want %q", inv.MeshProvider, evidence.MeshProviderFabric)
	}
}

func TestObserveInventoryEmptyBurstID(t *testing.T) {
	r := completeRunner()
	res := linodeResult()
	res.BurstID = ""

	_, err := r.observeInventory(context.Background(), res)
	if err == nil {
		t.Fatal("should fail with empty burstID")
	}
	if !strings.Contains(err.Error(), "empty burstID") {
		t.Fatalf("error should mention empty burstID, got %q", err.Error())
	}
}

func TestObserveInventoryWriteEvidenceIntegration(t *testing.T) {
	dir := t.TempDir()
	r := completeRunner()
	res := linodeResult()

	inv, err := r.observeInventory(context.Background(), res)
	if err != nil {
		t.Fatalf("observeInventory: %v", err)
	}

	_, evObs := completeEvidenceObs(t)
	evObs.Inventory = inv
	commitSHA := strings.Repeat("a", 40)

	artifactResult, err := writeEvidence(dir, res, "yt-inv-full", commitSHA, evObs)
	if err != nil {
		t.Fatalf("writeEvidence: %v", err)
	}
	if artifactResult != evidence.ResultPassed {
		t.Fatalf("artifact result = %q, want passed", artifactResult)
	}
}

func TestParseLinodeProviderID(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"linode://12345", 12345},
		{"linode://0", 0},
		{"linode://", 0},
		{"linode://-1", 0},
		{"aws://i-abc123", 0},
		{"", 0},
		{"12345", 0},
	}
	for _, tc := range tests {
		got := parseLinodeProviderID(tc.input)
		if got != tc.want {
			t.Errorf("parseLinodeProviderID(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestProviderResidueTotal(t *testing.T) {
	r := ProviderResidue{
		TaggedInstances: 1,
		ExactIDInstance: 1,
		AttachedVolumes: 2,
		FirewallDevices: 1,
	}
	if got := r.Total(); got != 5 {
		t.Fatalf("Total() = %d, want 5", got)
	}
}

func TestProviderResidueUniqueTotal(t *testing.T) {
	tests := []struct {
		name    string
		residue ProviderResidue
		want    int
	}{
		{
			name:    "same instance tagged and exact-ID does not double count",
			residue: ProviderResidue{TaggedInstances: 1, ExactIDInstance: 1, ExactIDTagged: true, AttachedVolumes: 1, FirewallDevices: 1},
			want:    3,
		},
		{
			name:    "tagged and exact-ID are distinct instances",
			residue: ProviderResidue{TaggedInstances: 1, ExactIDInstance: 1},
			want:    2,
		},
		{
			name:    "tagged only",
			residue: ProviderResidue{TaggedInstances: 2, ExactIDInstance: 0},
			want:    2,
		},
		{
			name:    "exact-ID only",
			residue: ProviderResidue{ExactIDInstance: 1, AttachedVolumes: 2},
			want:    3,
		},
		{
			name:    "all zero",
			residue: ProviderResidue{},
			want:    0,
		},
		{
			name:    "tagged exceeds exact-ID",
			residue: ProviderResidue{TaggedInstances: 3, ExactIDInstance: 1, ExactIDTagged: true},
			want:    3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.residue.UniqueTotal(); got != tc.want {
				t.Fatalf("UniqueTotal() = %d, want %d", got, tc.want)
			}
		})
	}
}
