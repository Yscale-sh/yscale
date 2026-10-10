package store

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testStore(t *testing.T, kek byte) *MemoryStore {
	t.Helper()
	s, err := NewWithKEK(bytes.Repeat([]byte{kek}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// BeginDeprovision must be atomic: exactly one of many concurrent callers starts
// the teardown, so concurrent DELETEs enqueue at most one deprovision job.
func TestBeginDeprovisionAtomicUnderConcurrency(t *testing.T) {
	s := testStore(t, 9)
	if err := s.SetBox(Box{TenantID: "t1", LoginServer: "ls1", Status: StatusReady}, "k"); err != nil {
		t.Fatal(err)
	}
	const n = 24
	var started int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.BeginDeprovision("t1", "ls1")
			if err != nil {
				t.Errorf("BeginDeprovision: %v", err)
				return
			}
			if ok {
				atomic.AddInt32(&started, 1)
			}
		}()
	}
	wg.Wait()
	if started != 1 {
		t.Fatalf("BeginDeprovision started=%d across %d concurrent callers, want exactly 1", started, n)
	}
}

// PromoteFabric must re-key ONLY the fabric whose staging endpoint it is given,
// repoint that fabric's provision job so GetFabric keeps resolving it, and leave
// every other tenant's staging record and job untouched.
func TestPromoteFabricReKeysTargetedFabric(t *testing.T) {
	s := testStore(t, 10)
	jobA, boxA, err := s.EnsureFabric("tenant-a", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	jobB, boxB, err := s.EnsureFabric("tenant-b", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	stagingA := boxA.LoginServer
	const realA = "https://203-0-113-7.ip.linodeusercontent.com"
	promoted := Box{TenantID: "tenant-a", LoginServer: realA, Backend: "linode", BackendID: "inst-a", HSUser: "tenant-a", Status: StatusReady}
	if err := s.PromoteFabric(stagingA, promoted, "real-key-a"); err != nil {
		t.Fatal(err)
	}

	// tenant-a: staging box gone, real box present, job repointed, GetFabric resolves the real box.
	if _, err := s.GetBox(stagingA); !errors.Is(err, ErrNotFound) {
		t.Fatalf("staging box survived promotion: %v", err)
	}
	fabricA, err := s.GetFabric("tenant-a")
	if err != nil || fabricA.LoginServer != realA || fabricA.Status != StatusReady {
		t.Fatalf("GetFabric(tenant-a) = %#v, %v; want ready box at %s", fabricA, err, realA)
	}
	if got, err := s.GetJob(jobA.ID); err != nil || got.LoginServer != realA {
		t.Fatalf("provision job not repointed: %#v, %v", got, err)
	}
	if key, err := s.DecryptAPIKey(realA); err != nil || key != "real-key-a" {
		t.Fatalf("DecryptAPIKey(realA) = %q, %v", key, err)
	}

	// tenant-b: completely untouched — staging endpoint and job still resolve.
	if got, err := s.GetJob(jobB.ID); err != nil || got.LoginServer != boxB.LoginServer {
		t.Fatalf("tenant-b job disturbed by tenant-a promotion: %#v, %v", got, err)
	}
	fabricB, err := s.GetFabric("tenant-b")
	if err != nil || fabricB.LoginServer != boxB.LoginServer {
		t.Fatalf("GetFabric(tenant-b) = %#v, %v; want untouched staging box", fabricB, err)
	}
	if bytes.Contains(fabricA.apiKeyEnc, []byte("real-key-a")) {
		t.Fatal("PromoteFabric left plaintext key material in the box record")
	}
}

func TestEnsureFabricIsIdempotent(t *testing.T) {
	s := testStore(t, 6)
	firstJob, firstBox, err := s.EnsureFabric("tenant-a", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	secondJob, secondBox, err := s.EnsureFabric("tenant-a", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if firstJob.ID != secondJob.ID || firstBox.LoginServer != secondBox.LoginServer {
		t.Fatal("same idempotency key created another fabric")
	}
	if firstJob.IdempotencyKey != "create-1" || firstJob.RequestHash == "" {
		t.Fatal("provision job did not retain idempotency metadata")
	}
	if _, _, err := s.EnsureFabric("tenant-a", "create-2"); err != nil {
		t.Fatal(err)
	}
	if len(s.ListBoxes()) != 1 || len(s.ListJobs("tenant-a")) != 1 {
		t.Fatal("tenant received more than one active fabric")
	}
}

func TestClaimJobClaimsPendingOnlyOnce(t *testing.T) {
	s := testStore(t, 7)
	first, err := s.EnqueueJob(Job{TenantID: "tenant-a", Kind: "provision", CreatedAt: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.EnqueueJob(Job{TenantID: "tenant-a", Kind: "deprovision", CreatedAt: time.Unix(2, 0)})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimJob()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != first.ID || claimed.Status != JobInProgress {
		t.Fatalf("ClaimJob() = %#v, want first in progress", claimed)
	}
	claimed, err = s.ClaimJob()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != second.ID {
		t.Fatalf("second ClaimJob() = %q, want %q", claimed.ID, second.ID)
	}
	if _, err := s.ClaimJob(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("third ClaimJob() error = %v, want ErrNotFound", err)
	}
}

func TestGetBoxForTenantIncludesDecommissioningBox(t *testing.T) {
	s := testStore(t, 8)
	box := Box{TenantID: "tenant-a", LoginServer: "https://box.example", Status: StatusDecommissioning}
	if err := s.SetBox(box, "key"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBoxForTenant("tenant-a", box.LoginServer)
	if err != nil || got.Status != StatusDecommissioning {
		t.Fatalf("GetBoxForTenant() = %#v, %v", got, err)
	}
	if _, err := s.GetBoxForTenant("tenant-b", box.LoginServer); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong tenant error = %v, want ErrNotFound", err)
	}
}

func TestBoxAPIKeyEnvelopeRoundTrip(t *testing.T) {
	s := testStore(t, 1)
	box := Box{TenantID: "tenant-a", LoginServer: "https://box.example", Status: StatusReady}
	if err := s.SetBox(box, "box-api-key"); err != nil {
		t.Fatal(err)
	}

	got, err := s.DecryptAPIKey(box.LoginServer)
	if err != nil {
		t.Fatal(err)
	}
	if got != "box-api-key" {
		t.Fatalf("DecryptAPIKey() = %q, want API key", got)
	}
}

func TestGetAndListDoNotExposePlaintextAPIKey(t *testing.T) {
	s := testStore(t, 2)
	box := Box{TenantID: "tenant-a", LoginServer: "https://box.example", Status: StatusReady}
	const apiKey = "plain-api-key-must-not-leak"
	if err := s.SetBox(box, apiKey); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetBox(box.LoginServer)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got.apiKeyEnc, []byte(apiKey)) {
		t.Fatal("GetBox returned plaintext API key material")
	}
	listed := s.ListBoxes()
	if len(listed) != 1 || bytes.Contains(listed[0].apiKeyEnc, []byte(apiKey)) {
		t.Fatal("ListBoxes returned plaintext API key material")
	}
}

func TestWrongKEKFailsToDecrypt(t *testing.T) {
	writer := testStore(t, 3)
	box := Box{TenantID: "tenant-a", LoginServer: "https://box.example"}
	if err := writer.SetBox(box, "box-api-key"); err != nil {
		t.Fatal(err)
	}

	reader := testStore(t, 4)
	reader.boxes[box.LoginServer] = writer.boxes[box.LoginServer]
	if _, err := reader.DecryptAPIKey(box.LoginServer); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("DecryptAPIKey() error = %v, want ErrDecrypt", err)
	}
}

func TestNewFailsClosedWithoutAValidKEK(t *testing.T) {
	original, wasSet := os.LookupEnv("FACTORY_KEK")
	t.Cleanup(func() {
		if wasSet {
			t.Setenv("FACTORY_KEK", original)
		} else {
			_ = os.Unsetenv("FACTORY_KEK")
		}
	})

	for _, value := range []string{"", "not-base64", base64.StdEncoding.EncodeToString(make([]byte, 31))} {
		t.Setenv("FACTORY_KEK", value)
		if _, err := New(); !errors.Is(err, ErrInvalidKEK) {
			t.Fatalf("New() error = %v, want ErrInvalidKEK for %q", err, value)
		}
	}
}

func TestSetBoxUsesUniqueNonces(t *testing.T) {
	s := testStore(t, 5)
	first := Box{TenantID: "tenant-a", LoginServer: "https://one.example"}
	second := Box{TenantID: "tenant-a", LoginServer: "https://two.example"}
	if err := s.SetBox(first, "same-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBox(second, "same-key"); err != nil {
		t.Fatal(err)
	}

	storedFirst, err := s.GetBox(first.LoginServer)
	if err != nil {
		t.Fatal(err)
	}
	storedSecond, err := s.GetBox(second.LoginServer)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(storedFirst.apiKeyEnc[:nonceSize], storedSecond.apiKeyEnc[:nonceSize]) {
		t.Fatal("SetBox reused an API key encryption nonce")
	}
}
