package state

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func gpuUpdate(utilization float64) BurstGPUTelemetryUpdate {
	return BurstGPUTelemetryUpdate{
		BurstID:     "b1",
		CustomerID:  "cust_a",
		ClusterID:   "cluster_1",
		NodeName:    "ys-burst-b1",
		Utilization: utilization,
		ObservedAt:  time.Now().UTC(),
	}
}

func seedGPUBurst(s *Store) *Burst {
	b := &Burst{
		ID:         "b1",
		CustomerID: "cust_a",
		ClusterID:  "cluster_1",
		NodeName:   "ys-burst-b1",
		Backend:    "linode",
		Status:     BurstStatusRunning,
	}
	s.bursts[b.ID] = b
	return b
}

func TestGPUTelemetryHappyPath(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(75.5)
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("expected applied=true")
	}
	b := s.bursts["b1"]
	if b.GPUUtilPercent != 75.5 {
		t.Fatalf("got GPUUtilPercent=%f, want 75.5", b.GPUUtilPercent)
	}
	if b.LastHeartbeatAt == nil {
		t.Fatal("LastHeartbeatAt should be set")
	}
}

func TestGPUTelemetryZeroPercent(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(0)
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("0% is a valid sample")
	}
	b := s.bursts["b1"]
	if b.GPUUtilPercent != 0 {
		t.Fatalf("got GPUUtilPercent=%f, want 0", b.GPUUtilPercent)
	}
	if b.LastHeartbeatAt == nil {
		t.Fatal("LastHeartbeatAt distinguishes 0% from unset")
	}
}

func TestGPUTelemetryRejectsUnknownBurst(t *testing.T) {
	s := New()
	u := gpuUpdate(50)
	u.BurstID = "nonexistent"
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should refuse an unknown burst")
	}
}

func TestGPUTelemetryRejectsWrongTenant(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(50)
	u.CustomerID = "cust_other"
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should refuse a sample for another tenant's burst")
	}
}

func TestGPUTelemetryRejectsWrongCluster(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(50)
	u.ClusterID = "cluster_other"
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should refuse a sample from the wrong cluster")
	}
}

func TestGPUTelemetryRejectsWrongNode(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(50)
	u.NodeName = "ys-burst-other"
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should refuse a sample from the wrong node")
	}
}

func TestGPUTelemetryRejectsEmptyClusterID(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(50)
	u.ClusterID = ""
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("empty ClusterID must be rejected")
	}
}

func TestGPUTelemetryRejectsEmptyNodeName(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	u := gpuUpdate(50)
	u.NodeName = ""
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("empty NodeName must be rejected")
	}
}

func TestGPUTelemetryRejectsOutOfRange(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	for _, v := range []float64{-1, 101, -0.001, 100.001} {
		u := gpuUpdate(v)
		applied, _ := s.UpdateBurstGPUTelemetry(context.Background(), u)
		if applied {
			t.Fatalf("should reject utilization=%f", v)
		}
	}
}

func TestGPUTelemetryRejectsNaNInf(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	for _, v := range []float64{math.Inf(1), math.Inf(-1)} {
		u := gpuUpdate(v)
		applied, _ := s.UpdateBurstGPUTelemetry(context.Background(), u)
		if applied {
			t.Fatalf("should reject utilization=%f", v)
		}
	}
	u := gpuUpdate(0)
	u.Utilization = math.NaN()
	applied, _ := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if applied {
		t.Fatal("should reject NaN")
	}
}

func TestGPUTelemetryRejectsStaleSample(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	first := gpuUpdate(50)
	first.ObservedAt = time.Now().UTC()
	s.UpdateBurstGPUTelemetry(context.Background(), first)

	stale := gpuUpdate(60)
	stale.ObservedAt = first.ObservedAt.Add(-time.Second)
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should reject a stale sample")
	}
	if s.bursts["b1"].GPUUtilPercent != 50 {
		t.Fatal("stale sample should not have overwritten")
	}
}

func TestGPUTelemetryCannotResurrectReapedBurst(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	delete(s.bursts, "b1")
	u := gpuUpdate(50)
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should not resurrect a missing burst")
	}
	if _, exists := s.bursts["b1"]; exists {
		t.Fatal("burst should not have been recreated")
	}
}

func TestGPUTelemetryCopyOnWrite(t *testing.T) {
	s := New()
	seedGPUBurst(s)
	before := s.bursts["b1"]
	u := gpuUpdate(42)
	s.UpdateBurstGPUTelemetry(context.Background(), u)
	after := s.bursts["b1"]
	if before == after {
		t.Fatal("update must replace the map entry, not mutate in place")
	}
	if before.GPUUtilPercent != 0 {
		t.Fatal("original pointer was mutated")
	}
}

// --- Guarded real-PostgreSQL integration tests ---

func TestPostgresGPUTelemetryIdentityAndFreshness(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_gpu", Token: "tok_gpu", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cl1", Backend: "linode",
		NodeName: "ys-burst-gpu", Status: BurstStatusRunning, CreatedAt: now,
	})

	// Happy path.
	t1 := now.Add(time.Minute)
	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cl1",
		NodeName: "ys-burst-gpu", Utilization: 42, ObservedAt: t1,
	})
	if err != nil || !applied {
		t.Fatalf("happy path: applied=%v err=%v", applied, err)
	}
	b, _ := s.GetBurst("burst_gpu")
	if b.GPUUtilPercent != 42 || b.LastHeartbeatAt == nil {
		t.Fatalf("not stored: util=%f hb=%v", b.GPUUtilPercent, b.LastHeartbeatAt)
	}

	// Strictly-newer wins: a newer sample supersedes.
	t2 := t1.Add(time.Second)
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cl1",
		NodeName: "ys-burst-gpu", Utilization: 99, ObservedAt: t2,
	})
	if !applied {
		t.Fatal("newer sample should be applied")
	}

	// Stale: an older sample is refused.
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cl1",
		NodeName: "ys-burst-gpu", Utilization: 10, ObservedAt: t1,
	})
	if applied {
		t.Fatal("stale sample should be refused")
	}
	b, _ = s.GetBurst("burst_gpu")
	if b.GPUUtilPercent != 99 {
		t.Fatalf("stale write overwrote: %f", b.GPUUtilPercent)
	}

	// Wrong tenant.
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_evil", ClusterID: "cl1",
		NodeName: "ys-burst-gpu", Utilization: 50, ObservedAt: t2.Add(time.Second),
	})
	if applied {
		t.Fatal("wrong tenant should be refused")
	}

	// Wrong cluster.
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "wrong",
		NodeName: "ys-burst-gpu", Utilization: 50, ObservedAt: t2.Add(time.Second),
	})
	if applied {
		t.Fatal("wrong cluster should be refused")
	}

	// Wrong node.
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_gpu", CustomerID: "cust_gpu", ClusterID: "cl1",
		NodeName: "ys-burst-other", Utilization: 50, ObservedAt: t2.Add(time.Second),
	})
	if applied {
		t.Fatal("wrong node should be refused")
	}

	// Unknown burst.
	applied, _ = s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_nope", CustomerID: "cust_gpu", ClusterID: "cl1",
		NodeName: "ys-burst-nope", Utilization: 50, ObservedAt: t2.Add(time.Second),
	})
	if applied {
		t.Fatal("unknown burst should be refused")
	}
}

func TestPostgresGPUTelemetryRestartPersistence(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_rp", Token: "tok_rp", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_rp", CustomerID: "cust_rp", ClusterID: "cl1", Backend: "linode",
		NodeName: "ys-burst-rp", Status: BurstStatusRunning, CreatedAt: now,
	})
	s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_rp", CustomerID: "cust_rp", ClusterID: "cl1",
		NodeName: "ys-burst-rp", Utilization: 77, ObservedAt: now.Add(time.Minute),
	})
	s.Close()

	s2, err := NewPostgres(context.Background(), os.Getenv("YSCALE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer s2.Close()

	b, err := s2.GetBurst("burst_rp")
	if err != nil {
		t.Fatalf("burst not found after restart: %v", err)
	}
	if b.GPUUtilPercent != 77 || b.LastHeartbeatAt == nil {
		t.Fatalf("telemetry not persisted: util=%f hb=%v", b.GPUUtilPercent, b.LastHeartbeatAt)
	}
}

func TestPostgresGPUTelemetryCannotResurrectDeleted(t *testing.T) {
	s := pgStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	s.AddCustomer(&Customer{ID: "cust_del", Token: "tok_del", Plan: "pro"})
	s.PutBurst(&Burst{
		ID: "burst_del", CustomerID: "cust_del", ClusterID: "cl1", Backend: "linode",
		NodeName: "ys-burst-del", Status: BurstStatusRunning, CreatedAt: now,
	})
	s.DeleteBurst("burst_del")

	applied, err := s.UpdateBurstGPUTelemetry(context.Background(), BurstGPUTelemetryUpdate{
		BurstID: "burst_del", CustomerID: "cust_del", ClusterID: "cl1",
		NodeName: "ys-burst-del", Utilization: 50, ObservedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("should not apply to a deleted burst")
	}
	if _, err := s.GetBurst("burst_del"); err == nil {
		t.Fatal("deleted burst was resurrected")
	}
}

func TestGPUTelemetryLegacyJSONCompatible(t *testing.T) {
	var legacy Burst
	stored := `{"ID":"burst_old","CustomerID":"cust_a","Backend":"linode","Status":"provisioning"}`
	if err := json.Unmarshal([]byte(stored), &legacy); err != nil {
		t.Fatalf("legacy burst no longer decodes: %v", err)
	}
	if legacy.GPUUtilPercent != 0 || legacy.LastHeartbeatAt != nil {
		t.Fatalf("legacy record decoded with invented telemetry: %+v", legacy)
	}
	raw, err := json.Marshal(&legacy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "GPUUtilPercent") || strings.Contains(string(raw), "LastHeartbeatAt") {
		t.Fatalf("re-marshalled legacy carries telemetry fields: %s", raw)
	}
}
