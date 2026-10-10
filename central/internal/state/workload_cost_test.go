package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const costBurst = "burst_costed"

var errCostWrite = errors.New("postgres is down")

func testObservation(usd float64, frozenAt time.Time) WorkloadCost {
	return WorkloadCost{
		EstimatedUSD: usd,
		HourlyUSD:    1.5,
		Runtime:      90 * time.Minute,
		FrozenAt:     frozenAt,
		Backend:      "linode",
		BurstID:      costBurst,
		Basis:        WorkloadCostBasisRateRuntime,
	}
}

// The observation a reap freezes has to be readable from this replica's own API
// immediately — the customer whose job just stopped is the one asking — and it
// has to reach the durable row, which is what every other replica and every
// restart reads.
func TestRecordWorkloadCostForBurstWritesMemoryAndDurable(t *testing.T) {
	frozenAt := time.Date(2026, 8, 12, 9, 30, 0, 0, time.UTC)
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_1", BurstID: costBurst, Status: "succeeded"},
	}}
	s.persist = p

	recorded, err := s.RecordWorkloadCostForBurst(context.Background(), testObservation(2.25, frozenAt))
	if err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst = %v, err %v; want true/nil", recorded, err)
	}

	got, err := s.GetWorkload("wl_1")
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if got.Cost == nil {
		t.Fatal("the workload this replica serves has no cost; the customer who just ran the job sees nothing")
	}
	if got.Cost.EstimatedUSD != 2.25 || got.Cost.HourlyUSD != 1.5 ||
		got.Cost.Runtime != 90*time.Minute || !got.Cost.FrozenAt.Equal(frozenAt) ||
		got.Cost.Backend != "linode" || got.Cost.BurstID != costBurst {
		t.Errorf("in-memory cost = %+v, want the observation as handed in", got.Cost)
	}
	if got.Cost.Basis != WorkloadCostBasisRateRuntime {
		t.Errorf("basis = %q, want %q — a figure with no stated basis cannot be defended later",
			got.Cost.Basis, WorkloadCostBasisRateRuntime)
	}
	if p.costWrites != 1 {
		t.Errorf("durable cost writes = %d, want 1", p.costWrites)
	}
	if row := p.rows[costBurst]; row.Cost == nil || row.Cost.EstimatedUSD != 2.25 {
		t.Errorf("durable row cost = %+v, want the same observation", row.Cost)
	}
}

func TestRecordWorkloadCostForBurstSurvivesRequestCancellation(t *testing.T) {
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_1", BurstID: costBurst, Status: "succeeded"},
	}}
	s.persist = p

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorded, err := s.RecordWorkloadCostForBurst(ctx, testObservation(2.25, time.Now().UTC()))
	if err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst with cancelled request = %v, err %v; want true/nil", recorded, err)
	}
	if p.costCtxErr != nil {
		t.Fatalf("durable write inherited request cancellation: %v", p.costCtxErr)
	}
}

// A reap on a replica that never held the workload is the ordinary case now:
// any replica can win any burst's claim. The durable row is the only place it
// can record, and it must still get there.
func TestRecordWorkloadCostForBurstWritesDurablyForAnotherReplicasWorkload(t *testing.T) {
	s := emptyStore()
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_theirs", BurstID: costBurst, Status: "succeeded"},
	}}
	s.persist = p

	recorded, err := s.RecordWorkloadCostForBurst(context.Background(),
		testObservation(4, time.Now().UTC()))
	if err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst = %v, err %v; want true/nil", recorded, err)
	}
	if row := p.rows[costBurst]; row.Cost == nil || row.Cost.EstimatedUSD != 4 {
		t.Errorf("durable row cost = %+v, want the observation this replica measured", row.Cost)
	}
}

// Nothing to observe is not a failure. A nodeOnly burst has no workload row, a
// reap can run for a burst whose workload was never written, and a blank burst
// id must not match the workloads that have no burst at all.
func TestRecordWorkloadCostForBurstRecordsNothingForAnUnknownBurst(t *testing.T) {
	tests := []struct {
		name    string
		burstID string
	}{
		{name: "a burst with no workload", burstID: "burst_nobody"},
		{name: "no burst id at all", burstID: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := emptyStore()
			s.workloads["wl_1"] = &Workload{ID: "wl_1", Status: "running"} // no burst yet
			p := &workloadPersister{rows: map[string]*Workload{}}
			s.persist = p

			observation := testObservation(1, time.Now().UTC())
			observation.BurstID = tc.burstID
			recorded, err := s.RecordWorkloadCostForBurst(context.Background(), observation)
			if err != nil || recorded {
				t.Fatalf("RecordWorkloadCostForBurst = %v, err %v; want false/nil", recorded, err)
			}
			w, err := s.GetWorkload("wl_1")
			if err != nil || w.Cost != nil {
				t.Errorf("workload = %+v (err %v), want no cost — nothing measured this run", w, err)
			}
			if p.costWrites != 0 {
				t.Errorf("durable cost writes = %d, want 0", p.costWrites)
			}
		})
	}
}

// First write wins. The claim elects one reaper per burst, so a second
// observation is a redelivered teardown or a re-reap — and the first one is the
// one taken when the node actually stopped. A record whose frozen timestamp
// creeps forward every redelivery is not a historical record.
func TestRecordWorkloadCostForBurstIsFirstWriteWins(t *testing.T) {
	first := time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	second := first.Add(2 * time.Hour)
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_1", BurstID: costBurst, Status: "succeeded"},
	}}
	s.persist = p
	ctx := context.Background()

	if recorded, err := s.RecordWorkloadCostForBurst(ctx, testObservation(2.25, first)); err != nil || !recorded {
		t.Fatalf("first record = %v, err %v; want true/nil", recorded, err)
	}
	if recorded, err := s.RecordWorkloadCostForBurst(ctx, testObservation(9.99, second)); err != nil || recorded {
		t.Fatalf("second record = %v, err %v; want false/nil — the observation is already frozen", recorded, err)
	}

	got, err := s.GetWorkload("wl_1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Cost.EstimatedUSD != 2.25 || !got.Cost.FrozenAt.Equal(first) {
		t.Errorf("in-memory cost = %+v, want the first observation (%.2f at %v)", got.Cost, 2.25, first)
	}
	if row := p.rows[costBurst]; row.Cost.EstimatedUSD != 2.25 || !row.Cost.FrozenAt.Equal(first) {
		t.Errorf("durable cost = %+v, want the first observation", row.Cost)
	}
	if p.costWrites != 1 {
		t.Errorf("durable cost writes = %d, want 1 — the second call must not reach the row", p.costWrites)
	}
}

// A durable backend that refuses the write is reported to the caller, and the
// in-memory mirror stands: this process's answer is correct either way, and the
// caller is a teardown that already happened. What it must never do is fail.
func TestRecordWorkloadCostForBurstReportsDurableFailureAndKeepsTheMirror(t *testing.T) {
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	s.persist = &workloadPersister{
		rows:    map[string]*Workload{costBurst: {ID: "wl_1", BurstID: costBurst}},
		costErr: errCostWrite,
	}

	recorded, err := s.RecordWorkloadCostForBurst(context.Background(),
		testObservation(2.25, time.Now().UTC()))
	if !errors.Is(err, errCostWrite) {
		t.Fatalf("err = %v, want %v — a caller told nothing went wrong would log nothing", err, errCostWrite)
	}
	if !recorded {
		t.Error("recorded = false, but this replica did record it in memory")
	}
	w, err := s.GetWorkload("wl_1")
	if err != nil || w.Cost == nil {
		t.Errorf("workload = %+v (err %v), want the in-memory observation kept", w, err)
	}
}

func TestEnsureWorkloadCostForBurstRetriesAfterMirrorOutlivesFailedWrite(t *testing.T) {
	first := testObservation(2.25, time.Now().UTC())
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	p := &workloadPersister{
		rows:    map[string]*Workload{costBurst: {ID: "wl_1", BurstID: costBurst}},
		costErr: errCostWrite,
	}
	s.persist = p

	if recorded, err := s.RecordWorkloadCostForBurst(context.Background(), first); !recorded || !errors.Is(err, errCostWrite) {
		t.Fatalf("initial write = %v err:%v, want mirrored true and durable error", recorded, err)
	}
	p.costErr = nil
	// A different retry observation must not move the first frozen timestamp.
	retry := testObservation(9.99, first.FrozenAt.Add(time.Hour))
	if confirmed, err := s.EnsureWorkloadCostForBurst(context.Background(), retry); err != nil || !confirmed {
		t.Fatalf("EnsureWorkloadCostForBurst = %v err:%v, want true/nil", confirmed, err)
	}
	if got := p.rows[costBurst].Cost; got == nil || *got != first {
		t.Fatalf("durable retry wrote %+v, want original %+v", got, first)
	}
}

// The OSS / single-binary store: no persister, so its map IS the workload set.
func TestRecordWorkloadCostForBurstWithoutADurableBackend(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl_1", CustomerID: DevCustomerID, BurstID: costBurst, Status: "succeeded"})

	recorded, err := s.RecordWorkloadCostForBurst(context.Background(),
		testObservation(2.25, time.Now().UTC()))
	if err != nil || !recorded {
		t.Fatalf("RecordWorkloadCostForBurst = %v, err %v; want true/nil", recorded, err)
	}
	w, err := s.GetWorkload("wl_1")
	if err != nil || w.Cost == nil || w.Cost.EstimatedUSD != 2.25 {
		t.Errorf("workload = %+v (err %v), want the observation", w, err)
	}
}

// The observation must survive the ordinary lifecycle writes that follow it. A
// terminal status arriving after the reap froze the cost — the common ordering
// on the cancel path — must not take the record back out.
func TestWorkloadLifecycleWritesKeepTheObservation(t *testing.T) {
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "running"}
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_1", BurstID: costBurst, Status: "running"},
	}}
	s.persist = p
	if _, err := s.RecordWorkloadCostForBurst(context.Background(),
		testObservation(2.25, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}

	s.FinishWorkload("wl_1", "cancelled", time.Now().UTC(), false)
	if w, err := s.GetWorkload("wl_1"); err != nil || w.Cost == nil {
		t.Fatalf("workload after finish = %+v (err %v), want the cost kept", w, err)
	}

	// The whole-record write: its caller built the record before the reap could
	// have measured anything, so the record it hands in carries no cost.
	s.PutWorkload(&Workload{ID: "wl_1", BurstID: costBurst, Status: "failed"})
	w, err := s.GetWorkload("wl_1")
	if err != nil || w.Cost == nil {
		t.Fatalf("workload after a whole-record write = %+v (err %v), want the cost kept", w, err)
	}
	if w.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled — a whole-record write must preserve the terminal observation as well as cost", w.Status)
	}
	if row := p.rows[costBurst]; row.Cost == nil {
		t.Error("the durable row lost the observation to a lifecycle write")
	}
}

// The cross-replica erase: a replica that never saw the reap writes the
// workload back — a status catch-up, a restart's record — and its document has
// no cost because that replica never measured one. The stored observation is
// not re-derivable (the burst it was measured from is deleted by the claim that
// froze it), so a write that drops it destroys the only copy.
func TestUpsertWorkloadKeepsAnAlreadyPersistedCost(t *testing.T) {
	p := &workloadPersister{rows: map[string]*Workload{}}
	frozen := testObservation(2.25, time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC))
	if err := p.upsertWorkload(&Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded", Cost: &frozen}); err != nil {
		t.Fatal(err)
	}

	// The stale replica's document: newer status, no observation.
	if err := p.upsertWorkload(&Workload{ID: "wl_1", BurstID: costBurst, Status: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	row := p.rows[costBurst]
	if row.Cost == nil || row.Cost.EstimatedUSD != 2.25 {
		t.Fatalf("durable cost = %+v, want the frozen observation preserved", row.Cost)
	}
	if row.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled — the guard holds back one field, not the write", row.Status)
	}
}

// A SHAPE check on the two workload statements, and only that: it reads the
// strings this package builds. It executes nothing and proves nothing about
// Postgres — TestWorkloadCostPostgres below is what runs them, and it skips
// unless YSCALE_TEST_DATABASE_URL names a throwaway database.
func TestWorkloadCostStatementShapes(t *testing.T) {
	upsert := upsertWorkloadStmt(tblWorkloads)
	// The stored row is the source of the preserved observation, not the
	// incoming document — an incoming cost is exactly what a stale writer lacks.
	if !strings.Contains(upsert, `jsonb_set(EXCLUDED.data, '{Cost}', `+tblWorkloads+`.data->'Cost')`) ||
		!strings.Contains(upsert, `COALESCE(`+tblWorkloads+`.data->'Cost', 'null'::jsonb) <> 'null'::jsonb`) {
		t.Errorf("the upsert does not carry a stored Cost onto an incoming document without one:\n%s", upsert)
	}
	// And only when the incoming document has none: a writer that does carry the
	// observation still writes its document whole.
	if !strings.Contains(upsert, `COALESCE(EXCLUDED.data->'Cost', 'null'::jsonb) = 'null'::jsonb`) {
		t.Errorf("the upsert preserves unconditionally; a document carrying a cost could never write it:\n%s", upsert)
	}
	if !strings.Contains(upsert, "ON CONFLICT (id) DO UPDATE") || !strings.Contains(upsert, "ELSE EXCLUDED.data") {
		t.Errorf("the statement is not the upsert every workload write depends on:\n%s", upsert)
	}

	record := recordWorkloadCostStmt(tblWorkloads)
	// Keyed by burst, because the reaping replica has the claimed burst and
	// nothing else.
	if !strings.Contains(record, `WHERE data->>'BurstID' = $1`) {
		t.Errorf("the cost write does not resolve the workload by burst id:\n%s", record)
	}
	// First-write-wins lives in the predicate, so two replicas cannot both win.
	if !strings.Contains(record, `COALESCE(data->'Cost', 'null'::jsonb) = 'null'::jsonb`) {
		t.Errorf("the cost write would overwrite an observation already frozen:\n%s", record)
	}
	// One key, so a lifecycle write landing concurrently keeps its status.
	if !strings.Contains(record, `jsonb_set(data, '{Cost}', $2::jsonb)`) {
		t.Errorf("the cost write is not a targeted patch of the Cost key:\n%s", record)
	}
}

// The JSONB encoding the two statements above address by name. A json tag or a
// rename on Workload.Cost — or a field on WorkloadCost that does not survive a
// round trip — turns every guard above into a no-op that silently drops the
// observation, and nothing in the Go types would notice.
func TestWorkloadCostJSONRoundTrip(t *testing.T) {
	frozen := testObservation(2.25, time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC))
	data, err := json.Marshal(&Workload{ID: "wl_1", BurstID: costBurst, Cost: &frozen})
	if err != nil {
		t.Fatal(err)
	}
	var keyed map[string]json.RawMessage
	if err := json.Unmarshal(data, &keyed); err != nil {
		t.Fatal(err)
	}
	if _, ok := keyed["Cost"]; !ok {
		t.Fatalf("the workload document has no 'Cost' key; the SQL guards address one that is not there:\n%s", data)
	}

	var back Workload
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Cost == nil || *back.Cost != frozen {
		t.Errorf("cost round trip = %+v, want %+v", back.Cost, frozen)
	}

	// A legacy record — every workload written before this field existed — must
	// decode with no observation rather than a zero-valued one.
	var legacy Workload
	if err := json.Unmarshal([]byte(`{"ID":"wl_old","BurstID":"burst_old"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.Cost != nil {
		t.Errorf("legacy record decoded a cost of %+v; $0.00 is a measurement nobody took", legacy.Cost)
	}
	if omitted, err := json.Marshal(&legacy); err != nil || strings.Contains(string(omitted), "Cost") {
		t.Errorf("a record with no observation encodes a Cost key: %s (err %v)", omitted, err)
	}
}

// Two reapers racing the same burst — a redelivered teardown job against a
// watchdog sweep, on one replica — must produce exactly one observation, and
// the readers running beside them must never see a half-written one. Run under
// -race, which is where the store lock is actually judged.
func TestRecordWorkloadCostForBurstConcurrent(t *testing.T) {
	const racers = 8
	s := emptyStore()
	s.workloads["wl_1"] = &Workload{ID: "wl_1", BurstID: costBurst, Status: "succeeded"}
	p := &workloadPersister{rows: map[string]*Workload{
		costBurst: {ID: "wl_1", BurstID: costBurst, Status: "succeeded"},
	}}
	s.persist = p

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		winners  int
		frozenAt = time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	)
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each racer measures its own runtime, so a second winner would be
			// visible as a different number, not just a second write.
			observation := testObservation(float64(i+1), frozenAt.Add(time.Duration(i)*time.Minute))
			recorded, err := s.RecordWorkloadCostForBurst(context.Background(), observation)
			if err != nil {
				t.Errorf("racer %d: %v", i, err)
			}
			if recorded {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Readers run against the same records the writers are patching.
			if w, err := s.GetWorkload("wl_1"); err == nil && w.Cost != nil && w.Cost.Basis != WorkloadCostBasisRateRuntime {
				t.Errorf("reader saw a half-written observation: %+v", w.Cost)
			}
			s.WorkloadsForCustomer("cust_a", 10)
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Errorf("racers that recorded = %d, want exactly 1", winners)
	}
	if p.costWrites != 1 {
		t.Errorf("durable cost writes = %d, want exactly 1", p.costWrites)
	}
	w, err := s.GetWorkload("wl_1")
	if err != nil || w.Cost == nil {
		t.Fatalf("workload = %+v (err %v), want one frozen observation", w, err)
	}
	if row := p.rows[costBurst]; row.Cost == nil || *row.Cost != *w.Cost {
		t.Errorf("durable cost %+v and in-memory cost %+v disagree; two racers each won a half",
			row.Cost, w.Cost)
	}
}

// The half of both statements that lives in SQL rather than in Go: the JSONB
// keys really are the Go field names, the cost write really is first-write-wins
// in the database (which is where two REPLICAS race, not two goroutines), and
// the upsert really does hold a stored observation back from a document that
// has none.
//
// Needs a real database. Gated on YSCALE_TEST_DATABASE_URL like
// TestWorkloadByBurstPostgres, so ordinary CI does NOT cover it; CI should set
// it against a throwaway Postgres.
func TestWorkloadCostPostgres(t *testing.T) {
	dsn := os.Getenv("YSCALE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set YSCALE_TEST_DATABASE_URL to a throwaway Postgres to run this integration test")
	}
	ctx := context.Background()
	p := &pgPersister{pool: freshSchemaPool(t, dsn, "yscale_test_workload_cost", 4)}
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	w := &Workload{ID: "wl_1", CustomerID: "cust_a", BurstID: costBurst, Status: "succeeded"}
	if err := p.upsertWorkload(w); err != nil {
		t.Fatalf("seed the workload row: %v", err)
	}

	first := testObservation(2.25, time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC))
	recorded, err := p.recordWorkloadCost(ctx, costBurst, &first)
	if err != nil || !recorded {
		t.Fatalf("recordWorkloadCost = %v, err %v; want true/nil — the JSONB burst key does not match the Go field", recorded, err)
	}

	// The second reaper: same burst, later measurement, nothing to write.
	second := testObservation(9.99, first.FrozenAt.Add(2*time.Hour))
	if recorded, err := p.recordWorkloadCost(ctx, costBurst, &second); err != nil || recorded {
		t.Fatalf("second recordWorkloadCost = %v, err %v; want false/nil", recorded, err)
	}

	// A burst with no workload row is nothing to record, not a failure.
	if recorded, err := p.recordWorkloadCost(ctx, "burst_nobody", &first); err != nil || recorded {
		t.Errorf("recordWorkloadCost for an unknown burst = %v, err %v; want false/nil", recorded, err)
	}

	// The stale replica's whole-document write.
	if err := p.upsertWorkload(&Workload{ID: "wl_1", CustomerID: "cust_a", BurstID: costBurst, Status: "cancelled"}); err != nil {
		t.Fatalf("stale replica upsert: %v", err)
	}
	got, err := p.workloadByBurst(ctx, costBurst)
	if err != nil || got == nil {
		t.Fatalf("workloadByBurst = %+v, err %v", got, err)
	}
	if got.Cost == nil {
		t.Fatal("the whole-document write erased the observation; the burst it was measured from is gone, so nothing can re-derive it")
	}
	if *got.Cost != first {
		t.Errorf("stored cost = %+v, want the first observation %+v", got.Cost, first)
	}
	if got.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled — the guard holds back one field, not the write", got.Status)
	}
}
