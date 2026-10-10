package state

import (
	"context"
	"strings"
	"testing"
)

func TestBillingHoldProtectionCoversLiveAndAmbiguousBursts(t *testing.T) {
	s := New()
	s.PutWorkload(&Workload{ID: "wl_live", CustomerID: "cust_a", Status: "provisioning", Billing: &WorkloadBilling{HoldID: 1}})
	s.PutWorkload(&Workload{ID: "wl_ambiguous", CustomerID: "cust_a", Status: "failed", Billing: &WorkloadBilling{HoldID: 2, ManualAttention: true}})
	s.PutWorkload(&Workload{ID: "wl_done", CustomerID: "cust_a", Status: "succeeded", Billing: &WorkloadBilling{HoldID: 3}})
	if err := s.PutBurst(&Burst{ID: "burst_booked", CustomerID: "cust_a", Billing: &WorkloadBilling{HoldID: 4, WorkloadRef: "wl_not_yet_persisted"}}); err != nil {
		t.Fatal(err)
	}
	if !s.BillingHoldProtected("cust_a", "wl_live") {
		t.Fatal("live workload hold is not protected")
	}
	if !s.BillingHoldProtected("cust_a", "wl_ambiguous") {
		t.Fatal("ambiguous create hold is not protected")
	}
	if s.BillingHoldProtected("cust_a", "wl_done") {
		t.Fatal("settled terminal workload hold remained protected")
	}
	if !s.BillingHoldProtected("cust_a", "wl_not_yet_persisted") {
		t.Fatal("durable burst hold is not protected before workload persistence")
	}
	if s.BillingHoldProtected("cust_b", "wl_live") {
		t.Fatal("cross-tenant hold protection matched")
	}
}

func TestMarkWorkloadBillingManualAttentionTargetsTheDurableAssociation(t *testing.T) {
	association := func(holdID int64) *WorkloadBilling {
		return &WorkloadBilling{HoldID: holdID, WorkloadRef: "wl_paid", ReservedMicroUSD: 900}
	}
	store := emptyStore()
	store.workloads["wl_paid"] = &Workload{
		ID: "wl_paid", CustomerID: "tenant-one", BurstID: "burst-paid", Status: "failed", Billing: association(7),
	}
	persist := &workloadPersister{rows: map[string]*Workload{
		"burst-paid": {
			ID: "wl_paid", CustomerID: "tenant-one", BurstID: "burst-paid", Status: "failed", Billing: association(7),
		},
		"burst-other": {
			ID: "wl_other", CustomerID: "tenant-one", BurstID: "burst-other", Status: "failed",
			Billing: &WorkloadBilling{HoldID: 8, WorkloadRef: "wl_other", ReservedMicroUSD: 900},
		},
	}}
	store.persist = persist

	marked, err := store.MarkWorkloadBillingManualAttention(context.Background(), "tenant-one", "wl_paid", 7)
	if err != nil || !marked {
		t.Fatalf("MarkWorkloadBillingManualAttention = %v, err %v", marked, err)
	}
	if persist.attentionWrites != 1 || !persist.rows["burst-paid"].Billing.ManualAttention {
		t.Fatalf("durable association = %+v, writes %d", persist.rows["burst-paid"].Billing, persist.attentionWrites)
	}
	if persist.rows["burst-other"].Billing.ManualAttention {
		t.Fatal("targeted billing patch changed another workload")
	}
	if !store.BillingHoldProtected("tenant-one", "wl_paid") {
		t.Fatal("manual-attention association does not protect the hold")
	}
	stale := &Workload{
		ID: "wl_paid", CustomerID: "tenant-one", BurstID: "burst-paid", Status: "cancelled", Billing: association(7),
	}
	store.PutWorkload(stale)
	if !persist.rows["burst-paid"].Billing.ManualAttention {
		t.Fatal("a stale whole-workload update erased durable manual attention")
	}
	workload, err := store.GetWorkload("wl_paid")
	if err != nil || workload.Billing == nil || !workload.Billing.ManualAttention {
		t.Fatalf("in-memory association = %+v, err %v", workload, err)
	}

	if marked, err := store.MarkWorkloadBillingManualAttention(context.Background(), "tenant-one", "wl_paid", 99); err != nil || marked {
		t.Fatalf("wrong hold association marked = %v, err %v", marked, err)
	}
}

func TestMarkWorkloadBillingManualAttentionStatementIsATargetedPatch(t *testing.T) {
	stmt := markWorkloadBillingManualAttentionStmt(tblWorkloads)
	for _, want := range []string{
		"UPDATE " + tblWorkloads,
		"jsonb_set(data, '{Billing,ManualAttention}', 'true'::jsonb)",
		"WHERE id = $2",
		"data->>'CustomerID' = $1",
		"data->'Billing'->>'WorkloadRef' = $2",
		"(data->'Billing'->>'HoldID')::bigint = $3",
	} {
		if !strings.Contains(stmt, want) {
			t.Fatalf("manual-attention statement missing %q: %s", want, stmt)
		}
	}
	if strings.Contains(stmt, "INSERT") || strings.Contains(stmt, "ON CONFLICT") {
		t.Fatalf("manual-attention update rewrites a workload row: %s", stmt)
	}

	upsert := upsertWorkloadStmt(tblWorkloads)
	for _, want := range []string{
		`COALESCE((` + tblWorkloads + `.data->'Billing'->>'ManualAttention')::bool, false)`,
		tblWorkloads + `.data->'Billing'->>'WorkloadRef' = EXCLUDED.data->'Billing'->>'WorkloadRef'`,
		tblWorkloads + `.data->'Billing'->>'HoldID' = EXCLUDED.data->'Billing'->>'HoldID'`,
		`jsonb_set((CASE`,
		`'{Billing,ManualAttention}', 'true'::jsonb`,
	} {
		if !strings.Contains(upsert, want) {
			t.Fatalf("workload upsert does not preserve exact manual-attention association %q: %s", want, upsert)
		}
	}
}
