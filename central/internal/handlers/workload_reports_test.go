package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
)

type reportDeleteReader struct {
	*fakeProviderDeletes
	record lifecycle.ProviderDeleteRecord
	err    error
}

func (f *reportDeleteReader) GetProviderDelete(_ context.Context, customerID, clusterID, burstID string) (lifecycle.ProviderDeleteRecord, error) {
	if f.err != nil {
		return lifecycle.ProviderDeleteRecord{}, f.err
	}
	if f.record.CustomerID != customerID || f.record.ClusterID != clusterID || f.record.BurstID != burstID {
		return lifecycle.ProviderDeleteRecord{}, lifecycle.ErrNotFound
	}
	return f.record, nil
}

func TestWorkloadReportsMissingBookingRequiresCleanupEvidence(t *testing.T) {
	for _, operation := range []string{"complete", "cancel"} {
		for _, proof := range []string{"none", "receipt", "foreign-receipt", "no-reader", "queued", "terminated", "foreign-delete", "reader-outage"} {
			t.Run(operation+"/"+proof, func(t *testing.T) {
				store, h, cust := completeStore(t)
				wl, _ := store.GetWorkload("wl1")
				wl.ClusterID = "report-cluster"
				if err := store.PutWorkloadDurable(wl); err != nil {
					t.Fatal(err)
				}
				if err := store.DeleteBurst("b1"); err != nil {
					t.Fatal(err)
				}
				wantCode, wantStatus := http.StatusServiceUnavailable, burstCleanupUnknownStatus
				deletes := newFakeProviderDeletes()
				switch proof {
				case "receipt", "foreign-receipt":
					owner := cust.ID
					if proof == "foreign-receipt" {
						owner = "foreign"
					} else {
						wantCode, wantStatus = http.StatusOK, burstAlreadyReapedStatus
					}
					if _, err := store.RecordBurstReap(context.Background(), "b1", owner); err != nil {
						t.Fatal(err)
					}
				case "no-reader":
					h.Deletes = deletes
					wantStatus = burstCleanupPendingStatus
				case "queued", "terminated", "foreign-delete", "reader-outage":
					reader := &reportDeleteReader{fakeProviderDeletes: deletes, record: lifecycle.ProviderDeleteRecord{CustomerID: cust.ID, ClusterID: wl.ClusterID, BurstID: wl.BurstID, State: lifecycle.ProviderDeleteQueued}}
					h.Deletes = reader
					wantCode, wantStatus = http.StatusOK, burstDeletingStatus
					switch proof {
					case "terminated":
						reader.record.State, wantStatus = lifecycle.ProviderDeleteTerminated, burstAlreadyReapedStatus
					case "foreign-delete":
						reader.record.CustomerID = "foreign"
						wantCode, wantStatus = http.StatusServiceUnavailable, burstCleanupPendingStatus
					case "reader-outage":
						reader.err = errors.New("synthetic reader outage")
						wantCode, wantStatus = http.StatusServiceUnavailable, burstCleanupUnknownStatus
					}
				}
				response := httptest.NewRecorder()
				if operation == "complete" {
					h.Complete(response, completeReq(cust, "wl1", `{"phase":"Succeeded"}`))
				} else {
					h.Cancel(response, cancelReq(cust, "wl1"))
				}
				if response.Code != wantCode || !strings.Contains(response.Body.String(), `"status":"`+wantStatus+`"`) {
					t.Fatalf("response=%d %s, want %d %s", response.Code, response.Body.String(), wantCode, wantStatus)
				}
				current, _ := store.GetWorkload("wl1")
				if current.FinishedAt == nil || deletes.requests != 0 || h.Reaper.(*fakeReaper).teardownCount("b1") != 0 {
					t.Fatal("lost terminal observation or fabricated provider cleanup")
				}
			})
		}
	}
}

func TestWorkloadReportsLostClaimIsNotProviderAbsence(t *testing.T) {
	store, h, _ := completeStore(t)
	wl, _ := store.GetWorkload("wl1")
	observed, _ := store.GetBurst("b1")
	if err := store.DeleteBurst("b1"); err != nil {
		t.Fatal(err)
	}
	if got := h.reapAuthorizedWorkload(context.Background(), wl, observed, "synthetic lost claim"); got != ReapOutcomeUnknown {
		t.Fatalf("lost claim reported %v without absence evidence", got)
	}
}
