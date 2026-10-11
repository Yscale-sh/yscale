package agent

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestIdleTeardownRetainsTerminatingAndUnknownPods(t *testing.T) {
	for _, opts := range []idlePodOpts{
		{name: "terminating", deleting: true},
		{name: "terminating-terminal", deleting: true, phase: corev1.PodSucceeded},
		{name: "unknown", phase: corev1.PodUnknown},
	} {
		t.Run(opts.name, func(t *testing.T) {
			node := burstNode("abc123abc123", corev1.ConditionTrue, time.Hour)
			opts.nodeName = node.Name
			w, rec, _ := newIdleWatcher(node, idlePod(opts))
			sweepPast(w, time.Now())
			if got := rec.snapshot(); len(got) != 0 {
				t.Fatalf("occupied node reported idle: %+v", got)
			}
			opts.nodeName = preflightNodeName
			h, _ := newPreflightHandler(preflightNode(false), idlePod(opts))
			if _, err := h.OnPrepareIdleTeardown(context.Background(), preflightCommand()); err == nil {
				t.Fatal("occupied node approved for teardown")
			}
		})
	}
}

func TestDrainRetriesEvictionErrorsUntilDeadline(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		recover bool
		cancel  bool
	}{
		{name: "transient", err: apierrors.NewInternalError(errors.New("webhook unavailable")), recover: true},
		{name: "persistent", err: errors.New("transport failed")},
		{name: "pdb", err: apierrors.NewTooManyRequests("PDB denied", 0)},
		{name: "cancelled", err: errors.New("transport failed"), cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				k8s := fake.NewSimpleClientset(
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: drainNodeName}},
					drainPod("default", "work", false),
				)
				h := NewRealHandler(k8s, nil, discardLogger())
				h.DrainTimeout = 3 * evictionPollInterval
				start := time.Now()
				attempts := 0
				k8s.PrependReactor("create", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
					if a.GetSubresource() != "eviction" {
						return false, nil, nil
					}
					attempts++
					if attempts > 1 && time.Since(start) < evictionPollInterval {
						t.Error("eviction retried without backoff")
					}
					if tc.cancel {
						cancel()
					}
					if tc.recover && attempts > 1 {
						return true, nil, nil
					}
					return true, nil, tc.err
				})
				k8s.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					if time.Since(start) < h.DrainTimeout {
						t.Error("pod deleted before drain deadline")
					}
					return false, nil, nil
				})
				err := h.drainAndDeleteNode(ctx, drainNodeName, false)
				if tc.cancel {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("drain error = %v, want cancellation", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				deletes := podActions(k8s, "delete")
				if tc.recover || tc.cancel {
					if len(deletes) != 0 {
						t.Fatalf("unexpected deletion: %v", deletes)
					}
				} else if len(deletes) != 1 {
					t.Fatalf("deletions = %v, want one after deadline", deletes)
				}
				if !tc.cancel && attempts < 2 {
					t.Fatalf("eviction attempts = %d, want retries", attempts)
				}
			})
		})
	}
}

func completionEvents(t *testing.T, cw *CompletionWatcher, k8s *fake.Clientset, events ...watch.Event) {
	t.Helper()
	wi := watch.NewFakeWithChanSize(len(events), false)
	for _, ev := range events {
		wi.Action(ev.Type, ev.Object)
	}
	wi.Stop()
	k8s.PrependWatchReactor("jobs", func(ktesting.Action) (bool, watch.Interface, error) { return true, wi, nil })
	if err := cw.runOnce(context.Background()); err == nil {
		t.Fatal("expected closed watch")
	}
}

func TestCompletionWatcherDeliversDeletedJobsAndRetries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition batchv1.JobConditionType
		want      string
	}{
		{name: "succeeded", condition: batchv1.JobComplete, want: "Succeeded"},
		{name: "failed", condition: batchv1.JobFailed, want: "Failed"},
		{name: "unknown", want: "Failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			central := &fakeCompleter{}
			cw := NewCompletionWatcher(k8s, nil, central, discardLogger())
			job := runningJob("work", "wl_deleted")
			if tc.condition != "" {
				job = terminalJob("work", "wl_deleted", tc.condition)
			}
			completionEvents(t, cw, k8s, watch.Event{Type: watch.Deleted, Object: job})
			out := central.onlyReceipt(t, "wl_deleted/"+tc.want)
			if tc.condition == "" && out.Compute.Reason != "completion-unknown: job deleted before terminal status was observed" {
				t.Fatalf("unknown completion reason = %q", out.Compute.Reason)
			}
			cw.resync(context.Background())
			central.onlyReceipt(t, "wl_deleted/"+tc.want)

			central = &fakeCompleter{doneErr: errors.New("central unavailable")}
			cw = NewCompletionWatcher(k8s, nil, central, discardLogger())
			// The delete may have no terminal status; retain the prior observation.
			completionEvents(t, cw, k8s,
				watch.Event{Type: watch.Modified, Object: job},
				watch.Event{Type: watch.Deleted, Object: runningJob("work", "wl_deleted")},
			)
			central.setDoneErr(nil)
			// Retry must not depend on the deleted Job or even a successful list.
			k8s.PrependReactor("list", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("apiserver unavailable")
			})
			cw.resync(context.Background())
			central.onlyReceipt(t, "wl_deleted/"+tc.want)
			cw.resync(context.Background())
			central.onlyReceipt(t, "wl_deleted/"+tc.want)
		})
	}
}

type retryCancelCentral struct {
	recordingCentral
	attempts []string
	err      error
}

func (c *retryCancelCentral) CancelWorkload(_ context.Context, id string) error {
	c.attempts = append(c.attempts, id)
	return c.err
}

func TestWorkloadCancellationRetainsTrackingAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		central := &retryCancelCentral{err: errors.New("central unavailable")}
		r := discardReconciler(central)
		r.resyncInterval = time.Second
		r.submitted["default/work"] = "wl_deleted"
		r.handleEvent(ctx, watch.Event{Type: watch.Deleted, Object: crWithStatus("default", "work", nil)})
		if r.submitted["default/work"] != "wl_deleted" {
			t.Error("failed cancellation discarded tracked id")
		}
		go r.statusSyncLoop(ctx)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if len(central.attempts) != 2 {
			t.Fatalf("cancellation attempts = %v, want periodic retry", central.attempts)
		}
		if r.submitted["default/work"] != "wl_deleted" {
			t.Fatal("failed retry discarded tracked id")
		}
		central.err = nil
		time.Sleep(time.Second)
		synctest.Wait()
		if len(central.attempts) != 3 || central.attempts[2] != "wl_deleted" {
			t.Fatalf("retry attempts = %v", central.attempts)
		}
		if _, ok := r.submitted["default/work"]; ok {
			t.Fatal("successful cancellation kept tracked id")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(central.attempts) != 3 {
			t.Fatal("successful cancellation was retried")
		}
	})
}
