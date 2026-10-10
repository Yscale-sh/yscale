package decider

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/yscale-sh/yscale/pkg/backends"
)

// TestSweepOrphansCoversEveryBackendField is a coverage test, not a behaviour
// test. TestSweepOrphansRunsConfiguredBackendsWithTrackedSet only ever wires
// d.fly and d.linode, so it proves the loop sums and skips nil correctly but
// can never notice a backend the loop forgot — which is exactly what happened:
// d.gcp and d.azure were constructed and routable while absent from
// SweepOrphans, leaving GCP with no cross-restart orphan backstop at all even
// though it was live in the deployed central.
//
// Rather than hardcode today's five, this enumerates every backends.Backend
// field on Decider by reflection and asserts each one is swept. A sixth
// backend added later fails here until it is added to the loop, so the whole
// class of omission stays impossible instead of being fixed once.
func TestSweepOrphansCoversEveryBackendField(t *testing.T) {
	d := New(Config{})

	dv := reflect.ValueOf(d).Elem()
	dt := dv.Type()
	backendIface := reflect.TypeOf((*backends.Backend)(nil)).Elem()

	doubles := map[string]*recordingBackend{}
	for i := 0; i < dt.NumField(); i++ {
		f := dt.Field(i)
		if f.Type != backendIface {
			continue
		}
		rec := &recordingBackend{name: f.Name, cleanupDestroyed: 1}
		// Unexported fields need the unsafe addr dance even in-package.
		reflect.NewAt(f.Type, unsafe.Pointer(dv.Field(i).UnsafeAddr())).
			Elem().Set(reflect.ValueOf(backends.Backend(rec)))
		doubles[f.Name] = rec
	}

	if len(doubles) == 0 {
		t.Fatal("no backends.Backend fields found on Decider — reflection assumption broke")
	}

	tracked := map[string]bool{"node-live": true}
	destroyed, err := d.SweepOrphans(context.Background(), tracked)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	for name, rec := range doubles {
		if rec.cleanupCalls != 1 {
			t.Errorf("Decider field %q is a backends.Backend but SweepOrphans never called its CleanupOrphans "+
				"(calls=%d) — a burst it created after a crash would never be reclaimed",
				name, rec.cleanupCalls)
		}
		if !rec.cleanupTracked["node-live"] {
			t.Errorf("Decider field %q did not receive the tracked set", name)
		}
	}

	if want := len(doubles); destroyed != want {
		t.Errorf("destroyed = %d, want %d (one per swept backend)", destroyed, want)
	}
}
