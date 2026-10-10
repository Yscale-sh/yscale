package decider

import (
	"testing"

	"github.com/yscale-sh/yscale/pkg/workload"
)

func TestResolveStorageArtifactIsWriteOnlyAndHasNoVolumeAffinity(t *testing.T) {
	d := New(Config{})
	wl := minimalPlanWorkload()
	wl.Spec.Storage = &workload.Storage{Artifacts: []workload.ArtifactSpec{{
		Name: "results", Target: "/outputs", MaxFiles: 50, MaxSizeGB: 2,
		To: workload.BucketRef{Bucket: "runs", Prefix: "team/", Endpoint: "https://r2.example", Region: "auto", CredentialsSecret: "r2-creds"},
	}}}

	resolved, err := d.resolveStorage(wl, "tenant-a")
	if err != nil {
		t.Fatalf("resolveStorage: %v", err)
	}
	if len(resolved.pending) != 0 || resolved.pinBackend != "" || resolved.pinDCRegion != "" {
		t.Fatalf("artifact created volume state or affinity: %+v", resolved)
	}
	if len(resolved.bindings) != 1 {
		t.Fatalf("bindings = %+v", resolved.bindings)
	}
	binding := resolved.bindings[0]
	if binding.Type != "artifact" || binding.Target != "/outputs" || binding.WriteTo == nil || binding.WriteTo.Bucket != "runs" || binding.Source != nil || binding.SnapshotTo != nil {
		t.Fatalf("artifact binding = %+v", binding)
	}
}
