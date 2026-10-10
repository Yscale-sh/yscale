package decider

import (
	"fmt"

	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/cache"
	"github.com/yscale-sh/yscale/pkg/protocol"
	"github.com/yscale-sh/yscale/pkg/workload"
)

// storageResolution is the decider's pre-flight pass over a workload's
// storage spec. It looks up existing volumes and determines (a) any
// affinity constraints (a workload referencing an existing volume must
// route to that volume's backend+DC) and (b) which protocol-level
// StorageBindings the agent will need.
//
// The resolution is computed after pure routing and GPU price admission,
// then validates that existing-volume affinity matches the selected target.
// Volumes the customer references for the first time become "pending creates"
// — allocated only after the backend successfully boots the burst.
type storageResolution struct {
	// Bindings to forward via AddPeer.Storage to the agent. Filled
	// for both existing-and-reused volumes and pending-create ones.
	bindings []protocol.StorageBinding

	// Pending: volumes that don't yet exist in state — will be
	// allocated on the chosen backend and persisted post-CreateNode.
	pending []*state.PersistentVolume

	// Affinity: when non-empty, restrict the backend choice to this
	// (backend, region) pair so the existing volume(s) attach.
	pinBackend  string
	pinDCRegion string
}

// resolveStorage computes the pre-flight storage plan. Returns nil
// (no storage needed) when the spec is empty or store is unwired.
func (d *Decider) resolveStorage(wl *workload.Workload, customerID string) (*storageResolution, error) {
	if wl.Spec.Storage == nil {
		return nil, nil
	}
	res := &storageResolution{}

	// CACHE volumes — keyed by (tenant, source URI).
	for _, c := range wl.Spec.Storage.Cache {
		key := cache.DeriveKey(customerID, c.Source.URI())
		var existing *state.PersistentVolume
		if d.store != nil {
			if v, err := d.store.GetCacheVolume(customerID, string(key)); err == nil {
				existing = v
			}
		}
		if existing != nil {
			if err := res.applyAffinity(existing); err != nil {
				return nil, fmt.Errorf("cache %q: %w", c.Name, err)
			}
			d.store.TouchPersistentVolume(existing.ID)
			res.bindings = append(res.bindings, protocol.StorageBinding{
				Name:      c.Name,
				Type:      "cache",
				LocalPath: localCachePath(string(key)),
				Target:    c.Target,
				CacheKey:  string(key),
				Source:    bucketRefToProtocol(&c.Source),
			})
			continue
		}
		// Pending create: backend + DC determined by the eventual route.
		// We stash the spec; populateAfterBackend fills Backend + DC
		// + ID after the burst is booked.
		res.pending = append(res.pending, &state.PersistentVolume{
			TenantID:  customerID,
			Type:      "cache",
			CacheKey:  string(key),
			Retention: c.Retention,
			SourceURI: c.Source.URI(),
			SizeGB:    cacheSizeGB(c),
			State:     "active",
		})
		res.bindings = append(res.bindings, protocol.StorageBinding{
			Name:      c.Name,
			Type:      "cache",
			LocalPath: localCachePath(string(key)),
			Target:    c.Target,
			CacheKey:  string(key),
			Source:    bucketRefToProtocol(&c.Source),
		})
	}

	// PERSISTENT volumes — keyed by (tenant, name).
	for _, p := range wl.Spec.Storage.Persistent {
		var existing *state.PersistentVolume
		if d.store != nil {
			if v, err := d.store.GetNamedVolume(customerID, p.Name); err == nil {
				existing = v
			}
		}
		if existing != nil {
			if err := res.applyAffinity(existing); err != nil {
				return nil, fmt.Errorf("persistent %q: %w", p.Name, err)
			}
			d.store.TouchPersistentVolume(existing.ID)
			res.bindings = append(res.bindings, persistentToBinding(p, p.Name))
			continue
		}
		res.pending = append(res.pending, &state.PersistentVolume{
			TenantID:  customerID,
			Type:      "persistent",
			Name:      p.Name,
			Retention: nonEmpty(p.Retention, "keep"),
			SizeGB:    p.SizeGB,
			State:     "active",
		})
		res.bindings = append(res.bindings, persistentToBinding(p, p.Name))
	}

	// ARTIFACT outputs are Job-local host paths, not provider volumes. They
	// impose no placement affinity and create no persistent-volume record; the
	// binding only authorizes the exact object sink the connector uploads to
	// before it reports the workload complete.
	for _, artifact := range wl.Spec.Storage.Artifacts {
		res.bindings = append(res.bindings, protocol.StorageBinding{
			Name:    artifact.Name,
			Type:    "artifact",
			Target:  artifact.Target,
			WriteTo: bucketRefToProtocol(&artifact.To),
		})
	}
	return res, nil
}

// storageAffinity resolves ONLY the placement constraint a workload's existing
// volumes impose: the (backend, DC) its data already lives in.
//
// It is the read-only twin of resolveStorage and exists because the placement
// decision must know where a workload may land while the decision is still
// free. resolveStorage cannot serve that: it stamps LastUsedAt on every volume
// it reuses, so calling it from a preview would let looking at a decision
// change durable state. This performs lookups and nothing else — no touch, no
// pending creates, no bindings — and derives the pin through the SAME
// applyAffinity rule, so the two cannot disagree about where a workload fits.
func (d *Decider) storageAffinity(wl *workload.Workload, customerID string) (string, string, error) {
	if wl.Spec.Storage == nil || d.store == nil {
		return "", "", nil
	}
	res := &storageResolution{}
	for _, c := range wl.Spec.Storage.Cache {
		key := cache.DeriveKey(customerID, c.Source.URI())
		v, err := d.store.GetCacheVolume(customerID, string(key))
		if err != nil {
			continue
		}
		if err := res.applyAffinity(v); err != nil {
			return "", "", fmt.Errorf("cache %q: %w", c.Name, err)
		}
	}
	for _, p := range wl.Spec.Storage.Persistent {
		v, err := d.store.GetNamedVolume(customerID, p.Name)
		if err != nil {
			continue
		}
		if err := res.applyAffinity(v); err != nil {
			return "", "", fmt.Errorf("persistent %q: %w", p.Name, err)
		}
	}
	return res.pinBackend, res.pinDCRegion, nil
}

// applyAffinity records the routing constraint imposed by an existing
// volume. If a previous volume already pinned a different
// (backend, DC), error — the workload references two volumes that
// can't coexist on the same burst.
func (r *storageResolution) applyAffinity(v *state.PersistentVolume) error {
	if r.pinBackend == "" {
		r.pinBackend = v.Backend
		r.pinDCRegion = v.DCRegion
		return nil
	}
	if r.pinBackend != v.Backend || r.pinDCRegion != v.DCRegion {
		return fmt.Errorf("storage affinity conflict: existing volume on %s/%s but already pinned to %s/%s",
			v.Backend, v.DCRegion, r.pinBackend, r.pinDCRegion)
	}
	return nil
}

// recordCreated finalizes a resolution post-CreateNode: writes any
// pending PVs to state with their newly-known Backend / BackendVolID /
// DCRegion. Bindings already point at the same paths so the agent
// sees a consistent view.
func (d *Decider) recordPending(res *storageResolution, backendName, dcRegion string) {
	if d.store == nil || res == nil {
		return
	}
	for _, v := range res.pending {
		v.ID = "pv_" + randHex(6)
		v.Backend = backendName
		v.DCRegion = dcRegion
		v.CreatedAt = nowUTC()
		v.LastUsedAt = v.CreatedAt
		// BackendVolID is set later when we wire actual NV provisioning
		// per backend. For now the field stays empty — the burst's
		// LocalPath under /local-cache or /local-persist is sufficient
		// to address the volume on the burst-side filesystem.
		d.store.PutPersistentVolume(v)
	}
}

func persistentToBinding(p workload.PersistentSpec, name string) protocol.StorageBinding {
	b := protocol.StorageBinding{
		Name:      name,
		Type:      "persistent",
		LocalPath: localPersistPath(name),
		Target:    p.Target,
	}
	if p.Snapshot != nil {
		b.SnapshotTo = bucketRefToProtocol(&p.Snapshot.To)
		if p.Snapshot.Interval > 0 {
			b.SnapshotInterval = p.Snapshot.Interval.String()
		}
	}
	return b
}

func bucketRefToProtocol(b *workload.BucketRef) *protocol.BucketRef {
	if b == nil {
		return nil
	}
	return &protocol.BucketRef{
		Bucket:            b.Bucket,
		Prefix:            b.Prefix,
		Endpoint:          b.Endpoint,
		Region:            b.Region,
		CredentialsSecret: b.CredentialsSecret,
	}
}

// localCachePath is the path on the burst node where backend cache
// volumes are mounted. /local-cache/<key>/ holds the actual files;
// the burst init bind-mounts that into the customer's pod at the
// workload-spec target.
func localCachePath(cacheKey string) string {
	return "/local-cache/" + cacheKey
}

// localPersistPath mirrors localCachePath for writable volumes.
func localPersistPath(name string) string { return "/local-persist/" + name }

func nonEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// cacheSizeGB picks a reasonable default backing-volume size for a
// cache spec. SizeHintGB wins when set; otherwise we ballpark 100GB
// — large enough for most ML models, small enough to not waste
// storage on tiny prefixes.
func cacheSizeGB(c workload.CacheSpec) int {
	if c.SizeHintGB > 0 {
		return c.SizeHintGB
	}
	return 100
}
