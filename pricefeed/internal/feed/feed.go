// Package feed is the in-memory price + availability index for the
// yscale-pricefeed service: provider Sources push Offerings in, the
// Feed dedupes and diffs them, and subscribers (the central decider)
// receive a snapshot plus a live delta stream.
package feed

import (
	"context"
	"sync"
	"time"
)

// Reliability tiers an Offering can carry.
const (
	OnDemand = "on-demand"
	Spot     = "spot"
)

// Offering is one priced, schedulable VM/GPU shape from one provider,
// in one region, at one reliability tier.
type Offering struct {
	Provider    string    `json:"provider"` // "aws" | "linode" | "flyio" | ...
	SKU         string    `json:"sku"`      // provider-native plan/instance-type id
	Kind        string    `json:"kind"`     // yscale abstract kind: "cpu", "a100", "rtx4000ada", ...
	Region      string    `json:"region"`
	GPU         bool      `json:"gpu"`
	GPUCount    int       `json:"gpu_count,omitempty"`
	VCPU        int       `json:"vcpu,omitempty"`
	MemoryMB    int       `json:"memory_mb,omitempty"`
	Reliability string    `json:"reliability"` // OnDemand | Spot
	USDPerHour  float64   `json:"usd_per_hour"`
	Available   bool      `json:"available"` // currently provisionable in this region
	ObservedAt  time.Time `json:"observed_at"`
}

// Key uniquely identifies an Offering across refreshes — used to diff
// successive Source fetches into upsert/remove deltas.
func (o Offering) Key() string {
	return o.Provider + "|" + o.Region + "|" + o.SKU + "|" + o.Reliability
}

// Delta is one change to the index, streamed to subscribers.
type Delta struct {
	Op       string   `json:"op"` // OpUpsert | OpRemove
	Offering Offering `json:"offering"`
}

const (
	OpUpsert = "upsert"
	OpRemove = "remove"
)

// Source is a provider price/availability fetcher. The aggregator
// polls each Source on its own Interval and feeds the result into the
// Feed. Implementations live in pricefeed/internal/sources.
type Source interface {
	// Name is the provider id ("aws", "linode", ...). One Source owns
	// all Offerings tagged with its Name in the Feed.
	Name() string
	// Interval is how often the aggregator should call Fetch.
	Interval() time.Duration
	// Fetch returns the provider's full current offering set; the
	// aggregator diffs it against the previous fetch for this provider.
	Fetch(ctx context.Context) ([]Offering, error)
}

// Feed is the deduped offering index plus a delta pub/sub.
type Feed struct {
	mu      sync.RWMutex
	index   map[string]Offering // by Offering.Key()
	subs    map[int]chan Delta
	nextSub int
}

// New returns an empty Feed.
func New() *Feed {
	return &Feed{
		index: make(map[string]Offering),
		subs:  make(map[int]chan Delta),
	}
}

// Snapshot returns every current Offering (order unspecified).
func (f *Feed) Snapshot() []Offering {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Offering, 0, len(f.index))
	for _, o := range f.index {
		out = append(out, o)
	}
	return out
}

// ReplaceProvider swaps in a provider's full offering set: anything new
// or changed is upserted, anything no longer present is removed. Each
// change is published to subscribers.
func (f *Feed) ReplaceProvider(provider string, offerings []Offering) {
	f.mu.Lock()
	defer f.mu.Unlock()

	next := make(map[string]Offering, len(offerings))
	for _, o := range offerings {
		next[o.Key()] = o
	}
	for k, o := range next {
		if cur, ok := f.index[k]; !ok || changed(cur, o) {
			f.index[k] = o
			f.publish(Delta{Op: OpUpsert, Offering: o})
		}
	}
	for k, o := range f.index {
		if o.Provider != provider {
			continue
		}
		if _, ok := next[k]; !ok {
			delete(f.index, k)
			f.publish(Delta{Op: OpRemove, Offering: o})
		}
	}
}

// Subscribe registers a buffered delta channel and returns its id. The
// caller must Unsubscribe. On overflow, deltas are dropped — a slow
// subscriber should re-sync via Snapshot.
func (f *Feed) Subscribe() (int, <-chan Delta) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextSub
	f.nextSub++
	ch := make(chan Delta, 256)
	f.subs[id] = ch
	return id, ch
}

// Unsubscribe drops a subscriber and closes its channel.
func (f *Feed) Unsubscribe(id int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ch, ok := f.subs[id]; ok {
		delete(f.subs, id)
		close(ch)
	}
}

// publish fans a delta out to all subscribers. Caller must hold f.mu;
// the send is non-blocking so a slow subscriber can't stall a refresh.
func (f *Feed) publish(d Delta) {
	for _, ch := range f.subs {
		select {
		case ch <- d:
		default:
		}
	}
}

// changed reports whether the priced fields of two offerings differ.
// ObservedAt is excluded so an otherwise-unchanged offering doesn't
// churn the delta stream on every refresh.
func changed(a, b Offering) bool {
	a.ObservedAt, b.ObservedAt = time.Time{}, time.Time{}
	return a != b
}
