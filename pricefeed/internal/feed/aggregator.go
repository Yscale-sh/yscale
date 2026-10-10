package feed

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ErrNotImplemented is returned by stub Sources. The aggregator logs it
// once and stops polling that source, so a skeleton build runs cleanly
// without log spam.
var ErrNotImplemented = errors.New("pricefeed: source not implemented")

// Aggregator drives each Source on its own Interval and feeds the
// results into the Feed.
type Aggregator struct {
	Feed    *Feed
	Sources []Source
	Log     *slog.Logger
}

// Run starts one polling goroutine per Source and blocks until ctx is
// done.
func (a *Aggregator) Run(ctx context.Context) {
	for _, src := range a.Sources {
		go a.poll(ctx, src)
	}
	<-ctx.Done()
}

// poll fetches immediately, then on the source's interval, until ctx
// is done or the source reports it is not implemented.
func (a *Aggregator) poll(ctx context.Context, src Source) {
	if !a.fetchOnce(ctx, src) {
		return
	}
	t := time.NewTicker(src.Interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !a.fetchOnce(ctx, src) {
				return
			}
		}
	}
}

// fetchOnce performs one refresh. It returns false when the source
// should no longer be polled (i.e. it is a stub).
func (a *Aggregator) fetchOnce(ctx context.Context, src Source) bool {
	offerings, err := src.Fetch(ctx)
	switch {
	case errors.Is(err, ErrNotImplemented):
		a.Log.Warn("pricefeed source not implemented; skipping", "source", src.Name())
		return false
	case err != nil:
		a.Log.Warn("pricefeed source fetch failed", "source", src.Name(), "error", err)
		return true
	default:
		a.Feed.ReplaceProvider(src.Name(), offerings)
		a.Log.Info("pricefeed refreshed", "source", src.Name(), "offerings", len(offerings))
		return true
	}
}
