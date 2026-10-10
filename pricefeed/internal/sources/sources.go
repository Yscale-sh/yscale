// Package sources holds the per-provider price + availability fetchers
// for yscale-pricefeed. Each implements feed.Source.
//
// One source per file: aws.go (Price List + spot), linode.go (live
// API), flyio.go (weekly HTML scrape). A provider only belongs here if
// it can run a kubelet and join as a real node.
package sources

import (
	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
)

// All returns every provider source the pricefeed should poll.
func All() []feed.Source {
	return []feed.Source{
		NewAWS(),
		NewLinode(),
		NewFlyio(),
	}
}
