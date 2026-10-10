// Package client exposes the factory-owned Headscale bridge to central.
package client

import (
	"context"

	"github.com/yscale-sh/yscale/factory/internal/hsclient"
)

type Headscale = hsclient.Headscale
type PolicyACL = hsclient.PolicyACL
type BoxInfo = hsclient.BoxInfo

func NewHeadscale(baseURL, apiKey, user string) *Headscale {
	return hsclient.NewHeadscale(baseURL, apiKey, user)
}

func RegisterHeadscale(ctx context.Context, box BoxInfo) (loginServer, apiKey, user, backendID string, err error) {
	return hsclient.RegisterHeadscale(ctx, box)
}
