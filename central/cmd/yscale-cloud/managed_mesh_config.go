// yscale:proprietary

package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// validateManagedMeshConfig fails before database access or worker startup.
// Empty configuration remains useful for local diagnostics and explicit
// HEADSCALE_* setups; it is not a usable factory-backed deployment.
func validateManagedMeshConfig() error {
	for _, key := range []string{"TS_OAUTH_CLIENT_ID", "TS_OAUTH_CLIENT_SECRET"} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("%s is not supported by the managed-mesh release; configure your own FACTORY_URL and FACTORY_BEARER_TOKEN (see docs/self-hosted-mesh.md)", key)
		}
	}
	endpoint := os.Getenv("FACTORY_URL")
	token := strings.TrimSpace(os.Getenv("FACTORY_BEARER_TOKEN"))
	if endpoint == "" && token == "" {
		return nil
	}
	if endpoint == "" || token == "" {
		return fmt.Errorf("FACTORY_URL and FACTORY_BEARER_TOKEN must be configured together")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		// Never echo an invalid URL: it may contain embedded credentials.
		return fmt.Errorf("FACTORY_URL must be an absolute http(s) URL without credentials, query or fragment")
	}
	return nil
}
