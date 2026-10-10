package aws

import (
	"os"
	"strings"
	"testing"
)

func TestAWSBootstrapsUseCanonicalNodeNameForGKEProviderID(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"bootstrap.sh", "bootstrap-baked.sh"} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			script := string(body)
			for _, want := range []string{
				`CLUSTER_CLOUD="$(echo "${BOOT_RESP}" | jq -r '.cloud_provider // empty'`,
				`if [ "${CLUSTER_CLOUD}" = "gcp" ]; then`,
				`PROVIDER_ID="aws://${NODE_NAME}"`,
				`--provider-id=${PROVIDER_ID}`,
			} {
				if !strings.Contains(script, want) {
					t.Errorf("%s missing GKE provider-ID contract %q", path, want)
				}
			}
		})
	}
}
