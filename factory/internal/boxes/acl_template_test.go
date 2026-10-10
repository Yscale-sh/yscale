package boxes

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

// TestShippedACLTemplateRenders renders the real deploy/headscale/acls.hujson
// that the factory reads at provisioning time. Every other ACL test uses a
// fixture, so an edit to the shipped template (for example a "<...>" example
// inside a comment) could otherwise fail every real provisioning while all
// unit tests pass.
func TestShippedACLTemplateRenders(t *testing.T) {
	tmpl, err := os.ReadFile("../../../deploy/headscale/acls.hujson")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := renderACL(tmpl, "cust_template_test", "10.42.0.0/16", "10.43.0.0/16", "10.0.0.0/24")
	if err != nil {
		t.Fatalf("shipped ACL template does not render: %v", err)
	}
	acl, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(acl), `"10.42.0.0/16"`) {
		t.Fatal("rendered ACL is missing the pod CIDR route approval")
	}
}
