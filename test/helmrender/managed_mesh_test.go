package helmrender

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestManagedMeshIsDefaultAndFailsClosed(t *testing.T) {
	out := render(t)
	guard := regexp.MustCompile(`(?m)if \[ -z "\$TS_LOGIN_SERVER" \]; then\n\s+echo "FATAL: managed mesh requires a login server[^\n]*\n\s+exit 1\n\s+fi`)
	guards := guard.FindAllString(out, -1)
	if len(guards) != 2 {
		t.Fatalf("want connector and gateway mesh guards; found %d", len(guards))
	}
	for _, script := range guards {
		if err := exec.Command("sh", "-c", "TS_LOGIN_SERVER=''; "+script).Run(); err == nil {
			t.Error("missing login server allowed a shared-mesh fallback")
		}
		if err := exec.Command("sh", "-c", "TS_LOGIN_SERVER=https://mesh.example.invalid; "+script).Run(); err != nil {
			t.Errorf("configured managed server rejected: %v", err)
		}
	}
	legacy := render(t, "--set", "managedMesh=false")
	if strings.Contains(legacy, "FATAL: managed mesh requires a login server") {
		t.Error("explicit legacy chart mode still enforces managed mesh")
	}
}
