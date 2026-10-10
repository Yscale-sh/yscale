package app

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	var output bytes.Buffer
	code := Run([]string{"version"}, Streams{Out: &output, Err: &output})
	if code != 0 || !strings.Contains(output.String(), "yscale-kube-bench") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestConfigInit(t *testing.T) {
	var output bytes.Buffer
	code := Run([]string{"config", "init"}, Streams{Out: &output, Err: &output})
	if code != 0 || !strings.Contains(output.String(), "BenchmarkConfig") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}
