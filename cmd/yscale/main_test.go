package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainUsage(t *testing.T) {
	if os.Getenv("YSCALE_TEST_MAIN") == "1" {
		os.Args = append([]string{"yscale"}, os.Args[3:]...)
		main()
		os.Exit(0)
	}

	for _, tc := range []struct {
		name string
		args []string
		code int
	}{
		{name: "no subcommand", code: 2},
		{name: "unknown subcommand", args: []string{"unknown"}, code: 2},
		{name: "controller flag", args: []string{"--config", "missing.yaml"}, code: 2},
		{name: "help", args: []string{"help"}},
		{name: "short help", args: []string{"-h"}},
		{name: "long help", args: []string{"--help"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-test.run=^TestMainUsage$", "--"}, tc.args...)
			cmd := exec.Command(os.Args[0], args...)
			cmd.Env = append(os.Environ(), "YSCALE_TEST_MAIN=1")
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatal(err)
				}
				code = exitErr.ExitCode()
			}
			if code != tc.code {
				t.Errorf("exit code = %d, want %d; output: %s", code, tc.code, out)
			}
			if !strings.Contains(string(out), "Usage:") {
				t.Errorf("usage missing from output: %s", out)
			}
			if strings.Contains(string(out), "legacy") || strings.Contains(string(out), "OSS controller") {
				t.Errorf("usage advertises the removed controller: %s", out)
			}
		})
	}
}
