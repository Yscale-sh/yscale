package seedcloudsecrets

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type secret struct {
	APIVersion string            `json:"apiVersion,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Metadata   map[string]any    `json:"metadata"`
	Type       string            `json:"type,omitempty"`
	Immutable  bool              `json:"immutable,omitempty"`
	Data       map[string]string `json:"data"`
}

type call struct {
	Args  []string        `json:"args"`
	Input json.RawMessage `json:"input,omitempty"`
}

// The child process is a fake kubectl, never the real cluster client.
func TestKubectlHelper(t *testing.T) {
	if os.Getenv("YSCALE_SEED_TEST_HELPER") != "1" {
		return
	}
	args := os.Args
	for len(args) != 0 && args[0] != "--" {
		args = args[1:]
	}
	args = args[1:]
	var input []byte
	for _, arg := range args {
		if arg == "-" || arg == "/dev/stdin" {
			input, _ = io.ReadAll(os.Stdin)
			break
		}
	}
	dir := os.Getenv("YSCALE_SEED_TEST_STATE")
	log, err := os.OpenFile(filepath.Join(dir, "calls"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(2)
	}
	_ = json.NewEncoder(log).Encode(call{Args: args, Input: input})
	_ = log.Close()
	// Namespace is always explicit, and no test may escape its fake state.
	if len(args) < 4 || args[0] != "-n" || args[1] != "seed-test" {
		os.Exit(2)
	}
	args = args[2:]
	verb := args[0]
	if os.Getenv("YSCALE_SEED_TEST_FAIL_VERB") == verb {
		fmt.Fprintln(os.Stdout, "fixture-sensitive-server-output")
		fmt.Fprintln(os.Stderr, "fixture-sensitive-server-error")
		os.Exit(1)
	}
	var incoming secret
	if len(input) != 0 && json.Unmarshal(input, &incoming) != nil {
		os.Exit(2)
	}
	name := ""
	if incoming.Metadata != nil {
		name, _ = incoming.Metadata["name"].(string)
	}
	if name == "" && len(args) >= 3 {
		name = args[2]
	}
	if verb == "create" && len(args) >= 4 && args[2] == "generic" {
		// Support the old script so the tests reproduce its destructive behavior.
		name = args[3]
		incoming = secret{Metadata: map[string]any{"name": name}, Data: map[string]string{}}
		for _, arg := range args[4:] {
			if key, value, ok := strings.Cut(strings.TrimPrefix(arg, "--from-literal="), "="); ok && strings.HasPrefix(arg, "--from-literal=") {
				incoming.Data[key] = base64.StdEncoding.EncodeToString([]byte(value))
			}
			if strings.HasPrefix(arg, "--from-file=key.json=") {
				b, err := os.ReadFile(strings.TrimPrefix(arg, "--from-file=key.json="))
				if err != nil {
					os.Exit(2)
				}
				incoming.Data["key.json"] = base64.StdEncoding.EncodeToString(b)
			}
		}
	}
	if name == "" || filepath.Base(name) != name {
		os.Exit(2)
	}
	path := filepath.Join(dir, name+".json")
	b, readErr := os.ReadFile(path)
	var current secret
	if readErr == nil && json.Unmarshal(b, &current) != nil {
		os.Exit(2)
	}
	switch verb {
	case "get":
		if readErr == nil {
			_, _ = os.Stdout.Write(b)
		}
	case "delete":
		_ = os.Remove(path)
	case "create":
		if readErr == nil {
			os.Exit(1)
		}
		current = incoming
	case "patch":
		if readErr != nil || os.Getenv("YSCALE_SEED_TEST_FAIL_PATCH") == "1" {
			// A real client's error can contain the submitted payload. The helper
			// must not forward it to the operator's logs.
			fmt.Fprintln(os.Stderr, "fixture-sensitive-server-error")
			os.Exit(1)
		}
		if os.Getenv("YSCALE_SEED_TEST_CONFLICT") == "1" {
			current.Metadata["resourceVersion"] = "8"
			current.Data["CONCURRENT"] = base64.StdEncoding.EncodeToString([]byte("fixture-other-writer"))
			b, _ := json.Marshal(current)
			if os.WriteFile(path, b, 0600) != nil {
				os.Exit(2)
			}
		}
		if version, ok := incoming.Metadata["resourceVersion"]; ok && version != current.Metadata["resourceVersion"] {
			os.Exit(1)
		}
		for key, value := range incoming.Data {
			current.Data[key] = value
		}
	default:
		os.Exit(2)
	}
	if verb == "create" || verb == "patch" {
		b, _ = json.Marshal(current)
		if os.WriteFile(path, b, 0600) != nil {
			os.Exit(2)
		}
	}
	os.Exit(0)
}

type harness struct {
	t      *testing.T
	dir    string
	script string
	env    []string
}

func setup(t *testing.T) *harness {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Fatal("secret bootstrap tests require python3")
	}
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wrapper := fmt.Sprintf("#!/bin/sh\nexec %q -test.run=^TestKubectlHelper$ -- \"$@\"\n", binary)
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("../../scripts/seed-cloud-secrets.sh")
	if err != nil {
		t.Fatal(err)
	}
	// Register the script as a Go test-cache input, not just an exec argument.
	if _, err := os.ReadFile(script); err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, dir: dir, script: script, env: append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"YSCALE_SEED_TEST_HELPER=1", "YSCALE_SEED_TEST_STATE="+dir,
		"NAMESPACE=seed-test", "SECRET_NAME=cloud", "GCP_SECRET_NAME=gcp",
		"ENV_FILE="+filepath.Join(dir, ".env"), "GCP_KEY_FILE="+filepath.Join(dir, "gcp-key.json"))}
}

func (h *harness) file(name, content string) {
	h.t.Helper()
	if err := os.WriteFile(filepath.Join(h.dir, name), []byte(content), 0600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) seed(name string, data map[string]string) {
	h.t.Helper()
	encoded := map[string]string{}
	for key, value := range data {
		encoded[key] = base64.StdEncoding.EncodeToString([]byte(value))
	}
	b, _ := json.Marshal(secret{APIVersion: "v1", Kind: "Secret", Type: "Opaque",
		Metadata: map[string]any{"name": name, "resourceVersion": "7", "labels": map[string]any{"fixture": "preserved"}}, Data: encoded})
	h.file(name+".json", string(b))
}

func (h *harness) change(name string, edit func(*secret)) {
	h.t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, name+".json"))
	if err != nil {
		h.t.Fatal(err)
	}
	var s secret
	if err := json.Unmarshal(b, &s); err != nil {
		h.t.Fatal(err)
	}
	edit(&s)
	b, err = json.Marshal(s)
	if err != nil {
		h.t.Fatal(err)
	}
	h.file(name+".json", string(b))
}

func (h *harness) run(wantOK bool) string {
	h.t.Helper()
	cmd := exec.Command("bash", h.script)
	cmd.Env = h.env
	out, err := cmd.CombinedOutput()
	if (err == nil) != wantOK {
		h.t.Fatalf("helper success=%v, want %v; output: %s", err == nil, wantOK, out)
	}
	return string(out)
}

func (h *harness) data(name string) map[string]string {
	h.t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, name+".json"))
	if err != nil {
		h.t.Fatal(err)
	}
	var s secret
	if err := json.Unmarshal(b, &s); err != nil {
		h.t.Fatal(err)
	}
	result := map[string]string{}
	for key, value := range s.Data {
		b, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			h.t.Fatal(err)
		}
		result[key] = string(b)
	}
	return result
}

func (h *harness) calls() []call {
	h.t.Helper()
	f, err := os.Open(filepath.Join(h.dir, "calls"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var calls []call
	for {
		var c call
		if err := dec.Decode(&c); err != nil {
			if err == io.EOF {
				break
			}
			h.t.Fatal(err)
		}
		calls = append(calls, c)
	}
	return calls
}

func (h *harness) assertNoWrites() {
	h.t.Helper()
	for _, c := range h.calls() {
		if len(c.Args) > 2 && c.Args[2] != "get" {
			h.t.Fatalf("unexpected mutation: %s", c.Args[2])
		}
	}
}

const completeEnv = "FLYIO_TOKEN=fixture-fly\nFLY_ORG=fixture-org\nFLY_REGION=fixture-region\nTS_OAUTH_CLIENT_ID=fixture-id\nTS_OAUTH_CLIENT_SECRET=fixture-client-secret\nTS_TAILNET=fixture-tailnet\nYSCALE_PLACEMENT_TOKEN_KEY=fixture-placement-key-for-local-tests\n"

func TestSeedPreservesUnspecifiedCredentials(t *testing.T) {
	h := setup(t)
	h.seed("cloud", map[string]string{"LINODE_TOKEN": "fixture-existing", "UNRELATED": "keep"})
	h.file(".env", completeEnv)
	h.run(true)
	got := h.data("cloud")
	if got["LINODE_TOKEN"] != "fixture-existing" || got["UNRELATED"] != "keep" || got["FLYIO_TOKEN"] != "fixture-fly" {
		t.Fatal("seeding must update supplied keys without discarding unspecified keys")
	}
	for _, c := range h.calls() {
		for _, arg := range c.Args {
			if strings.Contains(arg, "fixture-") || strings.HasPrefix(arg, "--from-literal=") {
				t.Fatal("credential value appeared in kubectl arguments")
			}
		}
		if len(c.Args) > 2 && c.Args[2] == "delete" {
			t.Fatal("seeding must never delete a Secret")
		}
	}
}

func TestSeedAcceptsPartialEnvAndPreservesSigningKey(t *testing.T) {
	h := setup(t)
	h.seed("cloud", map[string]string{"YSCALE_PLACEMENT_TOKEN_KEY": "fixture-existing-signing-key"})
	h.file(".env", "FLYIO_TOKEN=fixture-updated\n")
	h.run(true)
	if want := (map[string]string{"FLYIO_TOKEN": "fixture-updated", "YSCALE_PLACEMENT_TOKEN_KEY": "fixture-existing-signing-key"}); !reflect.DeepEqual(h.data("cloud"), want) {
		t.Fatal("partial input must preserve omitted signing keys")
	}
}

func TestSeedValidatesGCPBeforeAnyMutation(t *testing.T) {
	h := setup(t)
	before := map[string]string{"FLYIO_TOKEN": "fixture-before"}
	h.seed("cloud", before)
	h.file(".env", completeEnv)
	h.file("gcp-key.json", "not valid json")
	h.run(false)
	if !reflect.DeepEqual(h.data("cloud"), before) {
		t.Fatal("invalid GCP input changed the cloud Secret before failing")
	}
	h.assertNoWrites()
}

func TestSeedNoopAndEmptyValuesPreserveExistingKeys(t *testing.T) {
	h := setup(t)
	before := map[string]string{"FLYIO_TOKEN": "fixture-same", "YSCALE_PLACEMENT_TOKEN_KEY": "fixture-existing-signing-key"}
	h.seed("cloud", before)
	h.file(".env", "FLYIO_TOKEN=fixture-same\nYSCALE_PLACEMENT_TOKEN_KEY=\n")
	h.run(true)
	h.assertNoWrites()
	if !reflect.DeepEqual(h.data("cloud"), before) {
		t.Fatal("empty values must not erase existing keys")
	}
}

func TestSeedRefusesInvalidEnvBeforeAnyMutation(t *testing.T) {
	for name, input := range map[string]string{
		"empty":        "FLYIO_TOKEN=\n",
		"unrecognized": "NOT_A_CLOUD_KEY=fixture-value\n",
		"duplicate":    "FLYIO_TOKEN=fixture-first\nFLYIO_TOKEN=fixture-second\n",
		"quote":        "FLYIO_TOKEN=\"fixture-unclosed\n",
	} {
		t.Run(name, func(t *testing.T) {
			h := setup(t)
			h.seed("cloud", map[string]string{"UNRELATED": "keep"})
			h.file(".env", input)
			out := h.run(false)
			h.assertNoWrites()
			if strings.Contains(out, "fixture-") {
				t.Fatal("invalid-input output contains a credential value")
			}
		})
	}
}

func TestSeedPreservesLiteralQuotedValues(t *testing.T) {
	h := setup(t)
	marker := filepath.Join(h.dir, "must-not-exist")
	value := "$(touch " + marker + ")=literal value"
	h.file(".env", "# fixture\nIGNORED=no\n FLYIO_TOKEN='"+value+"'\r\n")
	h.run(true)
	if h.data("cloud")["FLYIO_TOKEN"] != value {
		t.Fatal("quoted value was not preserved literally")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("dotenv content was executed")
	}
}

func TestSeedPreflightsAllTargetsAndRefusesManagedSecrets(t *testing.T) {
	cases := map[string]func(*secret){
		"controller": func(s *secret) {
			s.Metadata["ownerReferences"] = []any{map[string]any{"controller": true, "kind": "ExternalSecret"}}
		},
		"flux": func(s *secret) {
			s.Metadata["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": "fixture-flux"}
		},
		"helm_label": func(s *secret) {
			s.Metadata["labels"] = map[string]any{"app.kubernetes.io/managed-by": "Helm"}
		},
		"helm_annotation": func(s *secret) {
			s.Metadata["annotations"] = map[string]any{"meta.helm.sh/release-name": "fixture-helm"}
		},
		"immutable": func(s *secret) { s.Immutable = true },
		"wrong_type": func(s *secret) {
			s.Type = "kubernetes.io/tls"
		},
		"no_version": func(s *secret) { delete(s.Metadata, "resourceVersion") },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			h := setup(t)
			h.seed("cloud", map[string]string{"FLYIO_TOKEN": "fixture-before"})
			h.seed("gcp", map[string]string{"key.json": "fixture-before"})
			h.change("gcp", edit)
			h.file(".env", "FLYIO_TOKEN=fixture-after\n")
			h.file("gcp-key.json", "{\"type\":\"service_account\",\"fixture\":true}")
			h.run(false)
			h.assertNoWrites()
		})
	}
}

func TestSeedSuppressesClientFailurePayloads(t *testing.T) {
	for _, verb := range []string{"get", "patch", "create"} {
		t.Run(verb, func(t *testing.T) {
			h := setup(t)
			if verb != "create" {
				h.seed("cloud", map[string]string{"FLYIO_TOKEN": "fixture-before"})
			}
			h.env = append(h.env, "YSCALE_SEED_TEST_FAIL_VERB="+verb)
			h.file(".env", "FLYIO_TOKEN=fixture-after\n")
			out := h.run(false)
			if strings.Contains(out, "fixture-") {
				t.Fatal("client stdout/stderr exposed a sensitive payload")
			}
			for _, c := range h.calls() {
				if c.Args[2] == "delete" {
					t.Fatal("failure recovery must not delete a Secret")
				}
			}
		})
	}
}

func TestSeedRefusesConcurrentChanges(t *testing.T) {
	h := setup(t)
	h.seed("cloud", map[string]string{"FLYIO_TOKEN": "fixture-before"})
	h.file(".env", "FLYIO_TOKEN=fixture-after\n")
	h.env = append(h.env, "YSCALE_SEED_TEST_CONFLICT=1")
	h.run(false)
	if want := (map[string]string{"FLYIO_TOKEN": "fixture-before", "CONCURRENT": "fixture-other-writer"}); !reflect.DeepEqual(h.data("cloud"), want) {
		t.Fatal("concurrent update was overwritten")
	}
}

func TestSeedCreateAndGCPMerge(t *testing.T) {
	h := setup(t)
	h.file(".env", completeEnv)
	h.seed("gcp", map[string]string{"unrelated": "keep"})
	h.file("gcp-key.json", "{\"type\":\"service_account\",\"fixture\":true}\n")
	h.run(true)
	if h.data("cloud")["FLYIO_TOKEN"] != "fixture-fly" || h.data("gcp")["unrelated"] != "keep" || h.data("gcp")["key.json"] == "" {
		t.Fatal("bootstrap must create the cloud Secret and merge only the GCP key")
	}
}
