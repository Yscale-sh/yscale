package boxes

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/factory/internal/hsclient"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

type fakeLinodeAPI struct {
	mu sync.Mutex

	firewallID      string
	firewallErr     error
	firewallCreates int
	ensureCalls     int
	createCalls     []InstanceSpec
	getCalls        []string
	deleteCalls     []string
	instances       map[string][]Instance
}

func (f *fakeLinodeAPI) EnsureFirewall(_ context.Context, _ string, _ []string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	if f.firewallErr != nil {
		return "", f.firewallErr
	}
	if f.firewallID == "" {
		f.firewallID = "firewall-1"
		f.firewallCreates++
	}
	return f.firewallID, nil
}

func (f *fakeLinodeAPI) CreateInstance(_ context.Context, spec InstanceSpec) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, spec)
	id := fmt.Sprintf("instance-%d", len(f.createCalls))
	if _, ok := f.instances[id]; !ok {
		f.instances[id] = []Instance{{ID: id, Status: "running", IPv4: []string{"203.0.113.7"}, Tags: spec.Tags}}
	}
	return Instance{ID: id}, nil
}

func (f *fakeLinodeAPI) GetInstance(_ context.Context, id string) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls = append(f.getCalls, id)
	sequence := f.instances[id]
	if len(sequence) == 0 {
		return Instance{}, fmt.Errorf("missing instance %s", id)
	}
	inst := sequence[0]
	if len(sequence) > 1 {
		f.instances[id] = sequence[1:]
	}
	return inst, nil
}

func (f *fakeLinodeAPI) ListInstancesByTag(context.Context, string) ([]Instance, error) {
	return nil, nil
}

func (f *fakeLinodeAPI) DeleteInstance(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, id)
	return nil
}

type fakeRegistrar struct {
	mu         sync.Mutex
	key        string
	newCalls   []string
	awaitCalls []string
	nextToken  string
}

func (f *fakeRegistrar) NewRegistration(_ context.Context, tenantID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.newCalls = append(f.newCalls, tenantID)
	if f.nextToken == "" {
		f.nextToken = "fake-handoff-token"
	}
	return f.nextToken, nil
}

func (f *fakeRegistrar) AwaitKey(_ context.Context, token, loginServer, user string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.awaitCalls = append(f.awaitCalls, token+"/"+loginServer+"/"+user)
	return f.key, nil
}

func testStore(t *testing.T) *store.MemoryStore {
	t.Helper()
	s, err := store.NewWithKEK(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testWorkerFiles(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	cloudInitPath := dir + "/cloud-init.sh"
	aclPath := dir + "/acls.hujson"
	if err := os.WriteFile(cloudInitPath, []byte("#!/bin/bash\necho bootstrap\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acl := "// discard this line\n\n{\n  \"user\": \"<CUSTOMER>\",\n  \"pod\": \"<POD_CIDR>\",\n  \"svc\": \"<SVC_CIDR>\",\n  \"node\": \"<NODE_CIDR>\"\n}\n"
	if err := os.WriteFile(aclPath, []byte(acl), 0o600); err != nil {
		t.Fatal(err)
	}
	return cloudInitPath, aclPath
}

func fakeHeadscaleRegistration(_ context.Context, box hsclient.BoxInfo) (string, string, string, string, error) {
	return "https://" + box.Hostname, box.APIKey, box.User, box.BackendID, nil
}

func noSleep(context.Context) error { return nil }

func TestLinodeWorkerProvision(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{
		"instance-1": {
			{ID: "instance-1", Status: "booting"},
			{ID: "instance-1", Status: "running", IPv4: []string{"203.0.113.7"}, Tags: []string{headscaleTag}},
		},
	}}
	reg := &fakeRegistrar{key: "box-api-key"}
	cloudInitPath, aclPath := testWorkerFiles(t)
	w := NewLinodeWorker(s, api, reg, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath), WithOpsHandoff("ops-auth-key", "http://100.64.0.1:8080"), WithOpsLoginServer("https://ops.example.com"), withSleep(noSleep), withHeadscaleRegistration(fakeHeadscaleRegistration))

	job, err := w.Provision(context.Background(), "tenant-a", "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(api.createCalls) != 1 {
		t.Fatalf("CreateInstance calls = %d, want 1", len(api.createCalls))
	}
	spec := api.createCalls[0]
	if len(spec.Tags) != 1 || spec.Tags[0] != headscaleTag || spec.FirewallID != "firewall-1" {
		t.Fatalf("CreateInstance spec = %#v, want tagged instance using firewall", spec)
	}
	if len(spec.RootPass) != 32 {
		t.Fatalf("root password length = %d, want 32", len(spec.RootPass))
	}
	userData, err := base64.StdEncoding.DecodeString(spec.UserDataB64)
	if err != nil || !bytes.Contains(userData, []byte("export HS_ACL_B64=")) {
		t.Fatalf("user_data decode = %q, %v", userData, err)
	}
	for _, want := range []string{"export HS_OPS_TOKEN='fake-handoff-token'", "export HS_OPS_AUTHKEY='ops-auth-key'", "export HS_OPS_FACTORY_URL='http://100.64.0.1:8080'", "export HS_OPS_LOGIN_SERVER='https://ops.example.com'"} {
		if !bytes.Contains(userData, []byte(want)) {
			t.Fatalf("user_data missing %q: %q", want, userData)
		}
	}
	if len(reg.newCalls) != 1 || len(reg.awaitCalls) != 1 {
		t.Fatalf("registrar calls = new %#v, await %#v", reg.newCalls, reg.awaitCalls)
	}
	if len(api.getCalls) != 2 {
		t.Fatalf("GetInstance calls = %d, want poll to running", len(api.getCalls))
	}

	loginServer := "https://203-0-113-7.ip.linodeusercontent.com"
	box, err := s.GetBox(loginServer)
	if err != nil {
		t.Fatal(err)
	}
	if box.Status != store.StatusReady || box.Backend != "linode" || box.BackendID != "instance-1" || box.HSUser != "tenant-a" {
		t.Fatalf("stored box = %#v", box)
	}
	if strings.Contains(fmt.Sprintf("%#v", box), reg.key) {
		t.Fatal("GetBox exposed plaintext API key")
	}
	key, err := s.DecryptAPIKey(loginServer)
	if err != nil || key != reg.key {
		t.Fatalf("DecryptAPIKey() = %q, %v", key, err)
	}
	if _, err := w.Provision(context.Background(), "tenant-a", "create-1"); err != nil {
		t.Fatal(err)
	}
	if len(api.createCalls) != 1 {
		t.Fatal("idempotent provision created a second instance")
	}
	if job.LoginServer != loginServer {
		t.Fatalf("job login server = %q, want %q", job.LoginServer, loginServer)
	}
}

func TestRenderACL(t *testing.T) {
	t.Run("fails closed", func(t *testing.T) {
		if _, err := renderACL([]byte(`{"user":"<CUSTOMER>","unknown":"<FOO>"}`), "tenant", "pod", "svc", "node"); err == nil {
			t.Fatal("renderACL accepted unrendered placeholder")
		}
	})
	t.Run("minifies", func(t *testing.T) {
		encoded, err := renderACL([]byte("// comment\n\n{\n  \"user\": \"<CUSTOMER>\"\n}\n"), "tenant", "pod", "svc", "node")
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(decoded, []byte("comment")) || bytes.Contains(decoded, []byte("<")) || bytes.Contains(decoded, []byte("\n\n")) {
			t.Fatalf("minified ACL = %q", decoded)
		}
	})
}

func TestCloudInitUserDataWithoutOpsHandoff(t *testing.T) {
	userData, err := cloudInitUserData("tenant-a", "encoded-acl", "", "", "", "", []byte("#!/bin/bash\necho bootstrap\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"export HS_OPS_TOKEN=''", "export HS_OPS_AUTHKEY=''", "export HS_OPS_FACTORY_URL=''", "export HS_OPS_LOGIN_SERVER=''"} {
		if !bytes.Contains(userData, []byte(want)) {
			t.Fatalf("user_data missing %q: %q", want, userData)
		}
	}
}

func TestLinodeWorkerRefusesInvalidACLBeforeCreate(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	reg := &fakeRegistrar{key: "box-api-key"}
	cloudInitPath, aclPath := testWorkerFiles(t)
	if err := os.WriteFile(aclPath, []byte(`{"user":"<CUSTOMER>","unknown":"<FOO>"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	w := NewLinodeWorker(s, api, reg, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath), WithOpsHandoff("ops-auth-key", "http://100.64.0.1:8080"), WithOpsLoginServer("https://ops.example.com"), withHeadscaleRegistration(fakeHeadscaleRegistration))
	if _, err := w.Provision(context.Background(), "tenant-a", "create-1"); err == nil {
		t.Fatal("Provision accepted an unrendered ACL placeholder")
	}
	if len(api.createCalls) != 0 {
		t.Fatal("invalid ACL created an instance")
	}
}

func TestLinodeWorkerDoesNotPersistBeforeHeadscaleValidation(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	reg := &fakeRegistrar{key: "box-api-key"}
	cloudInitPath, aclPath := testWorkerFiles(t)
	w := NewLinodeWorker(s, api, reg, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath), WithOpsHandoff("ops-auth-key", "http://100.64.0.1:8080"), WithOpsLoginServer("https://ops.example.com"), withSleep(noSleep), withHeadscaleRegistration(func(context.Context, hsclient.BoxInfo) (string, string, string, string, error) {
		return "", "", "", "", errors.New("not reachable")
	}))
	job, err := w.Provision(context.Background(), "tenant-a", "create-1")
	if err == nil {
		t.Fatal("Provision accepted a failed Headscale validation")
	}
	box, err := s.GetBox(job.LoginServer)
	if err != nil || box.Status != store.StatusDegraded {
		t.Fatalf("staged box = %#v, %v", box, err)
	}
	if _, err := s.GetBox("https://203-0-113-7.ip.linodeusercontent.com"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("validated endpoint was persisted after failed validation: %v", err)
	}
}

func TestLinodeWorkerReusesFirewall(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	reg := &fakeRegistrar{key: "box-api-key"}
	cloudInitPath, aclPath := testWorkerFiles(t)
	w := NewLinodeWorker(s, api, reg, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath), WithOpsHandoff("ops-auth-key", "http://100.64.0.1:8080"), WithOpsLoginServer("https://ops.example.com"), withSleep(noSleep), withHeadscaleRegistration(fakeHeadscaleRegistration))
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if _, err := w.Provision(context.Background(), tenant, "create-"+tenant); err != nil {
			t.Fatal(err)
		}
	}
	if api.firewallCreates != 1 || api.ensureCalls != 2 {
		t.Fatalf("firewall creates/calls = %d/%d, want 1/2", api.firewallCreates, api.ensureCalls)
	}
}

func TestLinodeWorkerDeprovisionTagSafetyAndIdempotency(t *testing.T) {
	t.Run("refuses non-owner tag", func(t *testing.T) {
		s := testStore(t)
		loginServer := "https://box.example"
		if err := s.SetBox(store.Box{TenantID: "tenant-a", LoginServer: loginServer, Backend: "linode", BackendID: "box-1", Status: store.StatusReady}, "key"); err != nil {
			t.Fatal(err)
		}
		api := &fakeLinodeAPI{
			instances: map[string][]Instance{
				"box-1": {{ID: "box-1", Tags: []string{"yscale-burst"}}},
			},
		}
		w := NewLinodeWorker(s, api, &fakeRegistrar{})
		if _, err := w.Deprovision(context.Background(), "tenant-a", loginServer); err == nil {
			t.Fatal("Deprovision deleted a non-owner tagged instance")
		}
		if len(api.deleteCalls) != 0 || len(api.getCalls) != 1 || api.getCalls[0] != "box-1" {
			t.Fatalf("delete/get calls = %#v/%#v", api.deleteCalls, api.getCalls)
		}
	})
	t.Run("is idempotent after delete", func(t *testing.T) {
		s := testStore(t)
		loginServer := "https://box.example"
		if err := s.SetBox(store.Box{TenantID: "tenant-a", LoginServer: loginServer, Backend: "linode", BackendID: "box-1", Status: store.StatusReady}, "key"); err != nil {
			t.Fatal(err)
		}
		api := &fakeLinodeAPI{
			instances: map[string][]Instance{
				"box-1": {{ID: "box-1", Tags: []string{headscaleTag}}},
			},
		}
		w := NewLinodeWorker(s, api, &fakeRegistrar{})
		job, err := w.Deprovision(context.Background(), "tenant-a", loginServer)
		if err != nil || job.Kind != "deprovision" {
			t.Fatalf("Deprovision() = %#v, %v", job, err)
		}
		job, err = w.Deprovision(context.Background(), "tenant-a", loginServer)
		if err != nil || job != (store.Job{}) || len(api.deleteCalls) != 1 {
			t.Fatalf("second Deprovision() = %#v, %v; deletes=%#v", job, err, api.deleteCalls)
		}
	})
}

func TestLinodeWorkerDeprovisionUsesRequestedBoxBackend(t *testing.T) {
	s := testStore(t)
	oldLogin := "https://old.example"
	if err := s.SetBox(store.Box{TenantID: "tenant-a", LoginServer: oldLogin, Backend: "linode", BackendID: "old-box", Status: store.StatusReady}, "key"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBox(store.Box{TenantID: "tenant-a", LoginServer: "https://replacement.example", Backend: "linode", BackendID: "replacement-box", Status: store.StatusReady}, "key"); err != nil {
		t.Fatal(err)
	}
	api := &fakeLinodeAPI{instances: map[string][]Instance{
		"old-box":         {{ID: "old-box", Tags: []string{headscaleTag}}},
		"replacement-box": {{ID: "replacement-box", Tags: []string{headscaleTag}}},
	}}
	w := NewLinodeWorker(s, api, &fakeRegistrar{})
	if _, err := w.Deprovision(context.Background(), "tenant-a", oldLogin); err != nil {
		t.Fatal(err)
	}
	if len(api.getCalls) != 1 || api.getCalls[0] != "old-box" || len(api.deleteCalls) != 1 || api.deleteCalls[0] != "old-box" {
		t.Fatalf("Deprovision resolved get/delete = %#v/%#v, want old-box", api.getCalls, api.deleteCalls)
	}
}

func TestLinodeWorkerUnavailableRegistrarFailsBeforeCreate(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	w := NewLinodeWorker(s, api, nil)
	if _, err := w.Provision(context.Background(), "tenant-a", "create-1"); err == nil || !strings.Contains(err.Error(), "ops key handoff not wired") {
		t.Fatalf("Provision() error = %v, want unavailable registrar error", err)
	}
	if len(api.createCalls) != 0 {
		t.Fatal("unavailable registrar still created an instance")
	}
}

// A real registrar without WithOpsHandoff is a misconfiguration: the box would
// have no channel to return its key, so provisioning must refuse BEFORE creating
// a billable instance rather than create one that can only time out and degrade.
func TestLinodeWorkerRealRegistrarRequiresOpsHandoff(t *testing.T) {
	s := testStore(t)
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	cloudInitPath, aclPath := testWorkerFiles(t)
	w := NewLinodeWorker(s, api, &fakeRegistrar{key: "box-api-key"}, WithCloudInitPath(cloudInitPath), WithACLTemplatePath(aclPath))
	if _, err := w.Provision(context.Background(), "tenant-a", "create-1"); err == nil || !strings.Contains(err.Error(), "ops handoff") {
		t.Fatalf("Provision() error = %v, want an ops-handoff-required error", err)
	}
	if len(api.createCalls) != 0 {
		t.Fatal("misconfigured registrar still created an instance")
	}
	if len(api.createCalls) != 0 {
		t.Fatal("unavailable registrar created an instance")
	}
}
