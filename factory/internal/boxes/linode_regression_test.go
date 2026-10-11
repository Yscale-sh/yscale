package boxes

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/factory/internal/hsclient"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

type lifecyclePersister struct {
	store.Persister
	boxes   map[string]store.Box
	jobs    map[string]store.Job
	failBox func(store.Box) bool
}

func (p *lifecyclePersister) UpsertBox(box store.Box) error {
	if p.failBox != nil && p.failBox(box) {
		return errors.New("persist failed")
	}
	p.boxes[box.LoginServer] = box
	return nil
}
func (p *lifecyclePersister) UpsertJob(job store.Job) error { p.jobs[job.ID] = job; return nil }
func (p *lifecyclePersister) DeleteBox(login string) error  { delete(p.boxes, login); return nil }

type lifecycleAPI struct {
	*fakeLinodeAPI
	get    func(context.Context, string) (Instance, error)
	delete func(context.Context, string) error
}

func (a *lifecycleAPI) GetInstance(ctx context.Context, id string) (Instance, error) {
	if a.get != nil {
		return a.get(ctx, id)
	}
	return a.fakeLinodeAPI.GetInstance(ctx, id)
}
func (a *lifecycleAPI) DeleteInstance(ctx context.Context, id string) error {
	if a.delete != nil {
		return a.delete(ctx, id)
	}
	return a.fakeLinodeAPI.DeleteInstance(ctx, id)
}

func regressionWorker(t *testing.T, s store.Store, api linodeAPI, opts ...Option) *LinodeWorker {
	t.Helper()
	cloudInit, acl := testWorkerFiles(t)
	base := []Option{WithCloudInitPath(cloudInit), WithACLTemplatePath(acl), WithOpsHandoff("auth", "http://100.64.0.1:8080"), WithOpsLoginServer("https://ops.example.com"), withSleep(noSleep), withHeadscaleRegistration(fakeHeadscaleRegistration)}
	return NewLinodeWorker(s, api, &fakeRegistrar{key: "key"}, append(base, opts...)...)
}

func TestLinodeWorkerFirewallFailurePreventsCreate(t *testing.T) {
	s := testStore(t)
	want := errors.New("firewall unavailable")
	api := &fakeLinodeAPI{firewallErr: want, instances: map[string][]Instance{}}
	job, err := regressionWorker(t, s, api).Provision(context.Background(), "tenant", "create")
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want firewall failure", err)
	}
	if len(api.createCalls) != 0 {
		t.Fatal("created instance without firewall")
	}
	if box, _ := s.GetBox(job.LoginServer); box.Status != store.StatusDegraded {
		t.Fatal("failed provision not degraded")
	}
}

func TestLinodeWorkerRecordsInstanceBeforePolling(t *testing.T) {
	p := &lifecyclePersister{boxes: map[string]store.Box{}, jobs: map[string]store.Job{}}
	s, err := store.NewWithKEKAndPersister(bytes.Repeat([]byte{1}, 32), p)
	if err != nil {
		t.Fatal(err)
	}
	api := &lifecycleAPI{fakeLinodeAPI: &fakeLinodeAPI{instances: map[string][]Instance{}}}
	api.get = func(ctx context.Context, id string) (Instance, error) {
		box, err := s.GetFabric("tenant")
		if err != nil || p.boxes[box.LoginServer].BackendID != id || box.Backend != "linode" {
			t.Fatal("instance ID not durable before polling")
		}
		return api.fakeLinodeAPI.GetInstance(ctx, id)
	}
	if _, err := regressionWorker(t, s, api).Provision(context.Background(), "tenant", "create"); err != nil {
		t.Fatal(err)
	}
}

func TestLinodeWorkerCleansUpProvisionFailure(t *testing.T) {
	for _, failure := range []string{"persist", "poll", "ipv4", "validate", "promote", "ready", "cancel", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			p := &lifecyclePersister{boxes: map[string]store.Box{}, jobs: map[string]store.Job{}}
			s, err := store.NewWithKEKAndPersister(bytes.Repeat([]byte{1}, 32), p)
			if err != nil {
				t.Fatal(err)
			}
			api := &lifecycleAPI{fakeLinodeAPI: &fakeLinodeAPI{instances: map[string][]Instance{}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := []Option{}
			switch failure {
			case "persist":
				p.failBox = func(b store.Box) bool { return b.BackendID != "" && b.Status == store.StatusProvisioning }
			case "promote":
				p.failBox = func(b store.Box) bool { return !strings.HasSuffix(b.LoginServer, ".factory.invalid") }
			case "ready":
				p.failBox = func(b store.Box) bool { return b.Status == store.StatusReady }
			case "ipv4":
				api.instances["instance-1"] = []Instance{{ID: "instance-1", Status: "running"}}
			case "validate":
				opts = append(opts, withHeadscaleRegistration(func(context.Context, hsclient.BoxInfo) (string, string, string, string, error) {
					return "", "", "", "", errors.New("invalid")
				}))
			default:
				api.get = func(context.Context, string) (Instance, error) {
					if failure == "cancel" {
						cancel()
						return Instance{}, ctx.Err()
					}
					return Instance{}, errors.New("poll failed")
				}
			}
			deletes := 0
			api.delete = func(cleanupCtx context.Context, id string) error {
				deletes++
				if cleanupCtx.Err() != nil || id != "instance-1" {
					t.Fatal("invalid cleanup context or instance")
				}
				if _, ok := cleanupCtx.Deadline(); !ok {
					t.Fatal("cleanup has no deadline")
				}
				if failure == "cleanup" {
					return errors.New("delete failed")
				}
				return nil
			}
			job, err := regressionWorker(t, s, api, opts...).Provision(ctx, "tenant", "create")
			if err == nil || deletes != 1 {
				t.Fatalf("error = %v, cleanup calls = %d", err, deletes)
			}
			recorded := p.jobs[job.ID].LastError
			if !strings.Contains(recorded, "instance-1") || !strings.Contains(recorded, "cleanup") {
				t.Fatalf("cleanup not recorded: %q", recorded)
			}
			if failure == "cleanup" && !strings.Contains(recorded, "delete failed") {
				t.Fatal("cleanup failure not recorded")
			}
			if _, err := regressionWorker(t, s, api).Provision(context.Background(), "tenant", "create"); err != nil {
				t.Fatal(err)
			}
			if len(api.createCalls) != 1 {
				t.Fatal("retry duplicated instance")
			}
		})
	}
}

func TestLinodeWorkerInterruptedProvisionDoesNotCreateDuplicate(t *testing.T) {
	s := testStore(t)
	_, box, err := s.EnsureFabric("tenant", "create")
	if err != nil {
		t.Fatal(err)
	}
	box.Backend, box.BackendID = "linode", "existing"
	if err := s.SetBox(box, ""); err != nil {
		t.Fatal(err)
	}
	api := &fakeLinodeAPI{instances: map[string][]Instance{}}
	_, err = regressionWorker(t, s, api).Provision(context.Background(), "tenant", "create")
	if err == nil {
		t.Fatal("interrupted provisioning not reported")
	}
	if len(api.createCalls) != 0 || len(api.deleteCalls) != 0 {
		t.Fatal("retry created a duplicate or disrupted the original instance")
	}
	if got, _ := s.GetBox(box.LoginServer); got.BackendID != "existing" {
		t.Fatal("original instance lost")
	}
}

func TestLinodeWorkerDeprovisionRetriesCloudFailure(t *testing.T) {
	for _, failure := range []string{"inspect", "delete"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			const login = "https://box.example"
			if err := s.SetBox(store.Box{TenantID: "tenant", LoginServer: login, BackendID: "existing", Status: store.StatusReady}, "key"); err != nil {
				t.Fatal(err)
			}
			api := &lifecycleAPI{fakeLinodeAPI: &fakeLinodeAPI{instances: map[string][]Instance{"existing": {{ID: "existing", Tags: []string{headscaleTag}}}}}}
			if failure == "inspect" {
				api.get = func(context.Context, string) (Instance, error) { return Instance{}, errors.New("temporary") }
			}
			if failure == "delete" {
				api.delete = func(context.Context, string) error { return errors.New("temporary") }
			}
			w := NewLinodeWorker(s, api, nil)
			if _, err := w.Deprovision(context.Background(), "tenant", login); err == nil {
				t.Fatal("expected cloud failure")
			}
			if box, _ := s.GetBox(login); box.Status != store.StatusReady {
				t.Fatal("failed delete committed decommissioning")
			}
			api.get, api.delete = nil, nil
			if _, err := w.Deprovision(context.Background(), "tenant", login); err != nil {
				t.Fatal(err)
			}
			if len(api.deleteCalls) != 1 {
				t.Fatal("retry did not delete instance")
			}
			if box, _ := s.GetBox(login); box.Status != store.StatusDead {
				t.Fatal("successful retry did not mark dead")
			}
			if len(s.ListJobs("tenant")) != 1 {
				t.Fatal("retry did not enqueue one teardown job")
			}
		})
	}
}

func TestLinodeWorkerDeprovisionConcurrentCompletion(t *testing.T) {
	s := testStore(t)
	const login = "https://box.example"
	if err := s.SetBox(store.Box{TenantID: "tenant", LoginServer: login, BackendID: "existing", Status: store.StatusReady}, "key"); err != nil {
		t.Fatal(err)
	}
	api := &lifecycleAPI{fakeLinodeAPI: &fakeLinodeAPI{instances: map[string][]Instance{"existing": {{ID: "existing", Tags: []string{headscaleTag}}}}}}
	var deleting sync.WaitGroup
	deleting.Add(2)
	api.delete = func(context.Context, string) error {
		deleting.Done()
		deleting.Wait()
		return nil
	}
	w := NewLinodeWorker(s, api, nil)
	var calls sync.WaitGroup
	calls.Add(2)
	for range 2 {
		go func() {
			defer calls.Done()
			if _, err := w.Deprovision(context.Background(), "tenant", login); err != nil {
				t.Error(err)
			}
		}()
	}
	calls.Wait()
	if len(s.ListJobs("tenant")) != 1 {
		t.Fatal("concurrent completion enqueued duplicate jobs")
	}
}

func TestLinodeWorkerDeprovisionRetriesExistingDecommissioning(t *testing.T) {
	s := testStore(t)
	const login = "https://box.example"
	if err := s.SetBox(store.Box{TenantID: "tenant", LoginServer: login, BackendID: "existing", Status: store.StatusDecommissioning}, "key"); err != nil {
		t.Fatal(err)
	}
	api := &fakeLinodeAPI{instances: map[string][]Instance{"existing": {{ID: "existing", Tags: []string{headscaleTag}}}}}
	if _, err := NewLinodeWorker(s, api, nil).Deprovision(context.Background(), "tenant", login); err != nil {
		t.Fatal(err)
	}
	if len(api.deleteCalls) != 1 {
		t.Fatal("previously decommissioning box was not deleted")
	}
	if box, _ := s.GetBox(login); box.Status != store.StatusDead {
		t.Fatal("deleted box was not marked dead")
	}
}
