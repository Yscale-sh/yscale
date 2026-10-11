package store

import (
	"errors"
	"testing"
)

type failingPersister struct {
	fakePersister
	fail string
	err  error
}

func (p *failingPersister) UpsertBox(box Box) error {
	if p.fail == "box" {
		return p.err
	}
	return p.fakePersister.UpsertBox(box)
}
func (p *failingPersister) UpsertJob(job Job) error {
	if p.fail == "job" {
		return p.err
	}
	return p.fakePersister.UpsertJob(job)
}
func (p *failingPersister) DeleteBox(login string) error {
	if p.fail == "delete" {
		return p.err
	}
	return p.fakePersister.DeleteBox(login)
}

func TestPromoteFabricPreservesStagingOnPersistenceFailure(t *testing.T) {
	for _, failure := range []string{"box", "job", "delete"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t, 1)
			job, staged, err := s.EnsureFabric("tenant", "create")
			if err != nil {
				t.Fatal(err)
			}
			p := &failingPersister{fail: failure, err: errors.New("write failed")}
			s.persister = p
			promoted := staged
			promoted.LoginServer = "https://real.example"
			if err := s.PromoteFabric(staged.LoginServer, promoted, "real-key"); !errors.Is(err, p.err) {
				t.Fatalf("promotion error = %v, want write failure", err)
			}
			if _, err := s.GetBox(staged.LoginServer); err != nil {
				t.Fatalf("staging lost: %v", err)
			}
			if got, _ := s.GetJob(job.ID); got.LoginServer != staged.LoginServer {
				t.Fatal("job moved on failed promotion")
			}
			if _, err := s.GetBox(promoted.LoginServer); !errors.Is(err, ErrNotFound) {
				t.Fatal("failed promotion published in memory")
			}
			if len(p.deleted) != 0 {
				t.Fatal("failed promotion deleted durable staging")
			}
			p.fail = ""
			if err := s.PromoteFabric(staged.LoginServer, promoted, "real-key"); err != nil {
				t.Fatal(err)
			}
			if key, err := s.DecryptAPIKey(promoted.LoginServer); err != nil || key != "real-key" {
				t.Fatal("retry lost credentials")
			}
		})
	}
}

func TestLifecycleWritesReturnPersistenceErrors(t *testing.T) {
	s := testStore(t, 1)
	job, box, err := s.EnsureFabric("tenant", "create")
	if err != nil {
		t.Fatal(err)
	}
	p := &failingPersister{fail: "box", err: errors.New("write failed")}
	s.persister = p
	box.BackendID = "instance-1"
	if err := s.SetBox(box, ""); !errors.Is(err, p.err) {
		t.Fatalf("SetBox error = %v", err)
	}
	if got, _ := s.GetBox(box.LoginServer); got.BackendID != "" {
		t.Fatal("failed write changed box")
	}
	if err := s.SetBoxStatus(box.LoginServer, StatusDead); !errors.Is(err, p.err) {
		t.Fatalf("SetBoxStatus error = %v", err)
	}
	if got, _ := s.GetBox(box.LoginServer); got.Status != StatusProvisioning {
		t.Fatal("failed write changed status")
	}
	p.fail = "job"
	job.LastError = "cleanup attempted"
	if err := s.SetJob(job); !errors.Is(err, p.err) {
		t.Fatalf("SetJob error = %v", err)
	}
	if got, _ := s.GetJob(job.ID); got.LastError != "" {
		t.Fatal("failed write changed job")
	}
}
