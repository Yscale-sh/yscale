package registry

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
)

// stubBackend implements backends.Backend for registry tests. Methods
// return zero values; we only need it to satisfy the interface.
type stubBackend struct{ name string }

func (s *stubBackend) Name() string { return s.name }
func (s *stubBackend) CreateNode(_ context.Context, _ *backends.NodeSpec) (string, error) {
	return "", nil
}
func (s *stubBackend) StartNode(_ context.Context, _ string) error  { return nil }
func (s *stubBackend) StopNode(_ context.Context, _ string) error   { return nil }
func (s *stubBackend) DeleteNode(_ context.Context, _ string) error { return nil }
func (s *stubBackend) GetNodeStatus(_ context.Context, _ string) (*backends.NodeStatus, error) {
	return nil, nil
}
func (s *stubBackend) ListPooledNodes(_ context.Context) ([]backends.PooledNode, error) {
	return nil, nil
}
func (s *stubBackend) CleanupOrphans(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}

func TestRegisterAndNew(t *testing.T) {
	reset()
	defer reset()

	Register("alpha", func(*config.Config) (backends.Backend, error) {
		return &stubBackend{name: "alpha"}, nil
	})

	b, err := New("alpha", &config.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Name() != "alpha" {
		t.Errorf("Name() = %q, want alpha", b.Name())
	}
}

func TestNewUnknownBackend(t *testing.T) {
	reset()
	defer reset()

	Register("known", func(*config.Config) (backends.Backend, error) {
		return &stubBackend{name: "known"}, nil
	})

	_, err := New("nope", &config.Config{})
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
	msg := err.Error()
	if !strings.Contains(msg, "unknown backend") {
		t.Errorf("error should mention 'unknown backend': %q", msg)
	}
	if !strings.Contains(msg, "known") {
		t.Errorf("error should list registered backends: %q", msg)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	reset()
	defer reset()

	Register("dupe", func(*config.Config) (backends.Backend, error) {
		return &stubBackend{name: "x"}, nil
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register("dupe", func(*config.Config) (backends.Backend, error) {
		return &stubBackend{name: "y"}, nil
	})
}

func TestRegisterEmptyNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty name")
		}
	}()
	Register("", func(*config.Config) (backends.Backend, error) { return nil, nil })
}

func TestRegisterNilFactoryPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil factory")
		}
	}()
	Register("nil-fac", nil)
}

func TestAllReturnsSorted(t *testing.T) {
	reset()
	defer reset()

	for _, name := range []string{"zoo", "apple", "mango"} {
		n := name // capture
		Register(n, func(*config.Config) (backends.Backend, error) {
			return &stubBackend{name: n}, nil
		})
	}
	got := All()
	want := []string{"apple", "mango", "zoo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("All() = %v, want %v", got, want)
	}
}

func TestRegistryConcurrentSafe(t *testing.T) {
	reset()
	defer reset()

	Register("base", func(*config.Config) (backends.Backend, error) {
		return &stubBackend{name: "base"}, nil
	})

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = New("base", &config.Config{})
			_ = All()
		}()
	}
	wg.Wait()
}
