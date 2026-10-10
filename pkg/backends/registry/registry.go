// Package registry implements the factory pattern for Backend
// construction. Each backend package registers its constructor in
// init() against a type name; the entry point (cmd/yscale) resolves
// backend(s) by config-driven name.
//
// Usage at the backend package:
//
//	func init() {
//	    registry.Register(backends.TypeFlyIO, func(cfg *config.Config) (backends.Backend, error) {
//	        return New(cfg.Backend.FlyIO.APIToken, cfg.Backend.FlyIO.Org, cfg.Backend.FlyIO.Region), nil
//	    })
//	}
//
// Usage at the call site:
//
//	import _ "github.com/yscale-sh/yscale/pkg/backends/flyio"  // side-effect import for init()
//	b, err := registry.New(cfg.Backend.Type, cfg)
package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/config"
)

// Factory builds a Backend from a Config. The factory inspects whatever
// section of cfg corresponds to its backend type.
type Factory func(*config.Config) (backends.Backend, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a backend factory under the given type name. Panics on
// duplicate registration so init-time conflicts surface immediately
// rather than mysteriously taking the wrong factory.
func Register(name string, f Factory) {
	if name == "" {
		panic("registry.Register: empty name")
	}
	if f == nil {
		panic("registry.Register: nil factory for " + name)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := factories[name]; exists {
		panic("registry.Register: duplicate name " + name)
	}
	factories[name] = f
}

// New constructs a Backend by type name.
func New(name string, cfg *config.Config) (backends.Backend, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown backend type %q (registered: %v)", name, All())
	}
	return f(cfg)
}

// All returns the names of registered backends, sorted for deterministic
// output (used in error messages and tests).
func All() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for name := range factories {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// reset clears the registry. Test-only.
func reset() {
	mu.Lock()
	defer mu.Unlock()
	factories = map[string]Factory{}
}
