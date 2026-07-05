// Package state defines the storage contract used by Vortara to persist
// batch watermarks, streaming offsets, run history, and delivery idempotency.
package state

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/rkshvish/vortaraos/pkg/config"
)

// Factory creates a StateStore for a backend config.
type Factory func(cfg config.StateConfig) (StateStore, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a state backend factory.
// It panics if the backend name is already registered.
func Register(backend string, factory Factory) {
	if backend == "" {
		panic("state backend name is required")
	}
	if factory == nil {
		panic("state backend factory is nil")
	}

	mu.Lock()
	defer mu.Unlock()
	if _, exists := factories[backend]; exists {
		panic(fmt.Sprintf("state backend %q already registered", backend))
	}
	factories[backend] = factory
}

// Build creates a StateStore for the configured backend.
// Returns an error listing valid backends if unknown.
func Build(cfg config.StateConfig) (StateStore, error) {
	backend := strings.TrimSpace(cfg.Backend)
	if backend == "" {
		backend = "sqlite"
	}

	mu.RLock()
	factory, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		names := List()
		return nil, fmt.Errorf("unknown state backend %q, valid: %s", backend, strings.Join(names, ", "))
	}
	return factory(cfg)
}

// List returns registered backend names sorted alphabetically.
func List() []string {
	mu.RLock()
	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	mu.RUnlock()
	sort.Strings(names)
	return names
}

func resetRegistryForTest() {
	mu.Lock()
	defer mu.Unlock()
	factories = map[string]Factory{}
}
