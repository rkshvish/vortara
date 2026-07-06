package state

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// stateConfig holds the minimal configuration needed to open a state backend.
type stateConfig struct {
	Backend    string
	Path       string
	Connection string
	KeyPrefix  string
}

// Factory creates a StateStore for a backend config.
type Factory func(cfg stateConfig) (StateStore, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a state backend factory. Panics if already registered.
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

// Build creates a StateStore from a backend name, path, and connection string.
func Build(backend, path, connection string) (StateStore, error) {
	backend = strings.TrimSpace(backend)
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
	return factory(stateConfig{Backend: backend, Path: path, Connection: connection})
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
