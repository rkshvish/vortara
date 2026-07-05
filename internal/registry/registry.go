// Package registry stores connector factories keyed by type name.
package registry

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Factory creates a new connector instance.
type Factory func() any

var (
	mu               sync.RWMutex
	batchSources     = map[string]Factory{}
	streamingSources = map[string]Factory{}
	destinations     = map[string]Factory{}
)

// RegisterBatchSource registers a batch source factory by type name.
// It panics if the type is already registered.
func RegisterBatchSource(typeName string, factory Factory) {
	register("batch source", batchSources, typeName, factory)
}

// RegisterStreamingSource registers a streaming source factory by type name.
// It panics if the type is already registered.
func RegisterStreamingSource(typeName string, factory Factory) {
	register("streaming source", streamingSources, typeName, factory)
}

// RegisterDestination registers a destination factory by type name.
// It panics if the type is already registered.
func RegisterDestination(typeName string, factory Factory) {
	register("destination", destinations, typeName, factory)
}

// GetBatchSource returns a new batch source instance for the given type name.
func GetBatchSource(typeName string) (any, error) {
	mu.RLock()
	factory, ok := batchSources[typeName]
	mu.RUnlock()
	if !ok {
		names := ListBatchSources()
		return nil, fmt.Errorf("unknown batch source type %q, registered types: %s", typeName, strings.Join(names, ", "))
	}
	return factory(), nil
}

// GetStreamingSource returns a new streaming source instance for the given type name.
func GetStreamingSource(typeName string) (any, error) {
	mu.RLock()
	factory, ok := streamingSources[typeName]
	mu.RUnlock()
	if !ok {
		names := ListStreamingSources()
		return nil, fmt.Errorf("unknown streaming source type %q, registered types: %s", typeName, strings.Join(names, ", "))
	}
	return factory(), nil
}

// GetDestination returns a new destination instance for the given type name.
func GetDestination(typeName string) (any, error) {
	mu.RLock()
	factory, ok := destinations[typeName]
	mu.RUnlock()
	if !ok {
		names := ListDestinations()
		return nil, fmt.Errorf("unknown destination type %q, registered types: %s", typeName, strings.Join(names, ", "))
	}
	return factory(), nil
}

// ListBatchSources returns all registered batch source type names.
func ListBatchSources() []string {
	return list(batchSources)
}

// ListStreamingSources returns all registered streaming source type names.
func ListStreamingSources() []string {
	return list(streamingSources)
}

// ListDestinations returns all registered destination type names.
func ListDestinations() []string {
	return list(destinations)
}

func register(kind string, target map[string]Factory, typeName string, factory Factory) {
	if typeName == "" {
		panic(fmt.Sprintf("%s type name is required", kind))
	}
	if factory == nil {
		panic(fmt.Sprintf("%s factory is nil", kind))
	}

	mu.Lock()
	defer mu.Unlock()
	if _, exists := target[typeName]; exists {
		panic(fmt.Sprintf("%s %q already registered", kind, typeName))
	}
	target[typeName] = factory
}

func get(kind string, target map[string]Factory, typeName string) (any, error) {
	mu.RLock()
	factory, ok := target[typeName]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown %s type %q", kind, typeName)
	}
	return factory(), nil
}

func list(target map[string]Factory) []string {
	mu.RLock()
	names := make([]string, 0, len(target))
	for name := range target {
		names = append(names, name)
	}
	mu.RUnlock()
	sort.Strings(names)
	return names
}

func resetForTest() {
	mu.Lock()
	defer mu.Unlock()
	batchSources = map[string]Factory{}
	streamingSources = map[string]Factory{}
	destinations = map[string]Factory{}
}
