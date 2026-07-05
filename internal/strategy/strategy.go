// Package strategy selects how rows are delivered to destinations.
package strategy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/row"
)

// LoadStrategy defines how rows are written to a destination.
type LoadStrategy interface {
	Name() string
	RequiresPrimaryKey() bool
	Execute(ctx context.Context, rows []row.Row, dest destination.Destination, store state.StateStore, pipeline, destName, matchOn string) (destination.LoadResult, error)
}

const (
	strategyNameKey = "vortara_strategy"
	deleteKeysKey   = "vortara_delete_keys"
	runIDKey        = "vortara_run_id"
)

var (
	mu        sync.RWMutex
	registry  = map[string]LoadStrategy{}
	listOrder = []string{}
)

// Register adds a strategy to the registry and panics on duplicate names.
func Register(s LoadStrategy) {
	if s == nil {
		panic("strategy: nil strategy")
	}
	name := s.Name()
	if name == "" {
		panic("strategy: empty strategy name")
	}

	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("strategy %q already registered", name))
	}
	registry[name] = s
	listOrder = append(listOrder, name)
	sort.Strings(listOrder)
}

// Get returns the strategy for the given name.
func Get(name string) (LoadStrategy, error) {
	mu.RLock()
	s, ok := registry[name]
	names := append([]string(nil), listOrder...)
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown strategy %q; valid: %v", name, names)
	}
	return s, nil
}

// List returns all registered strategy names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := append([]string(nil), listOrder...)
	return out
}

// RewriteStrategy applies destination-aware strategy rewrites.
func RewriteStrategy(strategy, destType, matchOn string) string {
	strategy = strings.TrimSpace(strategy)
	if strategy == "" {
		strategy = "merge"
	}
	destType = strings.TrimSpace(destType)
	matchOn = strings.TrimSpace(matchOn)

	appendOnlyDests := map[string]bool{
		"slack":        true,
		"googlesheets": true,
	}
	if appendOnlyDests[destType] {
		return "append"
	}
	if matchOn == "" && (strategy == "merge" || strategy == "delete+insert") {
		return "append"
	}
	return strategy
}

// StrategyName returns the strategy hint stored in ctx.
func StrategyName(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(strategyNameKey).(string); ok {
		return v
	}
	return ""
}

// WithStrategyName stores a strategy hint in ctx for destinations and loaders.
func WithStrategyName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, strategyNameKey, name)
}

// DeleteKeys returns the delete keys hint stored in ctx.
func DeleteKeys(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(deleteKeysKey).([]string); ok {
		return append([]string(nil), v...)
	}
	return nil
}

// WithRunID stores a run identifier in ctx for strategies that need per-run state.
func WithRunID(ctx context.Context, runID int64) context.Context {
	return context.WithValue(ctx, runIDKey, runID)
}

// RunID returns the run identifier stored in ctx, or zero if unset.
func RunID(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx.Value(runIDKey).(int64); ok {
		return v
	}
	return 0
}

func withStrategyName(ctx context.Context, name string) context.Context {
	return WithStrategyName(ctx, name)
}

func withDeleteKeys(ctx context.Context, keys []string) context.Context {
	return context.WithValue(ctx, deleteKeysKey, append([]string(nil), keys...))
}

func resetForTest() {
	mu.Lock()
	defer mu.Unlock()
	registry = map[string]LoadStrategy{}
	listOrder = []string{}
}
