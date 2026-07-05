package strategy

import (
	"context"
	"sync"

	"github.com/rkshvish/vortaraos/internal/connector/destination"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// ReplaceStrategy reloads a destination from scratch.
type ReplaceStrategy struct {
	mu           sync.Mutex
	truncatedRun map[int64]bool
}

// Name returns the replace strategy name.
func (s *ReplaceStrategy) Name() string { return "replace" }

// RequiresPrimaryKey reports that replace does not require a primary key.
func (s *ReplaceStrategy) RequiresPrimaryKey() bool { return false }

// Execute writes the batch and marks the first call in a run as replace.
func (s *ReplaceStrategy) Execute(ctx context.Context, rows []row.Row, dest destination.Destination, store state.StateStore, pipeline, destName, matchOn string) (destination.LoadResult, error) {
	if len(rows) == 0 {
		return destination.LoadResult{}, nil
	}

	runID := RunID(ctx)
	ctx = withStrategyName(ctx, s.Name())

	s.mu.Lock()
	if s.truncatedRun == nil {
		s.truncatedRun = make(map[int64]bool)
	}
	firstCall := !s.truncatedRun[runID]
	if firstCall {
		s.truncatedRun[runID] = true
	}
	s.mu.Unlock()

	if !firstCall {
		ctx = context.WithValue(ctx, strategyNameKey, "")
	}

	return dest.Load(ctx, rows, store, pipeline, destName)
}

func init() {
	Register(&ReplaceStrategy{})
}
