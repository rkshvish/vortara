package strategy

import (
	"context"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/row"
)

// AppendStrategy appends rows without delivery-log checks.
type AppendStrategy struct{}

// Name returns the append strategy name.
func (s *AppendStrategy) Name() string { return "append" }

// RequiresPrimaryKey reports that append does not require a primary key.
func (s *AppendStrategy) RequiresPrimaryKey() bool { return false }

// Execute writes the batch without consulting delivery state.
func (s *AppendStrategy) Execute(ctx context.Context, rows []row.Row, dest destination.Destination, store state.StateStore, pipeline, destName, matchOn string) (destination.LoadResult, error) {
	ctx = withStrategyName(ctx, s.Name())
	if len(rows) == 0 {
		return destination.LoadResult{}, nil
	}
	return dest.Load(ctx, rows, store, pipeline, destName)
}

func init() {
	Register(&AppendStrategy{})
}
