package strategy

import (
	"context"
	"fmt"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/row"
)

// DeleteInsertStrategy deletes matching rows and inserts the batch.
type DeleteInsertStrategy struct{}

// Name returns the delete+insert strategy name.
func (s *DeleteInsertStrategy) Name() string { return "delete+insert" }

// RequiresPrimaryKey reports that delete+insert requires a primary key.
func (s *DeleteInsertStrategy) RequiresPrimaryKey() bool { return true }

// Execute deletes the matching keys and then inserts the batch.
func (s *DeleteInsertStrategy) Execute(ctx context.Context, rows []row.Row, dest destination.Destination, store state.StateStore, pipeline, destName, matchOn string) (destination.LoadResult, error) {
	var result destination.LoadResult
	if len(rows) == 0 {
		return result, nil
	}

	keys := make([]string, 0, len(rows))
	for _, rw := range rows {
		key := rw.PrimaryKey
		if key == "" {
			key = fmt.Sprintf("%v", rw.Data[matchOn])
		}
		keys = append(keys, key)
	}

	ctx = withStrategyName(ctx, s.Name())
	ctx = withDeleteKeys(ctx, keys)
	return dest.Load(ctx, rows, store, pipeline, destName)
}

func init() {
	Register(&DeleteInsertStrategy{})
}
