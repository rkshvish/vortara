package strategy

import (
	"context"
	"fmt"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/row"
)

// MergeStrategy performs idempotent upserts.
type MergeStrategy struct{}

// Name returns the merge strategy name.
func (s *MergeStrategy) Name() string { return "merge" }

// RequiresPrimaryKey reports that merge needs a primary key.
func (s *MergeStrategy) RequiresPrimaryKey() bool { return true }

// Execute writes rows one-by-one after checking delivery state.
func (s *MergeStrategy) Execute(ctx context.Context, rows []row.Row, dest destination.Destination, store state.StateStore, pipeline, destName, matchOn string) (destination.LoadResult, error) {
	var result destination.LoadResult
	ctx = withStrategyName(ctx, s.Name())

	for _, rw := range rows {
		delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destName)
		if err != nil {
			result.Errors = append(result.Errors, destination.RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}

		loadRes, err := dest.Load(ctx, []row.Row{rw}, store, pipeline, destName)
		result.Loaded += loadRes.Loaded
		result.Skipped += loadRes.Skipped
		result.Errors = append(result.Errors, loadRes.Errors...)
		if err != nil {
			result.Errors = append(result.Errors, destination.RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if loadRes.Loaded > 0 {
			if err := store.MarkDelivered(ctx, rw.ID, pipeline, destName); err != nil {
				result.Errors = append(result.Errors, destination.RowError{RowID: rw.ID, Row: rw, Err: err})
				continue
			}
		}
	}

	return result, nil
}

func init() {
	Register(&MergeStrategy{})
}

func ensureMatchOn(matchOn string) error {
	if matchOn == "" {
		return fmt.Errorf("strategy requires match_on")
	}
	return nil
}
