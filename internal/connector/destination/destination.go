// Package destination contains destination connector interfaces and implementations.
package destination

import (
	"context"

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// RowError captures a single row that failed to load.
type RowError struct {
	RowID string
	Row   row.Row
	Err   error
}

// LoadResult is returned by every destination after a Load call.
type LoadResult struct {
	Loaded         int               // rows successfully written
	Skipped        int               // rows skipped (already delivered)
	Errors         []RowError        // rows that failed
	DestinationIDs map[string]string // rowID → destination-assigned record ID (e.g. HubSpot contact ID)
}

// Destination is implemented by all output connectors.
// Destinations receive Rows and write them to their target system.
// The same Destination interface is used for both batch and streaming.
type Destination interface {
	// Connect opens a connection to the destination.
	// Must be called before Load.
	Connect(ctx context.Context, cfg config.DestinationConfig) error

	// Load writes a batch of rows to the destination.
	// Implementations must:
	//   - Check IsDelivered before writing each row
	//   - Call MarkDelivered after each successful write
	//   - Collect per-row errors into LoadResult.Errors
	//   - Never return a top-level error for per-row failures
	//     (use LoadResult.Errors instead)
	//   - Only return a top-level error for fatal connection issues
	// For streaming pipelines, Load is called with a single-row slice.
	// For batch pipelines, Load is called with up to BatchSize rows.
	Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error)

	// Close releases all resources.
	Close() error
}

// RunFinalizer is an optional interface for destinations that defer work to
// the end of a batch run — e.g. atomic replace via a staging table. The
// engine calls FinalizeRun once per run after all Load calls have completed:
// succeeded=true commits the deferred work (swap staging into the target),
// succeeded=false discards it, leaving the target untouched.
type RunFinalizer interface {
	FinalizeRun(ctx context.Context, runID int64, succeeded bool) error
}
