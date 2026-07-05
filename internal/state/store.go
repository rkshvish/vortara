// Package state defines the storage contract used by Vortara to persist
// batch watermarks, streaming offsets, run history, and delivery idempotency.
package state

import (
	"context"
	"time"
)

// RunStats holds the result of a completed pipeline run.
type RunStats struct {
	RowsExtracted int
	RowsLoaded    int
	RowsSkipped   int
	RowsErrored   int
	Status        string // "success" | "failed" | "timeout"
	Error         string // empty if success
}

// RunLog is one entry from the run history.
type RunLog struct {
	ID            int64
	Pipeline      string
	Mode          string // "batch" | "streaming"
	StartedAt     time.Time
	FinishedAt    time.Time
	RowsExtracted int
	RowsLoaded    int
	RowsSkipped   int
	RowsErrored   int
	Status        string
	Error         string
}

// StateStore is the interface all state backends must implement.
type StateStore interface {
	// GetWatermark returns the last processed watermark for a pipeline+source.
	// Returns zero time.Time if no watermark exists yet (first run).
	GetWatermark(pipeline, source string) (time.Time, error)

	// SetWatermark saves the watermark for a pipeline+source.
	SetWatermark(pipeline, source string, wm time.Time) error

	// GetNumericWatermark returns the last integer cursor (0 if unset)
	// for sources using an integer watermark column.
	GetNumericWatermark(pipeline, source string) (int64, error)

	// SetNumericWatermark saves the integer cursor for a pipeline+source.
	SetNumericWatermark(pipeline, source string, wm int64) error

	// GetOffset returns the last committed offset for a topic+partition.
	// Returns -1 if no offset exists yet (start from beginning).
	GetOffset(pipeline, topic string, partition int) (int64, error)

	// SetOffset saves the committed offset for a topic+partition.
	SetOffset(pipeline, topic string, partition int, offset int64) error

	// StartRun creates a new run log entry and returns its ID.
	// mode is "batch" or "streaming".
	StartRun(pipeline, mode string) (int64, error)

	// FinishRun updates the run log entry with final stats.
	FinishRun(runID int64, stats RunStats) error

	// GetLastRun returns the most recent run log entry for a pipeline.
	// Returns error if no runs exist yet.
	GetLastRun(pipeline string) (RunLog, error)

	// GetRunHistory returns the most recent run log entries for a pipeline.
	GetRunHistory(pipeline string, limit int) ([]RunLog, error)

	// IsDelivered returns true if this row was already delivered
	// to this destination in this pipeline.
	IsDelivered(rowID, pipeline, destination string) (bool, error)

	// MarkDelivered records that a row was successfully delivered.
	MarkDelivered(rowID, pipeline, destination string) error

	// PruneDelivered deletes delivery-log entries older than the cutoff.
	// Safe because the watermark guarantees rows older than the extraction
	// horizon are never re-checked; returns the number of entries removed.
	PruneDelivered(olderThan time.Time) (int64, error)

	// BeginBatch starts buffering delivery writes in memory.
	BeginBatch(ctx context.Context) error

	// CommitBatch atomically flushes buffered delivery writes.
	CommitBatch(ctx context.Context) error

	// RollbackBatch discards buffered delivery writes.
	RollbackBatch() error

	// Close releases all resources held by the state store.
	Close() error
}
