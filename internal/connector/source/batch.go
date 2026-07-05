// Package source contains source connector interfaces and implementations.
package source

import (
	"context"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// BatchSource is implemented by all batch (polling) source connectors.
// Each connector extracts rows incrementally using a watermark timestamp.
// The watermark is the value of the configured watermark_column
// (typically updated_at) from the last successful run.
type BatchSource interface {
	// Connect opens a connection to the source using the provided config.
	// Must be called before Extract.
	Connect(ctx context.Context, cfg config.SourceConfig) error

	// Extract fetches all rows where watermark_column > watermark and, when
	// intervalEnd is non-zero, watermark_column <= intervalEnd.
	// Rows are ordered by watermark_column ASC and fetched in batches of cfg.BatchSize.
	// Each row is sent to the out channel as it is extracted.
	// Extract closes the out channel when done or on error.
	// Returns error if extraction fails partway through.
	Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error

	// GetWatermarkColumn returns the column name used for watermark
	// filtering. Typically "updated_at" from config.
	GetWatermarkColumn() string

	// Close releases all resources held by the source connector.
	Close() error
}

// Future implementations will add:
// var _ BatchSource = (*PostgresSource)(nil)

// NumericCursorSource is an optional interface for batch sources that
// support integer watermark columns (e.g. an auto-increment primary key).
// The engine detects the cursor kind after Connect and routes extraction
// through ExtractNumeric when the column is an integer type.
type NumericCursorSource interface {
	// CursorKind inspects the configured watermark column and reports
	// "time" for timestamp columns, "int" for integer columns, or an
	// error when the column is missing or unsupported. "snapshot" is
	// returned for watermark: none.
	CursorKind(ctx context.Context) (string, error)

	// ExtractNumeric fetches rows where the cursor column > cursor,
	// ordered ascending, using keyset pagination. When limit > 0 at most
	// limit rows are emitted. It closes out when done and returns the
	// highest cursor value emitted (or the input cursor when no rows
	// matched), which the engine persists for the next run.
	ExtractNumeric(ctx context.Context, cursor int64, limit int64, out chan<- row.Row) (int64, error)
}
