// Package engine coordinates extraction and loading for pipeline runs.
package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rkshvish/vortara/internal/connector/source"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/row"
)

var errMaxRowsReached = errors.New("max rows reached")

// Extractor handles watermark-based incremental extraction for batch pipeline runs.
type Extractor struct {
	source        source.BatchSource
	store         state.StateStore
	pipeline      string
	srcName       string
	maxRows       int64
	progress      *Progress
	rowsExtracted int64
}

// ExtractorOption configures an Extractor.
type ExtractorOption func(*Extractor)

// WithMaxRows limits extraction to at most n rows for a single run.
func WithMaxRows(n int64) ExtractorOption {
	return func(e *Extractor) {
		e.maxRows = n
	}
}

// WithProgress attaches a progress tracker to extraction.
func WithProgress(p *Progress) ExtractorOption {
	return func(e *Extractor) {
		e.progress = p
	}
}

// NewExtractor creates an Extractor.
// pipeline is the pipeline name from config and srcName identifies the source.
func NewExtractor(src source.BatchSource, store state.StateStore, pipeline string, srcName string, opts ...ExtractorOption) *Extractor {
	ex := &Extractor{
		source:   src,
		store:    store,
		pipeline: pipeline,
		srcName:  srcName,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(ex)
		}
	}
	return ex
}

// Extract loads the current watermark, captures the interval end at run start,
// calls source.Extract, fixes Row.Pipeline on each row, and sends rows to out.
// Extract closes out when done.
// It returns the interval-end watermark on a clean completion, or the highest
// watermark observed in rows sent to out when the run is interrupted.
func (e *Extractor) Extract(ctx context.Context, out chan<- row.Row) (newWatermark time.Time, err error) {
	defer close(out)

	l := vlogger.FromContext(ctx)
	wm, err := e.store.GetWatermark(e.pipeline, e.srcName)
	if err != nil {
		return time.Time{}, err
	}
	extractCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	intervalEnd := time.Now().UTC()
	startedAt := time.Now()
	rowsExtracted := 0
	var lastWatermark time.Time

	sourceCh := make(chan row.Row)
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.source.Extract(extractCtx, wm, intervalEnd, sourceCh)
	}()

	waitSourceErr := func() error {
		select {
		case sourceErr := <-errCh:
			return sourceErr
		case <-extractCtx.Done():
			return extractCtx.Err()
		}
	}

	for {
		select {
		case <-ctx.Done():
			cancel()
			if srcErr := waitSourceErr(); srcErr != nil && !errors.Is(srcErr, context.Canceled) && !errors.Is(srcErr, context.DeadlineExceeded) {
				return lastWatermark, srcErr
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				l.Warn("run exceeded max_runtime, stopping extraction",
					slog.String("pipeline", e.pipeline),
					slog.Int("rows_extracted", rowsExtracted),
				)
				return lastWatermark, ctx.Err()
			}
			return lastWatermark, ctx.Err()
		case r, ok := <-sourceCh:
			if !ok {
				if sourceErr := waitSourceErr(); sourceErr != nil {
					return lastWatermark, sourceErr
				}
				l.Info("batch extracted",
					slog.String("pipeline", e.pipeline),
					slog.String("source", e.srcName),
					slog.Int("rows", rowsExtracted),
					slog.Int64("duration_ms", time.Since(startedAt).Milliseconds()),
				)
				return intervalEnd, nil
			}
			r.Pipeline = e.pipeline
			r = r.WithContext(ctx)
			rowsExtracted++
			atomic.AddInt64(&e.rowsExtracted, 1)
			lastWatermark = r.Watermark
			l.Debug("row extracted",
				slog.String("pipeline", e.pipeline),
				slog.String("source", e.srcName),
				slog.String("row_id", r.ID),
				slog.String("primary_key", r.PrimaryKey),
				slog.Time("watermark", r.Watermark),
			)
			select {
			case out <- r:
				if e.progress != nil {
					e.progress.RecordExtracted(1)
				}
			case <-ctx.Done():
				cancel()
				if sourceErr := waitSourceErr(); sourceErr != nil && !errors.Is(sourceErr, context.Canceled) && !errors.Is(sourceErr, context.DeadlineExceeded) {
					return intervalEnd, sourceErr
				}
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					l.Warn("run exceeded max_runtime, stopping extraction",
						slog.String("pipeline", e.pipeline),
						slog.Int("rows_extracted", rowsExtracted),
					)
					return lastWatermark, ctx.Err()
				}
				return lastWatermark, ctx.Err()
			}
			if e.maxRows > 0 && int64(rowsExtracted) >= e.maxRows {
				l.Warn("max_rows limit reached, stopping extraction",
					slog.String("pipeline", e.pipeline),
					slog.Int64("max_rows", e.maxRows),
					slog.Int("rows_extracted", rowsExtracted),
				)
				cancel()
				if sourceErr := waitSourceErr(); sourceErr != nil && !errors.Is(sourceErr, context.Canceled) && !errors.Is(sourceErr, context.DeadlineExceeded) {
					return lastWatermark, sourceErr
				}
				return lastWatermark, errMaxRowsReached
			}
		}
	}
}

// RowsExtracted returns the number of rows emitted by the extractor.
func (e *Extractor) RowsExtracted() int64 {
	if e == nil {
		return 0
	}
	return atomic.LoadInt64(&e.rowsExtracted)
}
