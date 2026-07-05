package engine

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/connector/source"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/internal/steps"
	"github.com/rkshvish/vortara/internal/strategy"
	v2cfg "github.com/rkshvish/vortara/pkg/config/v2"
	"github.com/rkshvish/vortara/pkg/row"
)

func (e *Engine) runBatch(ctx context.Context, cfg *v2cfg.PipelineConfig, src source.BatchSource, proc *steps.Processor, router *router.Router, dests []destination.Destination, srcName string) error {
	if cfg == nil || src == nil || proc == nil || router == nil {
		return errors.New("batch run: invalid arguments")
	}
	if e.store == nil {
		return errors.New("batch run: nil state store")
	}

	if cfg.Cron != "" {
		return e.runBatchCron(ctx, cfg, src, proc, router, dests, srcName)
	}
	return e.runBatchOnce(ctx, cfg, src, proc, router, dests, srcName)
}

func (e *Engine) runBatchCron(ctx context.Context, cfg *v2cfg.PipelineConfig, src source.BatchSource, proc *steps.Processor, router *router.Router, dests []destination.Destination, srcName string) error {
	sched, err := cron.ParseStandard(cfg.Cron)
	if err != nil {
		return err
	}
	l := vlogger.FromContext(ctx)
	c := cron.New()
	_, err = c.AddFunc(cfg.Cron, func() {
		l.Info("scheduler triggered",
			slog.String("pipeline", cfg.Name),
			slog.String("cron", cfg.Cron),
		)
		_ = e.runBatchOnce(context.WithoutCancel(ctx), cfg, src, proc, router, dests, srcName)
	})
	if err != nil {
		return err
	}
	c.Start()
	defer func() {
		stopCtx := c.Stop()
		<-stopCtx.Done()
	}()

	if sched != nil {
		e.setNextRunAt(sched.Next(time.Now()))
	}
	<-ctx.Done()
	e.setNextRunAt(time.Time{})
	return ctx.Err()
}

func (e *Engine) runBatchOnce(ctx context.Context, cfg *v2cfg.PipelineConfig, src source.BatchSource, proc *steps.Processor, router *router.Router, dests []destination.Destination, srcName string) error {
	l := vlogger.FromContext(ctx)

	// settings.limits.max_runtime bounds this run; the run is marked
	// "timeout" and the watermark of delivered rows is saved so the next
	// run resumes where this one stopped.
	if d, err := time.ParseDuration(strings.TrimSpace(cfg.Settings.Limits.MaxRuntime)); err == nil && d > 0 {
		var cancelRuntime context.CancelFunc
		ctx, cancelRuntime = context.WithTimeout(ctx, d)
		defer cancelRuntime()
	}
	runID, err := e.store.StartRun(cfg.Name, "batch")
	if err != nil {
		return err
	}
	stats := state.RunStats{Status: "success"}
	defer func() {
		_ = e.store.FinishRun(runID, stats)
	}()

	if err := e.store.BeginBatch(ctx); err != nil {
		stats.Status = "failed"
		stats.Error = err.Error()
		return err
	}

	extractCh := make(chan row.Row, batchBufferSize(cfg))
	loadCh := make(chan row.Row, batchBufferSize(cfg))
	extractCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cursor kind decides the extraction path: timestamp window (default),
	// integer keyset cursor, or full snapshot.
	cursorKind := "time"
	var numericSrc source.NumericCursorSource
	if nc, ok := src.(source.NumericCursorSource); ok {
		kind, kindErr := nc.CursorKind(ctx)
		if kindErr != nil {
			stats.Status = "failed"
			stats.Error = kindErr.Error()
			_ = e.store.RollbackBatch()
			sendFailureAlert(ctx, cfg, kindErr)
			return kindErr
		}
		if kind == "int" {
			cursorKind = "int"
			numericSrc = nc
		}
	}

	var (
		newWatermark time.Time
		maxCursor    int64
		extractor    *Extractor
		extractErr   error
	)

	extractDone := make(chan struct{})
	if cursorKind == "int" {
		startCursor, curErr := e.store.GetNumericWatermark(cfg.Name, srcName)
		if curErr != nil {
			stats.Status = "failed"
			stats.Error = curErr.Error()
			_ = e.store.RollbackBatch()
			return curErr
		}
		go func() {
			defer close(extractDone)
			maxCursor, extractErr = numericSrc.ExtractNumeric(extractCtx, startCursor, cfg.Settings.Limits.MaxRows, extractCh)
		}()
	} else {
		var exOpts []ExtractorOption
		if cfg.Settings.Limits.MaxRows > 0 {
			// max_rows caps this run; like max_runtime, the saved watermark
			// makes the next run continue from the last delivered row.
			exOpts = append(exOpts, WithMaxRows(cfg.Settings.Limits.MaxRows))
		}
		extractor = NewExtractor(src, e.store, cfg.Name, srcName, exOpts...)
		go func() {
			defer close(extractDone)
			newWatermark, extractErr = extractor.Extract(extractCtx, extractCh)
		}()
	}

	workers := cfg.Settings.Concurrency.Workers
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range extractCh {
				transformed, ok := proc.Apply(r)
				if !ok {
					continue
				}
				loadCh <- transformed
			}
		}()
	}
	go func() {
		wg.Wait()
		close(loadCh)
	}()

	dlq, dlqErr := newDLQWriter(cfg)
	if dlqErr != nil {
		l.Warn("dlq unavailable, falling back to skip",
			slog.String("pipeline", cfg.Name),
			slog.String("error", dlqErr.Error()),
		)
	}
	defer dlq.Close()

	drainCtx := strategy.WithRunID(context.WithoutCancel(ctx), runID)
	var (
		highWatermark time.Time
		firstErr      error
	)

	// Micro-batching loader: rows are buffered per destination and flushed in
	// batches so destinations can use bulk paths (Postgres COPY, batch APIs)
	// instead of paying per-row Load overhead.
	flushSize := batchBufferSize(cfg)
	buffers := make([][]row.Row, len(dests))

	flush := func(idx int) {
		batch := buffers[idx]
		if len(batch) == 0 {
			return
		}
		buffers[idx] = nil

		res, err := e.loadBatch(drainCtx, cfg, dests, idx, batch)
		stats.RowsLoaded += res.Loaded
		stats.RowsSkipped += res.Skipped

		if err != nil {
			// Whole-batch failure: every row in the batch failed.
			stats.RowsErrored += len(batch)
			if dlq.Enabled() {
				for _, r := range batch {
					if writeErr := dlq.Write(r, err); writeErr != nil {
						l.Warn("dlq write failed",
							slog.String("pipeline", cfg.Name),
							slog.String("row_id", r.ID),
							slog.String("error", writeErr.Error()),
						)
					}
				}
			} else if firstErr == nil {
				firstErr = err
			}
			return
		}

		stats.RowsErrored += len(res.Errors)
		for _, re := range res.Errors {
			if dlq.Enabled() {
				if writeErr := dlq.Write(re.Row, re.Err); writeErr != nil {
					l.Warn("dlq write failed",
						slog.String("pipeline", cfg.Name),
						slog.String("row_id", re.RowID),
						slog.String("error", writeErr.Error()),
					)
				}
			} else if firstErr == nil {
				firstErr = re.Err
			}
		}
		if len(res.Errors) == 0 {
			for _, r := range batch {
				if !r.Watermark.IsZero() && r.Watermark.After(highWatermark) {
					highWatermark = r.Watermark
				}
			}
		}
	}

	for r := range loadCh {
		if err := ctx.Err(); err != nil && firstErr == nil {
			firstErr = err
		}
		for _, idx := range router.Route(r) {
			if idx < 0 || idx >= len(dests) {
				continue
			}
			buffers[idx] = append(buffers[idx], r)
			if len(buffers[idx]) >= flushSize {
				flush(idx)
			}
		}
	}
	for idx := range buffers {
		flush(idx)
	}

	<-extractDone

	// Run finalizers: destinations with deferred work (e.g. atomic replace
	// staging swaps) commit it only when the run succeeded so far.
	finalizeOK := firstErr == nil && ctx.Err() == nil &&
		(extractErr == nil || errors.Is(extractErr, errMaxRowsReached))
	for idx := range dests {
		dest := dests[idx]
		if override, ok := e.destinations[strconv.Itoa(idx)]; ok && override != nil {
			dest = override
		}
		f, ok := dest.(destination.RunFinalizer)
		if !ok {
			continue
		}
		if err := f.FinalizeRun(drainCtx, runID, finalizeOK); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if n := dlq.Count(); n > 0 {
		l.Warn("rows dead-lettered; they will NOT be retried on the next run — replay from the DLQ file",
			slog.String("pipeline", cfg.Name),
			slog.Int("rows", n),
			slog.String("path", dlq.Path()),
		)
	}
	commitErr := e.store.CommitBatch(drainCtx)
	if commitErr != nil {
		_ = e.store.RollbackBatch()
		stats.Status = "failed"
		stats.Error = commitErr.Error()
		sendFailureAlert(ctx, cfg, commitErr)
		return commitErr
	}

	rowsExtracted := 0
	if extractor != nil {
		rowsExtracted = int(extractor.RowsExtracted())
	}
	cancelled := ctx.Err() != nil || errors.Is(extractErr, context.Canceled) || errors.Is(extractErr, context.DeadlineExceeded) || errors.Is(extractErr, errMaxRowsReached)
	snapshot := cfg.Source.Watermark == "none"
	switch {
	case snapshot:
		// watermark: none — full extract every run, no cursor to advance.
	case cursorKind == "int":
		// Keyset cursor: save only after a clean pass (a max_rows-capped run
		// finishes cleanly, so the next run resumes from the last emitted id;
		// a cancelled run re-extracts and the delivery log dedupes).
		if extractErr == nil && firstErr == nil && !cancelled {
			_ = e.store.SetNumericWatermark(cfg.Name, srcName, maxCursor)
		}
	case cancelled:
		if !highWatermark.IsZero() {
			_ = e.store.SetWatermark(cfg.Name, srcName, highWatermark)
		}
	case extractErr == nil && firstErr == nil:
		_ = e.store.SetWatermark(cfg.Name, srcName, newWatermark)
	}

	if extractErr != nil && !errors.Is(extractErr, context.Canceled) && !errors.Is(extractErr, context.DeadlineExceeded) && !errors.Is(extractErr, errMaxRowsReached) {
		if firstErr == nil {
			firstErr = extractErr
		}
	}

	if firstErr != nil {
		stats.Status = "failed"
		stats.Error = firstErr.Error()
		l.Error("batch run failed",
			slog.String("pipeline", cfg.Name),
			slog.String("error", firstErr.Error()),
		)
		sendFailureAlert(ctx, cfg, firstErr)
		return firstErr
	}
	if cancelled {
		stats.Status = "timeout"
	}
	if rowsExtracted == 0 && extractErr == nil && ctx.Err() == nil {
		stats.RowsExtracted = 0
	}

	// After a fully successful run, prune delivery-log entries older than
	// settings.state.delivered_ttl so state does not grow without bound.
	if ttl, err := time.ParseDuration(strings.TrimSpace(cfg.Settings.State.DeliveredTTL)); err == nil && ttl > 0 && !cancelled {
		if n, err := e.store.PruneDelivered(time.Now().Add(-ttl)); err != nil {
			l.Warn("delivery-log prune failed",
				slog.String("pipeline", cfg.Name),
				slog.String("error", err.Error()),
			)
		} else if n > 0 {
			l.Info("delivery log pruned",
				slog.String("pipeline", cfg.Name),
				slog.Int64("removed", n),
				slog.String("ttl", ttl.String()),
			)
		}
	}
	return nil
}
