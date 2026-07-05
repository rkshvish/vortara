package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/state"
	v2cfg "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

// ReplayResult summarizes a DLQ replay pass.
type ReplayResult struct {
	Read     int // records read from the DLQ file
	Replayed int // rows delivered successfully and removed from the file
	Failed   int // rows that failed again and remain in the file
}

// ResolveDLQPath returns the DLQ file path for a pipeline config.
func ResolveDLQPath(cfg *v2cfg.PipelineConfig) string {
	if cfg == nil {
		return ""
	}
	if p := strings.TrimSpace(cfg.Settings.DLQPath); p != "" {
		return p
	}
	return cfg.Name + ".dlq.jsonl"
}

// ReadDLQRecords parses a JSONL dead-letter file.
func ReadDLQRecords(path string) ([]DLQRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []DLQRecord
	reader := bufio.NewReaderSize(f, 1<<20)
	lineNo := 0
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			trimmed := strings.TrimSpace(string(line))
			if trimmed != "" {
				var rec DLQRecord
				if uerr := json.Unmarshal([]byte(trimmed), &rec); uerr != nil {
					return nil, fmt.Errorf("dlq: bad record at line %d: %w", lineNo, uerr)
				}
				records = append(records, rec)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

// ReplayDLQ re-delivers dead-lettered rows through the pipeline's destinations.
// Rows that succeed are removed from the file; rows that fail again remain,
// with their error and timestamp refreshed. Transforms are NOT re-applied —
// DLQ records hold post-transform data. Watermarks are not touched.
func (e *Engine) ReplayDLQ(ctx context.Context, cfg *v2cfg.PipelineConfig, path string) (ReplayResult, error) {
	var result ReplayResult
	if e == nil || e.store == nil {
		return result, errors.New("engine: nil engine or store")
	}
	if cfg == nil {
		return result, errors.New("engine: nil config")
	}
	if path == "" {
		path = ResolveDLQPath(cfg)
	}

	records, err := ReadDLQRecords(path)
	if err != nil {
		if os.IsNotExist(err) {
			return result, fmt.Errorf("dlq: no dead-letter file at %s", path)
		}
		return result, err
	}
	result.Read = len(records)
	if len(records) == 0 {
		return result, nil
	}

	rt, err := router.New(cfg.Destinations)
	if err != nil {
		return result, err
	}
	dests, err := e.buildDestinations(cfg.Destinations)
	if err != nil {
		return result, err
	}
	defer closeDestinations(dests)

	runID, err := e.store.StartRun(ctx, cfg.Name, "replay")
	if err != nil {
		return result, err
	}
	stats := state.RunStats{Status: "success"}
	defer func() {
		_ = e.store.FinishRun(ctx, runID, stats)
	}()
	if err := e.store.BeginBatch(ctx); err != nil {
		stats.Status = "failed"
		stats.Error = err.Error()
		return result, err
	}

	rows := make([]row.Row, len(records))
	for i, rec := range records {
		rows[i] = row.Row{
			ID:          rec.RowID,
			Pipeline:    cfg.Name,
			PrimaryKey:  rec.PrimaryKey,
			Data:        rec.Data,
			ExtractedAt: time.Now(),
		}
	}

	// Dispatch in per-destination batches; collect rows that fail again.
	failedErr := make(map[string]string, 0)
	flushSize := batchBufferSize(cfg)
	buffers := make([][]row.Row, len(dests))
	flush := func(idx int) {
		batch := buffers[idx]
		if len(batch) == 0 {
			return
		}
		buffers[idx] = nil
		res, err := e.loadBatch(ctx, cfg, dests, idx, batch)
		stats.RowsLoaded += res.Loaded
		stats.RowsSkipped += res.Skipped
		if err != nil {
			stats.RowsErrored += len(batch)
			for _, r := range batch {
				failedErr[r.ID] = err.Error()
			}
			return
		}
		stats.RowsErrored += len(res.Errors)
		for _, re := range res.Errors {
			failedErr[re.RowID] = re.Err.Error()
		}
	}
	for _, r := range rows {
		for _, idx := range rt.Route(r) {
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

	if err := e.store.CommitBatch(ctx); err != nil {
		_ = e.store.RollbackBatch()
		stats.Status = "failed"
		stats.Error = err.Error()
		return result, err
	}

	// Rewrite the DLQ file with only the rows that failed again.
	var remaining []DLQRecord
	now := time.Now().UTC()
	for _, rec := range records {
		if msg, failed := failedErr[rec.RowID]; failed {
			rec.Error = msg
			rec.FailedAt = now
			remaining = append(remaining, rec)
		}
	}
	result.Failed = len(remaining)
	result.Replayed = result.Read - result.Failed
	if result.Failed > 0 {
		stats.Status = "failed"
		stats.Error = fmt.Sprintf("%d rows failed replay", result.Failed)
	}

	if len(remaining) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return result, err
		}
		return result, nil
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return result, err
	}
	w := bufio.NewWriter(f)
	for _, rec := range remaining {
		line, err := json.Marshal(rec)
		if err != nil {
			_ = f.Close()
			return result, err
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			_ = f.Close()
			return result, err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return result, err
	}
	if err := f.Close(); err != nil {
		return result, err
	}
	return result, os.Rename(tmp, path)
}
