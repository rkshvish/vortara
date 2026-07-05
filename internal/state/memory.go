// Package state provides a thread-safe in-memory StateStore implementation for use in tests only.
package state

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

// MemoryStore is a thread-safe in-memory StateStore used only in tests.
type MemoryStore struct {
	mu                sync.RWMutex
	watermarks        map[string]time.Time
	numericWatermarks map[string]int64
	offsets           map[string]int64
	runs       map[int64]RunLog
	nextRunID  int64
	deliveries map[string]bool
	batchMu    sync.Mutex
	pending    map[string]bool
	inBatch    bool
}

var _ StateStore = (*MemoryStore)(nil)

func init() {
	Register("memory", func(cfg config.StateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})
}

// NewMemoryStore returns an in-memory StateStore.
// All data is lost when the store is closed or process exits.
// Use only in tests.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		watermarks: make(map[string]time.Time),
		offsets:    make(map[string]int64),
		runs:       make(map[int64]RunLog),
		deliveries: make(map[string]bool),
		pending:    make(map[string]bool),
	}
}

func watermarkKey(pipeline, source string) string {
	return pipeline + ":" + source
}

func offsetKey(pipeline, topic string, partition int) string {
	return pipeline + ":" + topic + ":" + strconv.Itoa(partition)
}

func deliveryKey(rowID, pipeline, destination string) string {
	return rowID + ":" + pipeline + ":" + destination
}

// GetNumericWatermark returns the last integer cursor for a pipeline+source.
func (s *MemoryStore) GetNumericWatermark(ctx context.Context, pipeline, source string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.numericWatermarks[watermarkKey(pipeline, source)], nil
}

// SetNumericWatermark saves the integer cursor for a pipeline+source.
func (s *MemoryStore) SetNumericWatermark(ctx context.Context, pipeline, source string, wm int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.numericWatermarks == nil {
		s.numericWatermarks = make(map[string]int64)
	}
	s.numericWatermarks[watermarkKey(pipeline, source)] = wm
	return nil
}

// GetWatermark returns the last processed watermark for a pipeline and source.
func (s *MemoryStore) GetWatermark(ctx context.Context, pipeline, source string) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if wm, ok := s.watermarks[watermarkKey(pipeline, source)]; ok {
		return wm, nil
	}
	return time.Time{}, nil
}

// SetWatermark saves the watermark for a pipeline and source.
func (s *MemoryStore) SetWatermark(ctx context.Context, pipeline, source string, wm time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watermarks[watermarkKey(pipeline, source)] = wm.UTC()
	return nil
}

// GetOffset returns the last committed offset for a pipeline, topic, and partition.
func (s *MemoryStore) GetOffset(ctx context.Context, pipeline, topic string, partition int) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset, ok := s.offsets[offsetKey(pipeline, topic, partition)]; ok {
		return offset, nil
	}
	return -1, nil
}

// SetOffset saves the committed offset for a pipeline, topic, and partition.
func (s *MemoryStore) SetOffset(ctx context.Context, pipeline, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.offsets[offsetKey(pipeline, topic, partition)] = offset
	return nil
}

// StartRun creates a new run log entry and returns its ID.
func (s *MemoryStore) StartRun(ctx context.Context, pipeline, mode string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextRunID++
	id := s.nextRunID
	s.runs[id] = RunLog{
		ID:        id,
		Pipeline:  pipeline,
		Mode:      mode,
		StartedAt: time.Now().UTC(),
		Status:    "running",
	}
	return id, nil
}

// FinishRun updates a run log entry with final statistics.
func (s *MemoryStore) FinishRun(ctx context.Context, runID int64, stats RunStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("state: run %d not found", runID)
	}

	run.FinishedAt = time.Now().UTC()
	run.RowsExtracted = stats.RowsExtracted
	run.RowsLoaded = stats.RowsLoaded
	run.RowsSkipped = stats.RowsSkipped
	run.RowsErrored = stats.RowsErrored
	run.Status = stats.Status
	run.Error = stats.Error
	s.runs[runID] = run
	return nil
}

// GetLastRun returns the most recent run log entry for a pipeline.
func (s *MemoryStore) GetLastRun(ctx context.Context, pipeline string) (RunLog, error) {
	history, err := s.GetRunHistory(ctx, pipeline, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(history) == 0 {
		return RunLog{}, fmt.Errorf("state: no runs found for pipeline %q", pipeline)
	}
	return history[0], nil
}

// GetRunHistory returns the most recent run log entries for a pipeline.
func (s *MemoryStore) GetRunHistory(ctx context.Context, pipeline string, limit int) ([]RunLog, error) {
	if limit <= 0 {
		return []RunLog{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	logs := make([]RunLog, 0, len(s.runs))
	for _, run := range s.runs {
		if run.Pipeline == pipeline {
			logs = append(logs, run)
		}
	}

	sort.Slice(logs, func(i, j int) bool {
		if logs[i].StartedAt.Equal(logs[j].StartedAt) {
			return logs[i].ID > logs[j].ID
		}
		return logs[i].StartedAt.After(logs[j].StartedAt)
	})
	if len(logs) > limit {
		logs = logs[:limit]
	}
	return logs, nil
}

// IsDelivered reports whether a row has already been delivered.
func (s *MemoryStore) IsDelivered(ctx context.Context, rowID, pipeline, destination string) (bool, error) {
	key := deliveryKey(rowID, pipeline, destination)
	s.batchMu.Lock()
	if s.inBatch && s.pending[key] {
		s.batchMu.Unlock()
		return true, nil
	}
	s.batchMu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.deliveries[key], nil
}

// MarkDelivered records that a row was successfully delivered.
func (s *MemoryStore) MarkDelivered(ctx context.Context, rowID, pipeline, destination string) error {
	key := deliveryKey(rowID, pipeline, destination)
	s.batchMu.Lock()
	if s.inBatch {
		s.pending[key] = true
		s.batchMu.Unlock()
		return nil
	}
	s.batchMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.deliveries[key] = true
	return nil
}

// PruneDelivered is a no-op for the in-memory store (no timestamps kept).
func (s *MemoryStore) PruneDelivered(ctx context.Context, olderThan time.Time) (int64, error) {
	return 0, nil
}

// BeginBatch starts buffering delivery writes in memory.
func (s *MemoryStore) BeginBatch(ctx context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	for k := range s.pending {
		delete(s.pending, k)
	}
	s.inBatch = true
	return nil
}

// CommitBatch flushes buffered delivery writes atomically.
func (s *MemoryStore) CommitBatch(ctx context.Context) error {
	s.batchMu.Lock()
	pending := s.pending
	s.pending = make(map[string]bool)
	s.inBatch = false
	s.batchMu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range pending {
		s.deliveries[key] = true
	}
	return nil
}

// RollbackBatch discards buffered delivery writes.
func (s *MemoryStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.pending = make(map[string]bool)
	s.inBatch = false
	return nil
}

// Close releases all resources held by the store.
func (s *MemoryStore) Close() error {
	return nil
}
