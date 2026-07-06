// Package state provides a thread-safe in-memory StateStore for use in tests.
package state

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

func init() {
	Register("memory", func(cfg stateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})
}

type memLockEntry struct {
	owner     string
	expiresAt time.Time
}

// MemoryStore is a thread-safe in-memory StateStore used only in tests.
type MemoryStore struct {
	mu           sync.RWMutex
	entityStates map[string]*EntityState
	ruleFirings  map[string]time.Time
	decisions    []*DecisionEvent
	runs         map[int64]RunLog
	nextRunID    int64
	deliveries   map[string]bool
	batchMu      sync.Mutex
	pending      map[string]bool
	inBatch      bool
	locks        map[string]memLockEntry
}

var _ StateStore = (*MemoryStore)(nil)

// NewMemoryStore returns an empty in-memory StateStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entityStates: make(map[string]*EntityState),
		ruleFirings:  make(map[string]time.Time),
		decisions:    nil,
		runs:         make(map[int64]RunLog),
		deliveries:   make(map[string]bool),
		pending:      make(map[string]bool),
		locks:        make(map[string]memLockEntry),
	}
}

func entityStateKey(syncName, destination, entityKey string) string {
	return strings.Join([]string{syncName, destination, entityKey}, "\x00")
}

func ruleFiringKey(syncName, destination, entityKey, rule string) string {
	return strings.Join([]string{syncName, destination, entityKey, rule}, "\x00")
}

// --- Entity state ---

func (s *MemoryStore) GetEntityState(_ context.Context, syncName, destination, entityKey string) (*EntityState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	es := s.entityStates[entityStateKey(syncName, destination, entityKey)]
	if es == nil {
		return nil, nil
	}
	cp := *es
	return &cp, nil
}

func (s *MemoryStore) SaveEntityState(_ context.Context, es *EntityState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *es
	s.entityStates[entityStateKey(es.SyncName, es.Destination, es.EntityKey)] = &cp
	return nil
}

func (s *MemoryStore) ListEntityStates(_ context.Context, syncName, destination string, limit, offset int) ([]*EntityState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var all []*EntityState
	for _, es := range s.entityStates {
		if es.SyncName == syncName && es.Destination == destination {
			cp := *es
			all = append(all, &cp)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].UpdatedAt.After(all[j].UpdatedAt)
	})
	if offset >= len(all) {
		return nil, nil
	}
	all = all[offset:]
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (s *MemoryStore) ResetEntityState(_ context.Context, syncName, destination, entityKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entityStates, entityStateKey(syncName, destination, entityKey))
	return nil
}

// --- Rule firings ---

func (s *MemoryStore) HasRuleFired(_ context.Context, syncName, destination, entityKey, rule string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.ruleFirings[ruleFiringKey(syncName, destination, entityKey, rule)]
	return ok, nil
}

func (s *MemoryStore) MarkRuleFired(_ context.Context, syncName, destination, entityKey, rule string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ruleFirings[ruleFiringKey(syncName, destination, entityKey, rule)] = time.Now().UTC()
	return nil
}

// --- Decision events ---

func (s *MemoryStore) RecordDecision(_ context.Context, event *DecisionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *event
	s.decisions = append(s.decisions, &cp)
	return nil
}

func (s *MemoryStore) GetDecisionHistory(_ context.Context, syncName, destination, entityKey string, limit int) ([]*DecisionEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*DecisionEvent
	for i := len(s.decisions) - 1; i >= 0; i-- {
		ev := s.decisions[i]
		if ev.SyncName == syncName && ev.Destination == destination && ev.EntityKey == entityKey {
			cp := *ev
			out = append(out, &cp)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// --- Run log ---

func (s *MemoryStore) StartRun(_ context.Context, syncName, mode string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextRunID++
	id := s.nextRunID
	s.runs[id] = RunLog{
		ID: id, SyncName: syncName, Mode: mode,
		StartedAt: time.Now().UTC(), Status: "running",
	}
	return id, nil
}

func (s *MemoryStore) FinishRun(_ context.Context, runID int64, stats RunStats) error {
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

func (s *MemoryStore) GetLastRun(ctx context.Context, syncName string) (RunLog, error) {
	history, err := s.GetRunHistory(ctx, syncName, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(history) == 0 {
		return RunLog{}, fmt.Errorf("state: no runs found for %q", syncName)
	}
	return history[0], nil
}

func (s *MemoryStore) GetRunHistory(_ context.Context, syncName string, limit int) ([]RunLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var logs []RunLog
	for _, run := range s.runs {
		if run.SyncName == syncName {
			logs = append(logs, run)
		}
	}
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].StartedAt.After(logs[j].StartedAt)
	})
	if limit > 0 && len(logs) > limit {
		logs = logs[:limit]
	}
	return logs, nil
}

// --- Delivery idempotency ---

func memDeliveryKey(rowID, syncName, destination string) string {
	return rowID + "\x00" + syncName + "\x00" + destination
}

func (s *MemoryStore) IsDelivered(_ context.Context, rowID, syncName, destination string) (bool, error) {
	key := memDeliveryKey(rowID, syncName, destination)
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

func (s *MemoryStore) MarkDelivered(_ context.Context, rowID, syncName, destination string) error {
	key := memDeliveryKey(rowID, syncName, destination)
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

func (s *MemoryStore) BeginBatch(_ context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	for k := range s.pending {
		delete(s.pending, k)
	}
	s.inBatch = true
	return nil
}

func (s *MemoryStore) CommitBatch(_ context.Context) error {
	s.batchMu.Lock()
	pending := s.pending
	s.pending = make(map[string]bool)
	s.inBatch = false
	s.batchMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range pending {
		s.deliveries[k] = true
	}
	return nil
}

func (s *MemoryStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.pending = make(map[string]bool)
	s.inBatch = false
	return nil
}

func (s *MemoryStore) Close() error { return nil }

// --- Pipeline locks ---

func (s *MemoryStore) LockRun(_ context.Context, syncName, owner string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if entry, ok := s.locks[syncName]; ok && entry.expiresAt.After(now) {
		return fmt.Errorf("state: sync %q is already running (lock held by %q) — use 'vortara state unlock' to clear a stale lock", syncName, entry.owner)
	}
	s.locks[syncName] = memLockEntry{owner: owner, expiresAt: now.Add(ttl)}
	return nil
}

func (s *MemoryStore) UnlockRun(_ context.Context, syncName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.locks, syncName)
	return nil
}

func (s *MemoryStore) HeartbeatLock(_ context.Context, syncName, owner string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.locks[syncName]; ok && entry.owner == owner {
		entry.expiresAt = time.Now().Add(ttl)
		s.locks[syncName] = entry
	}
	return nil
}
