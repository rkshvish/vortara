// Package engine coordinates sync runs using the Programmable State model.
package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// Engine owns the sync runtime and shared state store.
type Engine struct {
	store        state.StateStore
	destOverride destination.Destination // set in dry-run mode
	dryRun       bool                    // true when destOverride is a dry-run destination
	running      atomic.Bool
	shutdownCh   chan struct{}
	doneCh       chan struct{}
	statsMu      sync.RWMutex
	nextRunAt    time.Time
	lastStats    *state.RunStats
	approvalHash string // operator-supplied hash to bypass an approval gate
	shutdownOnce sync.Once
	doneOnce     sync.Once
	closeOnce    sync.Once
}

// SyncStats exposes current sync health and last-run metrics.
type SyncStats struct {
	Name            string
	Status          string
	LastRunAt       time.Time
	LastRunDuration time.Duration
	LastStatus      string
	RowsLoaded      int64
	RowsSkipped     int64
	RowsErrored     int64
	NextRunAt       time.Time
}

// NewEngine creates an engine backed by the supplied state store.
func NewEngine(store state.StateStore) *Engine {
	return &Engine{
		store:      store,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Store returns the engine's state store.
func (e *Engine) Store() state.StateStore {
	return e.store
}

// SetDryRunDestination installs a destination override for dry-run mode.
// In dry-run mode the engine never writes entity state, marks rules as fired,
// or records delivery idempotency — all state reads still use the real store.
func (e *Engine) SetDryRunDestination(dest destination.Destination) {
	e.destOverride = dest
	e.dryRun = true
}

// Shutdown signals shutdown and waits for in-flight work to stop.
func (e *Engine) Shutdown(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	e.shutdownOnce.Do(func() { close(e.shutdownCh) })
	e.markDone()
	return e.Close()
}

// Close releases all resources.
func (e *Engine) Close() error {
	var firstErr error
	e.closeOnce.Do(func() {
		if e.store != nil {
			if err := e.store.Close(); err != nil {
				firstErr = err
			}
		}
		e.markDone()
	})
	return firstErr
}

// Stats returns current sync statistics.
func (e *Engine) Stats(ctx context.Context, cfg *synccfg.SyncSpec) SyncStats {
	stats := SyncStats{}
	if cfg != nil {
		stats.Name = cfg.Name
	}
	stats.Status = "idle"
	if e.running.Load() {
		stats.Status = "running"
	}
	if e.store != nil && cfg != nil {
		if lastRun, err := e.store.GetLastRun(ctx, cfg.Name); err == nil {
			stats.LastRunAt = lastRun.StartedAt
			if !lastRun.FinishedAt.IsZero() {
				stats.LastRunDuration = lastRun.FinishedAt.Sub(lastRun.StartedAt)
			}
			stats.LastStatus = lastRun.Status
			stats.RowsLoaded = int64(lastRun.RowsLoaded)
			stats.RowsSkipped = int64(lastRun.RowsSkipped)
			stats.RowsErrored = int64(lastRun.RowsErrored)
		}
	}
	e.statsMu.RLock()
	stats.NextRunAt = e.nextRunAt
	e.statsMu.RUnlock()
	return stats
}

func (e *Engine) setNextRunAt(t time.Time) {
	e.statsMu.Lock()
	e.nextRunAt = t
	e.statsMu.Unlock()
}

// LastStats returns the RunStats from the most recently completed run, or nil if no run has finished yet.
func (e *Engine) LastStats() *state.RunStats {
	e.statsMu.RLock()
	defer e.statsMu.RUnlock()
	return e.lastStats
}

// SetApprovalHash supplies the operator-provided snapshot hash to bypass an approval gate.
// Call before Run() when the operator re-runs with --approve-snapshot.
func (e *Engine) SetApprovalHash(hash string) {
	e.statsMu.Lock()
	e.approvalHash = hash
	e.statsMu.Unlock()
}

func (e *Engine) markDone() {
	e.doneOnce.Do(func() { close(e.doneCh) })
}
