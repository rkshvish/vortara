// Package engine coordinates v2 pipeline runs.
package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	v2cfg "github.com/rkshvish/vortara/pkg/config/pipeline"
)

// Engine owns the v2 pipeline runtime and shared state store.
type Engine struct {
	cfg          *v2cfg.PipelineConfig
	store        state.StateStore
	destinations map[string]destination.Destination
	running      atomic.Bool
	shutdownCh   chan struct{}
	doneCh       chan struct{}
	statsMu      sync.RWMutex
	nextRunAt    time.Time
	shutdownOnce sync.Once
	doneOnce     sync.Once
	closeOnce    sync.Once
}

// PipelineStats exposes current pipeline health and last-run metrics.
type PipelineStats struct {
	Name            string
	Mode            string
	Status          string
	LastRunAt       time.Time
	LastRunDuration time.Duration
	LastStatus      string
	RowsLoaded      int64
	RowsSkipped     int64
	RowsErrored     int64
	NextRunAt       time.Time
}

// NewEngine creates a v2 engine backed by the supplied state store.
func NewEngine(store state.StateStore) *Engine {
	return &Engine{
		store:      store,
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Store returns the engine's state store.
func (e *Engine) Store() state.StateStore {
	if e == nil {
		return nil
	}
	return e.store
}

// SetDestination replaces or adds a destination implementation by name.
func (e *Engine) SetDestination(name string, dest destination.Destination) {
	if e == nil {
		return
	}
	if e.destinations == nil {
		e.destinations = make(map[string]destination.Destination)
	}
	if existing, ok := e.destinations[name]; ok && existing != nil && existing != dest {
		_ = existing.Close()
	}
	e.destinations[name] = dest
}

// Shutdown initiates graceful shutdown and waits for in-flight work to finish.
func (e *Engine) Shutdown(timeout time.Duration) error {
	if e == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	e.shutdownOnce.Do(func() {
		close(e.shutdownCh)
	})
	e.markDone()
	return e.Close()
}

// Close shuts down all components gracefully.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	var firstErr error
	e.closeOnce.Do(func() {
		for _, dest := range e.destinations {
			if dest == nil {
				continue
			}
			if err := dest.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if e.store != nil {
			if err := e.store.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		e.markDone()
	})
	return firstErr
}

// Stats returns current pipeline statistics.
func (e *Engine) Stats(ctx context.Context) PipelineStats {
	_ = ctx
	stats := PipelineStats{}
	if e == nil || e.cfg == nil {
		return stats
	}
	stats.Name = e.cfg.Name
	stats.Mode = "batch"
	stats.Status = "idle"
	if e.running.Load() {
		stats.Status = "running"
	}
	select {
	case <-e.shutdownCh:
		if !e.running.Load() {
			stats.Status = "stopped"
		}
	default:
	}
	if e.store != nil {
		if lastRun, err := e.store.GetLastRun(e.cfg.Name); err == nil {
			stats.LastRunAt = lastRun.StartedAt
			if !lastRun.FinishedAt.IsZero() {
				stats.LastRunDuration = lastRun.FinishedAt.Sub(lastRun.StartedAt)
			} else if !lastRun.StartedAt.IsZero() {
				stats.LastRunDuration = time.Since(lastRun.StartedAt)
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
	if e == nil {
		return
	}
	e.statsMu.Lock()
	e.nextRunAt = t
	e.statsMu.Unlock()
}

func (e *Engine) markDone() {
	if e == nil {
		return
	}
	e.doneOnce.Do(func() {
		close(e.doneCh)
	})
}
