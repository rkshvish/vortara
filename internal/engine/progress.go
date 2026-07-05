// Package engine coordinates extraction and loading for pipeline runs.
package engine

import (
	"log/slog"
	"sync"
	"time"
)

// Metrics tracks in-flight run counters.
type Metrics struct {
	RowsExtracted  int64
	RowsLoaded     int64
	RowsSkipped    int64
	RowsErrored    int64
	BytesProcessed int64
	StartTime      time.Time
	LastUpdateAt   time.Time
}

// Duration returns elapsed run time.
func (m Metrics) Duration() time.Duration {
	if m.StartTime.IsZero() {
		return 0
	}
	return time.Since(m.StartTime)
}

// RowsPerSec returns the current load throughput.
func (m Metrics) RowsPerSec() float64 {
	seconds := m.Duration().Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(m.RowsLoaded) / seconds
}

// ErrorRate returns errored rows divided by extracted rows.
func (m Metrics) ErrorRate() float64 {
	if m.RowsExtracted == 0 {
		return 0
	}
	return float64(m.RowsErrored) / float64(m.RowsExtracted)
}

// Progress tracks per-run progress and logs it periodically.
type Progress struct {
	mu       sync.Mutex
	metrics  Metrics
	logger   *slog.Logger
	pipeline string
	ticker   *time.Ticker
	stopCh   chan struct{}
	once     sync.Once
	started  sync.Once
}

// NewProgress creates a Progress tracker.
func NewProgress(pipeline string, l *slog.Logger, interval time.Duration) *Progress {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if l == nil {
		l = slog.Default()
	}
	now := time.Now()
	return &Progress{
		logger:   l,
		pipeline: pipeline,
		ticker:   time.NewTicker(interval),
		stopCh:   make(chan struct{}),
		metrics: Metrics{
			StartTime:    now,
			LastUpdateAt: now,
		},
	}
}

// Start begins periodic logging.
func (p *Progress) Start() {
	if p == nil {
		return
	}
	p.started.Do(func() {
		go func() {
			for {
				select {
				case <-p.stopCh:
					return
				case <-p.ticker.C:
					p.logProgress("pipeline progress", p.Snapshot())
				}
			}
		}()
	})
}

// Stop halts periodic logging and prints the final summary.
func (p *Progress) Stop() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.ticker != nil {
			p.ticker.Stop()
		}
		close(p.stopCh)
		p.logFinal(p.Snapshot())
	})
}

// RecordExtracted adds n to the extracted row count.
func (p *Progress) RecordExtracted(n int64) {
	if p == nil || n == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics.RowsExtracted += n
	p.metrics.LastUpdateAt = time.Now()
}

// RecordLoaded adds to the loaded, skipped, and errored counters.
func (p *Progress) RecordLoaded(loaded, skipped, errored int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics.RowsLoaded += loaded
	p.metrics.RowsSkipped += skipped
	p.metrics.RowsErrored += errored
	p.metrics.LastUpdateAt = time.Now()
}

// Snapshot returns a copy of current metrics.
func (p *Progress) Snapshot() Metrics {
	if p == nil {
		return Metrics{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.metrics
}

func (p *Progress) logProgress(msg string, m Metrics) {
	p.logger.Info(msg,
		slog.String("pipeline", p.pipeline),
		slog.Int64("rows_extracted", m.RowsExtracted),
		slog.Int64("rows_loaded", m.RowsLoaded),
		slog.Int64("rows_skipped", m.RowsSkipped),
		slog.Int64("rows_errored", m.RowsErrored),
		slog.Float64("rows_per_sec", m.RowsPerSec()),
		slog.Duration("elapsed", m.Duration()),
	)
}

func (p *Progress) logFinal(m Metrics) {
	p.logger.Info("run complete",
		slog.String("pipeline", p.pipeline),
		slog.Int64("rows_extracted", m.RowsExtracted),
		slog.Int64("rows_loaded", m.RowsLoaded),
		slog.Int64("rows_skipped", m.RowsSkipped),
		slog.Int64("rows_errored", m.RowsErrored),
		slog.Float64("rows_per_sec", m.RowsPerSec()),
		slog.Float64("error_rate_pct", m.ErrorRate()*100),
		slog.Duration("total_duration", m.Duration()),
	)
}
