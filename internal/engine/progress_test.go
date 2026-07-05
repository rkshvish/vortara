package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordHandler) countMessage(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, r := range h.records {
		if r.Message == msg {
			count++
		}
	}
	return count
}

func TestProgress_RecordExtracted(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), time.Second)
	p.RecordExtracted(100)
	if got := p.Snapshot().RowsExtracted; got != 100 {
		t.Fatalf("RowsExtracted = %d, want 100", got)
	}
}

func TestProgress_RecordLoaded(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), time.Second)
	p.RecordLoaded(80, 15, 5)
	got := p.Snapshot()
	if got.RowsLoaded != 80 || got.RowsSkipped != 15 || got.RowsErrored != 5 {
		t.Fatalf("unexpected metrics: %+v", got)
	}
}

func TestProgress_RowsPerSec(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), time.Second)
	p.RecordLoaded(100, 0, 0)
	time.Sleep(100 * time.Millisecond)
	if got := p.Snapshot().RowsPerSec(); got <= 0 {
		t.Fatalf("RowsPerSec = %f, want > 0", got)
	}
}

func TestProgress_ErrorRate(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), time.Second)
	p.RecordExtracted(100)
	p.RecordLoaded(90, 0, 10)
	if got := p.Snapshot().ErrorRate(); got != 0.10 {
		t.Fatalf("ErrorRate = %f, want 0.10", got)
	}
}

func TestProgress_Concurrent(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.RecordExtracted(10)
		}()
	}
	wg.Wait()
	if got := p.Snapshot().RowsExtracted; got != 100 {
		t.Fatalf("RowsExtracted = %d, want 100", got)
	}
}

func TestProgress_Stop_DoesNotPanic(t *testing.T) {
	p := NewProgress("pipe", slog.New(&recordHandler{}), 10*time.Millisecond)
	p.Start()
	p.Stop()
	p.Stop()
}

func TestProgress_PeriodicLog(t *testing.T) {
	h := &recordHandler{}
	p := NewProgress("pipe", slog.New(h), 10*time.Millisecond)
	p.Start()
	time.Sleep(50 * time.Millisecond)
	p.Stop()
	if got := h.countMessage("pipeline progress"); got < 3 {
		t.Fatalf("progress log count = %d, want >= 3", got)
	}
}
