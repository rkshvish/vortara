package engine

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	destpkg "github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/internal/steps"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	pipeline "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

// flakyDestination fails the first failUntil Load calls, then succeeds.
type flakyDestination struct {
	mu        sync.Mutex
	calls     int
	failUntil int
}

func (f *flakyDestination) Connect(ctx context.Context, cfg conncfg.DestinationConfig) error {
	return nil
}

func (f *flakyDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (destpkg.LoadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failUntil {
		return destpkg.LoadResult{}, errors.New("transient failure")
	}
	return destpkg.LoadResult{Loaded: len(rows)}, nil
}

func (f *flakyDestination) Close() error { return nil }

func TestDispatchWithPolicy_RetrySucceeds(t *testing.T) {
	cfg := &pipeline.PipelineConfig{
		Name:         "retry-test",
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{OnError: "retry"},
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	eng := NewEngine(state.NewMemoryStore())
	dest := &flakyDestination{failUntil: 2} // fails twice, succeeds on 3rd attempt

	r := row.NewRow("src", "retry-test", "pk1", map[string]interface{}{"x": 1}, time.Now())
	_, ok, err := eng.dispatchWithPolicy(context.Background(), cfg, rt, []destpkg.Destination{dest}, r)
	if err != nil || !ok {
		t.Fatalf("dispatchWithPolicy() = ok=%v err=%v, want success after retries", ok, err)
	}
	if dest.calls != 3 {
		t.Fatalf("calls = %d, want 3", dest.calls)
	}
}

func TestDispatchWithPolicy_RetryExhausted(t *testing.T) {
	cfg := &pipeline.PipelineConfig{
		Name:         "retry-test",
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{OnError: "retry"},
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	eng := NewEngine(state.NewMemoryStore())
	dest := &flakyDestination{failUntil: 100}

	r := row.NewRow("src", "retry-test", "pk1", map[string]interface{}{"x": 1}, time.Now())
	_, _, err = eng.dispatchWithPolicy(context.Background(), cfg, rt, []destpkg.Destination{dest}, r)
	if err == nil {
		t.Fatal("dispatchWithPolicy() = nil, want error after exhausted retries")
	}
	if dest.calls != dispatchRetryAttempts {
		t.Fatalf("calls = %d, want %d", dest.calls, dispatchRetryAttempts)
	}
}

func TestDispatchWithPolicy_SkipModeNoRetry(t *testing.T) {
	cfg := &pipeline.PipelineConfig{
		Name:         "skip-test",
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{OnError: "skip"},
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	eng := NewEngine(state.NewMemoryStore())
	dest := &flakyDestination{failUntil: 100}

	r := row.NewRow("src", "skip-test", "pk1", map[string]interface{}{"x": 1}, time.Now())
	_, _, err = eng.dispatchWithPolicy(context.Background(), cfg, rt, []destpkg.Destination{dest}, r)
	if err == nil {
		t.Fatal("want error")
	}
	if dest.calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry in skip mode)", dest.calls)
	}
}

func TestStreamingDLQ_AcksFailedRows(t *testing.T) {
	registerV2Mocks()
	dlqPath := filepath.Join(t.TempDir(), "stream.dlq.jsonl")
	stream := &mockV2StreamSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"x": 1}, time.Now()),
		},
	}
	cfg := &pipeline.PipelineConfig{
		Name:         "stream-dlq",
		Source:       pipeline.SourceConfig{Type: "v2test-stream", Topic: "events", GroupID: "g"},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings: pipeline.SettingsConfig{
			OnError:     "dlq",
			DLQPath:     dlqPath,
			Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 1},
		},
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}

	eng := NewEngine(state.NewMemoryStore())
	dest := &failingDestination{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- eng.runStreaming(ctx, cfg, stream, proc, rt, []destpkg.Destination{dest}, "v2test-stream")
	}()

	// Wait until the failed row is acked (DLQ mode acks so the stream advances).
	deadline := time.After(5 * time.Second)
	for {
		stream.mu.Lock()
		acked := len(stream.acks)
		stream.mu.Unlock()
		if acked == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for DLQ ack")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("runStreaming() error = %v", err)
	}

	f, err := os.Open(dlqPath)
	if err != nil {
		t.Fatalf("open dlq: %v", err)
	}
	defer f.Close()
	lines := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines++
	}
	if lines != 1 {
		t.Fatalf("dlq lines = %d, want 1", lines)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.nacks) != 0 {
		t.Fatalf("nacks = %v, want none in dlq mode", stream.nacks)
	}
}
