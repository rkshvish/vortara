package engine

import (
	"context"
	"testing"
	"time"

	destpkg "github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/internal/steps"
	pipeline "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

// TestRunBatchCron_InvalidCron verifies that a malformed cron expression
// returns an error without blocking.
func TestRunBatchCron_InvalidCron(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 0, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{}

	cfg := &pipeline.PipelineConfig{
		Name: "cron-invalid",
		Cron: "not-a-cron",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
	}
	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(nil)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	err = eng.runBatchCron(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch")
	if err == nil {
		t.Fatal("expected error for invalid cron expression, got nil")
	}
}

// TestRunBatchCron_CancelStops verifies that cancelling the context causes
// runBatchCron to return context.Canceled without executing any run.
func TestRunBatchCron_CancelStops(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 0, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{}

	// Schedule far in the future — the cron should never fire.
	cfg := &pipeline.PipelineConfig{
		Name: "cron-cancel",
		Cron: "0 0 1 1 *", // 00:00 on 1 Jan — will not fire in the test
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{Concurrency: pipeline.ConcurrencySettings{Workers: 1}},
	}
	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(nil)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- eng.runBatchCron(ctx, cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch")
	}()

	// Give the goroutine a moment to start, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil || err.Error() != context.Canceled.Error() {
			t.Fatalf("runBatchCron() = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runBatchCron did not return after context cancel")
	}

	// The cron never fired, so no rows should have been delivered.
	if n := len(dest.rows); n != 0 {
		t.Fatalf("expected 0 rows delivered, got %d", n)
	}
}

// TestRunBatch_SelectsCronPath verifies that runBatch delegates to runBatchCron
// when cfg.Cron is non-empty.
func TestRunBatch_SelectsCronPath(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 0, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{}

	cfg := &pipeline.PipelineConfig{
		Name: "cron-path-select",
		Cron: "not-valid-cron",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
	}
	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(nil)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	// An invalid cron expression ensures runBatch entered the cron path (not
	// runBatchOnce, which would succeed on an empty source).
	err = eng.runBatch(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch")
	if err == nil {
		t.Fatal("expected error from invalid cron, got nil")
	}
}

// TestRunBatch_SelectsOncePath verifies that runBatch delegates to runBatchOnce
// when cfg.Cron is empty.
func TestRunBatch_SelectsOncePath(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 1, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"x": 1}, time.Now().UTC()),
		},
	}
	cfg := &pipeline.PipelineConfig{
		Name: "once-path-select",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10}},
	}
	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(nil)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	if err := eng.runBatch(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch"); err != nil {
		t.Fatalf("runBatch() error = %v", err)
	}
	<-dest.done
	if n := len(dest.rows); n != 1 {
		t.Fatalf("want 1 row delivered, got %d", n)
	}
}
