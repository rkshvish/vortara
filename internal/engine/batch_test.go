package engine

import (
	"context"
	"sync/atomic"
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

// slowBatchSource blocks on each row send so max_runtime kicks in mid-extract.
type slowBatchSource struct {
	rows     []row.Row
	delayPer time.Duration
	emitted  atomic.Int32
}

func (s *slowBatchSource) Connect(_ context.Context, _ conncfg.SourceConfig) error { return nil }
func (s *slowBatchSource) GetWatermarkColumn() string                               { return "updated_at" }
func (s *slowBatchSource) Close() error                                             { return nil }

func (s *slowBatchSource) Extract(ctx context.Context, _, _ time.Time, out chan<- row.Row) error {
	defer close(out)
	for _, r := range s.rows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delayPer):
		}
		select {
		case out <- r:
			s.emitted.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// TestBatchRun_MaxRuntime verifies that a run bounded by max_runtime:
//   - terminates without error (timeout is not treated as a hard failure),
//   - saves the high watermark of rows delivered before the cutoff so the
//     next run resumes from where this one stopped.
func TestBatchRun_MaxRuntime(t *testing.T) {
	registerV2Mocks()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := &slowBatchSource{
		delayPer: 30 * time.Millisecond,
	}
	// 10 rows spaced 1 hour apart in watermark time.
	for i := 0; i < 10; i++ {
		src.rows = append(src.rows, row.NewRow(
			"src", "rt-test", "pk"+string(rune('a'+i)),
			map[string]interface{}{"n": i},
			base.Add(time.Duration(i)*time.Hour),
		))
	}

	cfg := &pipeline.PipelineConfig{
		Name: "rt-test",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings: pipeline.SettingsConfig{
			Limits:      pipeline.LimitsSettings{MaxRuntime: "80ms"},
			Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 2},
		},
	}

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(nil)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	dest := &mockV2Destination{}

	err = eng.runBatchOnce(context.Background(), cfg, src, proc, rt, []destpkg.Destination{dest}, "rt-src")
	// A max_runtime cancellation is not returned as an error.
	if err != nil {
		t.Fatalf("runBatchOnce() error = %v, want nil (timeout is not a hard error)", err)
	}

	// Some rows must have been delivered.
	delivered := len(dest.rows)
	if delivered == 0 {
		t.Fatal("no rows delivered before max_runtime; test timing is too tight")
	}
	if delivered == 10 {
		t.Fatal("all 10 rows delivered; max_runtime did not fire (test timing is too loose)")
	}

	// Watermark must be saved at the last delivered row's timestamp so the
	// next run can resume from there.
	ctx := context.Background()
	wm, err := store.GetWatermark(ctx, cfg.Name, "rt-src")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if wm.IsZero() {
		t.Fatal("watermark is zero after timed-out run; high-watermark was not saved")
	}
	// The saved watermark must be <= the last delivered row's timestamp.
	lastDelivered := dest.rows[len(dest.rows)-1]
	if wm.After(lastDelivered.Watermark) {
		t.Fatalf("saved watermark %v is after the last delivered row's watermark %v", wm, lastDelivered.Watermark)
	}
}

// TestBatchRun_WatermarkAdvances verifies that a clean (uncapped) run saves
// the extraction-window end as the new watermark for the next run.
func TestBatchRun_WatermarkAdvances(t *testing.T) {
	ctx := context.Background()
	registerV2Mocks()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []row.Row{
		row.NewRow("src", "wm-test", "pk1", map[string]interface{}{"x": 1}, base),
		row.NewRow("src", "wm-test", "pk2", map[string]interface{}{"x": 2}, base.Add(time.Hour)),
	}

	cfg := &pipeline.PipelineConfig{
		Name: "wm-test",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "t",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10}},
	}

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	rt, _ := router.New(cfg.Destinations)
	proc, _ := steps.New(nil)

	// Verify watermark is zero before first run.
	wm, _ := store.GetWatermark(ctx, cfg.Name, "wm-src")
	if !wm.IsZero() {
		t.Fatalf("initial watermark = %v, want zero", wm)
	}

	dest := &mockV2Destination{}
	currentV2BatchSource = &mockV2BatchSource{rows: rows}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "wm-src"); err != nil {
		t.Fatalf("runBatchOnce() error = %v", err)
	}
	if n := len(dest.rows); n != 2 {
		t.Fatalf("delivered %d rows, want 2", n)
	}

	// Watermark must now be non-zero.
	wm, err := store.GetWatermark(ctx, cfg.Name, "wm-src")
	if err != nil || wm.IsZero() {
		t.Fatalf("watermark after clean run = %v err %v, want non-zero", wm, err)
	}
}
