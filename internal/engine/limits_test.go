package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	destpkg "github.com/rkshvish/vortara/internal/connector/destination"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/internal/steps"
	pipeline "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

// TestLimits_MaxRowsResumesFromState verifies the limit+state contract:
// a run capped by max_rows saves the watermark of delivered rows, and the
// next run resumes from there instead of re-extracting.
func TestLimits_MaxRowsResumesFromState(t *testing.T) {
	ctx := context.Background()
	registerV2Mocks()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	allRows := make([]row.Row, 5)
	for i := range allRows {
		allRows[i] = row.NewRow("src", "limit-test", "pk"+string(rune('a'+i)),
			map[string]interface{}{"n": i}, base.Add(time.Duration(i)*time.Hour))
	}

	cfg := &pipeline.PipelineConfig{
		Name: "limit-test",
		Source: pipeline.SourceConfig{
			Type: "v2test-batch", Table: "t", Watermark: "updated_at", BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings: pipeline.SettingsConfig{
			Limits:      pipeline.LimitsSettings{MaxRows: 3},
			Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10},
		},
	}

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}

	// Run 1: capped at 3 rows.
	dest1 := &mockV2Destination{}
	currentV2BatchSource = &mockV2BatchSource{rows: allRows}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest1}, "src1"); err != nil {
		t.Fatalf("run 1 error = %v", err)
	}
	if got := len(dest1.rows); got != 3 {
		t.Fatalf("run 1 delivered %d rows, want 3 (max_rows cap)", got)
	}
	wm, err := store.GetWatermark(ctx, cfg.Name, "src1")
	if err != nil || wm.IsZero() {
		t.Fatalf("watermark after capped run = %v err %v, want the last delivered row's watermark", wm, err)
	}

	// Run 2: the source only returns rows newer than the saved watermark —
	// simulate that filtering as a real DB source would.
	var remaining []row.Row
	for _, r := range allRows {
		if r.Watermark.After(wm) {
			remaining = append(remaining, r)
		}
	}
	if len(remaining) != 2 {
		t.Fatalf("remaining after watermark %v = %d rows, want 2", wm, len(remaining))
	}
	dest2 := &mockV2Destination{}
	currentV2BatchSource = &mockV2BatchSource{rows: remaining}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest2}, "src1"); err != nil {
		t.Fatalf("run 2 error = %v", err)
	}
	if got := len(dest2.rows); got != 2 {
		t.Fatalf("run 2 delivered %d rows, want exactly the 2 remaining", got)
	}
}

// TestSnapshot_NoWatermarkSaved verifies watermark: none never advances the
// cursor: every run is a full extract and the stored watermark stays zero.
func TestSnapshot_NoWatermarkSaved(t *testing.T) {
	ctx := context.Background()
	registerV2Mocks()
	rows := []row.Row{
		row.NewRow("src", "snap-test", "pk1", map[string]interface{}{"n": 1}, time.Time{}),
		row.NewRow("src", "snap-test", "pk2", map[string]interface{}{"n": 2}, time.Time{}),
	}
	cfg := &pipeline.PipelineConfig{
		Name: "snap-test",
		Source: pipeline.SourceConfig{
			Type: "v2test-batch", Table: "countries", Watermark: "none", BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     pipeline.SettingsConfig{Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10}},
	}

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}

	for run := 1; run <= 2; run++ {
		dest := &mockV2Destination{}
		currentV2BatchSource = &mockV2BatchSource{rows: rows}
		if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "src1"); err != nil {
			t.Fatalf("run %d error = %v", run, err)
		}
		if got := len(dest.rows); got != 2 {
			t.Fatalf("run %d delivered %d rows, want full snapshot of 2", run, got)
		}
	}
	wm, err := store.GetWatermark(ctx, cfg.Name, "src1")
	if err != nil || !wm.IsZero() {
		t.Fatalf("watermark after snapshot runs = %v err %v, want zero (never saved)", wm, err)
	}
}

// mockNumericSource implements BatchSource + NumericCursorSource.
type mockNumericSource struct {
	rows []row.Row // ordered by cursor; Data["id"] is the cursor
}

func (m *mockNumericSource) Connect(ctx context.Context, cfg conncfg.SourceConfig) error { return nil }
func (m *mockNumericSource) GetWatermarkColumn() string                                  { return "id" }
func (m *mockNumericSource) Close() error                                                { return nil }
func (m *mockNumericSource) Extract(ctx context.Context, wm, intervalEnd time.Time, out chan<- row.Row) error {
	close(out)
	return errors.New("time-window Extract must not be called for numeric cursors")
}
func (m *mockNumericSource) CursorKind(ctx context.Context) (string, error) { return "int", nil }
func (m *mockNumericSource) ExtractNumeric(ctx context.Context, cursor, limit int64, out chan<- row.Row) (int64, error) {
	defer close(out)
	last := cursor
	emitted := int64(0)
	for _, r := range m.rows {
		id := r.Data["id"].(int64)
		if id <= cursor {
			continue
		}
		if limit > 0 && emitted >= limit {
			break
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return last, ctx.Err()
		}
		last = id
		emitted++
	}
	return last, nil
}

// TestNumericCursor_EnginePersistsAndResumes verifies the engine routes
// integer-cursor sources through ExtractNumeric, persists the cursor, and
// resumes: capped run delivers the first chunk, next run the remainder.
func TestNumericCursor_EnginePersistsAndResumes(t *testing.T) {
	ctx := context.Background()
	registerV2Mocks()
	src := &mockNumericSource{}
	for i := int64(1); i <= 5; i++ {
		src.rows = append(src.rows, row.Row{
			ID: fmt.Sprintf("r%d", i), PrimaryKey: fmt.Sprintf("id=%d", i),
			Data: map[string]interface{}{"id": i, "name": fmt.Sprintf("n%d", i)},
		})
	}
	cfg := &pipeline.PipelineConfig{
		Name:         "numcur-test",
		Source:       pipeline.SourceConfig{Type: "v2test-batch", Table: "t", Watermark: "id", BatchSize: 10},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings: pipeline.SettingsConfig{
			Limits:      pipeline.LimitsSettings{MaxRows: 3},
			Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10},
		},
	}
	store := state.NewMemoryStore()
	eng := NewEngine(store)
	rt, _ := router.New(cfg.Destinations)
	proc, _ := steps.New(cfg.Transform)

	// Run 1: capped at 3 → cursor lands on id 3.
	dest1 := &mockV2Destination{}
	if err := eng.runBatchOnce(context.Background(), cfg, src, proc, rt, []destpkg.Destination{dest1}, "numsrc"); err != nil {
		t.Fatalf("run 1 error = %v", err)
	}
	if got := len(dest1.rows); got != 3 {
		t.Fatalf("run 1 delivered %d, want 3", got)
	}
	cur, err := store.GetNumericWatermark(ctx, cfg.Name, "numsrc")
	if err != nil || cur != 3 {
		t.Fatalf("cursor after run 1 = %d err %v, want 3", cur, err)
	}

	// Run 2: exactly the remaining 2 rows, cursor advances to 5.
	dest2 := &mockV2Destination{}
	if err := eng.runBatchOnce(context.Background(), cfg, src, proc, rt, []destpkg.Destination{dest2}, "numsrc"); err != nil {
		t.Fatalf("run 2 error = %v", err)
	}
	if got := len(dest2.rows); got != 2 {
		t.Fatalf("run 2 delivered %d, want 2", got)
	}
	cur, _ = store.GetNumericWatermark(ctx, cfg.Name, "numsrc")
	if cur != 5 {
		t.Fatalf("cursor after run 2 = %d, want 5", cur)
	}
	// The time watermark must remain untouched.
	wm, _ := store.GetWatermark(ctx, cfg.Name, "numsrc")
	if !wm.IsZero() {
		t.Fatalf("time watermark = %v, want zero for numeric pipelines", wm)
	}
}
