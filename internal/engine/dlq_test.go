package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

type failingDestination struct{}

func (f *failingDestination) Connect(ctx context.Context, cfg conncfg.DestinationConfig) error {
	return nil
}

func (f *failingDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (destpkg.LoadResult, error) {
	return destpkg.LoadResult{}, errors.New("destination exploded")
}

func (f *failingDestination) Close() error { return nil }

func TestDLQ_CapturesFailedRows(t *testing.T) {
	registerV2Mocks()
	dlqPath := filepath.Join(t.TempDir(), "test.dlq.jsonl")
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
			row.NewRow("src", "pipe", "pk2", map[string]interface{}{"status": "lost"}, time.Now().UTC()),
		},
	}

	cfg := &pipeline.PipelineConfig{
		Name: "dlq-test",
		Source: pipeline.SourceConfig{
			Type:      "v2test-batch",
			Table:     "deals",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []pipeline.DestinationConfig{{Type: "v2test-dest"}},
		Settings: pipeline.SettingsConfig{
			OnError:     "dlq",
			DLQPath:     dlqPath,
			Concurrency: pipeline.ConcurrencySettings{Workers: 1, BatchSize: 10},
		},
	}

	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}

	dest := &failingDestination{}
	// With on_error: dlq, a run whose rows all fail should still succeed.
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch"); err != nil {
		t.Fatalf("runBatchOnce() error = %v, want nil (dlq absorbs failures)", err)
	}

	f, err := os.Open(dlqPath)
	if err != nil {
		t.Fatalf("open dlq file: %v", err)
	}
	defer f.Close()
	var records []DLQRecord
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec DLQRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			t.Fatalf("bad dlq line: %v", err)
		}
		records = append(records, rec)
	}
	if len(records) != 2 {
		t.Fatalf("dlq records = %d, want 2", len(records))
	}
	if records[0].Pipeline != "dlq-test" || records[0].Error != "destination exploded" {
		t.Fatalf("record = %+v", records[0])
	}
	if records[0].Data["status"] == nil {
		t.Fatalf("record data missing: %+v", records[0])
	}
}

func TestDLQ_DisabledForSkipMode(t *testing.T) {
	w, err := newDLQWriter(&pipeline.PipelineConfig{Name: "x", Settings: pipeline.SettingsConfig{OnError: "skip"}})
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}
	if w.Enabled() {
		t.Fatal("dlq should be disabled for on_error: skip")
	}
	// Nil-safety.
	if err := w.Write(row.Row{}, errors.New("x")); err != nil {
		t.Fatalf("Write on disabled writer = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close on disabled writer = %v", err)
	}
}
