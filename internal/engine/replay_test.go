package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	destpkg "github.com/rkshvish/vortaraos/internal/connector/destination"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/router"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/internal/steps"
	v2 "github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// replayDest is the swappable destination used by the replay tests.
var currentReplayDest destpkg.Destination

func registerReplayMock() {
	registry.RegisterDestination("replay-dest", func() any { return currentReplayDest })
}

var replayMockOnce = false

func replayCfg(dlqPath string) *v2.PipelineConfig {
	return &v2.PipelineConfig{
		Name: "replay-test",
		Source: v2.SourceConfig{
			Type:      "v2test-batch",
			Table:     "deals",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []v2.DestinationConfig{{Type: "replay-dest"}},
		Settings: v2.SettingsConfig{
			OnError:     "dlq",
			DLQPath:     dlqPath,
			Concurrency: v2.ConcurrencySettings{Workers: 1, BatchSize: 10},
		},
	}
}

func seedDLQ(t *testing.T, cfg *v2.PipelineConfig, store state.StateStore) {
	t.Helper()
	registerV2Mocks()
	if !replayMockOnce {
		registerReplayMock()
		replayMockOnce = true
	}
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "replay-test", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
			row.NewRow("src", "replay-test", "pk2", map[string]interface{}{"status": "lost"}, time.Now().UTC()),
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
	eng := NewEngine(store)
	failing := &failingDestination{}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{failing}, "v2test-batch"); err != nil {
		t.Fatalf("seed run error = %v", err)
	}
	recs, err := ReadDLQRecords(cfg.Settings.DLQPath)
	if err != nil || len(recs) != 2 {
		t.Fatalf("seed dlq = %d records, err %v; want 2", len(recs), err)
	}
}

func TestReplayDLQ_Success(t *testing.T) {
	dlqPath := filepath.Join(t.TempDir(), "replay.dlq.jsonl")
	cfg := replayCfg(dlqPath)
	store := state.NewMemoryStore()
	seedDLQ(t, cfg, store)

	good := &mockV2Destination{}
	currentReplayDest = good

	eng := NewEngine(store)
	res, err := eng.ReplayDLQ(context.Background(), cfg, "")
	if err != nil {
		t.Fatalf("ReplayDLQ() error = %v", err)
	}
	if res.Read != 2 || res.Replayed != 2 || res.Failed != 0 {
		t.Fatalf("result = %+v, want read=2 replayed=2 failed=0", res)
	}
	if got := len(good.rows); got != 2 {
		t.Fatalf("destination rows = %d, want 2", got)
	}
	if _, err := os.Stat(dlqPath); !os.IsNotExist(err) {
		t.Fatalf("dlq file should be removed after full replay, stat err = %v", err)
	}
}

func TestReplayDLQ_StillFailing(t *testing.T) {
	dlqPath := filepath.Join(t.TempDir(), "replay.dlq.jsonl")
	cfg := replayCfg(dlqPath)
	store := state.NewMemoryStore()
	seedDLQ(t, cfg, store)

	currentReplayDest = &failingDestination{}

	eng := NewEngine(store)
	res, err := eng.ReplayDLQ(context.Background(), cfg, "")
	if err != nil {
		t.Fatalf("ReplayDLQ() error = %v", err)
	}
	if res.Read != 2 || res.Replayed != 0 || res.Failed != 2 {
		t.Fatalf("result = %+v, want read=2 replayed=0 failed=2", res)
	}
	recs, err := ReadDLQRecords(dlqPath)
	if err != nil {
		t.Fatalf("re-read dlq: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("remaining records = %d, want 2", len(recs))
	}
	if recs[0].Error != "destination exploded" {
		t.Fatalf("record error = %q, want refreshed error", recs[0].Error)
	}
}

func TestReplayDLQ_MissingFile(t *testing.T) {
	cfg := replayCfg(filepath.Join(t.TempDir(), "nope.dlq.jsonl"))
	eng := NewEngine(state.NewMemoryStore())
	if _, err := eng.ReplayDLQ(context.Background(), cfg, ""); err == nil {
		t.Fatal("ReplayDLQ() with missing file should error")
	}
}
