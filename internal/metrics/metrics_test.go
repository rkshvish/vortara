package metrics

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/state"
)

func TestRecordRun_WritesPromFile(t *testing.T) {
	dir := t.TempDir()
	rec := New(dir)

	stats := state.RunStats{
		RowsExtracted: 100,
		RowsLoaded:    80,
		RowsSkipped:   15,
		RowsErrored:   5,
		Creates:       30,
		Updates:       50,
		Deletes:       0,
		Status:        "success",
	}

	if err := rec.RecordRun("my-sync", stats, 2500*time.Millisecond); err != nil {
		t.Fatalf("RecordRun: %v", err)
	}

	promFile := filepath.Join(dir, "my_sync.prom")
	data, err := os.ReadFile(promFile)
	if err != nil {
		t.Fatalf("expected .prom file at %s: %v", promFile, err)
	}
	content := string(data)

	checks := []string{
		`vortara_run_total{sync="my-sync",status="success"} 1`,
		`vortara_run_duration_seconds{sync="my-sync"} 2.5`,
		`vortara_source_rows_read_total{sync="my-sync"} 100`,
		`vortara_decisions_total{sync="my-sync",decision="create"} 30`,
		`vortara_decisions_total{sync="my-sync",decision="update"} 50`,
		`vortara_decisions_total{sync="my-sync",decision="skip"} 15`,
		`vortara_delivery_failures_total{sync="my-sync"} 5`,
	}
	for _, want := range checks {
		if !strings.Contains(content, want) {
			t.Errorf("missing line %q in:\n%s", want, content)
		}
	}
}

func TestRecordRun_Idempotent(t *testing.T) {
	dir := t.TempDir()
	rec := New(dir)
	stats := state.RunStats{Status: "success", RowsExtracted: 10, Creates: 10}

	for i := 0; i < 3; i++ {
		if err := rec.RecordRun("test-sync", stats, time.Second); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	entries, _ := os.ReadDir(dir)
	var promFiles int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".prom") {
			promFiles++
		}
	}
	if promFiles != 1 {
		t.Errorf("expected 1 .prom file, found %d", promFiles)
	}
}

func TestRecordRun_CreatesDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "metrics", "subdir")
	rec := New(dir)

	stats := state.RunStats{Status: "success"}
	if err := rec.RecordRun("s", stats, time.Millisecond); err != nil {
		t.Fatalf("should create dir: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("dir not created: %v", err)
	}
}
