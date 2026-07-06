package artifacts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rkshvish/vortara/internal/state"
)

func TestWriter_Disabled(t *testing.T) {
	w := New(Config{BasePath: "", SyncName: "s", RunID: 1})
	w.RecordCreate("k1", map[string]any{"x": 1})
	w.RecordSkip("k2")
	if err := w.Flush("success"); err != nil {
		t.Fatalf("Flush on disabled writer: %v", err)
	}
}

func TestWriter_WritesFiles(t *testing.T) {
	dir := t.TempDir()
	w := New(Config{BasePath: dir, SyncName: "my-sync", RunID: 42, MaxSamples: 5})

	w.RecordCreate("id=1", map[string]any{"email": "a@b.com"})
	w.RecordCreate("id=2", map[string]any{"email": "c@d.com"})
	w.RecordUpdate("id=3", map[string]any{"email": "e@f.com"}, []string{"email", "name"})
	w.RecordSkip("id=4")
	w.RecordDecision(&state.DecisionEvent{
		SyncName: "my-sync", EntityKey: "id=1", Decision: "create",
	})

	if err := w.Flush("success"); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	runDir := filepath.Join(dir, "my-sync", "run-42")

	// summary.json
	sumData, err := os.ReadFile(filepath.Join(runDir, "summary.json"))
	if err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	var sum Summary
	if err := json.Unmarshal(sumData, &sum); err != nil {
		t.Fatalf("parse summary: %v", err)
	}
	if sum.SyncName != "my-sync" || sum.RunID != 42 || sum.Creates != 2 || sum.Updates != 1 || sum.Skips != 1 {
		t.Fatalf("unexpected summary: %+v", sum)
	}
	if sum.Status != "success" {
		t.Fatalf("expected status=success, got %s", sum.Status)
	}
	if sum.DurationMS < 0 {
		t.Fatal("negative duration")
	}

	// decisions.jsonl
	decData, err := os.ReadFile(filepath.Join(runDir, "decisions.jsonl"))
	if err != nil {
		t.Fatalf("decisions.jsonl: %v", err)
	}
	if len(decData) == 0 {
		t.Fatal("decisions.jsonl is empty")
	}

	// field-diff-summary.json
	fdData, err := os.ReadFile(filepath.Join(runDir, "field-diff-summary.json"))
	if err != nil {
		t.Fatalf("field-diff-summary.json: %v", err)
	}
	var fds []fieldDiffEntry
	if err := json.Unmarshal(fdData, &fds); err != nil {
		t.Fatalf("parse field-diff: %v", err)
	}
	if len(fds) != 2 {
		t.Fatalf("expected 2 field entries, got %d", len(fds))
	}

	// samples/creates.jsonl
	crData, err := os.ReadFile(filepath.Join(runDir, "samples", "creates.jsonl"))
	if err != nil {
		t.Fatalf("creates.jsonl: %v", err)
	}
	if len(crData) == 0 {
		t.Fatal("creates.jsonl is empty")
	}
}

func TestWriter_MaxSamples(t *testing.T) {
	dir := t.TempDir()
	w := New(Config{BasePath: dir, SyncName: "s", RunID: 1, MaxSamples: 3})

	for i := range 10 {
		w.RecordCreate(string(rune('a'+i)), map[string]any{"i": i})
	}
	if err := w.Flush("success"); err != nil {
		t.Fatal(err)
	}
	// Summary count should be 10 even though only 3 are sampled
	sumData, _ := os.ReadFile(filepath.Join(dir, "s", "run-1", "summary.json"))
	var sum Summary
	_ = json.Unmarshal(sumData, &sum)
	if sum.Creates != 10 {
		t.Fatalf("expected Creates=10, got %d", sum.Creates)
	}
}
