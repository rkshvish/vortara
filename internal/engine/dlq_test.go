package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rkshvish/vortara/pkg/row"
)

func TestDLQ_WritesRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.dlq.jsonl")
	w, err := newDLQWriter("my-sync", path)
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}
	if !w.Enabled() {
		t.Fatal("expected writer to be enabled")
	}

	r := row.Row{ID: "row-1", PrimaryKey: "id=1", Data: map[string]any{"status": "won"}}
	if err := w.Write(r, "id=1", errors.New("dest exploded")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open dlq: %v", err)
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
	if len(records) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(records))
	}
	if records[0].SyncName != "my-sync" {
		t.Fatalf("SyncName = %q, want my-sync", records[0].SyncName)
	}
	if records[0].Error != "dest exploded" {
		t.Fatalf("Error = %q, want dest exploded", records[0].Error)
	}
	if records[0].Data["status"] != "won" {
		t.Fatalf("Data = %+v", records[0].Data)
	}
	if w.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", w.Count())
	}
}

func TestDLQ_Disabled(t *testing.T) {
	w, err := newDLQWriter("x", "")
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}
	if w != nil && w.Enabled() {
		t.Fatal("empty path should produce disabled writer")
	}
	// nil-safe methods
	var nilWriter *dlqWriter
	if nilWriter.Enabled() {
		t.Fatal("nil writer should not be enabled")
	}
	if err := nilWriter.Write(row.Row{}, "", errors.New("x")); err != nil {
		t.Fatalf("Write on nil writer = %v", err)
	}
	if err := nilWriter.Close(); err != nil {
		t.Fatalf("Close on nil writer = %v", err)
	}
	if nilWriter.Count() != 0 {
		t.Fatal("Count on nil writer should be 0")
	}
}

func TestDLQ_ResolvePath(t *testing.T) {
	if got := ResolveDLQPath("my-sync", ""); got != "./dlq/my-sync.dlq.jsonl" {
		t.Fatalf("ResolveDLQPath default = %q", got)
	}
	if got := ResolveDLQPath("my-sync", "/tmp/custom.jsonl"); got != "/tmp/custom.jsonl" {
		t.Fatalf("ResolveDLQPath custom = %q", got)
	}
}
