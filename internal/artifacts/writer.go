// Package artifacts writes per-run output files for CI review, auditing, and debugging.
package artifacts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rkshvish/vortara/internal/state"
)

// Config controls where and what to write.
type Config struct {
	// BasePath is the root directory for artifacts, e.g. "./artifacts".
	// If empty, no artifacts are written.
	BasePath string
	// SyncName is the name of the sync run.
	SyncName string
	// RunID is the numeric run identifier from the state store.
	RunID int64
	// MaxSamples caps per-category sample counts (creates, updates, skips).
	MaxSamples int
}

// Writer accumulates per-run data and flushes it to disk on Close.
type Writer struct {
	cfg     Config
	runDir  string
	enabled bool

	startedAt time.Time
	summary   Summary
	decisions []*state.DecisionEvent

	creates []map[string]any
	updates []map[string]any
	skips   []map[string]any

	fieldCounts map[string]int // field name → number of times it changed
}

// Summary is written as summary.json.
type Summary struct {
	SyncName   string    `json:"sync_name"`
	RunID      int64     `json:"run_id"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationMS int64     `json:"duration_ms"`
	Creates    int       `json:"creates"`
	Updates    int       `json:"updates"`
	Skips      int       `json:"skips"`
	Errors     int       `json:"errors"`
	Missing    int       `json:"missing"`
	Status     string    `json:"status"`
}

// fieldDiffEntry is one row of field-diff-summary.json.
type fieldDiffEntry struct {
	Field   string `json:"field"`
	Changes int    `json:"changes"`
}

// New returns a Writer. If cfg.BasePath is empty, the writer is a no-op.
func New(cfg Config) *Writer {
	if cfg.MaxSamples <= 0 {
		cfg.MaxSamples = 10
	}
	w := &Writer{
		cfg:         cfg,
		enabled:     cfg.BasePath != "" && cfg.SyncName != "",
		startedAt:   time.Now().UTC(),
		fieldCounts: make(map[string]int),
	}
	if w.enabled {
		w.runDir = filepath.Join(cfg.BasePath, cfg.SyncName, fmt.Sprintf("run-%d", cfg.RunID))
	}
	return w
}

// RecordDecision adds a decision event to be written in decisions.jsonl.
func (w *Writer) RecordDecision(ev *state.DecisionEvent) {
	if !w.enabled {
		return
	}
	w.decisions = append(w.decisions, ev)
}

// RecordCreate records a created entity sample.
func (w *Writer) RecordCreate(entityKey string, data map[string]any) {
	if !w.enabled {
		return
	}
	w.summary.Creates++
	if len(w.creates) < w.cfg.MaxSamples {
		w.creates = append(w.creates, tagSample(entityKey, data))
	}
}

// RecordUpdate records an updated entity sample and the changed fields.
func (w *Writer) RecordUpdate(entityKey string, data map[string]any, changedFields []string) {
	if !w.enabled {
		return
	}
	w.summary.Updates++
	for _, f := range changedFields {
		w.fieldCounts[f]++
	}
	if len(w.updates) < w.cfg.MaxSamples {
		w.updates = append(w.updates, tagSample(entityKey, data))
	}
}

// RecordSkip records a skipped entity.
func (w *Writer) RecordSkip(entityKey string) {
	if !w.enabled {
		return
	}
	w.summary.Skips++
	if len(w.skips) < w.cfg.MaxSamples {
		w.skips = append(w.skips, map[string]any{"entity_key": entityKey})
	}
}

// RecordError increments the error count.
func (w *Writer) RecordError() {
	if !w.enabled {
		return
	}
	w.summary.Errors++
}

// RecordMissing increments the missing entity count.
func (w *Writer) RecordMissing() {
	if !w.enabled {
		return
	}
	w.summary.Missing++
}

// Flush writes all accumulated data to the run directory. Safe to call multiple times.
func (w *Writer) Flush(status string) error {
	if !w.enabled {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(w.runDir, "samples"), 0o755); err != nil {
		return fmt.Errorf("artifacts mkdir: %w", err)
	}

	now := time.Now().UTC()
	w.summary.SyncName = w.cfg.SyncName
	w.summary.RunID = w.cfg.RunID
	w.summary.StartedAt = w.startedAt
	w.summary.FinishedAt = now
	w.summary.DurationMS = now.Sub(w.startedAt).Milliseconds()
	w.summary.Status = status

	if err := writeJSON(filepath.Join(w.runDir, "summary.json"), w.summary); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(w.runDir, "decisions.jsonl"), w.decisions); err != nil {
		return err
	}
	if err := writeFieldDiff(filepath.Join(w.runDir, "field-diff-summary.json"), w.fieldCounts); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(w.runDir, "samples", "creates.jsonl"), w.creates); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(w.runDir, "samples", "updates.jsonl"), w.updates); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(w.runDir, "samples", "skips.jsonl"), w.skips); err != nil {
		return err
	}
	return nil
}

func tagSample(entityKey string, data map[string]any) map[string]any {
	out := make(map[string]any, len(data)+1)
	out["_entity_key"] = entityKey
	for k, v := range data {
		out[k] = v
	}
	return out
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeJSONL(path string, items any) error {
	// items must be a slice of any — use reflection-free approach via encoding/json
	b, err := json.Marshal(items)
	if err != nil {
		return err
	}
	// Unmarshal back to []any so we can write line-by-line
	var rows []any
	if err := json.Unmarshal(b, &rows); err != nil || len(rows) == 0 {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}

func writeFieldDiff(path string, counts map[string]int) error {
	if len(counts) == 0 {
		return nil
	}
	var entries []fieldDiffEntry
	for field, n := range counts {
		entries = append(entries, fieldDiffEntry{Field: field, Changes: n})
	}
	// Sort by changes descending (simple insertion)
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].Changes > entries[j-1].Changes; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	return writeJSON(path, entries)
}
