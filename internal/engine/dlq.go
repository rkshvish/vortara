package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v2cfg "github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// DLQRecord is one dead-lettered row, serialized as a JSON line.
type DLQRecord struct {
	RowID      string                 `json:"row_id"`
	Pipeline   string                 `json:"pipeline"`
	PrimaryKey string                 `json:"primary_key"`
	Error      string                 `json:"error"`
	FailedAt   time.Time              `json:"failed_at"`
	Data       map[string]interface{} `json:"data"`
}

// dlqWriter appends failed rows to a JSONL dead-letter file.
// The zero value (nil) is a disabled writer; all methods are nil-safe.
type dlqWriter struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	pipeline string
	count    int
}

// newDLQWriter returns an active writer when settings.on_error is "dlq",
// or a disabled (nil) writer for skip/retry modes. The file is opened lazily
// on the first Write so successful runs never create an empty DLQ file.
func newDLQWriter(cfg *v2cfg.PipelineConfig) (*dlqWriter, error) {
	if cfg == nil || strings.ToLower(strings.TrimSpace(cfg.Settings.OnError)) != "dlq" {
		return nil, nil
	}
	return &dlqWriter{pipeline: cfg.Name, path: ResolveDLQPath(cfg)}, nil
}

// Enabled reports whether dead-lettering is active.
func (w *dlqWriter) Enabled() bool { return w != nil }

// Write appends one failed row to the DLQ file.
func (w *dlqWriter) Write(r row.Row, cause error) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		if dir := filepath.Dir(w.path); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w.f = f
	}
	rec := DLQRecord{
		RowID:      r.ID,
		Pipeline:   w.pipeline,
		PrimaryKey: r.PrimaryKey,
		Error:      cause.Error(),
		FailedAt:   time.Now().UTC(),
		Data:       r.Data,
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := w.f.Write(append(line, '\n')); err != nil {
		return err
	}
	w.count++
	return nil
}

// Count returns the number of rows dead-lettered so far.
func (w *dlqWriter) Count() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// Path returns the dead-letter file path.
func (w *dlqWriter) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

// Close closes the underlying file if one was opened.
func (w *dlqWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
