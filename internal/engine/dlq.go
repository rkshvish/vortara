package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rkshvish/vortara/pkg/row"
)

// DLQRecord is one dead-lettered row serialized as a JSON line.
type DLQRecord struct {
	RowID     string         `json:"row_id"`
	SyncName  string         `json:"sync_name"`
	EntityKey string         `json:"entity_key"`
	Error     string         `json:"error"`
	FailedAt  time.Time      `json:"failed_at"`
	Data      map[string]any `json:"data"`
}

// dlqWriter appends failed rows to a JSONL dead-letter file.
// The zero value (nil) is a disabled writer; all methods are nil-safe.
type dlqWriter struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	syncName string
	count    int
}

// newDLQWriter returns an active writer if path is non-empty.
func newDLQWriter(syncName, path string) (*dlqWriter, error) {
	if path == "" {
		return nil, nil
	}
	return &dlqWriter{syncName: syncName, path: path}, nil
}

func (w *dlqWriter) Enabled() bool { return w != nil }

func (w *dlqWriter) Write(r row.Row, entityKey string, cause error) error {
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
		RowID: r.ID, SyncName: w.syncName, EntityKey: entityKey,
		Error: cause.Error(), FailedAt: time.Now().UTC(), Data: r.Data,
	}
	line, _ := json.Marshal(rec)
	_, err := w.f.Write(append(line, '\n'))
	if err == nil {
		w.count++
	}
	return err
}

func (w *dlqWriter) Count() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

func (w *dlqWriter) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

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

// ResolveDLQPath is exported for use by CLI commands.
func ResolveDLQPath(syncName, dlqPath string) string {
	if dlqPath != "" {
		return dlqPath
	}
	return "./dlq/" + syncName + ".dlq.jsonl"
}
