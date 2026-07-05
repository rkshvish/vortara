// Package engine coordinates extraction and loading for pipeline runs.
package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/rkshvish/vortaraos/pkg/row"
)

// DedupWindow tracks recently seen streaming event keys.
type DedupWindow struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	window  time.Duration
	maxSize int
}

// NewDedupWindow creates a new dedup window.
func NewDedupWindow(window time.Duration, maxSize int) *DedupWindow {
	if window == 0 {
		return nil
	}
	if maxSize <= 0 {
		maxSize = 100000
	}
	return &DedupWindow{
		seen:    make(map[string]time.Time, maxSize),
		window:  window,
		maxSize: maxSize,
	}
}

// IsDuplicate reports whether a key has been seen within the configured window.
func (d *DedupWindow) IsDuplicate(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if len(d.seen) > d.maxSize/2 {
		for k, t := range d.seen {
			if now.Sub(t) > d.window {
				delete(d.seen, k)
			}
		}
	}
	if len(d.seen) >= d.maxSize {
		var oldestKey string
		var oldestTime time.Time
		for k, t := range d.seen {
			if oldestTime.IsZero() || t.Before(oldestTime) {
				oldestKey = k
				oldestTime = t
			}
		}
		delete(d.seen, oldestKey)
	}

	if firstSeen, ok := d.seen[key]; ok && now.Sub(firstSeen) <= d.window {
		return true
	}
	d.seen[key] = now
	return false
}

// Size returns the number of tracked keys.
func (d *DedupWindow) Size() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

func extractKey(r row.Row, keyField string) string {
	if keyField != "" {
		if v, ok := r.Data[keyField]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return r.ID
}
