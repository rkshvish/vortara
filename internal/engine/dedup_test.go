package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/row"
)

func TestDedupWindow_FirstOccurrence(t *testing.T) {
	d := NewDedupWindow(time.Minute, 10)
	if d.IsDuplicate("key1") {
		t.Fatal("expected first occurrence to be new")
	}
}

func TestDedupWindow_DuplicateWithinWindow(t *testing.T) {
	d := NewDedupWindow(time.Minute, 10)
	if d.IsDuplicate("key1") {
		t.Fatal("expected first occurrence to be new")
	}
	if !d.IsDuplicate("key1") {
		t.Fatal("expected second occurrence to be duplicate")
	}
}

func TestDedupWindow_ExpiredEntry(t *testing.T) {
	d := NewDedupWindow(10*time.Millisecond, 10)
	if d.IsDuplicate("key1") {
		t.Fatal("expected first occurrence to be new")
	}
	time.Sleep(20 * time.Millisecond)
	if d.IsDuplicate("key1") {
		t.Fatal("expected expired entry to be treated as new")
	}
}

func TestDedupWindow_DifferentKeys(t *testing.T) {
	d := NewDedupWindow(time.Minute, 10)
	if d.IsDuplicate("key1") || d.IsDuplicate("key2") {
		t.Fatal("expected different keys to be new")
	}
	if !d.IsDuplicate("key1") || !d.IsDuplicate("key2") {
		t.Fatal("expected repeated keys to be duplicates")
	}
}

func TestDedupWindow_MaxSize(t *testing.T) {
	d := NewDedupWindow(time.Minute, 5)
	for i := 0; i < 6; i++ {
		d.IsDuplicate(string(rune('a' + i)))
	}
	if got := d.Size(); got > 5 {
		t.Fatalf("Size() = %d, want <= 5", got)
	}
}

func TestDedupWindow_Disabled(t *testing.T) {
	if d := NewDedupWindow(0, 0); d != nil {
		t.Fatalf("expected nil dedup window, got %#v", d)
	}
}

func TestDedupWindow_Concurrent(t *testing.T) {
	d := NewDedupWindow(time.Minute, 100)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d.IsDuplicate(string(rune('a' + i)))
		}(i)
	}
	wg.Wait()
}

func TestExtractKey_FromData(t *testing.T) {
	r := row.Row{ID: "row-1", Data: map[string]interface{}{"event_id": "evt_123"}}
	if got := extractKey(r, "event_id"); got != "evt_123" {
		t.Fatalf("extractKey() = %q, want evt_123", got)
	}
}

func TestExtractKey_FallbackToRowID(t *testing.T) {
	r := row.Row{ID: "row-1", Data: map[string]interface{}{}}
	if got := extractKey(r, ""); got != "row-1" {
		t.Fatalf("extractKey() = %q, want row-1", got)
	}
}
