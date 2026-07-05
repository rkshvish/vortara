package state

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestMemoryStore_Watermark verifies watermark reads and writes.
func TestMemoryStore_Watermark(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	wm, err := store.GetWatermark(ctx, "pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !wm.IsZero() {
		t.Fatalf("expected zero watermark, got %v", wm)
	}

	first := time.Now().UTC().Truncate(time.Second)
	if err := store.SetWatermark(ctx, "pipeline-a", "source-a", first); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}

	got, err := store.GetWatermark(ctx, "pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !got.Equal(first) {
		t.Fatalf("expected %v, got %v", first, got)
	}

	second := first.Add(2 * time.Minute)
	if err := store.SetWatermark(ctx, "pipeline-a", "source-a", second); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}

	got, err = store.GetWatermark(ctx, "pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !got.Equal(second) {
		t.Fatalf("expected latest watermark %v, got %v", second, got)
	}
}

// TestMemoryStore_KafkaOffset verifies offset reads and writes.
func TestMemoryStore_KafkaOffset(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	offset, err := store.GetOffset(ctx, "pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if offset != -1 {
		t.Fatalf("expected -1 offset, got %d", offset)
	}

	if err := store.SetOffset(ctx, "pipeline-a", "topic-a", 0, 42); err != nil {
		t.Fatalf("SetOffset() error = %v", err)
	}

	got, err := store.GetOffset(ctx, "pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	if err := store.SetOffset(ctx, "pipeline-a", "topic-a", 0, 99); err != nil {
		t.Fatalf("SetOffset() error = %v", err)
	}

	got, err = store.GetOffset(ctx, "pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if got != 99 {
		t.Fatalf("expected latest offset 99, got %d", got)
	}
}

// TestMemoryStore_RunLog verifies run creation, update, and lookup.
func TestMemoryStore_RunLog(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if _, err := store.GetLastRun(ctx, "pipeline-a"); err == nil {
		t.Fatal("expected error for empty run log")
	}

	runID, err := store.StartRun(ctx, "pipeline-a", "batch")
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	if runID <= 0 {
		t.Fatalf("expected run ID > 0, got %d", runID)
	}

	stats := RunStats{
		RowsExtracted: 10,
		RowsLoaded:    8,
		RowsSkipped:   1,
		RowsErrored:   1,
		Status:        "failed",
		Error:         "boom",
	}
	if err := store.FinishRun(ctx, runID, stats); err != nil {
		t.Fatalf("FinishRun() error = %v", err)
	}

	secondRunID, err := store.StartRun(ctx, "pipeline-a", "batch")
	if err != nil {
		t.Fatalf("StartRun() second error = %v", err)
	}
	if secondRunID <= runID {
		t.Fatalf("expected later run ID, got %d after %d", secondRunID, runID)
	}

	got, err := store.GetLastRun(ctx, "pipeline-a")
	if err != nil {
		t.Fatalf("GetLastRun() error = %v", err)
	}
	if got.ID != secondRunID || got.Status != "running" || got.Error != "" {
		t.Fatalf("unexpected run log: %+v", got)
	}
}

// TestMemoryStore_Delivery verifies delivery idempotency tracking.
func TestMemoryStore_Delivery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	ok, err := store.IsDelivered(ctx, "row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected row to be undelivered")
	}

	if err := store.MarkDelivered(ctx, "row-1", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if err := store.MarkDelivered(ctx, "row-1", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() second call error = %v", err)
	}

	ok, err = store.IsDelivered(ctx, "row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected row to be delivered")
	}
}

// TestMemoryStore_Concurrent verifies concurrent writes do not race or error.
func TestMemoryStore_Concurrent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.SetWatermark(ctx, "pipeline-a", "source-"+string(rune('a'+i)), time.Unix(int64(i), 0).UTC())
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SetWatermark() error = %v", err)
		}
	}
}
