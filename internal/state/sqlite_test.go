package state

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

// TestSQLiteStore_Watermark verifies watermark reads and writes.
func TestSQLiteStore_Watermark(t *testing.T) {
	store := newTestStore(t)

	wm, err := store.GetWatermark("pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !wm.IsZero() {
		t.Fatalf("expected zero watermark, got %v", wm)
	}

	first := time.Now().UTC().Truncate(time.Second)
	if err := store.SetWatermark("pipeline-a", "source-a", first); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}

	got, err := store.GetWatermark("pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !got.Equal(first) {
		t.Fatalf("expected %v, got %v", first, got)
	}

	second := first.Add(2 * time.Minute)
	if err := store.SetWatermark("pipeline-a", "source-a", second); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}

	got, err = store.GetWatermark("pipeline-a", "source-a")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !got.Equal(second) {
		t.Fatalf("expected latest watermark %v, got %v", second, got)
	}
}

// TestSQLiteStore_KafkaOffset verifies offset reads and writes.
func TestSQLiteStore_KafkaOffset(t *testing.T) {
	store := newTestStore(t)

	offset, err := store.GetOffset("pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if offset != -1 {
		t.Fatalf("expected -1 offset, got %d", offset)
	}

	if err := store.SetOffset("pipeline-a", "topic-a", 0, 42); err != nil {
		t.Fatalf("SetOffset() error = %v", err)
	}

	got, err := store.GetOffset("pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	if err := store.SetOffset("pipeline-a", "topic-a", 0, 99); err != nil {
		t.Fatalf("SetOffset() error = %v", err)
	}

	got, err = store.GetOffset("pipeline-a", "topic-a", 0)
	if err != nil {
		t.Fatalf("GetOffset() error = %v", err)
	}
	if got != 99 {
		t.Fatalf("expected latest offset 99, got %d", got)
	}
}

// TestSQLiteStore_RunLog verifies run creation, update, and lookup.
func TestSQLiteStore_RunLog(t *testing.T) {
	store := newTestStore(t)

	if _, err := store.GetLastRun("pipeline-a"); err == nil {
		t.Fatal("expected error for empty run log")
	}

	runID, err := store.StartRun("pipeline-a", "batch")
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
	if err := store.FinishRun(runID, stats); err != nil {
		t.Fatalf("FinishRun() error = %v", err)
	}

	secondRunID, err := store.StartRun("pipeline-a", "batch")
	if err != nil {
		t.Fatalf("StartRun() second error = %v", err)
	}
	if secondRunID <= runID {
		t.Fatalf("expected later run ID, got %d after %d", secondRunID, runID)
	}

	got, err := store.GetLastRun("pipeline-a")
	if err != nil {
		t.Fatalf("GetLastRun() error = %v", err)
	}
	if got.ID != secondRunID || got.Status != "running" || got.Error != "" {
		t.Fatalf("unexpected run log: %+v", got)
	}
}

// TestSQLiteStore_Delivery verifies delivery idempotency tracking.
func TestSQLiteStore_Delivery(t *testing.T) {
	store := newTestStore(t)

	ok, err := store.IsDelivered("row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected row to be undelivered")
	}

	if err := store.MarkDelivered("row-1", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if err := store.MarkDelivered("row-1", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() second call error = %v", err)
	}

	ok, err = store.IsDelivered("row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected row to be delivered")
	}
}

func TestSQLiteStore_BeginBatch_BuffersWrites(t *testing.T) {
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := store.MarkDelivered("row-1", "pipeline-a", "dest-a"); err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	ok, err := store.IsDelivered("row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected in-batch delivery to be visible")
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM delivery_log`).Scan(&count); err != nil {
		t.Fatalf("count delivery_log: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no committed delivery rows, got %d", count)
	}
}

func TestSQLiteStore_CommitBatch_WritesAll(t *testing.T) {
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := store.MarkDelivered(
			"row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano),
			"pipeline-a",
			"dest-a",
		); err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	if err := store.CommitBatch(context.Background()); err != nil {
		t.Fatalf("CommitBatch() error = %v", err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM delivery_log`).Scan(&count); err != nil {
		t.Fatalf("count delivery_log: %v", err)
	}
	if count != 100 {
		t.Fatalf("expected 100 committed rows, got %d", count)
	}
}

func TestSQLiteStore_RollbackBatch_DiscardsPending(t *testing.T) {
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.MarkDelivered("row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano), "pipeline-a", "dest-a"); err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	if err := store.RollbackBatch(); err != nil {
		t.Fatalf("RollbackBatch() error = %v", err)
	}
	ok, err := store.IsDelivered("row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected rollback to discard pending writes")
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM delivery_log`).Scan(&count); err != nil {
		t.Fatalf("count delivery_log: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected empty delivery_log, got %d", count)
	}
}

func TestSQLiteStore_IsDelivered_ChecksBoth(t *testing.T) {
	store := newTestStore(t)

	if err := store.MarkDelivered("row-1", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	if err := store.MarkDelivered("row-2", "pipeline-a", "dest-a"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	ok, err := store.IsDelivered("row-1", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected committed row to be visible")
	}
	ok, err = store.IsDelivered("row-2", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected pending row to be visible")
	}
	ok, err = store.IsDelivered("row-3", "pipeline-a", "dest-a")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected unknown row to be absent")
	}
}

func TestSQLiteStore_BatchConcurrent(t *testing.T) {
	store := newTestStore(t)
	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.MarkDelivered(
				"row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano),
				"pipeline-a",
				"dest-a",
			)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	if err := store.CommitBatch(context.Background()); err != nil {
		t.Fatalf("CommitBatch() error = %v", err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM delivery_log`).Scan(&count); err != nil {
		t.Fatalf("count delivery_log: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 committed rows, got %d", count)
	}
}

// TestSQLiteStore_Concurrent verifies concurrent writes do not race or error.
func TestSQLiteStore_Concurrent(t *testing.T) {
	store := newTestStore(t)

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.SetWatermark("pipeline-a", "source-"+string(rune('a'+i)), time.Unix(int64(i), 0).UTC())
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
