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

func TestSQLiteStore_EntityState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	es, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if es != nil {
		t.Fatal("expected nil for unseen entity")
	}

	want := &EntityState{
		SyncName:           "sync-a",
		Destination:        "hubspot",
		EntityKey:          "id=1",
		CurrentFingerprint: "abc123",
		LastDecision:       "upsert",
		LastStatus:         "ok",
		Version:            1,
		UpdatedAt:          time.Now().UTC().Truncate(time.Second),
	}
	if err := store.SaveEntityState(ctx, want); err != nil {
		t.Fatalf("SaveEntityState() error = %v", err)
	}

	got, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() after save error = %v", err)
	}
	if got == nil || got.CurrentFingerprint != "abc123" || got.Version != 1 {
		t.Fatalf("unexpected entity state: %+v", got)
	}

	states, err := store.ListEntityStates(ctx, "sync-a", "hubspot", 10, 0)
	if err != nil {
		t.Fatalf("ListEntityStates() error = %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}

	if err := store.ResetEntityState(ctx, "sync-a", "hubspot", "id=1"); err != nil {
		t.Fatalf("ResetEntityState() error = %v", err)
	}
	es, err = store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() after reset error = %v", err)
	}
	if es != nil {
		t.Fatal("expected nil after reset")
	}
}

func TestSQLiteStore_RuleFirings(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	fired, err := store.HasRuleFired(ctx, "sync-a", "hubspot", "id=1", "welcome-rule")
	if err != nil {
		t.Fatalf("HasRuleFired() error = %v", err)
	}
	if fired {
		t.Fatal("expected rule not fired yet")
	}

	if err := store.MarkRuleFired(ctx, "sync-a", "hubspot", "id=1", "welcome-rule"); err != nil {
		t.Fatalf("MarkRuleFired() error = %v", err)
	}

	fired, err = store.HasRuleFired(ctx, "sync-a", "hubspot", "id=1", "welcome-rule")
	if err != nil {
		t.Fatalf("HasRuleFired() after mark error = %v", err)
	}
	if !fired {
		t.Fatal("expected rule to be marked fired")
	}
}

func TestSQLiteStore_RunLog(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if _, err := store.GetLastRun(ctx, "sync-a"); err == nil {
		t.Fatal("expected error for empty run log")
	}

	runID, err := store.StartRun(ctx, "sync-a", "once")
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

	secondRunID, err := store.StartRun(ctx, "sync-a", "once")
	if err != nil {
		t.Fatalf("StartRun() second error = %v", err)
	}
	if secondRunID <= runID {
		t.Fatalf("expected later run ID, got %d after %d", secondRunID, runID)
	}

	got, err := store.GetLastRun(ctx, "sync-a")
	if err != nil {
		t.Fatalf("GetLastRun() error = %v", err)
	}
	if got.ID != secondRunID || got.Status != "running" || got.Error != "" {
		t.Fatalf("unexpected run log: %+v", got)
	}
}

func TestSQLiteStore_Delivery(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	ok, err := store.IsDelivered(ctx, "row-1", "sync-a", "hubspot")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected row to be undelivered")
	}

	if err := store.MarkDelivered(ctx, "row-1", "sync-a", "hubspot"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if err := store.MarkDelivered(ctx, "row-1", "sync-a", "hubspot"); err != nil {
		t.Fatalf("MarkDelivered() second call error = %v", err)
	}

	ok, err = store.IsDelivered(ctx, "row-1", "sync-a", "hubspot")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected row to be delivered")
	}
}

func TestSQLiteStore_BeginBatch_BuffersWrites(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := store.MarkDelivered(ctx, "row-1", "sync-a", "hubspot"); err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	ok, err := store.IsDelivered(ctx, "row-1", "sync-a", "hubspot")
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
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := store.MarkDelivered(ctx,
			"row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano),
			"sync-a",
			"hubspot",
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
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.MarkDelivered(ctx, "row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano), "sync-a", "hubspot"); err != nil {
			t.Fatalf("MarkDelivered() error = %v", err)
		}
	}
	if err := store.RollbackBatch(); err != nil {
		t.Fatalf("RollbackBatch() error = %v", err)
	}
	ok, err := store.IsDelivered(ctx, "row-1", "sync-a", "hubspot")
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
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.MarkDelivered(ctx, "row-1", "sync-a", "hubspot"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch() error = %v", err)
	}
	if err := store.MarkDelivered(ctx, "row-2", "sync-a", "hubspot"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	ok, err := store.IsDelivered(ctx, "row-1", "sync-a", "hubspot")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected committed row to be visible")
	}
	ok, err = store.IsDelivered(ctx, "row-2", "sync-a", "hubspot")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected pending row to be visible")
	}
	ok, err = store.IsDelivered(ctx, "row-3", "sync-a", "hubspot")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if ok {
		t.Fatal("expected unknown row to be absent")
	}
}

func TestSQLiteStore_BatchConcurrent(t *testing.T) {
	ctx := context.Background()
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
			errs <- store.MarkDelivered(ctx,
				"row-"+time.Now().UTC().Add(time.Duration(i)).Format(time.RFC3339Nano),
				"sync-a",
				"hubspot",
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

func TestSQLiteStore_Concurrent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			es := &EntityState{
				SyncName:     "sync-a",
				Destination:  "hubspot",
				EntityKey:    "id=" + string(rune('0'+i)),
				LastDecision: "upsert",
				UpdatedAt:    time.Now().UTC(),
			}
			errs <- store.SaveEntityState(ctx, es)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SaveEntityState() error = %v", err)
		}
	}
}
