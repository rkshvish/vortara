package state

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore_EntityState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// Not seen yet.
	es, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if es != nil {
		t.Fatal("expected nil for unseen entity")
	}

	// Save.
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
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if got == nil || got.CurrentFingerprint != "abc123" || got.Version != 1 {
		t.Fatalf("unexpected entity state: %+v", got)
	}

	// List.
	states, err := store.ListEntityStates(ctx, "sync-a", "hubspot", 10, 0)
	if err != nil {
		t.Fatalf("ListEntityStates() error = %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}

	// Reset.
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

func TestMemoryStore_RuleFirings(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	fired, err := store.HasRuleFired(ctx, "sync-a", "hubspot", "id=1", "rule-once")
	if err != nil {
		t.Fatalf("HasRuleFired() error = %v", err)
	}
	if fired {
		t.Fatal("expected rule not fired yet")
	}

	if err := store.MarkRuleFired(ctx, "sync-a", "hubspot", "id=1", "rule-once"); err != nil {
		t.Fatalf("MarkRuleFired() error = %v", err)
	}

	fired, err = store.HasRuleFired(ctx, "sync-a", "hubspot", "id=1", "rule-once")
	if err != nil {
		t.Fatalf("HasRuleFired() error = %v", err)
	}
	if !fired {
		t.Fatal("expected rule to be marked fired")
	}

	// Different rule key → not fired.
	fired, _ = store.HasRuleFired(ctx, "sync-a", "hubspot", "id=1", "other-rule")
	if fired {
		t.Fatal("different rule should not be fired")
	}
}

func TestMemoryStore_Decisions(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	history, err := store.GetDecisionHistory(ctx, "sync-a", "hubspot", "id=1", 10)
	if err != nil {
		t.Fatalf("GetDecisionHistory() error = %v", err)
	}
	if len(history) != 0 {
		t.Fatal("expected empty history")
	}

	ev := &DecisionEvent{
		SyncName:    "sync-a",
		Destination: "hubspot",
		EntityKey:   "id=1",
		Decision:    "upsert",
		Reasons:     []string{"fingerprint_changed()"},
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.RecordDecision(ctx, ev); err != nil {
		t.Fatalf("RecordDecision() error = %v", err)
	}

	history, err = store.GetDecisionHistory(ctx, "sync-a", "hubspot", "id=1", 10)
	if err != nil {
		t.Fatalf("GetDecisionHistory() error = %v", err)
	}
	if len(history) != 1 || history[0].Decision != "upsert" {
		t.Fatalf("unexpected history: %+v", history)
	}
}

func TestMemoryStore_RunLog(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

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

func TestMemoryStore_Delivery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

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
