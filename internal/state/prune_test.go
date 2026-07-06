package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestSQLiteStore_EntityState_Update verifies that re-saving an entity state
// increments the version and updates the fingerprint.
func TestSQLiteStore_EntityState_Update(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	es := &EntityState{
		SyncName:           "sync-a",
		Destination:        "hubspot",
		EntityKey:          "id=1",
		CurrentFingerprint: "fp-v1",
		LastDecision:       "create",
		LastStatus:         "ok",
		Version:            1,
		UpdatedAt:          time.Now().UTC().Truncate(time.Second),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState v1: %v", err)
	}

	es.PreviousFingerprint = es.CurrentFingerprint
	es.CurrentFingerprint = "fp-v2"
	es.LastDecision = "update"
	es.Version = 2
	es.UpdatedAt = time.Now().UTC().Truncate(time.Second)
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState v2: %v", err)
	}

	got, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState: %v", err)
	}
	if got == nil {
		t.Fatal("expected entity state")
	}
	if got.CurrentFingerprint != "fp-v2" || got.PreviousFingerprint != "fp-v1" {
		t.Fatalf("fingerprint mismatch: %+v", got)
	}
	if got.Version != 2 || got.LastDecision != "update" {
		t.Fatalf("unexpected state: %+v", got)
	}
}

// TestSQLiteStore_DecisionHistory verifies that decisions are recorded and
// returned in reverse chronological order.
func TestSQLiteStore_DecisionHistory(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	runID, _ := store.StartRun(ctx, "sync-a", "once")

	for i, action := range []string{"create", "update", "skip"} {
		ev := &DecisionEvent{
			SyncName:    "sync-a",
			Destination: "hubspot",
			EntityKey:   "id=1",
			RunID:       runID,
			Decision:    action,
			Reasons:     []string{"rule-" + action},
			CreatedAt:   time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		if err := store.RecordDecision(ctx, ev); err != nil {
			t.Fatalf("RecordDecision(%s): %v", action, err)
		}
	}

	history, err := store.GetDecisionHistory(ctx, "sync-a", "hubspot", "id=1", 10)
	if err != nil {
		t.Fatalf("GetDecisionHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 events, got %d", len(history))
	}
	if history[0].Decision != "skip" {
		t.Fatalf("expected latest first (skip), got %s", history[0].Decision)
	}
}
