package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStore_PruneDelivered(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		if err := store.MarkDelivered(id, "p", "d"); err != nil {
			t.Fatalf("MarkDelivered: %v", err)
		}
	}
	if err := store.CommitBatch(context.Background()); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}

	// Cutoff in the past removes nothing.
	n, err := store.PruneDelivered(time.Now().Add(-24 * time.Hour))
	if err != nil || n != 0 {
		t.Fatalf("prune(past) = %d, %v; want 0 removed", n, err)
	}
	delivered, _ := store.IsDelivered("r1", "p", "d")
	if !delivered {
		t.Fatal("r1 should still be delivered")
	}

	// Cutoff in the future removes all three.
	n, err = store.PruneDelivered(time.Now().Add(24 * time.Hour))
	if err != nil || n != 3 {
		t.Fatalf("prune(future) = %d, %v; want 3 removed", n, err)
	}
	delivered, _ = store.IsDelivered("r1", "p", "d")
	if delivered {
		t.Fatal("r1 should be pruned")
	}
}
