package state

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_LockUnlock(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.LockRun(ctx, "my-sync", "owner-1", 5*time.Minute); err != nil {
		t.Fatalf("first lock should succeed: %v", err)
	}
	// Second lock while first is live should fail.
	if err := s.LockRun(ctx, "my-sync", "owner-2", 5*time.Minute); err == nil {
		t.Fatal("second lock should fail while first is live")
	}
	// Unlock then re-acquire.
	if err := s.UnlockRun(ctx, "my-sync"); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}
	if err := s.LockRun(ctx, "my-sync", "owner-3", 5*time.Minute); err != nil {
		t.Fatalf("lock after unlock should succeed: %v", err)
	}
}

func TestMemoryStore_LockExpiry(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	// Lock with an already-expired TTL.
	if err := s.LockRun(ctx, "my-sync", "owner-1", -1*time.Second); err != nil {
		t.Fatalf("inserting expired lock: %v", err)
	}
	// A new lock should succeed because the previous one is expired.
	if err := s.LockRun(ctx, "my-sync", "owner-2", 5*time.Minute); err != nil {
		t.Fatalf("lock after expired lock should succeed: %v", err)
	}
}

func TestMemoryStore_HeartbeatLock(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.LockRun(ctx, "my-sync", "owner-1", 5*time.Minute); err != nil {
		t.Fatalf("lock: %v", err)
	}
	// Heartbeat extends the TTL — should not error.
	if err := s.HeartbeatLock(ctx, "my-sync", "owner-1", 10*time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// After heartbeat the lock should still be held.
	if err := s.LockRun(ctx, "my-sync", "owner-2", 5*time.Minute); err == nil {
		t.Fatal("lock should still be held after heartbeat")
	}
}

func TestSQLiteStore_LockUnlock(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer s.Close()

	if err := s.LockRun(ctx, "my-sync", "owner-1", 5*time.Minute); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if err := s.LockRun(ctx, "my-sync", "owner-2", 5*time.Minute); err == nil {
		t.Fatal("second lock should fail")
	}
	if err := s.UnlockRun(ctx, "my-sync"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := s.LockRun(ctx, "my-sync", "owner-3", 5*time.Minute); err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
}

func TestSQLiteStore_LockExpiry(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer s.Close()

	// Insert a lock that is already expired.
	if err := s.LockRun(ctx, "my-sync", "stale", -1*time.Second); err != nil {
		t.Fatalf("insert expired lock: %v", err)
	}
	// A new acquisition should succeed.
	if err := s.LockRun(ctx, "my-sync", "new-owner", 5*time.Minute); err != nil {
		t.Fatalf("lock after expired: %v", err)
	}
}

func TestSQLiteStore_DifferentSyncsDoNotConflict(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer s.Close()

	if err := s.LockRun(ctx, "sync-a", "owner-1", 5*time.Minute); err != nil {
		t.Fatalf("lock sync-a: %v", err)
	}
	// Different sync name — should succeed.
	if err := s.LockRun(ctx, "sync-b", "owner-1", 5*time.Minute); err != nil {
		t.Fatalf("lock sync-b: %v", err)
	}
}
