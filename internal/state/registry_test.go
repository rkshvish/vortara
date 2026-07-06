package state

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRegister_Success(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)

	Register("test", func(cfg stateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})

	store, err := Build("test", "", "")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if store == nil {
		t.Fatal("expected store instance")
	}
}

func TestRegister_Duplicate(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)

	Register("dup", func(cfg stateConfig) (StateStore, error) { return NewMemoryStore(), nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	Register("dup", func(cfg stateConfig) (StateStore, error) { return NewMemoryStore(), nil })
}

func TestBuild_UnknownBackend(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	_, err := Build("nonexistent", "", "")
	if err == nil || !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("expected unknown backend error, got %v", err)
	}
	if !strings.Contains(err.Error(), "memory") || !strings.Contains(err.Error(), "sqlite") {
		t.Fatalf("expected valid backend list in error, got %v", err)
	}
}

func TestBuild_DefaultBackend(t *testing.T) {
	ctx := context.Background()
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Build("", path, "")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	es := &EntityState{
		SyncName: "pipe", Destination: "dest", EntityKey: "id=1",
		LastDecision: "upsert", UpdatedAt: time.Now().UTC(),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState() error = %v", err)
	}
	got, err := store.GetEntityState(ctx, "pipe", "dest", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if got == nil {
		t.Fatal("expected entity state round-trip")
	}
}

func TestBuild_SQLite(t *testing.T) {
	ctx := context.Background()
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Build("sqlite", path, "")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	es := &EntityState{
		SyncName: "pipe", Destination: "dest", EntityKey: "id=1",
		LastDecision: "upsert", UpdatedAt: time.Now().UTC(),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState() error = %v", err)
	}
	got, err := store.GetEntityState(ctx, "pipe", "dest", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if got == nil {
		t.Fatal("expected entity state round-trip")
	}
}

func TestBuild_Memory(t *testing.T) {
	ctx := context.Background()
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	store, err := Build("memory", "", "")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	es := &EntityState{
		SyncName: "pipe", Destination: "dest", EntityKey: "id=1",
		LastDecision: "create", UpdatedAt: time.Now().UTC(),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState() error = %v", err)
	}
	got, err := store.GetEntityState(ctx, "pipe", "dest", "id=1")
	if err != nil {
		t.Fatalf("GetEntityState() error = %v", err)
	}
	if got == nil || got.LastDecision != "create" {
		t.Fatalf("unexpected state: %+v", got)
	}
}

func TestList_ContainsRegistered(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	names := List()
	if len(names) < 2 || names[0] != "memory" || names[1] != "sqlite" {
		t.Fatalf("expected sorted registered backends, got %v", names)
	}
}

func registerBuiltinsForTest() {
	Register("sqlite", func(cfg stateConfig) (StateStore, error) {
		return NewSQLiteStore(cfg.Path)
	})
	Register("memory", func(cfg stateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})
}
