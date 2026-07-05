package state

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

func TestRegister_Success(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)

	Register("test", func(cfg config.StateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})

	store, err := Build(config.StateConfig{Backend: "test"})
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

	Register("dup", func(cfg config.StateConfig) (StateStore, error) { return NewMemoryStore(), nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	Register("dup", func(cfg config.StateConfig) (StateStore, error) { return NewMemoryStore(), nil })
}

func TestBuild_UnknownBackend(t *testing.T) {
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	_, err := Build(config.StateConfig{Backend: "nonexistent"})
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
	store, err := Build(config.StateConfig{Path: path})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	if err := store.SetWatermark(ctx, "pipe", "src", time.Unix(789, 0).UTC()); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}
	got, err := store.GetWatermark(ctx, "pipe", "src")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if got.IsZero() {
		t.Fatal("expected watermark round-trip")
	}
}

func TestBuild_SQLite(t *testing.T) {
	ctx := context.Background()
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Build(config.StateConfig{Backend: "sqlite", Path: path})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	if err := store.SetWatermark(ctx, "pipe", "src", time.Unix(123, 0).UTC()); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}
	got, err := store.GetWatermark(ctx, "pipe", "src")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if got.IsZero() {
		t.Fatal("expected watermark round-trip")
	}
}

func TestBuild_Memory(t *testing.T) {
	ctx := context.Background()
	resetRegistryForTest()
	t.Cleanup(resetRegistryForTest)
	registerBuiltinsForTest()

	store, err := Build(config.StateConfig{Backend: "memory"})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer store.Close()

	want := time.Unix(456, 0).UTC()
	if err := store.SetWatermark(ctx, "pipe", "src", want); err != nil {
		t.Fatalf("SetWatermark() error = %v", err)
	}
	got, err := store.GetWatermark(ctx, "pipe", "src")
	if err != nil {
		t.Fatalf("GetWatermark() error = %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("expected watermark %v, got %v", want, got)
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
	Register("sqlite", func(cfg config.StateConfig) (StateStore, error) {
		return NewSQLiteStore(cfg.Path)
	})
	Register("memory", func(cfg config.StateConfig) (StateStore, error) {
		return NewMemoryStore(), nil
	})
}
