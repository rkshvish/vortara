//go:build integration

package state

import (
	"context"
	"fmt"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startStatePostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "vortara",
			"POSTGRES_PASSWORD": "vortara",
			"POSTGRES_DB":       "vortara",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping integration test; unable to start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	return fmt.Sprintf("postgres://vortara:vortara@%s:%s/vortara?sslmode=disable", host, port.Port())
}

// TestPostgresStore_Integration_FullContract exercises every StateStore
// method against real Postgres, including persistence across a store
// reopen — the property that makes shared state possible.
func TestPostgresStore_Integration_FullContract(t *testing.T) {
	ctx := context.Background()
	dsn := startStatePostgres(t)

	store, err := NewPostgresStore(dsn, "vortara")
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	// Entity state: nil when unseen.
	es, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil || es != nil {
		t.Fatalf("initial entity state = %v, %v; want nil", es, err)
	}

	// Save and read back.
	want := &EntityState{
		SyncName:           "sync-a",
		Destination:        "hubspot",
		EntityKey:          "id=1",
		CurrentFingerprint: "fp-v1",
		LastDecision:       "create",
		LastStatus:         "ok",
		Version:            1,
		UpdatedAt:          time.Now().UTC().Truncate(time.Second),
	}
	if err := store.SaveEntityState(ctx, want); err != nil {
		t.Fatalf("SaveEntityState: %v", err)
	}
	got, err := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if err != nil || got == nil || got.CurrentFingerprint != "fp-v1" {
		t.Fatalf("GetEntityState = %+v, %v", got, err)
	}

	// Update fingerprint.
	want.PreviousFingerprint = want.CurrentFingerprint
	want.CurrentFingerprint = "fp-v2"
	want.LastDecision = "update"
	want.Version = 2
	if err := store.SaveEntityState(ctx, want); err != nil {
		t.Fatalf("SaveEntityState v2: %v", err)
	}
	got, _ = store.GetEntityState(ctx, "sync-a", "hubspot", "id=1")
	if got.CurrentFingerprint != "fp-v2" || got.Version != 2 {
		t.Fatalf("after update: %+v", got)
	}

	// List.
	states, err := store.ListEntityStates(ctx, "sync-a", "hubspot", 10, 0)
	if err != nil || len(states) != 1 {
		t.Fatalf("ListEntityStates = %d, %v", len(states), err)
	}

	// Reset.
	if err := store.ResetEntityState(ctx, "sync-a", "hubspot", "id=1"); err != nil {
		t.Fatalf("ResetEntityState: %v", err)
	}
	if es, _ := store.GetEntityState(ctx, "sync-a", "hubspot", "id=1"); es != nil {
		t.Fatal("entity state should be nil after reset")
	}

	// Rule firings.
	if fired, err := store.HasRuleFired(ctx, "sync-a", "hubspot", "id=2", "welcome"); err != nil || fired {
		t.Fatalf("HasRuleFired initial = %v, %v", fired, err)
	}
	if err := store.MarkRuleFired(ctx, "sync-a", "hubspot", "id=2", "welcome"); err != nil {
		t.Fatalf("MarkRuleFired: %v", err)
	}
	if fired, _ := store.HasRuleFired(ctx, "sync-a", "hubspot", "id=2", "welcome"); !fired {
		t.Fatal("rule should be marked fired")
	}

	// Decision events.
	runID, err := store.StartRun(ctx, "sync-a", "once")
	if err != nil || runID == 0 {
		t.Fatalf("StartRun = %d, %v", runID, err)
	}
	for i, action := range []string{"create", "update"} {
		ev := &DecisionEvent{
			SyncName:    "sync-a",
			Destination: "hubspot",
			EntityKey:   "id=3",
			RunID:       runID,
			Decision:    action,
			Reasons:     []string{"rule-" + action},
			CreatedAt:   time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		if err := store.RecordDecision(ctx, ev); err != nil {
			t.Fatalf("RecordDecision(%s): %v", action, err)
		}
	}
	history, err := store.GetDecisionHistory(ctx, "sync-a", "hubspot", "id=3", 10)
	if err != nil || len(history) != 2 {
		t.Fatalf("GetDecisionHistory = %d, %v", len(history), err)
	}
	if history[0].Decision != "update" {
		t.Fatalf("expected latest first (update), got %s", history[0].Decision)
	}

	// Run log lifecycle.
	if err := store.FinishRun(ctx, runID, RunStats{RowsLoaded: 7, Status: "success"}); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	last, err := store.GetLastRun(ctx, "sync-a")
	if err != nil || last.ID != runID || last.RowsLoaded != 7 || last.Status != "success" {
		t.Fatalf("GetLastRun = %+v, %v", last, err)
	}
	if last.FinishedAt.IsZero() || last.StartedAt.IsZero() {
		t.Fatalf("run timestamps not set: %+v", last)
	}
	hist, err := store.GetRunHistory(ctx, "sync-a", 10)
	if err != nil || len(hist) != 1 {
		t.Fatalf("history = %d entries, %v", len(hist), err)
	}

	// Delivery log with batch buffering.
	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for i := 0; i < 2500; i++ {
		if err := store.MarkDelivered(ctx, fmt.Sprintf("r%d", i), "sync-a", "hubspot"); err != nil {
			t.Fatalf("MarkDelivered: %v", err)
		}
	}
	if ok, _ := store.IsDelivered(ctx, "r1", "sync-a", "hubspot"); !ok {
		t.Fatal("r1 should be visible inside the batch")
	}
	if err := store.CommitBatch(context.Background()); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if ok, _ := store.IsDelivered(ctx, "r2499", "sync-a", "hubspot"); !ok {
		t.Fatal("r2499 should be delivered after commit")
	}
	if ok, _ := store.IsDelivered(ctx, "nope", "sync-a", "hubspot"); ok {
		t.Fatal("unknown row should not be delivered")
	}

	// Persistence across reopen.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store2, err := NewPostgresStore(dsn, "vortara")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()

	if last, err := store2.GetLastRun(ctx, "sync-a"); err != nil || last.RowsLoaded != 7 {
		t.Fatalf("run log after reopen = %+v, %v", last, err)
	}
}

// TestPostgresStore_Integration_RegistryAndPrefix verifies Build() routing
// and custom table prefixes.
func TestPostgresStore_Integration_RegistryAndPrefix(t *testing.T) {
	ctx := context.Background()
	dsn := startStatePostgres(t)

	store, err := NewPostgresStore(dsn, "custom_prefix")
	if err != nil {
		t.Fatalf("custom prefix: %v", err)
	}
	es := &EntityState{
		SyncName: "p", Destination: "d", EntityKey: "id=1",
		LastDecision: "create", UpdatedAt: time.Now().UTC(),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("write with custom prefix: %v", err)
	}
	_ = store.Close()

	if _, err := NewPostgresStore(dsn, "bad; DROP TABLE x"); err == nil {
		t.Fatal("malicious prefix must be rejected")
	}
	if _, err := NewPostgresStore("", "vortara"); err == nil {
		t.Fatal("empty connection must be rejected")
	}
}

// TestPostgresStore_Integration_PoolOptions verifies that WithMaxOpenConns,
// WithMaxIdleConns, and WithConnMaxLifetime are accepted without error.
func TestPostgresStore_Integration_PoolOptions(t *testing.T) {
	ctx := context.Background()
	dsn := startStatePostgres(t)

	store, err := NewPostgresStore(dsn, "vortara",
		WithMaxOpenConns(3),
		WithMaxIdleConns(2),
		WithConnMaxLifetime(30*time.Second),
	)
	if err != nil {
		t.Fatalf("NewPostgresStore with pool options: %v", err)
	}
	defer store.Close()

	es := &EntityState{
		SyncName: "pool-p", Destination: "pool-d", EntityKey: "id=1",
		LastDecision: "create", UpdatedAt: time.Now().UTC(),
	}
	if err := store.SaveEntityState(ctx, es); err != nil {
		t.Fatalf("SaveEntityState: %v", err)
	}
	got, err := store.GetEntityState(ctx, "pool-p", "pool-d", "id=1")
	if err != nil || got == nil {
		t.Fatalf("GetEntityState = %v, %v", got, err)
	}
}

// TestPostgresStore_Integration_RollbackBatch verifies that rows buffered
// inside a batch are discarded by RollbackBatch and never reach the DB.
func TestPostgresStore_Integration_RollbackBatch(t *testing.T) {
	ctx := context.Background()
	dsn := startStatePostgres(t)

	store, err := NewPostgresStore(dsn, "vortara")
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	defer store.Close()

	if err := store.BeginBatch(ctx); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for i := 0; i < 5; i++ {
		rowID := fmt.Sprintf("rollback-row-%d", i)
		if err := store.MarkDelivered(ctx, rowID, "p-rollback", "d1"); err != nil {
			t.Fatalf("MarkDelivered: %v", err)
		}
	}
	if ok, _ := store.IsDelivered(ctx, "rollback-row-0", "p-rollback", "d1"); !ok {
		t.Fatal("row should be visible inside the batch before rollback")
	}

	if err := store.RollbackBatch(); err != nil {
		t.Fatalf("RollbackBatch: %v", err)
	}

	for i := 0; i < 5; i++ {
		rowID := fmt.Sprintf("rollback-row-%d", i)
		if ok, err := store.IsDelivered(ctx, rowID, "p-rollback", "d1"); err != nil || ok {
			t.Fatalf("row %d should NOT be delivered after rollback (ok=%v, err=%v)", i, ok, err)
		}
	}
}
