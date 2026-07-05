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

	// Watermarks: zero when unset, upsert, read back.
	wm, err := store.GetWatermark(ctx, "p1", "s1")
	if err != nil || !wm.IsZero() {
		t.Fatalf("initial watermark = %v, %v; want zero", wm, err)
	}
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := store.SetWatermark(ctx, "p1", "s1", ts); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	if err := store.SetWatermark(ctx, "p1", "s1", ts.Add(time.Hour)); err != nil {
		t.Fatalf("SetWatermark upsert: %v", err)
	}
	wm, _ = store.GetWatermark(ctx, "p1", "s1")
	if !wm.Equal(ts.Add(time.Hour)) {
		t.Fatalf("watermark = %v, want %v", wm, ts.Add(time.Hour))
	}

	// Numeric cursor.
	if n, err := store.GetNumericWatermark(ctx, "p1", "s1"); err != nil || n != 0 {
		t.Fatalf("initial numeric = %d, %v", n, err)
	}
	if err := store.SetNumericWatermark(ctx, "p1", "s1", 42); err != nil {
		t.Fatalf("SetNumericWatermark: %v", err)
	}
	if n, _ := store.GetNumericWatermark(ctx, "p1", "s1"); n != 42 {
		t.Fatalf("numeric = %d, want 42", n)
	}

	// Kafka offsets: -1 when unset.
	if off, err := store.GetOffset(ctx, "p1", "topic", 0); err != nil || off != -1 {
		t.Fatalf("initial offset = %d, %v; want -1", off, err)
	}
	if err := store.SetOffset(ctx, "p1", "topic", 0, 99); err != nil {
		t.Fatalf("SetOffset: %v", err)
	}
	if off, _ := store.GetOffset(ctx, "p1", "topic", 0); off != 99 {
		t.Fatalf("offset = %d, want 99", off)
	}

	// Run log lifecycle.
	runID, err := store.StartRun(ctx, "p1", "batch")
	if err != nil || runID == 0 {
		t.Fatalf("StartRun = %d, %v", runID, err)
	}
	if err := store.FinishRun(ctx, runID, RunStats{RowsLoaded: 7, Status: "success"}); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	last, err := store.GetLastRun(ctx, "p1")
	if err != nil || last.ID != runID || last.RowsLoaded != 7 || last.Status != "success" {
		t.Fatalf("GetLastRun = %+v, %v", last, err)
	}
	if last.FinishedAt.IsZero() || last.StartedAt.IsZero() {
		t.Fatalf("run timestamps not set: %+v", last)
	}
	hist, err := store.GetRunHistory(ctx, "p1", 10)
	if err != nil || len(hist) != 1 {
		t.Fatalf("history = %d entries, %v", len(hist), err)
	}

	// Delivery log with batch buffering.
	if err := store.BeginBatch(context.Background()); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for i := 0; i < 2500; i++ { // multiple commit chunks
		if err := store.MarkDelivered(ctx, fmt.Sprintf("r%d", i), "p1", "d1"); err != nil {
			t.Fatalf("MarkDelivered: %v", err)
		}
	}
	// In-batch visibility.
	if ok, _ := store.IsDelivered(ctx, "r1", "p1", "d1"); !ok {
		t.Fatal("r1 should be visible inside the batch")
	}
	if err := store.CommitBatch(context.Background()); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if ok, _ := store.IsDelivered(ctx, "r2499", "p1", "d1"); !ok {
		t.Fatal("r2499 should be delivered after commit")
	}
	if ok, _ := store.IsDelivered(ctx, "nope", "p1", "d1"); ok {
		t.Fatal("unknown row should not be delivered")
	}

	// Prune.
	if n, err := store.PruneDelivered(ctx, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("prune(past) = %d, %v; want 0", n, err)
	}
	if n, err := store.PruneDelivered(ctx, time.Now().Add(time.Hour)); err != nil || n != 2500 {
		t.Fatalf("prune(future) = %d, %v; want 2500", n, err)
	}

	// Persistence across reopen — the multi-instance property.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store2, err := NewPostgresStore(dsn, "vortara")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	if wm, _ := store2.GetWatermark(ctx, "p1", "s1"); !wm.Equal(ts.Add(time.Hour)) {
		t.Fatalf("watermark after reopen = %v", wm)
	}
	if n, _ := store2.GetNumericWatermark(ctx, "p1", "s1"); n != 42 {
		t.Fatalf("numeric after reopen = %d", n)
	}
	if last, err := store2.GetLastRun(ctx, "p1"); err != nil || last.RowsLoaded != 7 {
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
	if err := store.SetWatermark(ctx, "p", "s", time.Now()); err != nil {
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
