//go:build e2e

// Package e2e drives the compiled vortara binary against real
// infrastructure — the closest test to what a user runs.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "vortara")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/vortara")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build binary: %v\n%s", err, out)
	}
	return bin
}

func startPostgres(t *testing.T) (dsn string) {
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
		t.Skipf("skipping e2e; unable to start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	return fmt.Sprintf("postgres://vortara:vortara@%s:%s/vortara?sslmode=disable", host, port.Port())
}

func pgExec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	for _, s := range stmts {
		if _, err := pool.Exec(context.Background(), s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func pgCount(t *testing.T, dsn, query string) int {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	var n int
	if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func runCLI(t *testing.T, bin string, args ...string) string {
	t.Helper()
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("vortara %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// TestE2E_IncrementalPipeline covers the full user journey with the real
// binary: validate → run (full) → run (incremental, zero rows) →
// new data → run (delta only) → status.
func TestE2E_IncrementalPipeline(t *testing.T) {
	bin := buildBinary(t)
	dsn := startPostgres(t)

	pgExec(t, dsn,
		`CREATE TABLE orders (
			id BIGSERIAL PRIMARY KEY, item TEXT,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE orders_copy (id BIGINT PRIMARY KEY, item TEXT, updated_at TIMESTAMPTZ)`,
		`INSERT INTO orders (item) SELECT 'item-' || i FROM generate_series(1, 50) i`,
	)

	dir := t.TempDir()
	pipeline := filepath.Join(dir, "pipeline.yaml")
	yaml := fmt.Sprintf(`name: e2e-orders
source:
  type: postgres
  url: %s
  table: orders
  watermark: updated_at
destinations:
  - type: postgres
    url: %s
    table: orders_copy
    match_on: [id]
    strategy: merge
settings:
  state:
    backend: sqlite
    path: %s
`, dsn, dsn, filepath.Join(dir, "state.db"))
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	out := runCLI(t, bin, "validate", pipeline)
	if !strings.Contains(out, "is valid") {
		t.Fatalf("validate output: %s", out)
	}

	// Run 1: full extraction.
	out = runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "loaded=50") {
		t.Fatalf("run 1 output: %s", out)
	}
	if n := pgCount(t, dsn, "SELECT COUNT(*) FROM orders_copy"); n != 50 {
		t.Fatalf("sink rows after run 1 = %d, want 50", n)
	}

	// Run 2: nothing new.
	out = runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "loaded=0") {
		t.Fatalf("run 2 should load 0 rows: %s", out)
	}

	// New + updated rows → only the delta syncs.
	pgExec(t, dsn,
		`INSERT INTO orders (item) VALUES ('new-1'), ('new-2')`,
		`UPDATE orders SET item = 'changed', updated_at = NOW() WHERE id = 1`,
	)
	out = runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "loaded=3") {
		t.Fatalf("run 3 should load exactly the 3-row delta: %s", out)
	}
	if n := pgCount(t, dsn, "SELECT COUNT(*) FROM orders_copy"); n != 52 {
		t.Fatalf("sink rows after run 3 = %d, want 52", n)
	}
	if n := pgCount(t, dsn, "SELECT COUNT(*) FROM orders_copy WHERE item = 'changed'"); n != 1 {
		t.Fatalf("updated row not merged into sink")
	}

	// Status reflects the last run.
	out = runCLI(t, bin, "status", pipeline)
	if !strings.Contains(out, "success") || !strings.Contains(out, "e2e-orders") {
		t.Fatalf("status output: %s", out)
	}
}

// TestE2E_DLQReplay covers the reliability loop with the real binary:
// failed rows dead-letter, the run stays green, replay re-delivers.
func TestE2E_DLQReplay(t *testing.T) {
	bin := buildBinary(t)
	dsn := startPostgres(t)

	pgExec(t, dsn,
		`CREATE TABLE src (id BIGSERIAL PRIMARY KEY, v TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())`,
		`INSERT INTO src (v) VALUES ('a'), ('b'), ('c')`,
	)

	dir := t.TempDir()
	dlqPath := filepath.Join(dir, "e2e.dlq.jsonl")
	pipeline := filepath.Join(dir, "pipeline.yaml")
	yaml := fmt.Sprintf(`name: e2e-dlq
source:
  type: postgres
  url: %s
  table: src
  watermark: updated_at
destinations:
  - type: postgres
    url: %s
    table: missing_sink
    match_on: [id]
    strategy: merge
settings:
  state:
    backend: sqlite
    path: %s
  on_error: dlq
  dlq_path: %s
`, dsn, dsn, filepath.Join(dir, "state.db"), dlqPath)
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	// Sink table missing → all rows dead-letter, run still exits 0.
	out := runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "errors=3") {
		t.Fatalf("run should dead-letter 3 rows: %s", out)
	}
	if _, err := os.Stat(dlqPath); err != nil {
		t.Fatalf("dlq file missing: %v", err)
	}

	// Create the sink, replay: rows deliver, file removed.
	pgExec(t, dsn, `CREATE TABLE missing_sink (id BIGINT PRIMARY KEY, v TEXT, updated_at TIMESTAMPTZ)`)
	out = runCLI(t, bin, "dlq", "replay", pipeline)
	if !strings.Contains(out, "replayed=3") {
		t.Fatalf("replay output: %s", out)
	}
	if n := pgCount(t, dsn, "SELECT COUNT(*) FROM missing_sink"); n != 3 {
		t.Fatalf("sink rows after replay = %d, want 3", n)
	}
	if _, err := os.Stat(dlqPath); !os.IsNotExist(err) {
		t.Fatalf("dlq file should be removed after full replay")
	}
}

// TestE2E_PostgresStateBackend runs the binary with shared Postgres state
// (settings.state.backend: postgres) — the multi-instance deployment mode.
func TestE2E_PostgresStateBackend(t *testing.T) {
	bin := buildBinary(t)
	dsn := startPostgres(t)

	pgExec(t, dsn,
		`CREATE TABLE items (id BIGSERIAL PRIMARY KEY, v TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())`,
		`CREATE TABLE items_copy (id BIGINT PRIMARY KEY, v TEXT, updated_at TIMESTAMPTZ)`,
		`INSERT INTO items (v) SELECT 'v-' || i FROM generate_series(1, 25) i`,
	)

	dir := t.TempDir()
	pipeline := filepath.Join(dir, "pipeline.yaml")
	yaml := fmt.Sprintf(`name: e2e-pgstate
source:
  type: postgres
  url: %s
  table: items
  watermark: updated_at
destinations:
  - type: postgres
    url: %s
    table: items_copy
    match_on: [id]
    strategy: merge
settings:
  state:
    backend: postgres
    connection: %s
`, dsn, dsn, dsn)
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	out := runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "loaded=25") {
		t.Fatalf("run 1: %s", out)
	}
	// Second run reads the watermark from Postgres — no local state exists.
	out = runCLI(t, bin, "run", pipeline)
	if !strings.Contains(out, "loaded=0") {
		t.Fatalf("run 2 should load 0 via shared state: %s", out)
	}
	// State tables live in the shared database.
	if n := pgCount(t, dsn, "SELECT COUNT(*) FROM vortara_watermarks"); n != 1 {
		t.Fatalf("vortara_watermarks rows = %d, want 1", n)
	}
	out = runCLI(t, bin, "watermark", "get", pipeline)
	if !strings.Contains(out, "Watermark: 2") { // year 2xxx timestamp
		t.Fatalf("watermark get: %s", out)
	}
}
