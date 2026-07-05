//go:build integration

package destination

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func startPostgresDestinationContainer(t *testing.T) (dsn string, cleanup func()) {
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

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container port: %v", err)
	}

	dsn = fmt.Sprintf("postgres://vortara:vortara@%s:%s/vortara?sslmode=disable", host, port.Port())
	return dsn, func() {
		_ = container.Terminate(context.Background())
	}
}

func openDestinationTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping: %v", err)
	}
	return db
}

func createDestinationTestTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS deals (
	id BIGINT PRIMARY KEY,
	name TEXT NOT NULL,
	value BIGINT NOT NULL
);`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func newDestinationTestConfig(dsn string) config.DestinationConfig {
	return config.DestinationConfig{
		Connection: dsn,
		MatchOn:    "id",
		Options:    map[string]string{"table": "deals"},
	}
}

func newDestinationCopyConfig(dsn, strategy string) config.DestinationConfig {
	cfg := newDestinationTestConfig(dsn)
	cfg.Strategy = strategy
	return cfg
}

func TestPostgresDestination_Upsert(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	createDestinationTestTable(t, db)

	dst := NewPostgresDestination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dst.Connect(ctx, newDestinationTestConfig(dsn)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": int64(1), "name": "foo", "value": int64(10)}, time.Now()),
		row.NewRow("source", "pipeline", "id=2", map[string]interface{}{"id": int64(2), "name": "bar", "value": int64(20)}, time.Now()),
	}

	res, err := dst.Load(ctx, rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 2 {
		t.Fatalf("expected 2 loaded, got %d", res.Loaded)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM deals`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}

	res, err = dst.Load(ctx, rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}
	if res.Skipped != 2 {
		t.Fatalf("expected 2 skipped, got %d", res.Skipped)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM deals`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after idempotent reload, got %d", count)
	}
}

func TestPostgresDestination_UpsertUpdate(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	createDestinationTestTable(t, db)

	dst := NewPostgresDestination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dst.Connect(ctx, newDestinationTestConfig(dsn)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()

	store := state.NewMemoryStore()
	first := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": int64(1), "name": "foo", "value": int64(10)}, time.Now())
	second := row.NewRow("source", "pipeline", "id=1b", map[string]interface{}{"id": int64(1), "name": "bar", "value": int64(20)}, time.Now())

	if _, err := dst.Load(ctx, []row.Row{first}, store, "pipeline", "dest"); err != nil {
		t.Fatalf("first Load() error = %v", err)
	}
	if _, err := dst.Load(ctx, []row.Row{second}, store, "pipeline", "dest"); err != nil {
		t.Fatalf("second Load() error = %v", err)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM deals WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if name != "bar" {
		t.Fatalf("expected updated name bar, got %q", name)
	}
}

func TestPostgresDestination_ReservedWordTable(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	_, err := db.Exec(`
DROP TABLE IF EXISTS "order";
CREATE TABLE "order" (
	id BIGINT PRIMARY KEY,
	name TEXT NOT NULL
);`)
	if err != nil {
		t.Fatalf("create reserved-word table: %v", err)
	}

	dst := NewPostgresDestination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dst.Connect(ctx, config.DestinationConfig{
		Connection: dsn,
		MatchOn:    "id",
		Options:    map[string]string{"table": "order"},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": int64(1), "name": "foo"}, time.Now())
	res, err := dst.Load(ctx, []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("expected 1 loaded, got %d", res.Loaded)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM "order" WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if name != "foo" {
		t.Fatalf("expected inserted name foo, got %q", name)
	}
}

func TestPostgresDestination_COPY_Append(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	createDestinationTestTable(t, db)

	dst := NewPostgresDestination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dst.Connect(ctx, newDestinationCopyConfig(dsn, "append")); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()

	store := state.NewMemoryStore()
	rows := make([]row.Row, 0, 1000)
	for i := 0; i < 1000; i++ {
		rows = append(rows, row.NewRow("source", "pipeline", fmt.Sprintf("id=%d", i), map[string]interface{}{"id": int64(i), "name": fmt.Sprintf("name-%d", i), "value": int64(i)}, time.Now()))
	}

	start := time.Now()
	res, err := dst.Load(ctx, rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	t.Logf("COPY append duration: %s", time.Since(start))
	if res.Loaded != 1000 {
		t.Fatalf("expected 1000 loaded, got %d", res.Loaded)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM deals`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1000 {
		t.Fatalf("expected 1000 rows, got %d", count)
	}
}

func TestPostgresDestination_COPY_Replace(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	createDestinationTestTable(t, db)

	dst := NewPostgresDestination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dst.Connect(ctx, newDestinationCopyConfig(dsn, "replace")); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()

	store := state.NewMemoryStore()
	first := make([]row.Row, 0, 500)
	second := make([]row.Row, 0, 500)
	for i := 0; i < 500; i++ {
		first = append(first, row.NewRow("source", "pipeline", fmt.Sprintf("id=%d", i), map[string]interface{}{"id": int64(i), "name": fmt.Sprintf("first-%d", i), "value": int64(i)}, time.Now()))
		second = append(second, row.NewRow("source", "pipeline", fmt.Sprintf("id=%d", i+1000), map[string]interface{}{"id": int64(i + 1000), "name": fmt.Sprintf("second-%d", i), "value": int64(i)}, time.Now()))
	}

	if _, err := dst.Load(ctx, first, store, "pipeline", "dest"); err != nil {
		t.Fatalf("first Load() error = %v", err)
	}
	if _, err := dst.Load(ctx, second, store, "pipeline", "dest"); err != nil {
		t.Fatalf("second Load() error = %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM deals`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 500 {
		t.Fatalf("expected 500 rows after replace, got %d", count)
	}
}

// TestPostgresDestination_AtomicReplace proves the staging swap contract:
// a failed replace run leaves the target completely untouched; a successful
// one swaps the new snapshot in atomically.
func TestPostgresDestination_AtomicReplace(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	createDestinationTestTable(t, db)

	// Pre-existing data that must survive a failed run.
	if _, err := db.Exec(`INSERT INTO deals (id, name, value) SELECT i, 'old-' || i, i FROM generate_series(1, 40) i`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dst := NewPostgresDestination()
	ctx, cancelConn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelConn()
	if err := dst.Connect(ctx, newDestinationCopyConfig(dsn, "replace")); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()
	store := state.NewMemoryStore()

	mkRows := func(n int, prefix string) []row.Row {
		rows := make([]row.Row, n)
		for i := range rows {
			rows[i] = row.NewRow("s", "p", fmt.Sprintf("id=%d", i+1),
				map[string]interface{}{"id": int64(i + 1), "name": fmt.Sprintf("%s-%d", prefix, i+1), "value": int64(i)}, time.Now())
		}
		return rows
	}
	count := func(q string) int {
		var n int
		if err := db.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Run 1 fails after loading into staging: target untouched.
	runCtx := withStrategy(withRunID(context.Background(), 101), "replace")
	if _, err := dst.Load(runCtx, mkRows(500, "half"), store, "p", "d"); err != nil {
		t.Fatalf("staging load: %v", err)
	}
	if n := count("SELECT COUNT(*) FROM deals"); n != 40 {
		t.Fatalf("target during staging = %d rows, want original 40", n)
	}
	if err := dst.FinalizeRun(context.Background(), 101, false); err != nil {
		t.Fatalf("FinalizeRun(failed): %v", err)
	}
	if n := count("SELECT COUNT(*) FROM deals WHERE name LIKE 'old-%'"); n != 40 {
		t.Fatalf("failed run corrupted target: %d old rows, want 40", n)
	}
	if n := count("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'deals_vstg_%'"); n != 0 {
		t.Fatalf("staging table leaked after failed run")
	}

	// Run 2 succeeds: exactly the new snapshot, old rows gone.
	runCtx = withStrategy(withRunID(context.Background(), 102), "replace")
	if _, err := dst.Load(runCtx, mkRows(300, "new"), store, "p", "d"); err != nil {
		t.Fatalf("staging load 2: %v", err)
	}
	if err := dst.FinalizeRun(context.Background(), 102, true); err != nil {
		t.Fatalf("FinalizeRun(success): %v", err)
	}
	if n := count("SELECT COUNT(*) FROM deals"); n != 300 {
		t.Fatalf("target after swap = %d rows, want 300", n)
	}
	if n := count("SELECT COUNT(*) FROM deals WHERE name LIKE 'old-%'"); n != 0 {
		t.Fatalf("old rows survived the swap")
	}
	if n := count("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'deals_vstg_%'"); n != 0 {
		t.Fatalf("staging table leaked after success")
	}
}

// TestPostgresDestination_SCD2 verifies type-2 dimension semantics: changed
// rows close the current version and insert a new one; unchanged rows do
// nothing; history is preserved.
func TestPostgresDestination_SCD2(t *testing.T) {
	dsn, cleanup := startPostgresDestinationContainer(t)
	defer cleanup()

	db := openDestinationTestDB(t, dsn)
	if _, err := db.Exec(`CREATE TABLE dims (biz_id BIGINT, name TEXT, tier TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dst := NewPostgresDestination()
	ctx, cancelConn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelConn()
	cfg := newDestinationCopyConfig(dsn, "scd2")
	cfg.MatchOn = "biz_id"
	cfg.Options["table"] = "dims"
	if err := dst.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer dst.Close()
	store := state.NewMemoryStore()

	mkRow := func(id int64, name, tier, rowID string) row.Row {
		return row.Row{
			ID: rowID, PrimaryKey: fmt.Sprintf("biz_id=%d", id),
			Data: map[string]interface{}{"biz_id": id, "name": name, "tier": tier},
		}
	}
	count := func(q string) int {
		var n int
		if err := db.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}
	loadCtx := withStrategy(context.Background(), "scd2")

	// Load v1 of two dimensions.
	res, err := dst.Load(loadCtx, []row.Row{
		mkRow(1, "acme", "silver", "r1"),
		mkRow(2, "globex", "gold", "r2"),
	}, store, "p", "d")
	if err != nil || res.Loaded != 2 {
		t.Fatalf("load v1 = %+v, %v", res, err)
	}
	if n := count(`SELECT COUNT(*) FROM dims WHERE "_scd_is_current"`); n != 2 {
		t.Fatalf("current rows = %d, want 2", n)
	}

	// acme changes tier; globex is unchanged.
	res, err = dst.Load(loadCtx, []row.Row{
		mkRow(1, "acme", "platinum", "r3"),
		mkRow(2, "globex", "gold", "r4"),
	}, store, "p", "d")
	if err != nil || res.Loaded != 2 {
		t.Fatalf("load v2 = %+v, %v", res, err)
	}

	if n := count(`SELECT COUNT(*) FROM dims`); n != 3 {
		t.Fatalf("total versions = %d, want 3 (2 acme + 1 globex)", n)
	}
	if n := count(`SELECT COUNT(*) FROM dims WHERE biz_id = 1 AND "_scd_is_current" AND tier = 'platinum'`); n != 1 {
		t.Fatalf("acme current version wrong")
	}
	if n := count(`SELECT COUNT(*) FROM dims WHERE biz_id = 1 AND NOT "_scd_is_current" AND tier = 'silver' AND "_scd_valid_to" IS NOT NULL`); n != 1 {
		t.Fatalf("acme history version wrong")
	}
	if n := count(`SELECT COUNT(*) FROM dims WHERE biz_id = 2`); n != 1 {
		t.Fatalf("globex should still have exactly 1 version")
	}

	// Redelivery of the same row IDs is skipped by the delivery log.
	res, err = dst.Load(loadCtx, []row.Row{mkRow(1, "acme", "platinum", "r3")}, store, "p", "d")
	if err != nil || res.Skipped != 1 {
		t.Fatalf("redelivery = %+v, %v; want 1 skipped", res, err)
	}
}
