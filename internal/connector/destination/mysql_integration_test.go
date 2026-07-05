//go:build integration

package destination

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func startMySQLDestContainer(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "vortara",
			"MYSQL_DATABASE":      "vortara",
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping integration test; unable to start mysql container: %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container port: %v", err)
	}
	dsn = fmt.Sprintf("root:vortara@tcp(%s:%s)/vortara?parseTime=true", host, port.Port())
	return dsn, func() { _ = container.Terminate(context.Background()) }
}

func mysqlDestExec(t *testing.T, dsn string, stmts ...string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err = db.Ping(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mysql never became ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	return db
}

func mysqlCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

func mysqlDestRows(n, offset int) []row.Row {
	rows := make([]row.Row, n)
	for i := range rows {
		id := offset + i
		rows[i] = row.Row{
			ID:         fmt.Sprintf("row-%d", id),
			PrimaryKey: fmt.Sprintf("id=%d", id),
			Data: map[string]interface{}{
				"id":   int64(id),
				"name": fmt.Sprintf("name-%d", id),
			},
			ExtractedAt: time.Now(),
		}
	}
	return rows
}

func connectMySQLDest(t *testing.T, dsn, strategy, matchOn string) *MySQLDestination {
	t.Helper()
	d := NewMySQLDestination()
	if err := d.Connect(context.Background(), config.DestinationConfig{
		Connection: dsn,
		Strategy:   strategy,
		MatchOn:    matchOn,
		Options:    map[string]string{"table": "deals"},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return d
}

func TestMySQLDestination_Integration_AllStrategies(t *testing.T) {
	dsn, cleanup := startMySQLDestContainer(t)
	defer cleanup()

	db := mysqlDestExec(t, dsn,
		"CREATE TABLE deals (id BIGINT PRIMARY KEY, name TEXT)",
	)
	defer db.Close()
	clear := func() { mysqlDestExec(t, dsn, "TRUNCATE TABLE deals") }

	t.Run("merge upserts and dedupes", func(t *testing.T) {
		clear()
		d := connectMySQLDest(t, dsn, "merge", "id")
		defer d.Close()
		store := state.NewMemoryStore()

		res, err := d.Load(context.Background(), mysqlDestRows(100, 0), store, "p", "mysql")
		if err != nil || res.Loaded != 100 {
			t.Fatalf("load = %+v err %v, want 100 loaded", res, err)
		}
		// Same primary keys, new row IDs, changed names → updates not dupes.
		updated := mysqlDestRows(100, 0)
		for i := range updated {
			updated[i].ID = fmt.Sprintf("run2-row-%d", i)
			updated[i].Data["name"] = fmt.Sprintf("updated-%d", i)
		}
		if _, err := d.Load(context.Background(), updated, store, "p", "mysql"); err != nil {
			t.Fatalf("second load: %v", err)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals"); n != 100 {
			t.Fatalf("rows after merge = %d, want 100", n)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals WHERE name LIKE 'updated-%'"); n != 100 {
			t.Fatalf("updated rows = %d, want 100", n)
		}
		// Delivery-log idempotency: replaying identical row IDs skips.
		res, err = d.Load(context.Background(), updated, store, "p", "mysql")
		if err != nil || res.Skipped != 100 {
			t.Fatalf("replay = %+v err %v, want 100 skipped", res, err)
		}
	})

	t.Run("append inserts duplicates", func(t *testing.T) {
		clear()
		mysqlDestExec(t, dsn, "ALTER TABLE deals DROP PRIMARY KEY")
		defer mysqlDestExec(t, dsn, "TRUNCATE TABLE deals", "ALTER TABLE deals ADD PRIMARY KEY (id)")
		d := connectMySQLDest(t, dsn, "append", "")
		defer d.Close()
		store := state.NewMemoryStore()
		for i := 0; i < 2; i++ {
			if _, err := d.Load(context.Background(), mysqlDestRows(50, 0), store, "p", "mysql"); err != nil {
				t.Fatalf("append load %d: %v", i, err)
			}
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals"); n != 100 {
			t.Fatalf("rows after 2 appends = %d, want 100 (duplicates allowed)", n)
		}
	})

	t.Run("replace truncates per run", func(t *testing.T) {
		clear()
		d := connectMySQLDest(t, dsn, "replace", "")
		defer d.Close()
		store := state.NewMemoryStore()

		// Run 1 (run ID 1): two Load calls must truncate only once.
		ctx1 := context.WithValue(context.Background(), "vortara_run_id", int64(1))
		if _, err := d.Load(ctx1, mysqlDestRows(30, 0), store, "p", "mysql"); err != nil {
			t.Fatalf("replace load a: %v", err)
		}
		if _, err := d.Load(ctx1, mysqlDestRows(30, 100), store, "p", "mysql"); err != nil {
			t.Fatalf("replace load b: %v", err)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals"); n != 60 {
			t.Fatalf("rows after run 1 = %d, want 60 (truncate once per run)", n)
		}

		// Run 2 truncates again.
		ctx2 := context.WithValue(context.Background(), "vortara_run_id", int64(2))
		if _, err := d.Load(ctx2, mysqlDestRows(10, 0), store, "p", "mysql"); err != nil {
			t.Fatalf("replace run 2: %v", err)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals"); n != 10 {
			t.Fatalf("rows after run 2 = %d, want 10 (fresh truncate)", n)
		}
	})

	t.Run("delete+insert replaces matching keys", func(t *testing.T) {
		clear()
		d := connectMySQLDest(t, dsn, "delete+insert", "id")
		defer d.Close()
		store := state.NewMemoryStore()
		if _, err := d.Load(context.Background(), mysqlDestRows(20, 0), store, "p", "mysql"); err != nil {
			t.Fatalf("initial load: %v", err)
		}
		redo := mysqlDestRows(5, 0)
		for i := range redo {
			redo[i].ID = fmt.Sprintf("redo-%d", i)
			redo[i].Data["name"] = "redone"
		}
		if _, err := d.Load(context.Background(), redo, store, "p", "mysql"); err != nil {
			t.Fatalf("redo load: %v", err)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals"); n != 20 {
			t.Fatalf("rows = %d, want 20 (5 replaced in place)", n)
		}
		if n := mysqlCount(t, db, "SELECT COUNT(*) FROM deals WHERE name = 'redone'"); n != 5 {
			t.Fatalf("redone rows = %d, want 5", n)
		}
	})
}
