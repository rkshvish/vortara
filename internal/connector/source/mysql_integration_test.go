//go:build integration

package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func startMySQLContainer(t *testing.T) (dsn string, cleanup func()) {
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
	return dsn, func() {
		_ = container.Terminate(context.Background())
	}
}

func mysqlExec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	// The container accepts TCP connections slightly before auth is ready.
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
}

func extractMySQLRows(t *testing.T, src *MySQLSource, watermark, intervalEnd time.Time) []row.Row {
	t.Helper()
	out := make(chan row.Row, 100)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), watermark, intervalEnd, out)
	}()
	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	return rows
}

func TestMySQLSource_Integration_ExtractAndTypes(t *testing.T) {
	dsn, cleanup := startMySQLContainer(t)
	defer cleanup()

	mysqlExec(t, dsn,
		`CREATE TABLE deals (
			id         BIGINT AUTO_INCREMENT PRIMARY KEY,
			name       TEXT,
			meta       JSON,
			amount     DECIMAL(12,2),
			active     BOOLEAN,
			nothing    TEXT,
			updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
		)`,
		`INSERT INTO deals (name, meta, amount, active, nothing, updated_at) VALUES
			('alpha', '{"tier": "gold", "score": 9}', 1234.56, TRUE,  NULL, '2026-01-01 10:00:00'),
			('beta',  '[1, 2, 3]',                    0.01,   FALSE, NULL, '2026-01-02 10:00:00'),
			('gamma', NULL,                            99.99,  TRUE,  NULL, '2026-01-03 10:00:00')`,
	)

	src := NewMySQLSource()
	err := src.Connect(context.Background(), config.SourceConfig{
		Connection:      dsn,
		Table:           "deals",
		WatermarkColumn: "updated_at",
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	rows := extractMySQLRows(t, src, time.Time{}, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	if len(rows) != 3 {
		t.Fatalf("extracted %d rows, want 3", len(rows))
	}

	first := rows[0]
	if first.PrimaryKey != "id=1" {
		t.Fatalf("PrimaryKey = %q, want id=1", first.PrimaryKey)
	}
	if first.Watermark.IsZero() {
		t.Fatal("Watermark not populated from updated_at")
	}

	// Type fidelity: JSON columns must round-trip as valid JSON text,
	// not driver bytes or Go map syntax.
	metaStr, ok := first.Data["meta"].(string)
	if !ok {
		t.Fatalf("meta type = %T, want string", first.Data["meta"])
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		t.Fatalf("meta %q is not valid JSON: %v", metaStr, err)
	}
	if meta["tier"] != "gold" {
		t.Fatalf("meta = %v, want tier=gold", meta)
	}

	if name, ok := first.Data["name"].(string); !ok || name != "alpha" {
		t.Fatalf("name = %v (%T), want string alpha", first.Data["name"], first.Data["name"])
	}
	if _, ok := first.Data["updated_at"].(time.Time); !ok {
		t.Fatalf("updated_at type = %T, want time.Time", first.Data["updated_at"])
	}
	if first.Data["nothing"] != nil {
		t.Fatalf("NULL column = %v, want nil", first.Data["nothing"])
	}
	// DECIMAL arrives as text from the driver; it must at least be a
	// clean numeric string, never raw bytes.
	if amt, ok := first.Data["amount"].(string); !ok || amt != "1234.56" {
		t.Fatalf("amount = %v (%T), want string 1234.56", first.Data["amount"], first.Data["amount"])
	}
}

func TestMySQLSource_Integration_WatermarkWindow(t *testing.T) {
	dsn, cleanup := startMySQLContainer(t)
	defer cleanup()

	mysqlExec(t, dsn,
		`CREATE TABLE events (
			id         BIGINT AUTO_INCREMENT PRIMARY KEY,
			name       TEXT,
			updated_at TIMESTAMP(6)
		)`,
		`INSERT INTO events (name, updated_at) VALUES
			('old-1',      '2026-01-01 00:00:00'),
			('old-2',      '2026-01-02 00:00:00'),
			('boundary',   '2026-01-03 00:00:00'),
			('new-1',      '2026-01-04 00:00:00'),
			('too-new',    '2026-01-06 00:00:00')`,
	)

	src := NewMySQLSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection:      dsn,
		Table:           "events",
		WatermarkColumn: "updated_at",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	boundary := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	intervalEnd := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

	// First run: zero watermark extracts everything up to the boundary inclusive.
	rows := extractMySQLRows(t, src, time.Time{}, boundary)
	if len(rows) != 3 {
		t.Fatalf("first window extracted %d rows, want 3 (old-1, old-2, boundary)", len(rows))
	}

	// Second run: (boundary, intervalEnd] — the boundary row must NOT repeat.
	rows = extractMySQLRows(t, src, boundary, intervalEnd)
	if len(rows) != 1 {
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = fmt.Sprintf("%v", r.Data["name"])
		}
		t.Fatalf("second window extracted %d rows %v, want exactly [new-1]", len(rows), names)
	}
	if rows[0].Data["name"] != "new-1" {
		t.Fatalf("second window row = %v, want new-1", rows[0].Data["name"])
	}
}

// TestMySQLSource_Integration_SnapshotAndValidation covers watermark: none
// full-snapshot extraction and clear errors for bad watermark columns.
func TestMySQLSource_Integration_SnapshotAndValidation(t *testing.T) {
	dsn, cleanup := startMySQLContainer(t)
	defer cleanup()

	mysqlExec(t, dsn,
		"CREATE TABLE countries (code VARCHAR(2) PRIMARY KEY, name TEXT)",
		"INSERT INTO countries VALUES ('IN', 'India'), ('US', 'United States')",
		"CREATE TABLE seq_rows (id BIGINT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
		"INSERT INTO seq_rows (name) VALUES ('a')",
	)

	// Snapshot: full table both runs, no timestamp column needed.
	src := NewMySQLSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection: dsn, Table: "countries", WatermarkColumn: "none",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	for run := 1; run <= 2; run++ {
		rows := extractMySQLRows(t, src, time.Time{}, time.Now())
		if len(rows) != 2 {
			t.Fatalf("run %d extracted %d rows, want 2", run, len(rows))
		}
	}
	_ = src.Close()

	// Integer watermark column → clear error.
	src = NewMySQLSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection: dsn, Table: "seq_rows", WatermarkColumn: "id",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	out := make(chan row.Row, 10)
	done := make(chan error, 1)
	go func() { done <- src.Extract(context.Background(), time.Time{}, time.Now(), out) }()
	for range out {
	}
	if err := <-done; err == nil || !strings.Contains(err.Error(), "ExtractNumeric") {
		t.Fatalf("Extract() with integer watermark = %v, want clear type error", err)
	}
	_ = src.Close()
}

// TestMySQLSource_Integration_NumericCursor covers integer-PK keyset
// extraction: full pass, empty delta, new-row delta, and limits.
func TestMySQLSource_Integration_NumericCursor(t *testing.T) {
	dsn, cleanup := startMySQLContainer(t)
	defer cleanup()

	mysqlExec(t, dsn,
		"CREATE TABLE seq_events (id BIGINT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
		"INSERT INTO seq_events (name) SELECT CONCAT('e-', n) FROM (WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 250) SELECT n FROM seq) t",
	)

	src := NewMySQLSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection: dsn, Table: "seq_events", WatermarkColumn: "id", BatchSize: 100,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	kind, err := src.CursorKind(context.Background())
	if err != nil || kind != "int" {
		t.Fatalf("CursorKind() = %q, %v; want int", kind, err)
	}

	extract := func(cursor, limit int64) (int, int64) {
		out := make(chan row.Row, 300)
		done := make(chan error, 1)
		var maxCur int64
		go func() {
			var e error
			maxCur, e = src.ExtractNumeric(context.Background(), cursor, limit, out)
			done <- e
		}()
		n := 0
		for range out {
			n++
		}
		if err := <-done; err != nil {
			t.Fatalf("ExtractNumeric() error = %v", err)
		}
		return n, maxCur
	}

	if n, cur := extract(0, 0); n != 250 || cur != 250 {
		t.Fatalf("full pass = %d rows cursor %d, want 250/250", n, cur)
	}
	if n, cur := extract(250, 0); n != 0 || cur != 250 {
		t.Fatalf("empty delta = %d rows cursor %d, want 0/250", n, cur)
	}
	mysqlExec(t, dsn, "INSERT INTO seq_events (name) VALUES ('new-1'), ('new-2')")
	// MySQL auto-increment may reserve gaps for multi-row inserts, so the
	// new ids can exceed 252 — assert the delta count and cursor progress.
	if n, cur := extract(250, 0); n != 2 || cur <= 250 {
		t.Fatalf("delta = %d rows cursor %d, want 2 rows with advanced cursor", n, cur)
	}
	if n, cur := extract(0, 120); n != 120 || cur != 120 {
		t.Fatalf("capped = %d rows cursor %d, want 120/120", n, cur)
	}
}
