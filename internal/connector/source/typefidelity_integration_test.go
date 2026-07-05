//go:build integration

package source

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func pgExec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	for _, stmt := range stmts {
		if _, err := pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
}

func extractPGRows(t *testing.T, src *PostgresSource, watermark, intervalEnd time.Time) []row.Row {
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

// TestPostgresSource_Integration_TypeFidelity seeds one row of every
// interesting column type and asserts the exact Go type and value of each
// extracted cell. This is the regression net for the class of bug where a
// driver value is stringified wrongly (e.g. JSONB rendered as Go map syntax).
func TestPostgresSource_Integration_TypeFidelity(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE fidelity (
			id          BIGSERIAL PRIMARY KEY,
			s_text      TEXT,
			n_int       INTEGER,
			n_big       BIGINT,
			n_num       NUMERIC(12,2),
			f_float     DOUBLE PRECISION,
			b_bool      BOOLEAN,
			j_jsonb     JSONB,
			j_json      JSON,
			u_uuid      UUID,
			t_tstz      TIMESTAMPTZ,
			x_null      TEXT,
			updated_at  TIMESTAMPTZ DEFAULT NOW()
		)`,
		`INSERT INTO fidelity
			(s_text, n_int, n_big, n_num, f_float, b_bool, j_jsonb, j_json, u_uuid, t_tstz, x_null, updated_at)
		VALUES (
			'hello', 42, 9007199254740993, 1234.56, 2.5, TRUE,
			'{"tier": "gold", "n": 7}', '[1, "two"]',
			'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
			'2026-01-15T12:30:00Z', NULL, '2026-01-01T00:00:00Z'
		)`,
	)

	src := NewPostgresSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection:      dsn,
		Table:           "fidelity",
		WatermarkColumn: "updated_at",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	rows := extractPGRows(t, src, time.Time{}, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	if len(rows) != 1 {
		t.Fatalf("extracted %d rows, want 1", len(rows))
	}
	d := rows[0].Data

	if v, ok := d["s_text"].(string); !ok || v != "hello" {
		t.Errorf("s_text = %v (%T), want string hello", d["s_text"], d["s_text"])
	}
	if v, ok := d["n_int"].(int64); !ok || v != 42 {
		t.Errorf("n_int = %v (%T), want int64 42", d["n_int"], d["n_int"])
	}
	if v, ok := d["n_big"].(int64); !ok || v != 9007199254740993 {
		t.Errorf("n_big = %v (%T), want int64 9007199254740993 (exact, no float rounding)", d["n_big"], d["n_big"])
	}
	if v, ok := d["n_num"].(float64); !ok || v != 1234.56 {
		t.Errorf("n_num = %v (%T), want float64 1234.56", d["n_num"], d["n_num"])
	}
	if v, ok := d["f_float"].(float64); !ok || v != 2.5 {
		t.Errorf("f_float = %v (%T), want float64 2.5", d["f_float"], d["f_float"])
	}
	if v, ok := d["b_bool"].(bool); !ok || v != true {
		t.Errorf("b_bool = %v (%T), want bool true", d["b_bool"], d["b_bool"])
	}

	// JSONB/JSON must arrive as valid JSON text.
	jsonbStr, ok := d["j_jsonb"].(string)
	if !ok {
		t.Fatalf("j_jsonb = %v (%T), want string", d["j_jsonb"], d["j_jsonb"])
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(jsonbStr), &obj); err != nil {
		t.Fatalf("j_jsonb %q is not valid JSON: %v", jsonbStr, err)
	}
	if obj["tier"] != "gold" {
		t.Errorf("j_jsonb = %v, want tier=gold", obj)
	}
	jsonStr, ok := d["j_json"].(string)
	if !ok {
		t.Fatalf("j_json = %v (%T), want string", d["j_json"], d["j_json"])
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
		t.Fatalf("j_json %q is not valid JSON: %v", jsonStr, err)
	}

	if v, ok := d["u_uuid"].(string); !ok || v != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("u_uuid = %v (%T), want uuid string", d["u_uuid"], d["u_uuid"])
	}
	if v, ok := d["t_tstz"].(time.Time); !ok || !v.Equal(time.Date(2026, 1, 15, 12, 30, 0, 0, time.UTC)) {
		t.Errorf("t_tstz = %v (%T), want time.Time 2026-01-15T12:30Z", d["t_tstz"], d["t_tstz"])
	}
	if d["x_null"] != nil {
		t.Errorf("x_null = %v (%T), want nil", d["x_null"], d["x_null"])
	}
}

// TestPostgresSource_Integration_WatermarkWindow verifies the bounded-window
// contract: (watermark, intervalEnd] — boundary rows land in exactly one run.
func TestPostgresSource_Integration_WatermarkWindow(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE events (
			id         BIGSERIAL PRIMARY KEY,
			name       TEXT,
			updated_at TIMESTAMPTZ
		)`,
		`INSERT INTO events (name, updated_at) VALUES
			('old-1',    '2026-01-01T00:00:00Z'),
			('old-2',    '2026-01-02T00:00:00Z'),
			('boundary', '2026-01-03T00:00:00Z'),
			('new-1',    '2026-01-04T00:00:00Z'),
			('too-new',  '2026-01-06T00:00:00Z')`,
	)

	src := NewPostgresSource()
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

	// First run: zero watermark → everything up to and including the boundary.
	rows := extractPGRows(t, src, time.Time{}, boundary)
	if len(rows) != 3 {
		t.Fatalf("first window extracted %d rows, want 3", len(rows))
	}

	// Second run: strictly after the boundary, up to intervalEnd.
	// The boundary row must not repeat; too-new must wait for run 3.
	rows = extractPGRows(t, src, boundary, intervalEnd)
	if len(rows) != 1 || rows[0].Data["name"] != "new-1" {
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = rows[i].Data["name"].(string)
			_ = r
		}
		t.Fatalf("second window = %v, want exactly [new-1]", names)
	}
}

// TestRedshiftSource_Integration_PostgresWire verifies the redshift source
// end-to-end over the postgres wire protocol (which is what Redshift speaks),
// including the redshift:// scheme normalization.
func TestRedshiftSource_Integration_PostgresWire(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE rs_events (
			id         BIGSERIAL PRIMARY KEY,
			name       TEXT,
			updated_at TIMESTAMPTZ
		)`,
		`INSERT INTO rs_events (name, updated_at) VALUES
			('a', '2026-01-01T00:00:00Z'),
			('b', '2026-01-02T00:00:00Z')`,
	)

	redshiftURI := "redshift://" + strings.TrimPrefix(dsn, "postgres://")

	src := NewRedshiftSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Type:            "redshift",
		Connection:      redshiftURI,
		Table:           "rs_events",
		WatermarkColumn: "updated_at",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row, 10)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), out)
	}()
	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("extracted %d rows, want 2", len(rows))
	}
	if rows[0].Source != "redshift.rs_events" {
		t.Fatalf("Source = %q, want redshift.rs_events", rows[0].Source)
	}
}

// TestPostgresSource_Integration_SnapshotMode: watermark: none extracts the
// full table on every run, even for tables with no timestamp column.
func TestPostgresSource_Integration_SnapshotMode(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT)`,
		`INSERT INTO countries VALUES ('IN', 'India'), ('US', 'United States'), ('DE', 'Germany')`,
	)

	src := NewPostgresSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection:      dsn,
		Table:           "countries",
		WatermarkColumn: "none",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	for run := 1; run <= 2; run++ {
		rows := extractPGRows(t, src, time.Time{}, time.Now())
		if len(rows) != 3 {
			t.Fatalf("run %d extracted %d rows, want full snapshot of 3", run, len(rows))
		}
		if rows[0].PrimaryKey == "" || rows[0].PrimaryKey == rows[0].ID {
			t.Fatalf("run %d: PrimaryKey not derived from table PK: %q", run, rows[0].PrimaryKey)
		}
	}
}

// TestPostgresSource_Integration_WatermarkTypeValidation: a non-timestamp
// watermark column fails with a clear error, not a SQL type error.
func TestPostgresSource_Integration_WatermarkTypeValidation(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE seq_rows (id BIGSERIAL PRIMARY KEY, name TEXT)`,
		`INSERT INTO seq_rows (name) VALUES ('a')`,
	)

	// Integer watermark column → clear guidance.
	src := NewPostgresSource()
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
	err := <-done
	_ = src.Close()
	if err == nil || !strings.Contains(err.Error(), "ExtractNumeric") {
		t.Fatalf("Extract() with integer watermark = %v, want routing hint", err)
	}

	// Missing watermark column (the silent default updated_at) → clear guidance.
	src = NewPostgresSource()
	if err := src.Connect(context.Background(), config.SourceConfig{
		Connection: dsn, Table: "seq_rows", WatermarkColumn: "updated_at",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	out = make(chan row.Row, 10)
	done = make(chan error, 1)
	go func() { done <- src.Extract(context.Background(), time.Time{}, time.Now(), out) }()
	for range out {
	}
	err = <-done
	_ = src.Close()
	if err == nil || !strings.Contains(err.Error(), "not found in table") {
		t.Fatalf("Extract() with missing watermark column = %v, want clear missing-column error", err)
	}
}

// TestPostgresSource_Integration_NumericCursor covers integer-PK cursor
// extraction end to end: keyset pagination, cursor advancement, delta
// extraction, and max_rows-style limits.
func TestPostgresSource_Integration_NumericCursor(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pgExec(t, dsn,
		`CREATE TABLE seq_events (id BIGSERIAL PRIMARY KEY, name TEXT)`,
		`INSERT INTO seq_events (name) SELECT 'e-' || i FROM generate_series(1, 250) i`,
	)

	src := NewPostgresSource()
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

	extract := func(cursor, limit int64) ([]row.Row, int64) {
		out := make(chan row.Row, 300)
		done := make(chan error, 1)
		var maxCur int64
		go func() {
			var e error
			maxCur, e = src.ExtractNumeric(context.Background(), cursor, limit, out)
			done <- e
		}()
		var rows []row.Row
		for r := range out {
			rows = append(rows, r)
		}
		if err := <-done; err != nil {
			t.Fatalf("ExtractNumeric() error = %v", err)
		}
		return rows, maxCur
	}

	// Full pass across multiple keyset pages.
	rows, maxCur := extract(0, 0)
	if len(rows) != 250 || maxCur != 250 {
		t.Fatalf("full pass = %d rows, cursor %d; want 250/250", len(rows), maxCur)
	}

	// Delta: nothing new.
	rows, maxCur = extract(250, 0)
	if len(rows) != 0 || maxCur != 250 {
		t.Fatalf("empty delta = %d rows, cursor %d; want 0/250", len(rows), maxCur)
	}

	// New rows → exactly the delta.
	pgExec(t, dsn, `INSERT INTO seq_events (name) VALUES ('new-1'), ('new-2')`)
	rows, maxCur = extract(250, 0)
	if len(rows) != 2 || maxCur != 252 {
		t.Fatalf("delta = %d rows, cursor %d; want 2/252", len(rows), maxCur)
	}

	// Limit caps a run and the returned cursor resumes exactly.
	rows, maxCur = extract(0, 120)
	if len(rows) != 120 || maxCur != 120 {
		t.Fatalf("capped = %d rows, cursor %d; want 120/120", len(rows), maxCur)
	}
	rows, maxCur = extract(maxCur, 0)
	if len(rows) != 132 || maxCur != 252 {
		t.Fatalf("resume = %d rows, cursor %d; want 132/252", len(rows), maxCur)
	}
}
