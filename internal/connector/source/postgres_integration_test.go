//go:build integration

package source

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func startPostgresContainer(t *testing.T) (dsn string, cleanup func()) {
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

func openTestPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pool.Ping: %v", err)
	}
	return pool
}

func createTestTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
CREATE TABLE IF NOT EXISTS deals (
	id BIGINT PRIMARY KEY,
	name TEXT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func newTestConfig(dsn string) config.SourceConfig {
	return config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals",
		WatermarkColumn: "updated_at",
		BatchSize:       2,
		Options:         map[string]string{"pipeline": "sales-sync"},
	}
}

func TestPostgresSource_Connect(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := src.Connect(ctx, newTestConfig(dsn)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPostgresSource_Extract_Empty(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	createTestTable(t, pool)

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, newTestConfig(dsn)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	count := 0
	for range out {
		count++
	}

	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows, got %d", count)
	}
}

func TestPostgresSource_Extract_WithWatermark(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	createTestTable(t, pool)

	before := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	after := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	_, err := pool.Exec(context.Background(), `INSERT INTO deals (id, name, updated_at) VALUES ($1, $2, $3), ($4, $5, $6)`, 1, "old", before, 2, "new", after)
	if err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, newTestConfig(dsn)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, before, time.Time{}, out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0].PrimaryKey; got != "id=2" {
		t.Fatalf("expected primary key id=2, got %q", got)
	}
	if got := rows[0].Data["name"]; got != "new" {
		t.Fatalf("expected row name new, got %v", got)
	}
	if !rows[0].Watermark.Equal(after) {
		t.Fatalf("expected watermark %v, got %v", after, rows[0].Watermark)
	}
}

func TestPostgresSource_Extract_Pagination(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	createTestTable(t, pool)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		_, err := pool.Exec(context.Background(), `INSERT INTO deals (id, name, updated_at) VALUES ($1, $2, $3)`, i, fmt.Sprintf("deal-%d", i), ts)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	cfg := newTestConfig(dsn)
	cfg.BatchSize = 2

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	var ids []string
	for r := range out {
		ids = append(ids, r.PrimaryKey)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	if len(ids) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(ids))
	}
	for i, want := range []string{"id=1", "id=2", "id=3", "id=4", "id=5"} {
		if ids[i] != want {
			t.Fatalf("row %d: expected %q, got %q", i, want, ids[i])
		}
	}
}

func TestPostgresSource_Extract_CtxCancel(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	createTestTable(t, pool)

	base := time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		_, err := pool.Exec(context.Background(), `INSERT INTO deals (id, name, updated_at) VALUES ($1, $2, $3)`, i, fmt.Sprintf("deal-%d", i), ts)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	cfg := newTestConfig(dsn)
	cfg.BatchSize = 1

	src := NewPostgresSource()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	received := 0
	for range out {
		received++
		cancel()
	}

	if err := <-done; err == nil {
		t.Fatal("expected context cancellation error")
	}
	if received == 0 {
		t.Fatal("expected at least one row before cancellation")
	}
}

func TestPostgresSource_SchemaIntrospection(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_schema;
CREATE TABLE deals_schema (
	id SERIAL PRIMARY KEY,
	name TEXT,
	revenue NUMERIC(10,2),
	active BOOL,
	created_at TIMESTAMPTZ
);`)
	if err != nil {
		t.Fatalf("create schema table: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_schema",
		WatermarkColumn: "created_at",
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	schema, err := src.introspectSchema(ctx, "deals_schema")
	if err != nil {
		t.Fatalf("introspectSchema() error = %v", err)
	}
	if got, want := len(schema.Columns), 5; got != want {
		t.Fatalf("column count = %d, want %d", got, want)
	}
	if len(schema.PrimaryKeys) != 1 || schema.PrimaryKeys[0] != "id" {
		t.Fatalf("primary keys = %v, want [id]", schema.PrimaryKeys)
	}

	wantTypes := map[string]string{
		"id":         "int4",
		"name":       "text",
		"revenue":    "numeric",
		"active":     "bool",
		"created_at": "timestamptz",
	}
	for _, col := range schema.Columns {
		if got := col.DataType; got != wantTypes[col.Name] {
			t.Fatalf("column %s type = %q, want %q", col.Name, got, wantTypes[col.Name])
		}
	}
}

func TestPostgresSource_ValueConversion(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_values;
CREATE TABLE deals_values (
	id SERIAL PRIMARY KEY,
	name TEXT,
	revenue NUMERIC(10,2),
	active BOOL,
	created_at TIMESTAMPTZ
);`)
	if err != nil {
		t.Fatalf("create values table: %v", err)
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO deals_values (name, revenue, active, created_at) VALUES ($1, $2, $3, NOW())`, "Acme", 12345.67, true)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_values",
		WatermarkColumn: "created_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	data := rows[0].Data
	if _, ok := data["id"].(int64); !ok {
		t.Fatalf("id type = %T, want int64", data["id"])
	}
	if got := data["name"]; got != "Acme" {
		t.Fatalf("name = %v, want Acme", got)
	}
	if got, ok := data["revenue"].(float64); !ok || math.Abs(got-12345.67) > 1e-6 {
		t.Fatalf("revenue = %v, want 12345.67", data["revenue"])
	}
	if got, ok := data["active"].(bool); !ok || !got {
		t.Fatalf("active = %v, want true", data["active"])
	}
	if _, ok := data["created_at"].(time.Time); !ok {
		t.Fatalf("created_at type = %T, want time.Time", data["created_at"])
	}
}

func TestPostgresSource_PrimaryKeyFromSchema(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_pk;
CREATE TABLE deals_pk (
	tenant_id INT,
	deal_id INT,
	name TEXT,
	updated_at TIMESTAMPTZ,
	PRIMARY KEY (tenant_id, deal_id)
);`)
	if err != nil {
		t.Fatalf("create pk table: %v", err)
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO deals_pk (tenant_id, deal_id, name, updated_at) VALUES (1, 42, 'Acme', NOW())`)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_pk",
		WatermarkColumn: "updated_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0].PrimaryKey; got != "tenant_id=1,deal_id=42" {
		t.Fatalf("PrimaryKey = %q, want %q", got, "tenant_id=1,deal_id=42")
	}
}

func TestPostgresSource_CustomQuery_Aggregation(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_custom;
CREATE TABLE deals_custom (
	account_id INT,
	revenue NUMERIC,
	updated_at TIMESTAMPTZ DEFAULT NOW()
);`)
	if err != nil {
		t.Fatalf("create custom table: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
INSERT INTO deals_custom (account_id, revenue, updated_at) VALUES
	(1, 10, NOW()),
	(1, 15, NOW()),
	(1, 20, NOW()),
	(2, 5, NOW()),
	(2, 7, NOW()),
	(2, 8, NOW());`)
	if err != nil {
		t.Fatalf("insert aggregation rows: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:       "postgres",
		Connection: dsn,
		Query: `
SELECT
	account_id,
	SUM(revenue) AS total_revenue,
	MAX(updated_at) AS updated_at
FROM deals_custom
WHERE updated_at > {{watermark}}
  AND updated_at <= {{interval_end}}
GROUP BY account_id`,
		WatermarkColumn: "updated_at",
		Options:         map[string]string{"pipeline": "agg-sync"},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Now().UTC().Add(1*time.Minute), out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 aggregated rows, got %d", len(rows))
	}
	for _, r := range rows {
		if _, ok := r.Data["account_id"]; !ok {
			t.Fatalf("missing account_id in row: %#v", r.Data)
		}
		if total, ok := r.Data["total_revenue"].(float64); !ok || total <= 0 {
			t.Fatalf("expected numeric total_revenue, got %T %#v", r.Data["total_revenue"], r.Data["total_revenue"])
		}
	}
}

func TestPostgresSource_CustomQuery_Join(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_join;
DROP TABLE IF EXISTS accounts_join;
CREATE TABLE accounts_join (
	id INT PRIMARY KEY,
	name TEXT
);
CREATE TABLE deals_join (
	id INT PRIMARY KEY,
	account_id INT,
	revenue NUMERIC,
	updated_at TIMESTAMPTZ DEFAULT NOW()
);`)
	if err != nil {
		t.Fatalf("create join tables: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
INSERT INTO accounts_join (id, name) VALUES (1, 'Acme');
INSERT INTO deals_join (id, account_id, revenue, updated_at) VALUES (1, 1, 99, NOW());`)
	if err != nil {
		t.Fatalf("insert join rows: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:       "postgres",
		Connection: dsn,
		Query: `
SELECT
	d.id,
	a.name AS account_name,
	d.revenue,
	d.updated_at
FROM deals_join d
JOIN accounts_join a ON a.id = d.account_id
WHERE d.updated_at > {{watermark}}
  AND d.updated_at <= {{interval_end}}`,
		WatermarkColumn: "updated_at",
		Options:         map[string]string{"pipeline": "join-sync"},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Now().UTC().Add(1*time.Minute), out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 joined row, got %d", len(rows))
	}
	if got := rows[0].Data["account_name"]; got != "Acme" {
		t.Fatalf("expected joined account name, got %v", got)
	}
}

func TestPostgresSource_MixedCaseTable(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS "MyDeals";
CREATE TABLE "MyDeals" (
	id SERIAL PRIMARY KEY,
	name TEXT,
	updated_at TIMESTAMPTZ DEFAULT NOW()
);`)
	if err != nil {
		t.Fatalf("create mixed-case table: %v", err)
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO "MyDeals" (name) VALUES ('Acme')`)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "MyDeals",
		WatermarkColumn: "updated_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0].Data["name"]; got != "Acme" {
		t.Fatalf("name = %v, want Acme", got)
	}
}

func TestPostgresSource_BoundedWindow(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_window;
CREATE TABLE deals_window (
	id SERIAL PRIMARY KEY,
	name TEXT,
	updated_at TIMESTAMPTZ DEFAULT NOW()
);`)
	if err != nil {
		t.Fatalf("create bounded-window table: %v", err)
	}

	intervalEnd := time.Now().UTC()
	time.Sleep(100 * time.Millisecond)
	for _, name := range []string{"A", "B", "C"} {
		if _, err := pool.Exec(context.Background(), `INSERT INTO deals_window (name) VALUES ($1)`, name); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_window",
		WatermarkColumn: "updated_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, intervalEnd, out)
	}()

	count := 0
	for range out {
		count++
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows, got %d", count)
	}
}

func TestPostgresSource_BoundedWindow_IncludesRows(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_window2;
CREATE TABLE deals_window2 (
	id SERIAL PRIMARY KEY,
	name TEXT,
	updated_at TIMESTAMPTZ DEFAULT NOW()
);`)
	if err != nil {
		t.Fatalf("create bounded-window table: %v", err)
	}
	for _, name := range []string{"A", "B", "C"} {
		if _, err := pool.Exec(context.Background(), `INSERT INTO deals_window2 (name) VALUES ($1)`, name); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_window2",
		WatermarkColumn: "updated_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Now().UTC().Add(1*time.Minute), out)
	}()

	count := 0
	for range out {
		count++
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

func TestPostgresSource_ParallelExtract_CorrectRowCount(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_parallel;
CREATE TABLE deals_parallel (
    id   INT PRIMARY KEY,
    name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
INSERT INTO deals_parallel (id, name, updated_at)
SELECT i, 'deal-' || i, NOW() - (1000 - i) * interval '1 minute'
FROM generate_series(1, 1000) AS i;`)
	if err != nil {
		t.Fatalf("create/populate parallel table: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:               "postgres",
		Connection:         dsn,
		Table:              "deals_parallel",
		WatermarkColumn:    "updated_at",
		BatchSize:          500,
		ExtractParallelism: 4,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row, 128)
	done := make(chan error, 1)
	go func() { done <- src.Extract(ctx, time.Time{}, time.Time{}, out) }()

	seen := make(map[string]bool, 1000)
	for r := range out {
		if seen[r.PrimaryKey] {
			t.Errorf("duplicate row: %s", r.PrimaryKey)
		}
		seen[r.PrimaryKey] = true
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(seen) != 1000 {
		t.Fatalf("expected 1000 unique rows, got %d", len(seen))
	}
}

func TestPostgresSource_ParallelExtract_WithWatermark(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	boundary := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_parallel_wm;
CREATE TABLE deals_parallel_wm (
    id   INT PRIMARY KEY,
    name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);`)
	if err != nil {
		t.Fatalf("create parallel_wm table: %v", err)
	}
	// Insert 500 old rows (before boundary) and 500 new rows (after boundary).
	_, err = pool.Exec(context.Background(), `
INSERT INTO deals_parallel_wm (id, name, updated_at)
SELECT i, 'old-' || i, $1::timestamptz - (500 - i) * interval '1 second'
FROM generate_series(1, 500) AS i`, boundary)
	if err != nil {
		t.Fatalf("insert old rows: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
INSERT INTO deals_parallel_wm (id, name, updated_at)
SELECT 500 + i, 'new-' || i, $1::timestamptz + i * interval '1 second'
FROM generate_series(1, 500) AS i`, boundary)
	if err != nil {
		t.Fatalf("insert new rows: %v", err)
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:               "postgres",
		Connection:         dsn,
		Table:              "deals_parallel_wm",
		WatermarkColumn:    "updated_at",
		BatchSize:          500,
		ExtractParallelism: 4,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row, 128)
	done := make(chan error, 1)
	go func() { done <- src.Extract(ctx, boundary, time.Time{}, out) }()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 500 {
		t.Fatalf("expected 500 new rows after watermark, got %d", len(rows))
	}
	for _, r := range rows {
		name, _ := r.Data["name"].(string)
		if !strings.HasPrefix(name, "new-") {
			t.Fatalf("unexpected row name %q, expected new- prefix", name)
		}
	}
}

func TestPostgresSource_ConsecutiveRuns(t *testing.T) {
	dsn, cleanup := startPostgresContainer(t)
	defer cleanup()

	pool := openTestPool(t, dsn)
	_, err := pool.Exec(context.Background(), `
DROP TABLE IF EXISTS deals_window3;
CREATE TABLE deals_window3 (
	id SERIAL PRIMARY KEY,
	name TEXT,
	updated_at TIMESTAMPTZ NOT NULL
);`)
	if err != nil {
		t.Fatalf("create bounded-window table: %v", err)
	}

	base := time.Now().UTC().Add(-10 * time.Minute)
	for i, name := range []string{"A", "B", "C"} {
		if _, err := pool.Exec(context.Background(), `INSERT INTO deals_window3 (name, updated_at) VALUES ($1, $2)`, name, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	src := NewPostgresSource()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := src.Connect(ctx, config.SourceConfig{
		Type:            "postgres",
		Connection:      dsn,
		Table:           "deals_window3",
		WatermarkColumn: "updated_at",
		BatchSize:       10,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	run1End := base.Add(5 * time.Minute)
	out1 := make(chan row.Row)
	done1 := make(chan error, 1)
	go func() {
		done1 <- src.Extract(ctx, time.Time{}, run1End, out1)
	}()

	var firstRun []row.Row
	for r := range out1 {
		firstRun = append(firstRun, r)
	}
	if err := <-done1; err != nil {
		t.Fatalf("first Extract() error = %v", err)
	}
	if len(firstRun) != 3 {
		t.Fatalf("expected 3 rows on first run, got %d", len(firstRun))
	}

	for i, name := range []string{"D", "E"} {
		if _, err := pool.Exec(context.Background(), `INSERT INTO deals_window3 (name, updated_at) VALUES ($1, $2)`, name, run1End.Add(time.Duration(i+1)*time.Minute)); err != nil {
			t.Fatalf("insert second-run row %d: %v", i, err)
		}
	}

	run2End := run1End.Add(10 * time.Minute)
	out2 := make(chan row.Row)
	done2 := make(chan error, 1)
	go func() {
		done2 <- src.Extract(ctx, run1End, run2End, out2)
	}()

	var secondRun []row.Row
	for r := range out2 {
		secondRun = append(secondRun, r)
	}
	if err := <-done2; err != nil {
		t.Fatalf("second Extract() error = %v", err)
	}
	if len(secondRun) != 2 {
		t.Fatalf("expected 2 rows on second run, got %d", len(secondRun))
	}
	if got := secondRun[0].Data["name"]; got != "D" {
		t.Fatalf("expected first second-run row D, got %v", got)
	}
	if got := secondRun[1].Data["name"]; got != "E" {
		t.Fatalf("expected second second-run row E, got %v", got)
	}
}
