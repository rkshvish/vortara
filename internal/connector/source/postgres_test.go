//go:build !integration

package source

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

type fakePool struct {
	pingErr    error
	queryer    *fakeConn
	acquireErr error
	closed     bool
}

func (p *fakePool) Query(ctx context.Context, sql string, args ...any) (pgxRows, error) {
	if p.queryer == nil {
		return &fakeRows{}, nil
	}
	return p.queryer.Query(ctx, sql, args...)
}

func (p *fakePool) Ping(context.Context) error { return p.pingErr }

func (p *fakePool) Close() { p.closed = true }

func (p *fakePool) Acquire(_ context.Context) (pgxConn, error) {
	if p.acquireErr != nil {
		return nil, p.acquireErr
	}
	if p.queryer == nil {
		return &fakeConn{}, nil
	}
	return p.queryer, nil
}

type fakeConn struct {
	mu       sync.Mutex
	queries  []fakeQuery
	schema   *fakeRows
	pk       *fakeRows
	pages    map[int]*fakeRows
	queryErr error
}

type fakeQuery struct {
	sql    string
	args   []any
	offset int
	batch  int
}

func (c *fakeConn) Query(_ context.Context, sql string, args ...any) (pgxRows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	q := fakeQuery{sql: sql, args: append([]any(nil), args...)}
	if len(args) >= 2 {
		q.batch = asInt(args[len(args)-2])
		q.offset = asInt(args[len(args)-1])
	}
	if q.batch == 0 {
		q.batch = sqlClauseInt(sql, "LIMIT ")
	}
	if q.offset == 0 {
		q.offset = sqlClauseInt(sql, "OFFSET ")
	}
	c.queries = append(c.queries, q)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	switch {
	case strings.Contains(sql, "information_schema.columns"):
		if c.schema == nil {
			return &fakeRows{}, nil
		}
		c.schema.reset()
		return c.schema, nil
	case strings.Contains(sql, "pg_index"):
		if c.pk == nil {
			return &fakeRows{}, nil
		}
		c.pk.reset()
		return c.pk, nil
	}
	if rows, ok := c.pages[q.offset]; ok {
		rows = rows.filteredWithin(sql, args)
		rows.reset()
		return rows, nil
	}
	return &fakeRows{}, nil
}

func (c *fakeConn) Release() {}

type fakeRows struct {
	fields []pgconn.FieldDescription
	values [][]any
	idx    int
	err    error
}

func (r *fakeRows) Close() {}

func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.values) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.current()
	if row == nil {
		return ioEOF{}
	}
	for i := range dest {
		if i >= len(row) {
			break
		}
		if err := assignScanValue(dest[i], row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) {
	row := r.current()
	if row == nil {
		return nil, ioEOF{}
	}
	return row, nil
}

func (r *fakeRows) RawValues() [][]byte { return nil }

func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *fakeRows) Conn() *pgx.Conn { return nil }

func (r *fakeRows) reset() { r.idx = 0 }

func (r *fakeRows) current() []any {
	if r.idx == 0 || r.idx > len(r.values) {
		return nil
	}
	return r.values[r.idx-1]
}

func (r *fakeRows) filteredWithin(sql string, args []any) *fakeRows {
	filtered := &fakeRows{fields: append([]pgconn.FieldDescription(nil), r.fields...)}
	var start, end time.Time
	var hasStart, hasEnd bool
	timeArgs := args
	switch {
	case strings.Contains(sql, "<=") && !strings.Contains(sql, "> "):
		if len(timeArgs) > 0 {
			if ts, ok := timeArgs[0].(time.Time); ok && !ts.IsZero() {
				end = ts
				hasEnd = true
			}
		}
	case strings.Contains(sql, ">") && strings.Contains(sql, "<="):
		if len(timeArgs) > 0 {
			if wm, ok := timeArgs[0].(time.Time); ok && !wm.IsZero() {
				start = wm
				hasStart = true
			}
		}
		if len(timeArgs) > 1 {
			if ts, ok := timeArgs[1].(time.Time); ok && !ts.IsZero() {
				end = ts
				hasEnd = true
			}
		}
	default:
		if len(timeArgs) > 0 {
			if wm, ok := timeArgs[0].(time.Time); ok && !wm.IsZero() {
				start = wm
				hasStart = true
			}
		}
	}

	watermarkIdx := -1
	for i, field := range r.fields {
		if field.Name == "updated_at" {
			watermarkIdx = i
			break
		}
	}
	if watermarkIdx < 0 {
		filtered.values = append(filtered.values, r.values...)
		return filtered
	}

	for _, rowValues := range r.values {
		if watermarkIdx >= len(rowValues) {
			continue
		}
		ts, ok := rowValues[watermarkIdx].(time.Time)
		if !ok {
			filtered.values = append(filtered.values, rowValues)
			continue
		}
		if hasStart && !ts.After(start) {
			continue
		}
		if hasEnd && ts.After(end) {
			continue
		}
		filtered.values = append(filtered.values, rowValues)
	}
	return filtered
}

func assignScanValue(dest any, src any) error {
	switch d := dest.(type) {
	case *string:
		*d = fmt.Sprint(src)
	case *bool:
		switch v := src.(type) {
		case bool:
			*d = v
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			*d = b
		default:
			return fmt.Errorf("unsupported scan to bool from %T", src)
		}
	case *int:
		*d = int(toInt64(src))
	case *int64:
		*d = toInt64(src)
	case *time.Time:
		if ts, ok := src.(time.Time); ok {
			*d = ts
		} else if s, ok := src.(string); ok {
			ts, err := parseTimeString(s)
			if err != nil {
				return err
			}
			*d = ts
		} else {
			return fmt.Errorf("unsupported scan to time.Time from %T", src)
		}
	case *interface{}:
		*d = src
	default:
		return fmt.Errorf("unsupported scan destination %T", dest)
	}
	return nil
}

type ioEOF struct{}

func (ioEOF) Error() string { return "EOF" }

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	default:
		return 0
	}
}

func sqlClauseInt(sql, clause string) int {
	idx := strings.LastIndex(sql, clause)
	if idx < 0 {
		return 0
	}
	start := idx + len(clause)
	end := start
	for end < len(sql) && sql[end] >= '0' && sql[end] <= '9' {
		end++
	}
	if end == start {
		return 0
	}
	n, err := strconv.Atoi(sql[start:end])
	if err != nil {
		return 0
	}
	return n
}

func testSourceConfig() config.SourceConfig {
	return config.SourceConfig{
		Type:            "postgres",
		Connection:      "postgres://user:pass@localhost/db",
		Table:           "deals",
		WatermarkColumn: "updated_at",
		BatchSize:       2,
		Options:         map[string]string{"pipeline": "sales-sync"},
	}
}

func TestNormalizeConnectionString(t *testing.T) {
	got := normalizeConnectionString("postgresql+psycopg2://user:pass@host/db")
	if want := "postgres://user:pass@host/db"; got != want {
		t.Fatalf("normalizeConnectionString() = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "deals", want: `"deals"`},
		{in: "MyTable", want: `"MyTable"`},
		{in: "my table", want: `"my table"`},
		{in: `say "hello"`, want: `"say ""hello"""`},
		{in: "select", want: `"select"`},
	}

	for _, tt := range cases {
		if got := quoteIdentifier(tt.in); got != tt.want {
			t.Fatalf("quoteIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestQuoteTableName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "deals", want: `"public"."deals"`},
		{in: "public.deals", want: `"public"."deals"`},
		{in: "myschema.deals", want: `"myschema"."deals"`},
		{in: "MySchema.Deals", want: `"MySchema"."Deals"`},
	}

	for _, tt := range cases {
		if got := quoteTableName(tt.in); got != tt.want {
			t.Fatalf("quoteTableName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestIntrospectSchema_ParseTableName(t *testing.T) {
	cases := []struct {
		inSchema string
		inTable  string
		wantSch  string
		wantTab  string
	}{
		{inTable: "deals", wantSch: "public", wantTab: "deals"},
		{inTable: "public.deals", wantSch: "public", wantTab: "deals"},
		{inTable: "myschema.deals", wantSch: "myschema", wantTab: "deals"},
	}

	for _, tt := range cases {
		t.Run(tt.inTable, func(t *testing.T) {
			schema, table := parseTableName(tt.inTable)
			if schema != tt.wantSch || table != tt.wantTab {
				t.Fatalf("parseTableName(%q) = (%q,%q), want (%q,%q)", tt.inTable, schema, table, tt.wantSch, tt.wantTab)
			}
		})
	}
}

func TestPostgresSource_ExcludeColumns(t *testing.T) {
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"name", "text", "text", "YES"},
				{"secret", "text", "text", "YES"},
				{"internal_note", "text", "text", "YES"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk: &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{
			0: &fakeRows{
				fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}, {Name: "updated_at"}},
				values: [][]any{{int64(1), "deal", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.ExcludeColumns = []string{"secret", "INTERNAL_NOTE"}
	src.pool = &fakePool{queryer: conn}

	schema, err := src.introspectSchema(context.Background(), src.cfg.Table)
	if err != nil {
		t.Fatalf("introspectSchema() error = %v", err)
	}
	if len(schema.Columns) != 3 {
		t.Fatalf("expected 3 columns after exclusion, got %d", len(schema.Columns))
	}
	for _, col := range schema.Columns {
		if col.Name == "secret" || col.Name == "internal_note" {
			t.Fatalf("excluded column still present: %s", col.Name)
		}
	}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
	}()
	for range out {
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(conn.queries) < 3 {
		t.Fatalf("expected select query to be recorded, got %+v", conn.queries)
	}
	selectSQL := conn.queries[2].sql
	if strings.Contains(selectSQL, `"secret"`) || strings.Contains(selectSQL, `"internal_note"`) {
		t.Fatalf("excluded columns present in SELECT: %s", selectSQL)
	}
}

func TestPostgresSource_Connect(t *testing.T) {
	old := newPgxPool
	t.Cleanup(func() { newPgxPool = old })

	wantDSN := "postgres://user:pass@host/db"
	fp := &fakePool{}
	newPgxPool = func(_ context.Context, cfg *pgxpool.Config) (pgxPool, error) {
		if got := cfg.ConnString(); got != wantDSN {
			t.Fatalf("ConnString() = %q, want %q", got, wantDSN)
		}
		return fp, nil
	}

	src := NewPostgresSource()
	ctx := context.Background()
	cfg := testSourceConfig()
	cfg.Connection = "postgresql+psycopg2://user:pass@host/db"

	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if src.pool != fp {
		t.Fatalf("pool not stored on source")
	}
	if src.cfg.Table != "deals" {
		t.Fatalf("cfg not stored on source")
	}
}

func TestPostgresSource_CustomQuery_ResolvesPlaceholders(t *testing.T) {
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.Query = "SELECT * FROM t WHERE ts > {{watermark}} AND ts <= {{interval_end}} AND pipeline = '{{pipeline}}'"

	watermark := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	intervalEnd := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	got := src.resolveQuery(watermark, intervalEnd)

	if !strings.Contains(got, "'2024-01-01T00:00:00Z'") {
		t.Fatalf("resolved query missing watermark: %s", got)
	}
	if !strings.Contains(got, "'2024-01-02T00:00:00Z'") {
		t.Fatalf("resolved query missing interval end: %s", got)
	}
	if !strings.Contains(got, "sales-sync") {
		t.Fatalf("resolved query missing pipeline: %s", got)
	}
}

func TestPostgresSource_CustomQuery_ZeroWatermark(t *testing.T) {
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.Query = "SELECT * FROM t WHERE ts > {{watermark}}"
	conn := &fakeConn{
		pages: map[int]*fakeRows{
			0: &fakeRows{
				fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "updated_at"}},
				values: [][]any{{int64(1), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	}
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
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
	if rows[0].PrimaryKey != "1" {
		t.Fatalf("expected primary key 1, got %q", rows[0].PrimaryKey)
	}
	if rows[0].Watermark.IsZero() {
		t.Fatal("expected watermark to be populated")
	}
	if len(conn.queries) == 0 || !strings.Contains(conn.queries[0].sql, "'0001-01-01T00:00:00Z'") {
		t.Fatalf("expected zero watermark placeholder to be resolved, got %+v", conn.queries)
	}
}

func TestPostgresSource_Extract_Empty(t *testing.T) {
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.pool = &fakePool{queryer: &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk: &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{
			0: &fakeRows{
				fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "updated_at"}},
			},
		},
	}}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
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
	before := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	after := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)

	rows := &fakeRows{
		fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}, {Name: "updated_at"}},
		values: [][]any{
			{int64(1), "old", before},
			{int64(2), "new", after},
		},
	}
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"name", "text", "text", "YES"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk:    &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{0: rows},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), before, time.Time{}, out)
	}()

	var got []row.Row
	for r := range out {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0].PrimaryKey != "id=2" {
		t.Fatalf("PrimaryKey = %q, want %q", got[0].PrimaryKey, "id=2")
	}
	if got[0].Data["name"] != "new" {
		t.Fatalf("Data[name] = %v, want %v", got[0].Data["name"], "new")
	}
	if !got[0].Watermark.Equal(after) {
		t.Fatalf("Watermark = %v, want %v", got[0].Watermark, after)
	}
}

func TestPostgresSource_Extract_Pagination(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	page0 := &fakeRows{
		fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}, {Name: "updated_at"}},
		values: [][]any{
			{int64(1), "deal-1", base.Add(1 * time.Minute)},
			{int64(2), "deal-2", base.Add(2 * time.Minute)},
		},
	}
	page2 := &fakeRows{
		fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}, {Name: "updated_at"}},
		values: [][]any{
			{int64(3), "deal-3", base.Add(3 * time.Minute)},
		},
	}
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"name", "text", "text", "YES"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk:    &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{0: page0, 2: page2},
	}

	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.BatchSize = 2
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
	}()

	var got []row.Row
	for r := range out {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
	if len(conn.queries) != 4 {
		t.Fatalf("expected 4 queries, got %d", len(conn.queries))
	}
	if conn.queries[2].offset != 0 || conn.queries[3].offset != 2 {
		t.Fatalf("unexpected offsets: %+v", conn.queries)
	}
	if conn.queries[2].sql != `SELECT "id", "name", "updated_at" FROM "public"."deals" ORDER BY "updated_at" ASC LIMIT 2 OFFSET 0` {
		t.Fatalf("unexpected SQL: %s", conn.queries[2].sql)
	}
}

func TestPostgresSource_Extract_CtxCancel(t *testing.T) {
	page := &fakeRows{
		fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}, {Name: "updated_at"}},
		values: [][]any{
			{int64(1), "deal-1", time.Now().UTC()},
			{int64(2), "deal-2", time.Now().UTC()},
		},
	}
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"name", "text", "text", "YES"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk:    &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{0: page},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.BatchSize = 2
	src.pool = &fakePool{queryer: conn}

	ctx, cancel := context.WithCancel(context.Background())
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

	err := <-done
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if received == 0 {
		t.Fatal("expected at least one row before cancellation")
	}
}

func TestConvertPgValue(t *testing.T) {
	now := time.Date(2026, 1, 1, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		in     any
		pgType string
		want   any
	}{
		{name: "numeric", in: pgtype.Numeric{Int: bigInt(12345), Exp: -2, Valid: true}, pgType: "numeric", want: 123.45},
		{name: "timestamptz", in: pgtype.Timestamptz{Time: now, Valid: true}, pgType: "timestamptz", want: now},
		{name: "date", in: pgtype.Date{Time: now, Valid: true}, pgType: "date", want: now},
		{name: "bool", in: pgtype.Bool{Bool: true, Valid: true}, pgType: "bool", want: true},
		{name: "int4", in: pgtype.Int4{Int32: 42, Valid: true}, pgType: "int4", want: int64(42)},
		{name: "int8", in: pgtype.Int8{Int64: 84, Valid: true}, pgType: "int8", want: int64(84)},
		{name: "float4", in: pgtype.Float4{Float32: 1.5, Valid: true}, pgType: "float4", want: float64(1.5)},
		{name: "float8", in: pgtype.Float8{Float64: 2.5, Valid: true}, pgType: "float8", want: float64(2.5)},
		{name: "text", in: pgtype.Text{String: "hello", Valid: true}, pgType: "text", want: "hello"},
		{name: "uuid", in: pgtype.UUID{Bytes: [16]byte{1, 2, 3}, Valid: true}, pgType: "uuid", want: "01020300-0000-0000-0000-000000000000"},
		{name: "bytes", in: []byte{0xde, 0xad}, pgType: "", want: "dead"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := convertPgValue(tt.in, tt.pgType)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("convertPgValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertPgValue_Int(t *testing.T) {
	got := convertPgValue(pgtype.Int4{Int32: 42, Valid: true}, "int4")
	if got != int64(42) {
		t.Fatalf("convertPgValue() = %v, want %v", got, int64(42))
	}
}

func TestConvertPgValue_Numeric(t *testing.T) {
	got := convertPgValue(pgtype.Numeric{Int: bigInt(12345), Exp: -2, Valid: true}, "numeric")
	if fmt.Sprint(got) != fmt.Sprint(123.45) {
		t.Fatalf("convertPgValue() = %v, want %v", got, 123.45)
	}
}

func TestConvertPgValue_Timestamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 15, 0, 0, 0, time.UTC)
	got := convertPgValue(pgtype.Timestamptz{Time: now, Valid: true}, "timestamptz")
	ts, ok := got.(time.Time)
	if !ok || !ts.Equal(now) {
		t.Fatalf("convertPgValue() = %v, want %v", got, now)
	}
}

func TestConvertPgValue_Nil(t *testing.T) {
	if got := convertPgValue(nil, "text"); got != nil {
		t.Fatalf("convertPgValue() = %v, want nil", got)
	}
}

func TestConvertPgValue_Bool(t *testing.T) {
	got := convertPgValue(pgtype.Bool{Bool: true, Valid: true}, "bool")
	if got != true {
		t.Fatalf("convertPgValue() = %v, want true", got)
	}
}

func bigInt(v int64) *big.Int {
	return big.NewInt(v)
}

// TestPostgresSource_ExtractParallelism_Default verifies that ExtractParallelism=0 uses the
// sequential path (no parallel range scan is attempted).
func TestPostgresSource_ExtractParallelism_Default(t *testing.T) {
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"id", "integer", "int4", "NO"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk: &fakeRows{values: [][]any{{"id"}}},
		pages: map[int]*fakeRows{
			0: {
				fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "updated_at"}},
				values: [][]any{{int64(1), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.ExtractParallelism = 0 // default: sequential
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() { done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out) }()

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
}

// TestPostgresSource_ParallelExtract_NoPK verifies that a table with no primary key falls back
// to sequential extraction even when ExtractParallelism > 1.
func TestPostgresSource_ParallelExtract_NoPK(t *testing.T) {
	conn := &fakeConn{
		schema: &fakeRows{
			values: [][]any{
				{"name", "text", "text", "YES"},
				{"updated_at", "timestamp with time zone", "timestamptz", "NO"},
			},
		},
		pk: &fakeRows{}, // no primary key
		pages: map[int]*fakeRows{
			0: {
				fields: []pgconn.FieldDescription{{Name: "name"}, {Name: "updated_at"}},
				values: [][]any{{"Acme", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.ExtractParallelism = 4
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() { done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out) }()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row via sequential fallback, got %d", len(rows))
	}
}

// TestPostgresSource_ParallelExtract_CustomQuery verifies that custom query mode always uses
// the sequential custom query path, even when ExtractParallelism > 1.
func TestPostgresSource_ParallelExtract_CustomQuery(t *testing.T) {
	conn := &fakeConn{
		pages: map[int]*fakeRows{
			0: {
				fields: []pgconn.FieldDescription{{Name: "id"}, {Name: "updated_at"}},
				values: [][]any{{int64(42), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	}
	src := NewPostgresSource()
	src.cfg = testSourceConfig()
	src.cfg.Query = "SELECT id, updated_at FROM deals WHERE updated_at > {{watermark}}"
	src.cfg.ExtractParallelism = 4
	src.pool = &fakePool{queryer: conn}

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() { done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out) }()

	var rows []row.Row
	for r := range out {
		rows = append(rows, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row via custom query path, got %d", len(rows))
	}
}
