package source

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func newSnowflakeMockSource(t *testing.T) (*SnowflakeSource, sqlmock.Sqlmock) {
	t.Helper()

	oldOpen := openSnowflakeDB
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	openSnowflakeDB = func(string) (*sql.DB, error) {
		return db, nil
	}
	t.Cleanup(func() {
		openSnowflakeDB = oldOpen
		_ = db.Close()
	})

	return NewSnowflakeSource(), mock
}

func TestSnowflakeSource_ParseDSN(t *testing.T) {
	info, err := parseSnowflakeURL("snowflake://user:pass@myaccount/mydb/PUBLIC?warehouse=WH")
	if err != nil {
		t.Fatalf("parseSnowflakeURL() error = %v", err)
	}
	if info.Account != "myaccount" || info.Database != "mydb" || info.Schema != "PUBLIC" || info.Warehouse != "WH" {
		t.Fatalf("unexpected parsed info: %+v", info)
	}
}

func TestSnowflakeSource_ParseDSN_DefaultSchema(t *testing.T) {
	info, err := parseSnowflakeURL("snowflake://user:pass@myaccount/mydb?warehouse=WH")
	if err != nil {
		t.Fatalf("parseSnowflakeURL() error = %v", err)
	}
	if info.Schema != "PUBLIC" {
		t.Fatalf("schema = %q, want PUBLIC", info.Schema)
	}
}

func TestSnowflakeSource_QuoteIdentifier(t *testing.T) {
	if got := quoteIdentifier("my_table"); got != `"my_table"` {
		t.Fatalf("quoteIdentifier() = %q", got)
	}
	if got := quoteIdentifier("MY TABLE"); got != `"MY TABLE"` {
		t.Fatalf("quoteIdentifier() = %q", got)
	}
}

func TestSnowflakeSource_BuildQuery_Watermark(t *testing.T) {
	wm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	query, args, err := buildSnowflakeSelectQuery("DIM_ACCOUNTS", "UPDATED_AT", []string{"ID", "NAME"}, wm, until, 1000, 0)
	if err != nil {
		t.Fatalf("buildSnowflakeSelectQuery() error = %v", err)
	}
	for _, want := range []string{
		`SELECT "ID", "NAME" FROM "PUBLIC"."DIM_ACCOUNTS"`,
		`WHERE "UPDATED_AT" > ? AND "UPDATED_AT" <= ?`,
		`ORDER BY "UPDATED_AT" ASC`,
		`LIMIT 1000`,
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query %q does not contain %q", query, want)
		}
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
}

func TestSnowflakeSource_BuildQuery_ZeroWatermark(t *testing.T) {
	until := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	query, _, err := buildSnowflakeSelectQuery("DIM_ACCOUNTS", "UPDATED_AT", []string{"ID"}, time.Time{}, until, 1000, 0)
	if err != nil {
		t.Fatalf("buildSnowflakeSelectQuery() error = %v", err)
	}
	if strings.Contains(query, `> ?`) {
		t.Fatalf("unexpected lower bound in query: %q", query)
	}
	if !strings.Contains(query, `<= ?`) {
		t.Fatalf("expected upper bound in query: %q", query)
	}
}

func TestSnowflakeSource_CustomQuery_Resolves(t *testing.T) {
	src := &SnowflakeSource{cfg: config.SourceConfig{
		Query: `SELECT * FROM t WHERE ts > {{watermark}} AND ts <= {{interval_end}}`,
	}}
	wm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	got := src.resolveQuery(wm, until)
	for _, want := range []string{wm.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339)} {
		if !strings.Contains(got, want) {
			t.Fatalf("resolved query %q missing %q", got, want)
		}
	}
}

func TestSnowflakeSource_ConvertValue_Types(t *testing.T) {
	if got := convertSnowflakeValue(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
	if got := convertSnowflakeValue(int64(42)); got != int64(42) {
		t.Fatalf("expected int64(42), got %T %v", got, got)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if got := convertSnowflakeValue(now); got != now {
		t.Fatalf("expected time.Time, got %T %v", got, got)
	}
}

func TestSnowflakeSource_Extract_TableMode(t *testing.T) {
	src, mock := newSnowflakeMockSource(t)
	ctx := context.Background()

	cfg := config.SourceConfig{
		URL:             "snowflake://user:pass@myaccount/mydb/PUBLIC?warehouse=WH",
		Table:           "DIM_ACCOUNTS",
		WatermarkColumn: "UPDATED_AT",
		Type:            "snowflake",
	}
	mock.ExpectPing()
	mock.ExpectExec(regexp.QuoteMeta(snowflakeQueryTagSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`DESCRIBE TABLE "PUBLIC"."DIM_ACCOUNTS"`)).
		WillReturnRows(sqlmock.NewRows([]string{"name", "type", "null?"}).
			AddRow("ID", "NUMBER(38,0)", "N").
			AddRow("UPDATED_AT", "TIMESTAMP_NTZ(9)", "Y").
			AddRow("NAME", "VARCHAR", "Y"))

	wm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	query, args, err := buildSnowflakeSelectQuery("DIM_ACCOUNTS", "UPDATED_AT", []string{"ID", "UPDATED_AT", "NAME"}, wm, until, 1000, 0)
	if err != nil {
		t.Fatalf("buildSnowflakeSelectQuery() error = %v", err)
	}
	queryArgs := make([]driver.Value, len(args))
	for i, arg := range args {
		queryArgs[i] = arg
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(queryArgs...).
		WillReturnRows(sqlmock.NewRows([]string{"ID", "UPDATED_AT", "NAME"}).
			AddRow(int64(42), time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), "Acme"))

	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	out := make(chan row.Row, 1)
	go func() {
		if err := src.Extract(ctx, wm, until, out); err != nil {
			t.Errorf("Extract() error = %v", err)
		}
	}()

	got := <-out
	if got.PrimaryKey != "42" {
		t.Fatalf("PrimaryKey = %q, want 42", got.PrimaryKey)
	}
	if got.Data["NAME"] != "Acme" {
		t.Fatalf("unexpected row data: %+v", got.Data)
	}
	if got.Watermark.IsZero() {
		t.Fatal("expected watermark to be set")
	}
}
