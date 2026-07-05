package source

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"

	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

type fakeBQClient struct {
	queries []string
	runners map[string]*fakeBQRunner
	respond func(string) *fakeBQRunner
}

func (c *fakeBQClient) Query(sql string) bqQueryRunner {
	c.queries = append(c.queries, sql)
	if c.respond != nil {
		if r := c.respond(sql); r != nil {
			return r
		}
	}
	if c.runners == nil {
		c.runners = map[string]*fakeBQRunner{}
	}
	if r, ok := c.runners[sql]; ok {
		return r
	}
	trimmed := strings.TrimSpace(sql)
	for key, runner := range c.runners {
		if strings.TrimSpace(key) == trimmed {
			return runner
		}
	}
	r := &fakeBQRunner{}
	c.runners[sql] = r
	return r
}

func (c *fakeBQClient) Close() error { return nil }

type fakeBQRunner struct {
	params []bigquery.QueryParameter
	rows   [][]bigquery.Value
	idx    int
}

func (r *fakeBQRunner) SetParameters(params []bigquery.QueryParameter) {
	r.params = append([]bigquery.QueryParameter(nil), params...)
}

func (r *fakeBQRunner) Read(context.Context) (bqRowIterator, error) {
	return &fakeBQIter{rows: r.rows}, nil
}

type fakeBQIter struct {
	rows [][]bigquery.Value
	idx  int
}

func (it *fakeBQIter) Next(dst *[]bigquery.Value) error {
	if it.idx >= len(it.rows) {
		return iterator.Done
	}
	*dst = append([]bigquery.Value(nil), it.rows[it.idx]...)
	it.idx++
	return nil
}

func (it *fakeBQIter) Columns() []string {
	return []string{"id", "updated_at", "name"}
}

func TestBigQuerySource_ParseConfig(t *testing.T) {
	client := &fakeBQClient{}
	src := NewBigQuerySource(WithBigQueryClient(client))
	cfg := config.SourceConfig{
		Connection: "my-project",
		Table:      "fct_accounts",
		Type:       "bigquery",
		Options: map[string]string{
			"dataset": "analytics",
		},
	}
	if err := src.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if got := src.project; got != "my-project" {
		t.Fatalf("project = %q, want my-project", got)
	}
	if got := src.dataset; got != "analytics" {
		t.Fatalf("dataset = %q, want analytics", got)
	}
	if got := src.table; got != "fct_accounts" {
		t.Fatalf("table = %q, want fct_accounts", got)
	}
}

func TestBigQuerySource_QuoteTable(t *testing.T) {
	if got := bqQuoteTable("proj", "ds", "tbl"); got != "`proj.ds.tbl`" {
		t.Fatalf("bqQuoteTable() = %q", got)
	}
}

func TestBigQuerySource_BuildQuery_Watermark(t *testing.T) {
	wm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	query, params, err := buildBigQuerySelectQuery("proj", "ds", "tbl", "updated_at", []string{"id", "name"}, wm, until, 100, 0)
	if err != nil {
		t.Fatalf("buildBigQuerySelectQuery() error = %v", err)
	}
	for _, want := range []string{
		"SELECT `id`, `name` FROM `proj.ds.tbl`",
		"`updated_at` >= TIMESTAMP(@start)",
		"`updated_at` <= TIMESTAMP(@end)",
		"ORDER BY `updated_at` ASC",
		"LIMIT @limit",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query %q missing %q", query, want)
		}
	}
	if len(params) != 3 {
		t.Fatalf("params len = %d, want 3", len(params))
	}
}

func TestBigQuerySource_BuildQuery_ZeroWatermark(t *testing.T) {
	until := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	query, _, err := buildBigQuerySelectQuery("proj", "ds", "tbl", "updated_at", []string{"id"}, time.Time{}, until, 100, 0)
	if err != nil {
		t.Fatalf("buildBigQuerySelectQuery() error = %v", err)
	}
	if strings.Contains(query, "TIMESTAMP(@start)") {
		t.Fatalf("unexpected lower bound in query: %q", query)
	}
	if !strings.Contains(query, "TIMESTAMP(@end)") {
		t.Fatalf("expected upper bound in query: %q", query)
	}
}

func TestBigQuerySource_ConvertValue_CivilDate(t *testing.T) {
	got := convertBQValue(civil.Date{Year: 2024, Month: 1, Day: 15}, columnInfo{})
	ts, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", got)
	}
	want := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if !ts.Equal(want) {
		t.Fatalf("time = %v, want %v", ts, want)
	}
}

func TestBigQuerySource_ConvertValue_Nested(t *testing.T) {
	got := convertBQValue([]bigquery.Value{bigquery.Value("a"), bigquery.Value(int64(1))}, columnInfo{})
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if !strings.Contains(s, "a") || !strings.Contains(s, "1") {
		t.Fatalf("unexpected JSON string %q", s)
	}
}

func TestBigQuerySource_ConvertValue_Nil(t *testing.T) {
	if got := convertBQValue(nil, columnInfo{}); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestBigQuerySource_CustomQuery_Resolves(t *testing.T) {
	src := &BigQuerySource{cfg: config.SourceConfig{
		Query: `SELECT * FROM t WHERE ts > {{watermark}} AND ts <= {{interval_end}}`,
	}}
	wm := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	until := time.Date(2024, 1, 2, 11, 0, 0, 0, time.UTC)
	got := src.resolveQuery(wm, until)
	for _, want := range []string{wm.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339)} {
		if !strings.Contains(got, want) {
			t.Fatalf("resolved query %q missing %q", got, want)
		}
	}
}

func TestBigQuerySource_Extract_TableMode(t *testing.T) {
	client := &fakeBQClient{
		runners: map[string]*fakeBQRunner{},
		respond: func(sql string) *fakeBQRunner {
			switch {
			case strings.Contains(sql, "INFORMATION_SCHEMA.COLUMNS"):
				return &fakeBQRunner{
					rows: [][]bigquery.Value{
						{bigquery.Value("ID"), bigquery.Value("INTEGER"), bigquery.Value("YES")},
						{bigquery.Value("UPDATED_AT"), bigquery.Value("TIMESTAMP"), bigquery.Value("YES")},
						{bigquery.Value("NAME"), bigquery.Value("STRING"), bigquery.Value("YES")},
					},
				}
			case strings.Contains(sql, "TABLE_CONSTRAINTS"):
				return &fakeBQRunner{
					rows: [][]bigquery.Value{{bigquery.Value("ID")}},
				}
			case strings.Contains(sql, "FROM `my-project.analytics.fct_accounts`"):
				return &fakeBQRunner{
					rows: [][]bigquery.Value{
						{bigquery.Value(int64(42)), bigquery.Value(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)), bigquery.Value("Acme")},
					},
				}
			default:
				return nil
			}
		},
	}
	src := NewBigQuerySource(WithBigQueryClient(client))
	cfg := config.SourceConfig{
		Connection: "my-project",
		Table:      "fct_accounts",
		Type:       "bigquery",
		Options: map[string]string{
			"dataset": "analytics",
		},
	}
	if err := src.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	out := make(chan row.Row, 1)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), out)
	}()

	got, ok := <-out
	if !ok {
		t.Fatal("expected a row, channel closed early")
	}
	if got.PrimaryKey != "ID=42" {
		t.Fatalf("PrimaryKey = %q, want ID=42", got.PrimaryKey)
	}
	if got.Data["NAME"] != "Acme" {
		t.Fatalf("unexpected row data: %+v", got.Data)
	}
	if err := <-done; err != nil && !errors.Is(err, iterator.Done) {
		t.Fatalf("Extract() error = %v", err)
	}
}
