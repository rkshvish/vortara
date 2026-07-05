package source

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

func TestMySQLURLToDSN(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{
			name: "full url",
			url:  "mysql://user:pass@localhost:3306/mydb",
			want: "user:pass@tcp(localhost:3306)/mydb",
		},
		{
			name: "no credentials",
			url:  "mysql://localhost:3306/mydb",
			want: "tcp(localhost:3306)/mydb",
		},
		{
			name: "with params",
			url:  "mysql://u:p@host:3306/db?tls=true",
			want: "u:p@tcp(host:3306)/db?tls=true",
		},
		{
			name: "password containing at sign",
			url:  "mysql://user:p@ss@host:3306/db",
			want: "user:p@ss@tcp(host:3306)/db",
		},
		{
			name:    "missing database",
			url:     "mysql://localhost:3306",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mysqlURLToDSN(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("mysqlURLToDSN(%q) error = nil, want error", tt.url)
				}
				return
			}
			if err != nil {
				t.Fatalf("mysqlURLToDSN(%q) error = %v", tt.url, err)
			}
			if got != tt.want {
				t.Fatalf("mysqlURLToDSN(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestBuildMySQLExtractQuery(t *testing.T) {
	cols := []mysqlColumn{{Name: "id", IsPK: true}, {Name: "name"}, {Name: "updated_at"}}

	q := buildMySQLExtractQuery("deals", cols, "updated_at", false)
	want := "SELECT `id`, `name`, `updated_at` FROM `deals` WHERE `updated_at` > ? AND `updated_at` <= ? ORDER BY `updated_at`"
	if q != want {
		t.Fatalf("query = %q, want %q", q, want)
	}

	q = buildMySQLExtractQuery("deals", cols, "updated_at", true)
	want = "SELECT `id`, `name`, `updated_at` FROM `deals` WHERE `updated_at` <= ? ORDER BY `updated_at`"
	if q != want {
		t.Fatalf("first-run query = %q, want %q", q, want)
	}
}

func TestQuoteMySQLIdent(t *testing.T) {
	if got := quoteMySQLIdent("normal"); got != "`normal`" {
		t.Fatalf("quoteMySQLIdent = %q", got)
	}
	if got := quoteMySQLIdent("we`ird"); got != "`we``ird`" {
		t.Fatalf("quoteMySQLIdent escape = %q", got)
	}
}

func TestConvertMySQLValue(t *testing.T) {
	if got := convertMySQLValue([]byte("hello")); got != "hello" {
		t.Fatalf("[]byte = %v", got)
	}
	if got := convertMySQLValue(nil); got != nil {
		t.Fatalf("nil = %v", got)
	}
	loc := time.FixedZone("X", 3600)
	ts := time.Date(2026, 1, 2, 10, 0, 0, 0, loc)
	if got := convertMySQLValue(ts).(time.Time); !got.Equal(ts) || got.Location() != time.UTC {
		t.Fatalf("time = %v (loc %v), want UTC-normalized equal instant", got, got.Location())
	}
	if got := convertMySQLValue(int64(42)); got != int64(42) {
		t.Fatalf("int64 = %v", got)
	}
}

func TestMySQLSource_Connect_Validation(t *testing.T) {
	s := NewMySQLSource()
	err := s.Connect(context.Background(), config.SourceConfig{})
	if err == nil || !strings.Contains(err.Error(), "connection is required") {
		t.Fatalf("Connect() error = %v, want connection required", err)
	}

	err = s.Connect(context.Background(), config.SourceConfig{Connection: "user:pass@tcp(localhost:3306)/db"})
	if err == nil || !strings.Contains(err.Error(), "table or query is required") {
		t.Fatalf("Connect() error = %v, want table/query required", err)
	}

	err = s.Connect(context.Background(), config.SourceConfig{Connection: ":::not-a-dsn", Table: "t"})
	if err == nil {
		t.Fatal("Connect() with bad DSN should fail")
	}
}

func TestMySQLSource_GetWatermarkColumn(t *testing.T) {
	s := &MySQLSource{cfg: config.SourceConfig{WatermarkColumn: "modified"}}
	if got := s.GetWatermarkColumn(); got != "modified" {
		t.Fatalf("GetWatermarkColumn() = %q", got)
	}
	s = &MySQLSource{}
	if got := s.GetWatermarkColumn(); got != "updated_at" {
		t.Fatalf("GetWatermarkColumn() default = %q", got)
	}
}
