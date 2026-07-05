package destination

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func TestMySQLDestURLToDSN(t *testing.T) {
	got, err := mysqlDestURLToDSN("mysql://user:pass@localhost:3306/mydb")
	if err != nil || got != "user:pass@tcp(localhost:3306)/mydb" {
		t.Fatalf("dsn = %q, err %v", got, err)
	}
	if _, err := mysqlDestURLToDSN("mysql://localhost:3306"); err == nil {
		t.Fatal("missing db should fail")
	}
}

func TestBuildMySQLWriteSQL_Merge(t *testing.T) {
	batch := []row.Row{
		{ID: "1", Data: map[string]interface{}{"id": 1, "name": "a"}},
		{ID: "2", Data: map[string]interface{}{"id": 2, "name": "b"}},
	}
	query, args := buildMySQLWriteSQL("deals", []string{"id", "name"}, []string{"id"}, batch, "merge")
	want := "INSERT INTO `deals` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
	if len(args) != 4 || args[0] != 1 || args[3] != "b" {
		t.Fatalf("args = %v", args)
	}
}

func TestBuildMySQLWriteSQL_Append(t *testing.T) {
	batch := []row.Row{{ID: "1", Data: map[string]interface{}{"id": 1}}}
	query, _ := buildMySQLWriteSQL("logs", []string{"id"}, nil, batch, "append")
	if strings.Contains(query, "ON DUPLICATE") {
		t.Fatalf("append must not upsert: %q", query)
	}
}

func TestBuildMySQLWriteSQL_JSONArgs(t *testing.T) {
	batch := []row.Row{{ID: "1", Data: map[string]interface{}{"meta": map[string]interface{}{"a": 1}}}}
	_, args := buildMySQLWriteSQL("t", []string{"meta"}, nil, batch, "append")
	if s, ok := args[0].(string); !ok || s != `{"a":1}` {
		t.Fatalf("json arg = %v (%T), want valid JSON string", args[0], args[0])
	}
}

func TestMySQLDestination_Connect_Validation(t *testing.T) {
	d := NewMySQLDestination()
	if err := d.Connect(context.Background(), config.DestinationConfig{Options: map[string]string{}}); err == nil || !strings.Contains(err.Error(), "table is required") {
		t.Fatalf("want table required, got %v", err)
	}
	if err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"table": "t"}, Strategy: "merge",
	}); err == nil || !strings.Contains(err.Error(), "requires match_on") {
		t.Fatalf("want match_on required, got %v", err)
	}
	if err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"table": "t"}, Strategy: "append",
	}); err == nil || !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("want url required, got %v", err)
	}
}

func TestNormalizeMySQLArg(t *testing.T) {
	if got := normalizeMySQLArg(map[string]interface{}{"x": 1}); got != `{"x":1}` {
		t.Fatalf("map = %v", got)
	}
	ts := time.Date(2026, 1, 1, 5, 0, 0, 0, time.FixedZone("X", 3600))
	if got := normalizeMySQLArg(ts).(time.Time); got.Location() != time.UTC {
		t.Fatalf("time not UTC: %v", got)
	}
}
