package destination

import (
	"context"
	"strings"
	"testing"

	"github.com/rkshvish/vortara/pkg/config"
)

func TestParseSnowflakeDestURL(t *testing.T) {
	info, err := parseSnowflakeDestURL("snowflake://alice:secret@myacct/analytics/marts?warehouse=WH1&role=LOADER")
	if err != nil {
		t.Fatalf("parseSnowflakeDestURL() error = %v", err)
	}
	if info.User != "alice" || info.Password != "secret" || info.Account != "myacct" {
		t.Fatalf("credentials = %+v", info)
	}
	if info.Database != "analytics" || info.Schema != "MARTS" || info.Warehouse != "WH1" || info.Role != "LOADER" {
		t.Fatalf("location = %+v", info)
	}

	info, err = parseSnowflakeDestURL("snowflake://u:p@acct/db")
	if err != nil {
		t.Fatalf("minimal url error = %v", err)
	}
	if info.Schema != "PUBLIC" {
		t.Fatalf("default schema = %q, want PUBLIC", info.Schema)
	}

	if _, err := parseSnowflakeDestURL("snowflake://acct/db"); err == nil {
		t.Fatal("url without credentials should fail")
	}
	if _, err := parseSnowflakeDestURL("snowflake://u:p@acct"); err == nil {
		t.Fatal("url without database should fail")
	}
}

func TestBuildSnowflakeInsert(t *testing.T) {
	data := map[string]interface{}{"id": 1, "name": "Acme"}
	query, args := buildSnowflakeInsert(`"PUBLIC"."DEALS"`, []string{"id", "name"}, data)
	want := `INSERT INTO "PUBLIC"."DEALS" ("ID", "NAME") VALUES (?, ?)`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
	if len(args) != 2 || args[0] != 1 || args[1] != "Acme" {
		t.Fatalf("args = %v", args)
	}
}

func TestBuildSnowflakeMerge(t *testing.T) {
	data := map[string]interface{}{"id": 42, "name": "Acme", "tier": "gold"}
	query, args := buildSnowflakeMerge(`"PUBLIC"."DEALS"`, []string{"id", "name", "tier"}, []string{"id"}, data)

	if !strings.HasPrefix(query, `MERGE INTO "PUBLIC"."DEALS" t USING (SELECT ? AS "ID", ? AS "NAME", ? AS "TIER") s ON t."ID" = s."ID"`) {
		t.Fatalf("merge prefix = %q", query)
	}
	if !strings.Contains(query, `WHEN MATCHED THEN UPDATE SET t."NAME" = s."NAME", t."TIER" = s."TIER"`) {
		t.Fatalf("merge update clause = %q", query)
	}
	if strings.Contains(query, `UPDATE SET t."ID"`) {
		t.Fatalf("match key must not be updated: %q", query)
	}
	if !strings.Contains(query, `WHEN NOT MATCHED THEN INSERT ("ID", "NAME", "TIER") VALUES (s."ID", s."NAME", s."TIER")`) {
		t.Fatalf("merge insert clause = %q", query)
	}
	if len(args) != 3 || args[0] != 42 {
		t.Fatalf("args = %v", args)
	}
}

func TestBuildSnowflakeDelete(t *testing.T) {
	data := map[string]interface{}{"id": 7, "region": "eu"}
	query, args := buildSnowflakeDelete(`"PUBLIC"."DEALS"`, []string{"id", "region"}, data)
	want := `DELETE FROM "PUBLIC"."DEALS" WHERE "ID" = ? AND "REGION" = ?`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
	if len(args) != 2 || args[0] != 7 || args[1] != "eu" {
		t.Fatalf("args = %v", args)
	}
}

func TestQuoteSnowflakeIdent(t *testing.T) {
	if got := quoteSnowflakeIdent("name"); got != `"NAME"` {
		t.Fatalf("quote = %q", got)
	}
	if got := quoteSnowflakeIdent(`we"ird`); got != `"WE""IRD"` {
		t.Fatalf("quote escape = %q", got)
	}
}

func TestSnowflakeDestination_Connect_Validation(t *testing.T) {
	d := NewSnowflakeDestination()

	err := d.Connect(context.Background(), config.DestinationConfig{Options: map[string]string{}})
	if err == nil || !strings.Contains(err.Error(), "table is required") {
		t.Fatalf("Connect() = %v, want table required", err)
	}

	err = d.Connect(context.Background(), config.DestinationConfig{
		Options:  map[string]string{"table": "deals"},
		Strategy: "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "strategy") {
		t.Fatalf("Connect() = %v, want strategy error", err)
	}

	err = d.Connect(context.Background(), config.DestinationConfig{
		Options:  map[string]string{"table": "deals"},
		Strategy: "merge",
	})
	if err == nil || !strings.Contains(err.Error(), "requires match_on") {
		t.Fatalf("Connect() = %v, want match_on error", err)
	}

	err = d.Connect(context.Background(), config.DestinationConfig{
		Options:  map[string]string{"table": "deals"},
		Strategy: "append",
	})
	if err == nil || !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("Connect() = %v, want url required", err)
	}
}

func TestParseSnowflakeDestTable(t *testing.T) {
	schema, table := parseSnowflakeDestTable("marts.deals")
	if schema != "MARTS" || table != "DEALS" {
		t.Fatalf("parse = %q.%q", schema, table)
	}
	schema, table = parseSnowflakeDestTable("deals")
	if schema != "PUBLIC" || table != "DEALS" {
		t.Fatalf("default schema = %q.%q", schema, table)
	}
}

func TestNormalizeSnowflakeArg_JSON(t *testing.T) {
	got := normalizeSnowflakeArg(map[string]interface{}{"a": 1, "b": "x"})
	s, ok := got.(string)
	if !ok {
		t.Fatalf("map arg type = %T, want string", got)
	}
	if s != `{"a":1,"b":"x"}` {
		t.Fatalf("map arg = %q, want valid JSON", s)
	}
	got = normalizeSnowflakeArg([]interface{}{1, "two"})
	if got.(string) != `[1,"two"]` {
		t.Fatalf("slice arg = %v", got)
	}
	if got := normalizeSnowflakeArg("plain"); got != "plain" {
		t.Fatalf("string passthrough = %v", got)
	}
}
