package pipeline

import (
	"strings"
	"testing"
)

func TestValidate_MissingName(t *testing.T) {
	cfg := &PipelineConfig{Source: SourceConfig{Type: "postgres", Table: "deals"}, Destinations: []DestinationConfig{{Type: "salesforce", MatchOn: []string{"deal_id"}}}, Cron: "*/5 * * * *"}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("Validate() error = %v, want name is required", err)
	}
}

func TestValidate_UnknownSourceType(t *testing.T) {
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "oracle", Table: "deals"}, Destinations: []DestinationConfig{{Type: "salesforce", MatchOn: []string{"deal_id"}}}, Cron: "*/5 * * * *"}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "source.type \"oracle\" unknown") {
		t.Fatalf("Validate() error = %v, want unknown source type", err)
	}
}

func TestValidate_BothTableAndQuery(t *testing.T) {
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "postgres", Table: "deals", Query: "select 1"}, Destinations: []DestinationConfig{{Type: "salesforce", MatchOn: []string{"deal_id"}}}, Cron: "*/5 * * * *"}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "set either table or query, not both") {
		t.Fatalf("Validate() error = %v, want table/query conflict", err)
	}
}

func TestValidate_MissingMatchOn(t *testing.T) {
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "postgres", Table: "deals"}, Destinations: []DestinationConfig{{Type: "salesforce", Strategy: "merge"}}, Cron: "*/5 * * * *"}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "requires match_on") {
		t.Fatalf("Validate() error = %v, want match_on error", err)
	}
}

func TestValidate_InvalidCron(t *testing.T) {
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "postgres", Table: "deals"}, Destinations: []DestinationConfig{{Type: "salesforce", MatchOn: []string{"deal_id"}}}, Cron: "not a cron"}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("Validate() error = %v, want invalid cron", err)
	}
}

func TestValidate_BatchNoCronOK(t *testing.T) {
	// Batch without cron is a one-shot run.
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "postgres", Table: "deals"}, Destinations: []DestinationConfig{{Type: "salesforce", MatchOn: []string{"deal_id"}}}}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidate_StreamingNoCronOK(t *testing.T) {
	cfg := &PipelineConfig{Name: "x", Source: SourceConfig{Type: "kafka", Topic: "events"}, Destinations: []DestinationConfig{{Type: "slack", Strategy: "append"}}}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidate_SnapshotWithAppendRejected(t *testing.T) {
	cfg := &PipelineConfig{
		Name:         "x",
		Source:       SourceConfig{Type: "postgres", Table: "countries", Watermark: "none"},
		Destinations: []DestinationConfig{{Type: "restapi"}}, // defaults to append
	}
	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "would duplicate") {
		t.Fatalf("Validate() = %v, want snapshot+append rejection", err)
	}
}

func TestValidate_SnapshotWithMergeOK(t *testing.T) {
	cfg := &PipelineConfig{
		Name:         "x",
		Source:       SourceConfig{Type: "postgres", Table: "countries", Watermark: "none"},
		Destinations: []DestinationConfig{{Type: "postgres", MatchOn: []string{"id"}, Strategy: "merge"}},
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}
