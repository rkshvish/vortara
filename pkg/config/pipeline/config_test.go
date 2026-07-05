package pipeline

import "testing"

func TestParse_MinimalBatch(t *testing.T) {
	cfg, err := Parse([]byte(`name: deals-sync
source:
  type: postgres
  table: deals
  watermark: updated_at
destinations:
  - type: salesforce
    match_on: [deal_id]
cron: "*/5 * * * *"
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestParse_MinimalStreaming(t *testing.T) {
	cfg, err := Parse([]byte(`name: events
source:
  type: kafka
  topic: deals-events
  group_id: vortara
destinations:
  - type: slack
    strategy: append
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestParse_Defaults_Applied(t *testing.T) {
	cfg, err := Parse([]byte(`name: deals-sync
source:
  type: postgres
  table: deals
destinations:
  - type: salesforce
    match_on: [deal_id]
cron: "*/5 * * * *"
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg.Source.BatchSize != 1000 {
		t.Fatalf("Source.BatchSize = %d, want 1000", cfg.Source.BatchSize)
	}
	if cfg.Source.Parallelism != 1 {
		t.Fatalf("Source.Parallelism = %d, want 1", cfg.Source.Parallelism)
	}
	if cfg.Destinations[0].Strategy != "merge" {
		t.Fatalf("Destinations[0].Strategy = %q, want merge", cfg.Destinations[0].Strategy)
	}
	if cfg.Settings.State.Backend != "sqlite" {
		t.Fatalf("Settings.State.Backend = %q, want sqlite", cfg.Settings.State.Backend)
	}
	if cfg.Settings.Log.Level != "info" {
		t.Fatalf("Settings.Log.Level = %q, want info", cfg.Settings.Log.Level)
	}
}

func TestParse_InvalidYAML(t *testing.T) {
	if _, err := Parse([]byte(`name: [unterminated`)); err == nil {
		t.Fatal("Parse() error = nil, want error")
	}
}
