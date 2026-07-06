package safety

import (
	"testing"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

func TestAllow_CreateLimit(t *testing.T) {
	e := New(synccfg.SafetyConfig{MaxCreatesPerRun: 2})
	counts := RunCounts{}

	if err := e.Allow("create", counts); err != nil {
		t.Fatalf("first create should be allowed: %v", err)
	}
	e.Record("create", &counts)
	if err := e.Allow("create", counts); err != nil {
		t.Fatalf("second create should be allowed: %v", err)
	}
	e.Record("create", &counts)
	if err := e.Allow("create", counts); err == nil {
		t.Fatal("third create should be blocked by max_creates_per_run=2")
	}
}

func TestCheckFieldRatios_NoConfig(t *testing.T) {
	e := New(synccfg.SafetyConfig{})
	if err := e.CheckFieldRatios(map[string]int{"email": 50}, 100); err != nil {
		t.Fatalf("no config should never block: %v", err)
	}
}

func TestCheckFieldRatios_NoEntities(t *testing.T) {
	e := New(synccfg.SafetyConfig{
		BlockIfChangedFieldRatioAbove: map[string]float64{"email": 0.2},
	})
	if err := e.CheckFieldRatios(map[string]int{"email": 0}, 0); err != nil {
		t.Fatalf("zero entities should never block: %v", err)
	}
}

func TestCheckFieldRatios_BelowThreshold(t *testing.T) {
	e := New(synccfg.SafetyConfig{
		BlockIfChangedFieldRatioAbove: map[string]float64{"status": 0.20},
	})
	// 10 out of 100 changed = 10%, below 20% limit
	if err := e.CheckFieldRatios(map[string]int{"status": 10}, 100); err != nil {
		t.Fatalf("10%% should be allowed under 20%% limit: %v", err)
	}
}

func TestCheckFieldRatios_AtThreshold(t *testing.T) {
	e := New(synccfg.SafetyConfig{
		BlockIfChangedFieldRatioAbove: map[string]float64{"status": 0.20},
	})
	// Exactly 20% — not strictly above, so should be allowed.
	if err := e.CheckFieldRatios(map[string]int{"status": 20}, 100); err != nil {
		t.Fatalf("exactly at threshold should be allowed: %v", err)
	}
}

func TestCheckFieldRatios_AboveThreshold(t *testing.T) {
	e := New(synccfg.SafetyConfig{
		BlockIfChangedFieldRatioAbove: map[string]float64{"status": 0.20},
	})
	// 21 out of 100 = 21%, above 20%
	if err := e.CheckFieldRatios(map[string]int{"status": 21}, 100); err == nil {
		t.Fatal("21% should be blocked by 20% limit")
	}
}

func TestCheckFieldRatios_FieldNotChanged(t *testing.T) {
	e := New(synccfg.SafetyConfig{
		BlockIfChangedFieldRatioAbove: map[string]float64{"status": 0.20},
	})
	// Field "status" not in counts (0 changes) — should always pass.
	if err := e.CheckFieldRatios(map[string]int{"email": 99}, 100); err != nil {
		t.Fatalf("unconfigured field changes should not block: %v", err)
	}
}
