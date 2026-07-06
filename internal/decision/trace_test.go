package decision

import (
	"context"
	"testing"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// noopChecker never reports a rule as having fired.
type noopChecker struct{}

func (noopChecker) HasRuleFired(_ context.Context, _, _, _, _ string) (bool, error) {
	return false, nil
}

// firedChecker always reports every rule as having fired.
type firedChecker struct{}

func (firedChecker) HasRuleFired(_ context.Context, _, _, _, _ string) (bool, error) {
	return true, nil
}

func firstSeen() synccfg.WhenConfig {
	v := struct{}{}
	return synccfg.WhenConfig{FirstSeen: &v}
}

func fpChanged() synccfg.WhenConfig {
	v := struct{}{}
	return synccfg.WhenConfig{FingerprintChanged: &v}
}

func TestTrace_FirstMatch_Wins(t *testing.T) {
	cfg := synccfg.DecisionsConfig{
		Rules: []synccfg.RuleConfig{
			{Name: "create_new", When: firstSeen(), Action: "create"},
			{Name: "update_changed", When: fpChanged(), Action: "update"},
		},
		Default: "skip",
	}
	in := Input{IsFirstSeen: true, FingerprintChanged: true}

	plan, traces, err := Trace(context.Background(), cfg, in, "s", "d", "k", noopChecker{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionCreate {
		t.Errorf("expected create, got %q", plan.Action)
	}
	if len(traces) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(traces))
	}
	if !traces[0].Winner {
		t.Error("first rule should be winner")
	}
	if traces[1].Winner {
		t.Error("second rule should not be winner")
	}
	// Both rules matched — but only first is winner.
	if !traces[0].Matched || !traces[1].Matched {
		t.Error("both rules should report matched=true")
	}
}

func TestTrace_NoMatch_UsesDefault(t *testing.T) {
	cfg := synccfg.DecisionsConfig{
		Rules:   []synccfg.RuleConfig{{Name: "create_new", When: firstSeen(), Action: "create"}},
		Default: "skip",
	}
	in := Input{IsFirstSeen: false, FingerprintChanged: false}

	plan, traces, err := Trace(context.Background(), cfg, in, "s", "d", "k", noopChecker{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionSkip {
		t.Errorf("expected skip, got %q", plan.Action)
	}
	if len(traces) != 1 || traces[0].Matched {
		t.Error("single trace should show not matched")
	}
}

func TestTrace_OnceFired_MarksSkipped(t *testing.T) {
	cfg := synccfg.DecisionsConfig{
		Rules: []synccfg.RuleConfig{
			{Name: "once_rule", When: firstSeen(), Action: "create", Once: true},
		},
		Default: "skip",
	}
	in := Input{IsFirstSeen: true}

	plan, traces, err := Trace(context.Background(), cfg, in, "s", "d", "k", firedChecker{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionSkip {
		t.Errorf("expected skip (once already fired), got %q", plan.Action)
	}
	if len(traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(traces))
	}
	if !traces[0].FiredBefore {
		t.Error("trace should report FiredBefore=true")
	}
}

func TestTrace_MatchesEvaluate(t *testing.T) {
	cfg := synccfg.DecisionsConfig{
		Rules: []synccfg.RuleConfig{
			{Name: "a", When: firstSeen(), Action: "create"},
			{Name: "b", When: fpChanged(), Action: "update"},
		},
		Default: "skip",
	}

	tests := []struct {
		in     Input
		action Action
	}{
		{Input{IsFirstSeen: true}, ActionCreate},
		{Input{FingerprintChanged: true}, ActionUpdate},
		{Input{}, ActionSkip},
	}
	for _, tc := range tests {
		plan, _, err := Trace(context.Background(), cfg, tc.in, "s", "d", "k", noopChecker{})
		if err != nil {
			t.Fatalf("trace: %v", err)
		}
		evalPlan, err := Evaluate(context.Background(), cfg, tc.in, "s", "d", "k", noopChecker{})
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if plan.Action != evalPlan.Action {
			t.Errorf("Trace and Evaluate diverged: Trace=%q Evaluate=%q", plan.Action, evalPlan.Action)
		}
		if plan.Action != tc.action {
			t.Errorf("expected %q, got %q", tc.action, plan.Action)
		}
	}
}
