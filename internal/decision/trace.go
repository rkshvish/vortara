package decision

import (
	"context"
	"fmt"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// RuleTrace records the evaluation outcome for one rule in a Trace call.
type RuleTrace struct {
	Rule        string
	Action      string // the action this rule would apply (empty if predicate did not match)
	Matched     bool
	Reason      string
	FiredBefore bool // true when once:true and HasRuleFired returned true — rule was skipped
	Winner      bool // true for the first matched rule that sets the plan action
}

// Trace evaluates all rules (not just the first match) and records each outcome.
// The returned Plan is identical to what Evaluate would produce.
// Use the trace to show users which rules matched, which were skipped, and which won.
func Trace(
	ctx context.Context,
	cfg synccfg.DecisionsConfig,
	in Input,
	syncName, destination, entityKey string,
	checker RuleFiringChecker,
) (Plan, []RuleTrace, error) {
	plan := Plan{Remember: make(map[string]string)}
	var traces []RuleTrace
	won := false

	for _, rule := range cfg.Rules {
		tr := RuleTrace{Rule: rule.Name, Action: rule.Action}

		if rule.Once && checker != nil {
			fired, err := checker.HasRuleFired(ctx, syncName, destination, entityKey, rule.Name)
			if err != nil {
				return plan, traces, fmt.Errorf("check rule firing %q: %w", rule.Name, err)
			}
			if fired {
				tr.FiredBefore = true
				traces = append(traces, tr)
				continue
			}
		}

		matched, reason := evalWhen(rule.When, in)
		tr.Matched = matched
		tr.Reason = reason

		if matched && !won {
			won = true
			tr.Winner = true
			plan.Action = Action(rule.Action)
			plan.TriggeredRules = append(plan.TriggeredRules, rule.Name)
			plan.Reasons = append(plan.Reasons, reason)
			for k, expr := range rule.Remember {
				plan.Remember[k] = evalRememberExpr(expr)
			}
		}

		traces = append(traces, tr)
	}

	if plan.Action == "" {
		plan.Action = Action(cfg.Default)
		if plan.Action == "" {
			plan.Action = ActionSkip
		}
		plan.Reasons = append(plan.Reasons, "no rule matched, using default")
	}

	return plan, traces, nil
}
