package decision

import (
	"context"
	"fmt"
	"time"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// RuleFiringChecker is used to query whether a once-rule has already fired.
type RuleFiringChecker interface {
	HasRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) (bool, error)
}

// Evaluate produces a Plan for one entity given the decision config and input.
// ruleFired is called for each rule with once:true to skip already-fired rules.
func Evaluate(
	ctx context.Context,
	cfg synccfg.DecisionsConfig,
	in Input,
	syncName, destination, entityKey string,
	checker RuleFiringChecker,
) (Plan, error) {
	plan := Plan{
		Remember: make(map[string]string),
	}

	for _, rule := range cfg.Rules {
		// once: true — skip if this rule has already fired for this entity
		if rule.Once && checker != nil {
			fired, err := checker.HasRuleFired(ctx, syncName, destination, entityKey, rule.Name)
			if err != nil {
				return plan, fmt.Errorf("check rule firing %q: %w", rule.Name, err)
			}
			if fired {
				continue
			}
		}

		matched, reason := evalWhen(rule.When, in)
		if !matched {
			continue
		}

		// Rule matched: record action and reasons
		plan.Action = Action(rule.Action)
		plan.TriggeredRules = append(plan.TriggeredRules, rule.Name)
		plan.Reasons = append(plan.Reasons, reason)

		// Collect remember values
		for k, expr := range rule.Remember {
			plan.Remember[k] = evalRememberExpr(expr)
		}

		// First matching rule wins (rules are evaluated in order)
		break
	}

	// No rule matched: fall back to default action
	if plan.Action == "" {
		plan.Action = Action(cfg.Default)
		if plan.Action == "" {
			plan.Action = ActionSkip
		}
		plan.Reasons = append(plan.Reasons, "no rule matched, using default")
	}

	return plan, nil
}

// evalRememberExpr evaluates a remember expression.
// Currently only "now()" is supported; other values are returned as-is.
func evalRememberExpr(expr string) string {
	if expr == "now()" {
		return time.Now().UTC().Format(time.RFC3339)
	}
	return expr
}
