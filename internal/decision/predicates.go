package decision

import (
	"fmt"

	"github.com/rkshvish/vortara/internal/diff"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// Input holds everything needed to evaluate predicates for one entity.
type Input struct {
	IsFirstSeen        bool
	FingerprintChanged bool
	Diff               diff.Result
	PreviousPayload    map[string]any
	CurrentPayload     map[string]any
	RememberedState    map[string]any
}

// evalWhen evaluates a WhenConfig predicate against an Input.
// Returns (matched bool, reason string).
func evalWhen(when synccfg.WhenConfig, in Input) (bool, string) {
	// Leaf: first_seen()
	if when.FirstSeen != nil {
		return in.IsFirstSeen, "first_seen()"
	}

	// Leaf: fingerprint_changed()
	if when.FingerprintChanged != nil {
		return in.FingerprintChanged, "fingerprint_changed()"
	}

	// Structural: transitioned(field, from, to)
	if t := when.Transitioned; t != nil {
		matched := in.Diff.Transitioned(t.Field, t.From, t.To)
		reason := fmt.Sprintf("transitioned(%s, %s, %s)", t.Field, t.From, t.To)
		return matched, reason
	}

	// Structural: only_changed([fields...])
	if len(when.OnlyChanged) > 0 {
		matched := in.Diff.OnlyContains(when.OnlyChanged)
		reason := fmt.Sprintf("only_changed(%v)", when.OnlyChanged)
		return matched, reason
	}

	// Logical: any (OR)
	if len(when.Any) > 0 {
		for _, sub := range when.Any {
			if sub == nil {
				continue
			}
			if ok, reason := evalWhen(*sub, in); ok {
				return true, reason
			}
		}
		return false, "any(...) = false"
	}

	// Logical: all (AND)
	if len(when.All) > 0 {
		reasons := []string{}
		for _, sub := range when.All {
			if sub == nil {
				continue
			}
			ok, reason := evalWhen(*sub, in)
			if !ok {
				return false, reason
			}
			reasons = append(reasons, reason)
		}
		return true, fmt.Sprintf("all(%v)", reasons)
	}

	// Logical: not
	if when.Not != nil {
		ok, reason := evalWhen(*when.Not, in)
		return !ok, "not(" + reason + ")"
	}

	return false, "empty predicate"
}
