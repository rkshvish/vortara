// Package decision evaluates rules and produces per-entity decision plans.
package decision

// Action is the delivery action the engine should take for an entity.
type Action string

const (
	ActionUpsert Action = "upsert"
	ActionUpdate Action = "update"
	ActionCreate Action = "create"
	ActionDelete Action = "delete"
	ActionSkip   Action = "skip"
)

// Plan is the outcome of evaluating all rules for one entity.
type Plan struct {
	Action         Action
	TriggeredRules []string
	Reasons        []string
	Remember       map[string]string // key → value to persist in entity remembered state
}

// Skipped returns true when no delivery is needed.
func (p Plan) Skipped() bool {
	return p.Action == ActionSkip
}
