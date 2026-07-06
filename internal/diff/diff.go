// Package diff computes field-level diffs between two entity payloads.
package diff

import "fmt"

// FieldChange captures a single field's before/after values.
type FieldChange struct {
	Previous any `json:"previous"`
	Current  any `json:"current"`
}

// Result is the set of changed fields between two payloads.
// Keys are field names; fields with identical values are omitted.
type Result map[string]FieldChange

// Compute returns the diff between previous and current payloads.
// Fields present in only one payload are included (nil for the absent side).
func Compute(previous, current map[string]any) Result {
	out := make(Result)
	seen := make(map[string]bool)
	for k, cur := range current {
		seen[k] = true
		prev, existed := previous[k]
		if !existed || !equal(prev, cur) {
			out[k] = FieldChange{Previous: prev, Current: cur}
		}
	}
	for k, prev := range previous {
		if !seen[k] {
			out[k] = FieldChange{Previous: prev, Current: nil}
		}
	}
	return out
}

// IsEmpty returns true when there are no field changes.
func (r Result) IsEmpty() bool {
	return len(r) == 0
}

// Contains returns true if the named field was changed.
func (r Result) Contains(field string) bool {
	_, ok := r[field]
	return ok
}

// OnlyContains returns true if the diff contains exactly the listed fields
// and no others.
func (r Result) OnlyContains(fields []string) bool {
	if len(r) != len(fields) {
		return false
	}
	for _, f := range fields {
		if !r.Contains(f) {
			return false
		}
	}
	return true
}

// Transitioned returns true when the named field moved from the `from` value
// to the `to` value.
func (r Result) Transitioned(field, from, to string) bool {
	ch, ok := r[field]
	if !ok {
		return false
	}
	return fmt.Sprintf("%v", ch.Previous) == from &&
		fmt.Sprintf("%v", ch.Current) == to
}

// equal performs a deep value comparison suitable for diff purposes.
func equal(a, b any) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
