package diff

import (
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/fingerprint"
)

func TestCompute_BasicChange(t *testing.T) {
	prev := map[string]any{"revenue": 12000, "name": "Alice"}
	curr := map[string]any{"revenue": 13000, "name": "Alice"}
	r := Compute(prev, curr)
	if _, ok := r["revenue"]; !ok {
		t.Error("expected revenue in diff")
	}
	if _, ok := r["name"]; ok {
		t.Error("unchanged field name should not appear in diff")
	}
}

func TestCompute_NormalizedTimestamp_NoDiff(t *testing.T) {
	// Simulate: prevPayload stored as JSON string (from state), current comes
	// as time.Time from Postgres. After NormalizePayload both should be equal.
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

	prev := fingerprint.NormalizePayload(map[string]any{
		"revenue":      12000,
		"sessionStart": "2026-07-01T10:00:00Z", // stored as RFC3339 string
	})
	curr := fingerprint.NormalizePayload(map[string]any{
		"revenue":      12000,
		"sessionStart": ts, // extracted as time.Time
	})

	r := Compute(prev, curr)
	if _, ok := r["sessionStart"]; ok {
		t.Errorf("sessionStart should not appear in diff after normalization: got %+v", r["sessionStart"])
	}
	if len(r) != 0 {
		t.Errorf("expected empty diff, got fields: %v", r)
	}
}

func TestCompute_OnlyRevenueChanges_TimestampSilent(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

	prev := fingerprint.NormalizePayload(map[string]any{
		"revenue":      12000,
		"sessionStart": "2026-07-01T10:00:00Z",
	})
	curr := fingerprint.NormalizePayload(map[string]any{
		"revenue":      13000,
		"sessionStart": ts,
	})

	r := Compute(prev, curr)
	if _, ok := r["sessionStart"]; ok {
		t.Error("sessionStart should not appear in diff when only representation changed")
	}
	fc, ok := r["revenue"]
	if !ok {
		t.Fatal("revenue should appear in diff")
	}
	if fc.Previous != 12000 || fc.Current != 13000 {
		t.Errorf("revenue diff: expected 12000→13000, got %v→%v", fc.Previous, fc.Current)
	}
	if len(r) != 1 {
		t.Errorf("expected exactly 1 changed field (revenue), got %d: %v", len(r), r)
	}
}

func TestCompute_NewField(t *testing.T) {
	prev := map[string]any{"a": 1}
	curr := map[string]any{"a": 1, "b": 2}
	r := Compute(prev, curr)
	fc, ok := r["b"]
	if !ok {
		t.Fatal("new field b should appear in diff")
	}
	if fc.Previous != nil {
		t.Errorf("previous for new field should be nil, got %v", fc.Previous)
	}
}

func TestCompute_RemovedField(t *testing.T) {
	prev := map[string]any{"a": 1, "b": 2}
	curr := map[string]any{"a": 1}
	r := Compute(prev, curr)
	fc, ok := r["b"]
	if !ok {
		t.Fatal("removed field b should appear in diff")
	}
	if fc.Current != nil {
		t.Errorf("current for removed field should be nil, got %v", fc.Current)
	}
}

func TestCompute_EmptyPayloads(t *testing.T) {
	r := Compute(nil, nil)
	if !r.IsEmpty() {
		t.Error("nil vs nil should produce empty diff")
	}
	r2 := Compute(map[string]any{}, map[string]any{})
	if !r2.IsEmpty() {
		t.Error("empty vs empty should produce empty diff")
	}
}
