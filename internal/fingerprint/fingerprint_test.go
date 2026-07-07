package fingerprint

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func TestOf_NonEmptyDoesNotHashToEmptyBytes(t *testing.T) {
	fp := Of(map[string]any{"email": "alice@example.com"})
	if fp == emptyHash {
		t.Errorf("Of(non-empty map) returned SHA-256 of empty bytes — canonical encoding is broken")
	}
	if len(fp) != 64 {
		t.Errorf("expected 64-char hex string, got %d chars: %s", len(fp), fp)
	}
}

func TestOf_DeterministicAcrossInsertionOrder(t *testing.T) {
	a := map[string]any{"email": "alice@example.com", "score": 42, "active": true}
	// Go maps have non-deterministic iteration; build second map in a different way.
	b := map[string]any{"active": true, "score": 42, "email": "alice@example.com"}
	if Of(a) != Of(b) {
		t.Errorf("same data with different insertion order produced different fingerprints:\n  a=%s\n  b=%s", Of(a), Of(b))
	}
}

func TestOf_ExcludeChangesHash(t *testing.T) {
	data := map[string]any{"email": "alice@example.com", "synced_at": "2024-01-01T00:00:00Z"}
	withTS := Of(data)
	withoutTS := Of(data, "synced_at")
	if withTS == withoutTS {
		t.Error("excluding a field should change the fingerprint")
	}
}

func TestOf_ExcludeTimestampMakesHashStable(t *testing.T) {
	base := map[string]any{"email": "alice@example.com", "synced_at": "2024-01-01T00:00:00Z"}
	updated := map[string]any{"email": "alice@example.com", "synced_at": "2024-06-01T12:00:00Z"}
	if Of(base, "synced_at") != Of(updated, "synced_at") {
		t.Error("changing only an excluded field should not change the fingerprint")
	}
}

func TestOf_EmptyMapIsStableAndDistinctFromNonEmpty(t *testing.T) {
	empty1 := Of(map[string]any{})
	empty2 := Of(map[string]any{})
	if empty1 != empty2 {
		t.Error("empty map fingerprint should be stable across calls")
	}
	nonEmpty := Of(map[string]any{"k": "v"})
	if empty1 == nonEmpty {
		t.Error("empty map and non-empty map should have different fingerprints")
	}
	// Empty map encodes to "{}" — verify it is not the hash of empty bytes.
	expectedEmpty := fmt.Sprintf("%x", sha256.Sum256([]byte("{}")))
	if empty1 != expectedEmpty {
		t.Errorf("empty map: expected hash of '{}' (%s), got %s", expectedEmpty, empty1)
	}
}

func TestOf_NestedMapIsDeterministic(t *testing.T) {
	a := map[string]any{"user": map[string]any{"name": "Alice", "age": 30}}
	b := map[string]any{"user": map[string]any{"age": 30, "name": "Alice"}}
	if Of(a) != Of(b) {
		t.Error("nested map with different key order should produce same fingerprint")
	}
}

func TestOf_ExcludeOnlyAppliesToTopLevel(t *testing.T) {
	// "synced_at" at the top level is excluded; inside a nested map it is not.
	withNested := Of(map[string]any{
		"synced_at": "ts1",
		"meta":      map[string]any{"synced_at": "ts1"},
	}, "synced_at")
	// Changing the nested synced_at SHOULD change the fingerprint.
	withNestedChanged := Of(map[string]any{
		"synced_at": "ts1",
		"meta":      map[string]any{"synced_at": "ts2"},
	}, "synced_at")
	if withNested == withNestedChanged {
		t.Error("exclude should only apply to top-level keys; nested key change should alter fingerprint")
	}
}

func TestChanged(t *testing.T) {
	a := map[string]any{"x": 1}
	b := map[string]any{"x": 2}
	c := map[string]any{"x": 1}
	if !Changed(a, b) {
		t.Error("Changed(a, b) should be true when values differ")
	}
	if Changed(a, c) {
		t.Error("Changed(a, c) should be false when values are equal")
	}
}

// --- NormalizePayload tests ---

func TestNormalizePayload_TimeToBecomeRFC3339(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	got := NormalizePayload(map[string]any{"sessionStart": ts})
	v, ok := got["sessionStart"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", got["sessionStart"])
	}
	if v != "2026-07-01T10:00:00Z" {
		t.Errorf("unexpected RFC3339 value: %s", v)
	}
}

func TestNormalizePayload_TimeAndRFC3339StringAreEqual(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	fromTime := NormalizePayload(map[string]any{"sessionStart": ts})
	fromString := NormalizePayload(map[string]any{"sessionStart": "2026-07-01T10:00:00Z"})
	if fromTime["sessionStart"] != fromString["sessionStart"] {
		t.Errorf("time.Time and equivalent RFC3339 string should normalize to same value:\n  time=%v\n  string=%v",
			fromTime["sessionStart"], fromString["sessionStart"])
	}
}

func TestNormalizePayload_ExcludedTimestampDoesNotAppearAsChanged(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	// Simulate first-run payload stored as RFC3339 string (from JSON round-trip).
	prev := map[string]any{"revenue": 12000, "sessionStart": "2026-07-01T10:00:00Z"}
	// Next extraction returns time.Time from Postgres.
	curr := NormalizePayload(map[string]any{"revenue": 12000, "sessionStart": ts})

	// Fingerprints with sessionStart excluded must be equal.
	if Of(prev, "sessionStart") != Of(curr, "sessionStart") {
		t.Error("fingerprint should be equal when only excluded timestamp representation changed")
	}
}

func TestNormalizePayload_NumericValuesPreserved(t *testing.T) {
	got := NormalizePayload(map[string]any{"revenue": 13000, "ratio": 0.5, "count": int64(7)})
	if got["revenue"] != 13000 {
		t.Errorf("integer should be preserved, got %T %v", got["revenue"], got["revenue"])
	}
	if got["ratio"] != 0.5 {
		t.Errorf("float should be preserved, got %T %v", got["ratio"], got["ratio"])
	}
	if got["count"] != int64(7) {
		t.Errorf("int64 should be preserved, got %T %v", got["count"], got["count"])
	}
}

func TestNormalizePayload_NestedMapNormalized(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	got := NormalizePayload(map[string]any{
		"meta": map[string]any{"createdAt": ts},
	})
	nested, ok := got["meta"].(map[string]any)
	if !ok {
		t.Fatalf("nested map should remain map[string]any, got %T", got["meta"])
	}
	if nested["createdAt"] != "2026-07-01T10:00:00Z" {
		t.Errorf("nested time.Time should be normalized, got %v", nested["createdAt"])
	}
}

func TestNormalizePayload_SliceNormalized(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	got := NormalizePayload(map[string]any{"tags": []any{ts, "plain"}})
	sl, ok := got["tags"].([]any)
	if !ok {
		t.Fatalf("slice should remain []any, got %T", got["tags"])
	}
	if sl[0] != "2026-07-01T10:00:00Z" {
		t.Errorf("time.Time inside slice should be normalized, got %v", sl[0])
	}
	if sl[1] != "plain" {
		t.Errorf("string inside slice should be unchanged, got %v", sl[1])
	}
}

func TestNormalizePayload_OnlyTimestampChangesDoNotAlterDiff(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	prev := map[string]any{"revenue": 12000, "sessionStart": "2026-07-01T10:00:00Z"}
	// Same data but revenue changed; timestamp comes as time.Time from Postgres.
	curr := NormalizePayload(map[string]any{"revenue": 13000, "sessionStart": ts})

	// Only revenue should appear in fingerprint diff (sessionStart excluded).
	fpPrev := Of(prev, "sessionStart")
	fpCurr := Of(curr, "sessionStart")
	if fpPrev == fpCurr {
		t.Error("fingerprint should differ when revenue changed")
	}
	// The sessionStart values after normalization must be identical strings,
	// so it would not appear in a diff either.
	if prev["sessionStart"] != curr["sessionStart"] {
		t.Errorf("after normalization sessionStart should be identical string: prev=%v curr=%v",
			prev["sessionStart"], curr["sessionStart"])
	}
}
