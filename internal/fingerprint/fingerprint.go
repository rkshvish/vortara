// Package fingerprint computes deterministic SHA-256 hashes of entity payloads.
// Canonical JSON (sorted keys, no insignificant whitespace) ensures equal data
// produces equal fingerprints regardless of field insertion order.
package fingerprint

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Of returns a hex-encoded SHA-256 fingerprint of the given map.
// Top-level fields listed in exclude are omitted before hashing.
// Keys in nested maps are recursively sorted for stability.
func Of(data map[string]any, exclude ...string) string {
	excl := make(map[string]bool, len(exclude))
	for _, k := range exclude {
		excl[k] = true
	}
	var buf bytes.Buffer
	writeCanonical(&buf, data, excl, true)
	sum := sha256.Sum256(buf.Bytes())
	return fmt.Sprintf("%x", sum)
}

// writeCanonical writes a deterministic JSON encoding of v into buf.
// When topLevel is true and v is a map, keys present in excl are skipped.
func writeCanonical(buf *bytes.Buffer, v any, excl map[string]bool, topLevel bool) {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			if topLevel && excl[k] {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteByte(':')
			writeCanonical(buf, val[k], excl, false)
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonical(buf, item, excl, false)
		}
		buf.WriteByte(']')
	default:
		b, _ := json.Marshal(val)
		buf.Write(b)
	}
}

// Changed returns true if a and b produce different fingerprints.
func Changed(a, b map[string]any, exclude ...string) bool {
	return Of(a, exclude...) != Of(b, exclude...)
}

// NormalizePayload converts a mapped payload to a canonical, JSON-serialisable
// form before fingerprinting, diffing, or persisting. Currently it:
//   - converts time.Time → UTC RFC3339 string, eliminating cosmetic differences
//     between "2026-07-01T10:00:00Z" and "2026-07-01 10:00:00 +0000 UTC"
//   - recurses into nested map[string]any and []any
//   - preserves strings, numbers, booleans, and nil unchanged
func NormalizePayload(data map[string]any) map[string]any {
	out := make(map[string]any, len(data))
	for k, v := range data {
		out[k] = normalizeValue(v)
	}
	return out
}

func normalizeValue(v any) any {
	switch val := v.(type) {
	case time.Time:
		return val.UTC().Format(time.RFC3339)
	case *time.Time:
		if val == nil {
			return nil
		}
		return val.UTC().Format(time.RFC3339)
	case map[string]any:
		return NormalizePayload(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = normalizeValue(item)
		}
		return out
	default:
		return v
	}
}
