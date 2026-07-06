// Package fingerprint computes deterministic SHA-256 hashes of entity payloads.
// Canonical JSON (sorted keys, no insignificant whitespace) ensures equal data
// produces equal fingerprints regardless of field insertion order.
package fingerprint

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
)

// Of returns a hex-encoded SHA-256 fingerprint of the given map.
// Keys in nested maps are recursively sorted for stability.
// Fields listed in exclude are omitted before hashing.
func Of(data map[string]any, exclude ...string) string {
	excl := make(map[string]bool, len(exclude))
	for _, k := range exclude {
		excl[k] = true
	}
	canonical := canonical(data, excl)
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// canonical returns a sorted representation of the map suitable for
// deterministic JSON marshalling.
func canonical(v any, exclude map[string]bool) any {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			if !exclude[k] {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		out := make([]keyVal, 0, len(keys))
		for _, k := range keys {
			out = append(out, keyVal{K: k, V: canonical(val[k], nil)})
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = canonical(item, nil)
		}
		return out
	default:
		return val
	}
}

// keyVal is a JSON-marshalable key-value pair that preserves sort order.
type keyVal struct {
	K string
	V any
}

func (kv keyVal) MarshalJSON() ([]byte, error) {
	k, err := json.Marshal(kv.K)
	if err != nil {
		return nil, err
	}
	v, err := json.Marshal(kv.V)
	if err != nil {
		return nil, err
	}
	return append(append(append(k, ':'), v...), 0)[:len(k)+1+len(v)], nil
}

// Changed returns true if a and b produce different fingerprints.
func Changed(a, b map[string]any, exclude ...string) bool {
	return Of(a, exclude...) != Of(b, exclude...)
}
