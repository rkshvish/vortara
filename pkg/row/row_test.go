package row

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNewRow verifies NewRow populates the required fields.
func TestNewRow(t *testing.T) {
	watermark := time.Now().UTC()
	r := NewRow("postgres.deals", "pipeline-a", "deal_id=42", map[string]interface{}{"deal_id": 42}, watermark)

	if r.ID == "" {
		t.Fatal("expected ID to be set")
	}

	if r.ExtractedAt.IsZero() {
		t.Fatal("expected ExtractedAt to be set")
	}

	if r.Metadata == nil {
		t.Fatal("expected Metadata to be initialized")
	}
}

// TestRowClone verifies Clone deep copies the map fields.
func TestRowClone(t *testing.T) {
	original := NewRow("postgres.deals", "pipeline-a", "deal_id=42", map[string]interface{}{"deal_id": 42}, time.Now())
	original.Metadata["kind"] = "deal"

	clone := original.Clone()
	clone.Data["deal_id"] = 99
	clone.Metadata["kind"] = "other"

	if original.Data["deal_id"].(int) != 42 {
		t.Fatalf("expected original Data to remain unchanged, got %v", original.Data["deal_id"])
	}

	if original.Metadata["kind"].(string) != "deal" {
		t.Fatalf("expected original Metadata to remain unchanged, got %v", original.Metadata["kind"])
	}
}

// TestRowPool_GetPut verifies that Put resets the Row so a subsequent Get returns a clean object.
func TestRowPool_GetPut(t *testing.T) {
	r1 := Get()
	r1.ID = "test"
	Put(r1)

	r2 := Get()
	if r2.ID != "" {
		t.Fatalf("expected empty ID after pool reset, got %q", r2.ID)
	}
	Put(r2)
}

// TestRowPool_Concurrent verifies the pool is safe under concurrent use.
func TestRowPool_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				r := Get()
				r.ID = "x"
				r.Data["key"] = "value"
				Put(r)
			}
		}()
	}
	wg.Wait()
}

// TestRowPool_DataCleared verifies that Data map is empty after a Get following a Put.
func TestRowPool_DataCleared(t *testing.T) {
	r := Get()
	r.Data["key"] = "value"
	Put(r)

	r2 := Get()
	if len(r2.Data) != 0 {
		t.Fatalf("expected empty Data after pool reset, got %d entries", len(r2.Data))
	}
	Put(r2)
}

// TestRowString verifies String returns a compact log-friendly summary.
func TestRowString(t *testing.T) {
	r := NewRow("postgres.deals", "pipeline-a", "deal_id=42", map[string]interface{}{}, time.Now())

	got := r.String()
	for _, want := range []string{"pipeline=pipeline-a", "source=postgres.deals", "pk=deal_id=42"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q to contain %q", got, want)
		}
	}
}

func TestRow_WithContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "ok")
	r := NewRow("postgres.deals", "pipeline-a", "deal_id=42", map[string]interface{}{}, time.Now())
	got := r.WithContext(ctx).Context()
	if got != ctx {
		t.Fatal("expected attached context to round-trip")
	}
}

func TestRow_Context_Default(t *testing.T) {
	if got := (Row{}).Context(); got == nil {
		t.Fatal("expected default context to be non-nil")
	}
}

func TestRow_ContextPreservedThroughPool(t *testing.T) {
	r := Get()
	ctx := context.WithValue(context.Background(), struct{}{}, "ok")
	*r = r.WithContext(ctx)
	Put(r)

	r2 := Get()
	if got := r2.Context(); got == ctx {
		t.Fatal("expected pooled row context to be reset")
	}
	Put(r2)
}
