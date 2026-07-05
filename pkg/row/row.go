// Package row defines the canonical data container passed between Vortara
// packages. Sources emit Rows and destinations consume Rows; no other payload
// type should cross package boundaries.
package row

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// pool holds recycled Row objects.
var pool = sync.Pool{
	New: func() any {
		return &Row{
			Data:     make(map[string]interface{}, 16),
			Metadata: make(map[string]interface{}, 4),
		}
	},
}

// Get retrieves a Row from the pool.
// Caller must call Put() when done with the Row.
// Row fields are zeroed — safe to use immediately.
func Get() *Row {
	r := pool.Get().(*Row)
	r.reset()
	return r
}

// Put returns a Row to the pool for reuse.
// Do not use the Row after calling Put.
func Put(r *Row) {
	pool.Put(r)
}

// reset clears all fields for reuse.
func (r *Row) reset() {
	r.ID = ""
	r.Source = ""
	r.Pipeline = ""
	r.PrimaryKey = ""
	r.ExtractedAt = time.Time{}
	r.Watermark = time.Time{}
	r.ctx = nil
	for k := range r.Data {
		delete(r.Data, k)
	}
	for k := range r.Metadata {
		delete(r.Metadata, k)
	}
}

// Row is the canonical record passed between Vortara packages.
type Row struct {
	ID          string                 // UUID generated at extraction time
	Source      string                 // e.g. "postgres.deals"
	Pipeline    string                 // pipeline name from config
	PrimaryKey  string                 // e.g. "deal_id=42"
	Data        map[string]interface{} // actual row data (column -> value)
	ExtractedAt time.Time              // when this row was extracted
	Watermark   time.Time              // updated_at value of this row
	Metadata    map[string]interface{} // source-specific extras (optional)
	ctx         context.Context
}

// NewRow constructs a Row with a generated ID and extraction timestamp.
func NewRow(source, pipeline, primaryKey string, data map[string]interface{}, watermark time.Time) Row {
	if data == nil {
		data = map[string]interface{}{}
	}

	return Row{
		ID:          uuid.NewString(),
		Source:      source,
		Pipeline:    pipeline,
		PrimaryKey:  primaryKey,
		Data:        data,
		ExtractedAt: time.Now(),
		Watermark:   watermark,
		Metadata:    map[string]interface{}{},
	}
}

// WithContext returns a copy of the row with context attached.
func (r Row) WithContext(ctx context.Context) Row {
	r.ctx = ctx
	return r
}

// Context returns the row's context, or context.Background() if unset.
func (r Row) Context() context.Context {
	if r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// String returns a compact log-friendly representation of the Row.
func (r Row) String() string {
	return fmt.Sprintf("pipeline=%s source=%s pk=%s", r.Pipeline, r.Source, r.PrimaryKey)
}

// Clone returns a deep copy of the Row's map fields.
func (r Row) Clone() Row {
	clone := r

	if r.Data != nil {
		clone.Data = make(map[string]interface{}, len(r.Data))
		for k, v := range r.Data {
			clone.Data[k] = v
		}
	}

	if r.Metadata != nil {
		clone.Metadata = make(map[string]interface{}, len(r.Metadata))
		for k, v := range r.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}
