package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

// --- fixed in-memory batch source for engine tests ---

var testSourceRows []row.Row

type fixedSource struct{}

func (s *fixedSource) Connect(_ context.Context, _ conncfg.SourceConfig) error { return nil }
func (s *fixedSource) Extract(_ context.Context, _ time.Time, _ time.Time, out chan<- row.Row) error {
	for _, r := range testSourceRows {
		out <- r
	}
	close(out)
	return nil
}
func (s *fixedSource) GetWatermarkColumn() string { return "" }
func (s *fixedSource) Close() error               { return nil }

func init() {
	registry.RegisterBatchSource("fixed", func() any { return &fixedSource{} })
}

// --- helpers ---

type captureDestination struct {
	rows []row.Row
}

func (d *captureDestination) Connect(_ context.Context, _ conncfg.DestinationConfig) error {
	return nil
}
func (d *captureDestination) Load(_ context.Context, rows []row.Row, _ state.StateStore, _, _ string) (destination.LoadResult, error) {
	d.rows = append(d.rows, rows...)
	return destination.LoadResult{Loaded: len(rows)}, nil
}
func (d *captureDestination) Close() error { return nil }

func makeSimpleSync(name string) *synccfg.SyncFile {
	return &synccfg.SyncFile{
		Sync: synccfg.SyncSpec{
			Name: name,
			Source: synccfg.SourceConfig{
				Type:      "test",
				EntityKey: "id",
			},
			Destination: synccfg.DestinationConfig{Type: "test"},
			Decisions: synccfg.DecisionsConfig{
				Default: "upsert",
			},
		},
	}
}

// --- tests ---

func TestDryRunRequired_BlocksRealRun(t *testing.T) {
	f := makeSimpleSync("test-dry-required")
	f.Sync.Safety.DryRunRequired = true

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	defer eng.Close()

	// When running without a dry-run override, should error immediately.
	err := eng.Run(context.Background(), f)
	if err == nil {
		t.Fatal("expected error when dry_run_required and real destination used")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestDryRunRequired_AllowsDryRunOverride(t *testing.T) {
	f := makeSimpleSync("test-dry-allowed")
	f.Sync.Safety.DryRunRequired = true

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	eng.SetDryRunDestination(&captureDestination{})
	defer eng.Close()

	// With a dry-run override, dry_run_required should not block.
	// (The source type "test" doesn't exist, so we'll get a source error, not a dry_run_required error.)
	err := eng.Run(context.Background(), f)
	if err != nil && err.Error() == "safety: dry_run_required is set — use 'dry-run' instead of 'run'" {
		t.Fatal("dry_run_required should not block when a dry-run override is set")
	}
}

func TestMissingRequiredFields(t *testing.T) {
	missing := missingRequiredFields(
		map[string]any{"a": 1, "b": nil, "c": "x"},
		[]string{"a", "b", "c", "d"},
	)
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing fields (b=nil, d=absent), got %v", missing)
	}
	has := func(s string) bool {
		for _, m := range missing {
			if m == s {
				return true
			}
		}
		return false
	}
	if !has("b") || !has("d") {
		t.Fatalf("expected b and d in missing, got %v", missing)
	}
}

func TestFPFields_WithInclude(t *testing.T) {
	data := map[string]any{"a": 1, "b": 2, "c": 3}
	include := buildFPIncludeSet([]string{"a", "c"})
	filtered := fpFields(data, include)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 fields, got %d: %v", len(filtered), filtered)
	}
	if _, ok := filtered["b"]; ok {
		t.Fatal("field b should be excluded by include list")
	}
}

func TestFPFields_NoInclude(t *testing.T) {
	data := map[string]any{"a": 1, "b": 2}
	filtered := fpFields(data, nil)
	// nil include means pass-through
	if len(filtered) != 2 {
		t.Fatalf("expected all fields, got %v", filtered)
	}
}

func TestBuildFPIncludeSet(t *testing.T) {
	s := buildFPIncludeSet([]string{"x", "y"})
	if len(s) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(s))
	}
	if _, ok := s["x"]; !ok {
		t.Fatal("expected x in set")
	}
	if s2 := buildFPIncludeSet(nil); s2 != nil {
		t.Fatal("nil input should return nil set")
	}
}

func TestApplyMapping_EmptyMapping(t *testing.T) {
	data := map[string]any{"x": 1, "y": 2}
	out := applyMapping(data, nil)
	if len(out) != 2 {
		t.Fatalf("empty mapping should pass all fields; got %v", out)
	}
}

func TestApplyMapping_WithMapping(t *testing.T) {
	mapping := []synccfg.MappingEntry{
		{Source: "first_name", Dest: "firstName"},
		{Source: "email"},
	}
	data := map[string]any{"first_name": "Alice", "email": "a@b.com", "extra": "drop"}
	out := applyMapping(data, mapping)
	if out["firstName"] != "Alice" {
		t.Fatalf("expected firstName=Alice, got %v", out)
	}
	if out["email"] != "a@b.com" {
		t.Fatalf("expected email, got %v", out)
	}
	if _, ok := out["extra"]; ok {
		t.Fatal("extra field should be dropped by mapping")
	}
}

func TestBuildRedactedFieldSet(t *testing.T) {
	mapping := []synccfg.MappingEntry{
		{Source: "email", Redacted: true},
		{Source: "name", Dest: "full_name", Redacted: true},
		{Source: "id"},
	}
	rs := buildRedactedFieldSet(mapping)
	if len(rs) != 2 {
		t.Fatalf("expected 2 redacted fields, got %d", len(rs))
	}
	if _, ok := rs["email"]; !ok {
		t.Fatal("email should be in redacted set")
	}
	if _, ok := rs["full_name"]; !ok {
		t.Fatal("full_name (dest of name) should be in redacted set")
	}
	if _, ok := rs["id"]; ok {
		t.Fatal("id should NOT be in redacted set")
	}
}

func TestBuildRedactedFieldSet_Empty(t *testing.T) {
	if rs := buildRedactedFieldSet(nil); rs != nil {
		t.Fatal("nil mapping should return nil set")
	}
	mapping := []synccfg.MappingEntry{{Source: "id"}}
	if rs := buildRedactedFieldSet(mapping); rs != nil {
		t.Fatal("no redacted fields should return nil set")
	}
}

func TestRedactPayload(t *testing.T) {
	data := map[string]any{"email": "a@b.com", "name": "Alice", "id": "123"}
	redacted := map[string]struct{}{"email": {}, "name": {}}
	out := redactPayload(data, redacted)

	if out["email"] != "[REDACTED]" {
		t.Fatalf("email should be redacted, got %v", out["email"])
	}
	if out["name"] != "[REDACTED]" {
		t.Fatalf("name should be redacted, got %v", out["name"])
	}
	if out["id"] != "123" {
		t.Fatalf("id should be unchanged, got %v", out["id"])
	}
	// Original map must not be modified.
	if data["email"] != "a@b.com" {
		t.Fatal("original map should not be modified by redactPayload")
	}
}

func TestRedactPayload_NilRedacted(t *testing.T) {
	data := map[string]any{"x": 1}
	out := redactPayload(data, nil)
	if out["x"] != 1 {
		t.Fatal("nil redacted should pass map through unchanged")
	}
}

func TestPipelineLock_BlocksConcurrentRun(t *testing.T) {
	store := state.NewMemoryStore()
	eng := NewEngine(store)
	defer eng.Close()

	f := makeSimpleSync("lock-test")

	// Run once — will fail on source open (type "test" not registered),
	// but the lock must be acquired first and then released.
	// The important property is that after the run returns, the lock is free.
	_ = eng.Run(context.Background(), f)

	// Lock should be released after run completes. We verify by locking manually.
	if err := store.LockRun(context.Background(), "lock-test", "manual", 1*time.Minute); err != nil {
		t.Fatalf("lock should be free after run: %v", err)
	}
}

func TestApprovalGate_BlocksDelivery(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "1", Data: map[string]any{"id": "1", "name": "Alice"}},
		{ID: "2", Data: map[string]any{"id": "2", "name": "Bob"}},
	}
	defer func() { testSourceRows = nil }()

	f := makeSimpleSync("approval-block-test")
	f.Sync.Source.Type = "fixed"
	f.Sync.Source.EntityKey = "id"
	f.Sync.Safety.RequireApprovalFor = []string{"create"}

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	eng.SetDryRunDestination(&captureDestination{})
	defer eng.Close()

	err := eng.Run(context.Background(), f)
	if err == nil {
		t.Fatal("expected approval error, got nil")
	}
	if !strings.Contains(err.Error(), "approval required") {
		t.Fatalf("expected 'approval required' in error, got: %v", err)
	}

	st := eng.LastStats()
	if st == nil {
		t.Fatal("expected LastStats to be set after blocked run")
	}
	if !st.ApprovalRequired {
		t.Error("expected ApprovalRequired=true")
	}
	if st.ApprovalHash == "" {
		t.Error("expected non-empty ApprovalHash")
	}
	if !strings.HasPrefix(st.ApprovalHash, "appr-") {
		t.Errorf("expected hash to start with 'appr-', got %q", st.ApprovalHash)
	}
}

func TestApprovalGate_BypassWithHash(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "1", Data: map[string]any{"id": "1", "name": "Alice"}},
	}
	defer func() { testSourceRows = nil }()

	makeSyncF := func(name string) *synccfg.SyncFile {
		f := makeSimpleSync(name)
		f.Sync.Source.Type = "fixed"
		f.Sync.Source.EntityKey = "id"
		f.Sync.Safety.RequireApprovalFor = []string{"create"}
		return f
	}

	// Run 1: get the approval hash.
	f := makeSyncF("approval-bypass-test")
	store1 := state.NewMemoryStore()
	eng1 := NewEngine(store1)
	eng1.SetDryRunDestination(&captureDestination{})
	defer eng1.Close()

	if err := eng1.Run(context.Background(), f); err == nil {
		t.Fatal("first run should require approval")
	}
	st1 := eng1.LastStats()
	if st1 == nil || st1.ApprovalHash == "" {
		t.Fatal("expected ApprovalHash from first run")
	}
	hash := st1.ApprovalHash

	// Run 2: provide the hash, should succeed.
	f2 := makeSyncF("approval-bypass-test")
	store2 := state.NewMemoryStore()
	eng2 := NewEngine(store2)
	eng2.SetDryRunDestination(&captureDestination{})
	eng2.SetApprovalHash(hash)
	defer eng2.Close()

	if err := eng2.Run(context.Background(), f2); err != nil {
		t.Fatalf("approved run should succeed, got: %v", err)
	}
	st2 := eng2.LastStats()
	if st2 == nil || st2.ApprovalRequired {
		t.Error("second run should not set ApprovalRequired")
	}
}

func TestLastStats_PopulatedAfterRun(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "1", Data: map[string]any{"id": "1", "val": "x"}},
		{ID: "2", Data: map[string]any{"id": "2", "val": "y"}},
	}
	defer func() { testSourceRows = nil }()

	f := makeSimpleSync("laststats-test")
	f.Sync.Source.Type = "fixed"
	f.Sync.Source.EntityKey = "id"

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	eng.SetDryRunDestination(&captureDestination{})
	defer eng.Close()

	if err := eng.Run(context.Background(), f); err != nil {
		t.Fatalf("run: %v", err)
	}
	st := eng.LastStats()
	if st == nil {
		t.Fatal("LastStats should not be nil after successful run")
	}
	if st.RowsExtracted != 2 {
		t.Errorf("expected RowsExtracted=2, got %d", st.RowsExtracted)
	}
	if st.Creates != 2 {
		t.Errorf("expected Creates=2, got %d", st.Creates)
	}
	if st.Status != "success" {
		t.Errorf("expected status=success, got %q", st.Status)
	}
}
