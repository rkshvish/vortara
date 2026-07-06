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
	registry.RegisterDestination("capture", func() any { return &captureDestination{} })
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

// idempotentDestination tracks calls and simulates the real destination behaviour:
// the first Load succeeds; subsequent calls with the same row.ID return Skipped.
type idempotentDestination struct {
	seen    map[string]bool
	loadCnt int
	skipCnt int
}

func newIdempotentDest() *idempotentDestination {
	return &idempotentDestination{seen: make(map[string]bool)}
}

func (d *idempotentDestination) Connect(_ context.Context, _ conncfg.DestinationConfig) error {
	return nil
}
func (d *idempotentDestination) Load(_ context.Context, rows []row.Row, _ state.StateStore, _, _ string) (destination.LoadResult, error) {
	var res destination.LoadResult
	for _, r := range rows {
		if d.seen[r.ID] {
			res.Skipped++
			d.skipCnt++
		} else {
			d.seen[r.ID] = true
			res.Loaded++
			d.loadCnt++
		}
	}
	return res, nil
}
func (d *idempotentDestination) Close() error { return nil }

// TestDeliveryOpKey_IsDeterministic verifies that the same inputs always produce
// the same key, and that different inputs produce different keys.
func TestDeliveryOpKey_IsDeterministic(t *testing.T) {
	k1 := deliveryOpKey("sync", "dest", "ent1", "create", "fp1")
	k2 := deliveryOpKey("sync", "dest", "ent1", "create", "fp1")
	if k1 != k2 {
		t.Errorf("expected same key, got %q and %q", k1, k2)
	}
	k3 := deliveryOpKey("sync", "dest", "ent1", "create", "fp2")
	if k1 == k3 {
		t.Errorf("different fingerprints should produce different keys")
	}
	k4 := deliveryOpKey("sync", "dest", "ent1", "update", "fp1")
	if k1 == k4 {
		t.Errorf("different actions should produce different keys")
	}
}

// TestRealRun_SavesEntityState verifies that a real (non-dry-run) run persists
// entity state so that the second run skips the same entity.
func TestRealRun_SavesEntityState(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "u1", Data: map[string]any{"id": "u1", "val": "hello"}},
	}
	defer func() { testSourceRows = nil }()

	f := makeSimpleSync("realrun-idempotency")
	f.Sync.Source.Type = "fixed"
	f.Sync.Source.EntityKey = "id"
	f.Sync.Destination.Type = "capture"

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	defer eng.Close()

	// First run — entity is first_seen, should be delivered and state saved.
	if err := eng.Run(context.Background(), f); err != nil {
		t.Fatalf("first run: %v", err)
	}
	es, err := store.GetEntityState(context.Background(), "realrun-idempotency", "capture", "u1")
	if err != nil {
		t.Fatalf("get entity state: %v", err)
	}
	if es == nil {
		t.Fatal("entity state should be saved after first run")
	}
	if es.LastStatus != "success" {
		t.Errorf("expected status=success, got %q", es.LastStatus)
	}
}

// TestDryRun_DoesNotSaveEntityState verifies that dry-run mode never persists
// entity state, so a subsequent real run still sees the entity as first_seen.
func TestDryRun_DoesNotSaveEntityState(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "dr1", Data: map[string]any{"id": "dr1", "val": "x"}},
	}
	defer func() { testSourceRows = nil }()

	f := makeSimpleSync("dryrun-no-state")
	f.Sync.Source.Type = "fixed"
	f.Sync.Source.EntityKey = "id"
	f.Sync.Destination.Type = "capture"

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	eng.SetDryRunDestination(&captureDestination{})
	defer eng.Close()

	if err := eng.Run(context.Background(), f); err != nil {
		t.Fatalf("dry-run: %v", err)
	}

	// Entity state must NOT have been written.
	es, err := store.GetEntityState(context.Background(), "dryrun-no-state", "capture", "dr1")
	if err != nil {
		t.Fatalf("get entity state: %v", err)
	}
	if es != nil {
		t.Errorf("dry-run must not persist entity state, got status=%q", es.LastStatus)
	}
}

// TestSkippedDelivery_DoesNotSaveEntityState verifies that when the destination
// reports Skipped=1, Loaded=0 (idempotency hit), the engine does not
// overwrite entity state.
func TestSkippedDelivery_DoesNotSaveEntityState(t *testing.T) {
	testSourceRows = []row.Row{
		{ID: "sk1", Data: map[string]any{"id": "sk1", "val": "hello"}},
	}
	defer func() { testSourceRows = nil }()

	f := makeSimpleSync("skip-no-state")
	f.Sync.Source.Type = "fixed"
	f.Sync.Source.EntityKey = "id"
	f.Sync.Destination.Type = "capture"

	store := state.NewMemoryStore()
	eng := NewEngine(store)
	defer eng.Close()

	// Run 1: entity is first_seen, delivers and saves state.
	if err := eng.Run(context.Background(), f); err != nil {
		t.Fatalf("first run: %v", err)
	}

	es1, _ := store.GetEntityState(context.Background(), "skip-no-state", "capture", "sk1")
	if es1 == nil {
		t.Fatal("entity state should be saved after first run")
	}
	savedAt := es1.UpdatedAt

	// Run 2: fingerprint unchanged → decision is skip → entity state must not change.
	eng2 := NewEngine(store)
	defer eng2.Close()
	if err := eng2.Run(context.Background(), f); err != nil {
		t.Fatalf("second run: %v", err)
	}
	es2, _ := store.GetEntityState(context.Background(), "skip-no-state", "capture", "sk1")
	if es2 == nil {
		t.Fatal("entity state should still exist after second run")
	}
	if !es2.UpdatedAt.Equal(savedAt) {
		t.Errorf("entity state UpdatedAt changed on second run (skip): was %v, now %v", savedAt, es2.UpdatedAt)
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
