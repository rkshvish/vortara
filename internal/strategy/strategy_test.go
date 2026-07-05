package strategy

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

type mockDestination struct {
	mu             sync.Mutex
	calls          int
	loadCalls      int
	seenStrategies []string
	result         destination.LoadResult
	err            error
}

func (m *mockDestination) Connect(context.Context, config.DestinationConfig) error { return nil }

func (m *mockDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (destination.LoadResult, error) {
	m.mu.Lock()
	m.calls++
	m.loadCalls++
	m.seenStrategies = append(m.seenStrategies, StrategyName(ctx))
	m.mu.Unlock()
	return m.result, m.err
}

func (m *mockDestination) Close() error { return nil }

func resetRegistryForTest() {
	resetForTest()
	Register(&MergeStrategy{})
	Register(&AppendStrategy{})
	Register(&ReplaceStrategy{})
	Register(&DeleteInsertStrategy{})
}

func TestMergeStrategy_Name(t *testing.T) {
	if got := (&MergeStrategy{}).Name(); got != "merge" {
		t.Fatalf("Name() = %q, want merge", got)
	}
}

func TestMergeStrategy_RequiresPrimaryKey(t *testing.T) {
	if !(&MergeStrategy{}).RequiresPrimaryKey() {
		t.Fatal("RequiresPrimaryKey() = false, want true")
	}
}

func TestAppendStrategy_RequiresPrimaryKey(t *testing.T) {
	if (&AppendStrategy{}).RequiresPrimaryKey() {
		t.Fatal("RequiresPrimaryKey() = true, want false")
	}
}

func TestReplaceStrategy_RequiresPrimaryKey(t *testing.T) {
	if (&ReplaceStrategy{}).RequiresPrimaryKey() {
		t.Fatal("RequiresPrimaryKey() = true, want false")
	}
}

func TestMergeStrategy_Execute_Success(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipe", "id=1", map[string]interface{}{"id": 1}, time.Now())
	dest := &mockDestination{result: destination.LoadResult{Loaded: 1}}

	res, err := (&MergeStrategy{}).Execute(context.Background(), []row.Row{rw}, dest, store, "pipe", "dest", "id")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("Loaded = %d, want 1", res.Loaded)
	}
	if ok, err := store.IsDelivered(ctx, rw.ID, "pipe", "dest"); err != nil || !ok {
		t.Fatalf("IsDelivered() = %v, %v; want true, nil", ok, err)
	}
	if dest.loadCalls != 1 {
		t.Fatalf("Load calls = %d, want 1", dest.loadCalls)
	}
}

func TestMergeStrategy_Execute_AlreadyDelivered(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipe", "id=1", map[string]interface{}{"id": 1}, time.Now())
	if err := store.MarkDelivered(ctx, rw.ID, "pipe", "dest"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	dest := &mockDestination{result: destination.LoadResult{Loaded: 1}}

	res, err := (&MergeStrategy{}).Execute(context.Background(), []row.Row{rw}, dest, store, "pipe", "dest", "id")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if res.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", res.Skipped)
	}
	if dest.loadCalls != 0 {
		t.Fatalf("Load calls = %d, want 0", dest.loadCalls)
	}
}

func TestAppendStrategy_Execute_SkipsDeliveryCheck(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipe", "id=1", map[string]interface{}{"id": 1}, time.Now())
	if err := store.MarkDelivered(ctx, rw.ID, "pipe", "dest"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	dest := &mockDestination{result: destination.LoadResult{Loaded: 1}}

	res, err := (&AppendStrategy{}).Execute(context.Background(), []row.Row{rw}, dest, store, "pipe", "dest", "")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("Loaded = %d, want 1", res.Loaded)
	}
	if dest.loadCalls != 1 {
		t.Fatalf("Load calls = %d, want 1", dest.loadCalls)
	}
}

func TestReplaceStrategy_Execute_TruncatesOnce(t *testing.T) {
	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipe", "id=1", map[string]interface{}{"id": 1}, time.Now())
	dest := &mockDestination{result: destination.LoadResult{Loaded: 1}}
	ctx := WithRunID(context.Background(), 1)
	strat := &ReplaceStrategy{}

	if _, err := strat.Execute(ctx, []row.Row{rw}, dest, store, "pipe", "dest", "id"); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	if _, err := strat.Execute(ctx, []row.Row{rw}, dest, store, "pipe", "dest", "id"); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if len(dest.seenStrategies) != 2 {
		t.Fatalf("seen strategies = %d, want 2", len(dest.seenStrategies))
	}
	if dest.seenStrategies[0] != "replace" {
		t.Fatalf("first strategy = %q, want replace", dest.seenStrategies[0])
	}
	if dest.seenStrategies[1] != "" {
		t.Fatalf("second strategy = %q, want empty", dest.seenStrategies[1])
	}
}

func TestGet_UnknownStrategy(t *testing.T) {
	resetRegistryForTest()
	defer resetRegistryForTest()

	_, err := Get("nonexistent")
	if err == nil {
		t.Fatal("Get() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "merge") {
		t.Fatalf("Get() error = %q, want list of valid names", err)
	}
}

func TestList_ContainsAll(t *testing.T) {
	resetRegistryForTest()
	defer resetRegistryForTest()

	names := List()
	want := map[string]bool{
		"append":        false,
		"delete+insert": false,
		"merge":         false,
		"replace":       false,
	}
	for _, name := range names {
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("List() missing %q from %v", name, names)
		}
	}
}

func TestRewriteStrategy_AppendOnlyDest_Slack(t *testing.T) {
	if got := RewriteStrategy("merge", "slack", "email"); got != "append" {
		t.Fatalf("RewriteStrategy() = %q, want append", got)
	}
}

func TestRewriteStrategy_AppendOnlyDest_GoogleSheets(t *testing.T) {
	if got := RewriteStrategy("merge", "googlesheets", "id"); got != "append" {
		t.Fatalf("RewriteStrategy() = %q, want append", got)
	}
}

func TestRewriteStrategy_MergeNoMatchOn(t *testing.T) {
	if got := RewriteStrategy("merge", "postgres", ""); got != "append" {
		t.Fatalf("RewriteStrategy() = %q, want append", got)
	}
}

func TestRewriteStrategy_DeleteInsertNoMatchOn(t *testing.T) {
	if got := RewriteStrategy("delete+insert", "postgres", ""); got != "append" {
		t.Fatalf("RewriteStrategy() = %q, want append", got)
	}
}

func TestRewriteStrategy_MergeWithMatchOn(t *testing.T) {
	if got := RewriteStrategy("merge", "postgres", "id"); got != "merge" {
		t.Fatalf("RewriteStrategy() = %q, want merge", got)
	}
}

func TestRewriteStrategy_AppendUnchanged(t *testing.T) {
	if got := RewriteStrategy("append", "salesforce", ""); got != "append" {
		t.Fatalf("RewriteStrategy() = %q, want append", got)
	}
}

func TestRewriteStrategy_ReplacePostgres(t *testing.T) {
	if got := RewriteStrategy("replace", "postgres", ""); got != "replace" {
		t.Fatalf("RewriteStrategy() = %q, want replace", got)
	}
}
