package destination

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

type fakeSheets struct {
	calls  int
	ranges []string
	values [][][]interface{}
	err    error
}

func (f *fakeSheets) Append(ctx context.Context, spreadsheetID, rangeA1 string, values [][]interface{}) error {
	f.calls++
	f.ranges = append(f.ranges, rangeA1)
	f.values = append(f.values, values)
	return f.err
}

func withFakeSheets(t *testing.T, fake *fakeSheets) {
	t.Helper()
	orig := newSheetsService
	newSheetsService = func(ctx context.Context, cfg config.DestinationConfig) (sheetsService, error) {
		return fake, nil
	}
	t.Cleanup(func() { newSheetsService = orig })
}

func TestGoogleSheets_Connect_Validation(t *testing.T) {
	d := NewGoogleSheetsDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{Options: map[string]string{}})
	if err == nil || !strings.Contains(err.Error(), "spreadsheet_id is required") {
		t.Fatalf("Connect() = %v, want spreadsheet_id required", err)
	}
}

func TestGoogleSheets_Load_AppendsBatch(t *testing.T) {
	fake := &fakeSheets{}
	withFakeSheets(t, fake)

	d := NewGoogleSheetsDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"spreadsheet_id": "sheet-123", "sheet": "Deals"},
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("src", "pipe", "pk1", map[string]interface{}{"amount": 100, "name": "Acme"}, time.Now()),
		row.NewRow("src", "pipe", "pk2", map[string]interface{}{"amount": 200, "name": "Beta"}, time.Now()),
	}
	result, err := d.Load(context.Background(), rows, store, "pipe", "sheets")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 2 {
		t.Fatalf("Loaded = %d, want 2", result.Loaded)
	}
	if fake.calls != 1 {
		t.Fatalf("Append calls = %d, want 1 (batched)", fake.calls)
	}
	if fake.ranges[0] != "Deals" {
		t.Fatalf("range = %q, want Deals", fake.ranges[0])
	}
	// Columns sorted alphabetically: amount, name.
	got := fake.values[0]
	if len(got) != 2 || got[0][0] != 100 || got[0][1] != "Acme" || got[1][1] != "Beta" {
		t.Fatalf("values = %v", got)
	}

	// Re-loading the same rows should skip (idempotency).
	result, err = d.Load(context.Background(), rows, store, "pipe", "sheets")
	if err != nil {
		t.Fatalf("Load() second call error = %v", err)
	}
	if result.Skipped != 2 || fake.calls != 1 {
		t.Fatalf("second load = %+v calls=%d, want 2 skipped, no new append", result, fake.calls)
	}
}

func TestGoogleSheets_Load_ExplicitColumns(t *testing.T) {
	fake := &fakeSheets{}
	withFakeSheets(t, fake)

	d := NewGoogleSheetsDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{
			"spreadsheet_id": "sheet-123",
			"columns":        "name, amount",
		},
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("src", "pipe", "pk1", map[string]interface{}{"amount": 100, "name": "Acme", "extra": "x"}, time.Now()),
	}
	if _, err := d.Load(context.Background(), rows, store, "pipe", "sheets"); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got := fake.values[0][0]
	if len(got) != 2 || got[0] != "Acme" || got[1] != 100 {
		t.Fatalf("values = %v, want [Acme 100] in configured order", got)
	}
}

func TestGoogleSheets_Load_APIError(t *testing.T) {
	fake := &fakeSheets{err: errors.New("quota exceeded")}
	withFakeSheets(t, fake)

	d := NewGoogleSheetsDestination()
	if err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"spreadsheet_id": "s"},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("src", "pipe", "pk1", map[string]interface{}{"a": 1}, time.Now()),
	}
	result, err := d.Load(context.Background(), rows, store, "pipe", "sheets")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(result.Errors) != 1 || result.Loaded != 0 {
		t.Fatalf("result = %+v, want 1 row error", result)
	}
	delivered, _ := store.IsDelivered(rows[0].ID, "pipe", "sheets")
	if delivered {
		t.Fatal("failed row must not be marked delivered")
	}
}
