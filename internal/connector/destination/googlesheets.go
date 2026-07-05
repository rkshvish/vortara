package destination

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// GoogleSheetsDestination appends rows to a Google Sheet using the Sheets API v4.
// Strategy is always append; each Load call issues one values.append request.
type GoogleSheetsDestination struct {
	cfg           config.DestinationConfig
	svc           sheetsService
	spreadsheetID string
	sheetName     string
	columns       []string // explicit column order; empty = sorted row columns
	headerWritten bool
}

// sheetsService is the slice of the Sheets API the destination uses.
type sheetsService interface {
	Append(ctx context.Context, spreadsheetID, rangeA1 string, values [][]interface{}) error
}

var _ Destination = (*GoogleSheetsDestination)(nil)

func init() {
	registry.RegisterDestination("googlesheets", func() any {
		return NewGoogleSheetsDestination()
	})
}

// NewGoogleSheetsDestination returns a new GoogleSheetsDestination.
func NewGoogleSheetsDestination() *GoogleSheetsDestination {
	return &GoogleSheetsDestination{}
}

// newSheetsService builds the real Sheets API client. Overridable in tests.
var newSheetsService = func(ctx context.Context, cfg config.DestinationConfig) (sheetsService, error) {
	var opts []option.ClientOption
	if f := strings.TrimSpace(cfg.Options["credentials_file"]); f != "" {
		opts = append(opts, option.WithCredentialsFile(f))
	} else if j := strings.TrimSpace(cfg.Options["credentials_json"]); j != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(j)))
	}
	svc, err := sheets.NewService(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &googleSheetsAPI{svc: svc}, nil
}

type googleSheetsAPI struct {
	svc *sheets.Service
}

func (g *googleSheetsAPI) Append(ctx context.Context, spreadsheetID, rangeA1 string, values [][]interface{}) error {
	_, err := g.svc.Spreadsheets.Values.
		Append(spreadsheetID, rangeA1, &sheets.ValueRange{Values: values}).
		ValueInputOption("USER_ENTERED").
		InsertDataOption("INSERT_ROWS").
		Context(ctx).
		Do()
	return err
}

// Connect validates settings and builds the Sheets API client.
func (g *GoogleSheetsDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	spreadsheetID := strings.TrimSpace(cfg.Options["spreadsheet_id"])
	if spreadsheetID == "" {
		return errors.New("googlesheets destination: spreadsheet_id is required")
	}
	sheetName := strings.TrimSpace(cfg.Options["sheet"])
	if sheetName == "" {
		sheetName = "Sheet1"
	}
	var columns []string
	for _, c := range strings.Split(cfg.Options["columns"], ",") {
		if c = strings.TrimSpace(c); c != "" {
			columns = append(columns, c)
		}
	}

	svc, err := newSheetsService(ctx, cfg)
	if err != nil {
		return fmt.Errorf("googlesheets destination: %w", err)
	}
	g.cfg = cfg
	g.svc = svc
	g.spreadsheetID = spreadsheetID
	g.sheetName = sheetName
	g.columns = columns
	return nil
}

// Load appends rows to the sheet, skipping rows already delivered.
func (g *GoogleSheetsDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if g.svc == nil {
		return result, errors.New("googlesheets destination: not connected")
	}
	if len(rows) == 0 {
		return result, nil
	}

	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		delivered, err := store.IsDelivered(rw.ID, pipeline, destination)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	cols := g.columns
	if len(cols) == 0 {
		cols = unionColumns(pending)
	}
	values := make([][]interface{}, 0, len(pending))
	for _, rw := range pending {
		cells := make([]interface{}, len(cols))
		for i, c := range cols {
			cells[i] = sheetCell(rw.Data[c])
		}
		values = append(values, cells)
	}

	if err := g.svc.Append(ctx, g.spreadsheetID, g.sheetName, values); err != nil {
		for _, rw := range pending {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
		}
		return result, nil
	}

	for _, rw := range pending {
		if err := store.MarkDelivered(rw.ID, pipeline, destination); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		result.Loaded++
	}
	return result, nil
}

// Close is a no-op for the Sheets destination.
func (g *GoogleSheetsDestination) Close() error { return nil }

// unionColumns returns the sorted union of column names across rows.
func unionColumns(rows []row.Row) []string {
	seen := make(map[string]bool)
	for _, rw := range rows {
		for c := range rw.Data {
			seen[c] = true
		}
	}
	cols := make([]string, 0, len(seen))
	for c := range seen {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

// sheetCell converts a row value into a cell value the Sheets API accepts.
func sheetCell(v any) interface{} {
	switch val := v.(type) {
	case nil:
		return ""
	case time.Time:
		return val.UTC().Format(time.RFC3339)
	case string, bool, int, int32, int64, float32, float64:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
