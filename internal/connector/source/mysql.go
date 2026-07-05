package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	vlogger "github.com/rkshvish/vortaraos/internal/logger"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// MySQLSource extracts rows incrementally from a MySQL table.
type MySQLSource struct {
	cfg    config.SourceConfig
	db     *sql.DB
	dbName string
}

var _ BatchSource = (*MySQLSource)(nil)

func init() {
	registry.RegisterBatchSource("mysql", func() any {
		return NewMySQLSource()
	})
}

// NewMySQLSource returns a new MySQLSource.
func NewMySQLSource() *MySQLSource {
	return &MySQLSource{}
}

// Connect opens the MySQL database and verifies connectivity.
func (m *MySQLSource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	dsn := strings.TrimSpace(cfg.Connection)
	if dsn == "" {
		dsn = strings.TrimSpace(cfg.URL)
	}
	if dsn == "" {
		return errors.New("mysql source: connection is required")
	}
	// Accept both DSN (user:pass@tcp(host:port)/db) and URL (mysql://user:pass@host:port/db) forms.
	if strings.HasPrefix(dsn, "mysql://") {
		converted, err := mysqlURLToDSN(dsn)
		if err != nil {
			return err
		}
		dsn = converted
	}
	mycfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("mysql source: invalid dsn: %w", err)
	}
	mycfg.ParseTime = true
	if cfg.Table == "" && cfg.Query == "" {
		return errors.New("mysql source: table or query is required")
	}

	db, err := sql.Open("mysql", mycfg.FormatDSN())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(90 * time.Second)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return fmt.Errorf("mysql source: ping: %w", err)
	}
	m.cfg = cfg
	m.db = db
	m.dbName = mycfg.DBName
	return nil
}

// mysqlURLToDSN converts mysql://user:pass@host:port/db?params to go-sql-driver DSN form.
func mysqlURLToDSN(rawURL string) (string, error) {
	trimmed := strings.TrimPrefix(rawURL, "mysql://")
	slash := strings.Index(trimmed, "/")
	if slash < 0 {
		return "", fmt.Errorf("mysql source: url %q missing database name", rawURL)
	}
	hostPart, dbPart := trimmed[:slash], trimmed[slash+1:]
	creds, addr := "", hostPart
	if at := strings.LastIndex(hostPart, "@"); at >= 0 {
		creds, addr = hostPart[:at], hostPart[at+1:]
	}
	dsn := ""
	if creds != "" {
		dsn = creds + "@"
	}
	dsn += "tcp(" + addr + ")/" + dbPart
	return dsn, nil
}

// GetWatermarkColumn returns the configured watermark column or the default.
func (m *MySQLSource) GetWatermarkColumn() string {
	if v := strings.TrimSpace(m.cfg.WatermarkColumn); v != "" {
		return v
	}
	return "updated_at"
}

// Close closes the database handle.
func (m *MySQLSource) Close() error {
	if m.db != nil {
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

type mysqlColumn struct {
	Name     string
	DataType string
	IsPK     bool
}

// introspect returns column and primary-key metadata from information_schema.
func (m *MySQLSource) introspect(ctx context.Context, table string) ([]mysqlColumn, error) {
	const q = `
		SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_KEY = 'PRI'
		FROM information_schema.COLUMNS c
		WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION`
	rows, err := m.db.QueryContext(ctx, q, m.dbName, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []mysqlColumn
	excluded := make(map[string]bool, len(m.cfg.ExcludeColumns))
	for _, c := range m.cfg.ExcludeColumns {
		excluded[strings.ToLower(strings.TrimSpace(c))] = true
	}
	for rows.Next() {
		var col mysqlColumn
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsPK); err != nil {
			return nil, err
		}
		if excluded[strings.ToLower(col.Name)] {
			continue
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("mysql source: table %q not found or has no visible columns", table)
	}
	return cols, nil
}

// quoteMySQLIdent quotes an identifier with backticks.
func quoteMySQLIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// buildMySQLExtractQuery builds the incremental extraction query.
// A zero watermark omits the lower bound (first run extracts everything up
// to intervalEnd). An empty wmCol builds a full-snapshot query.
func buildMySQLExtractQuery(table string, cols []mysqlColumn, wmCol string, zeroWatermark bool) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteMySQLIdent(c.Name)
	}
	q := "SELECT " + strings.Join(names, ", ") + " FROM " + quoteMySQLIdent(table)
	if wmCol == "" {
		return q
	}
	wm := quoteMySQLIdent(wmCol)
	if zeroWatermark {
		q += " WHERE " + wm + " <= ?"
	} else {
		q += " WHERE " + wm + " > ? AND " + wm + " <= ?"
	}
	q += " ORDER BY " + wm
	return q
}

// snapshotMode reports whether the source runs without a watermark.
func (m *MySQLSource) snapshotMode() bool {
	return strings.EqualFold(strings.TrimSpace(m.cfg.WatermarkColumn), "none")
}

// mysqlTimeTypes are the column types usable as a time watermark cursor.
var mysqlTimeTypes = map[string]bool{
	"timestamp": true,
	"datetime":  true,
	"date":      true,
}

// mysqlIntTypes are the column types usable as a numeric cursor.
var mysqlIntTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"bigint":    true,
}

// validateWatermarkColumn fails fast when the watermark column is missing
// or not a time type.
func (m *MySQLSource) validateWatermarkColumn(cols []mysqlColumn) error {
	if m.snapshotMode() {
		return nil
	}
	wmCol := m.GetWatermarkColumn()
	for _, c := range cols {
		if c.Name != wmCol {
			continue
		}
		if mysqlIntTypes[strings.ToLower(c.DataType)] {
			return fmt.Errorf("mysql source: watermark column %q is an integer; time-window extraction does not apply — the engine routes integer cursors through ExtractNumeric automatically", wmCol)
		}
		if !mysqlTimeTypes[strings.ToLower(c.DataType)] {
			return fmt.Errorf("mysql source: watermark column %q has type %q; supported watermark columns are timestamp/datetime/date or integer types — use watermark: none for full-snapshot extraction", wmCol, c.DataType)
		}
		return nil
	}
	return fmt.Errorf("mysql source: watermark column %q not found in table %q — set watermark: to an existing time column, or watermark: none for full-snapshot extraction", wmCol, m.cfg.Table)
}

// Extract streams rows updated within (watermark, intervalEnd] to out.
func (m *MySQLSource) Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	if m.db == nil {
		return errors.New("mysql source: not connected")
	}
	defer close(out)

	if strings.TrimSpace(m.cfg.Query) != "" {
		return m.extractCustomQuery(ctx, watermark, intervalEnd, out)
	}

	cols, err := m.introspect(ctx, m.cfg.Table)
	if err != nil {
		return err
	}
	if err := m.validateWatermarkColumn(cols); err != nil {
		return err
	}
	wmCol := m.GetWatermarkColumn()

	var args []any
	if m.snapshotMode() {
		wmCol = "" // full-snapshot query: no window, no args
	} else if watermark.IsZero() {
		args = []any{intervalEnd}
	} else {
		args = []any{watermark, intervalEnd}
	}
	query := buildMySQLExtractQuery(m.cfg.Table, cols, wmCol, watermark.IsZero())

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	pkCols := make([]string, 0, 2)
	for _, c := range cols {
		if c.IsPK {
			pkCols = append(pkCols, c.Name)
		}
	}

	count, err := m.streamRows(ctx, rows, colNames(cols), pkCols, wmCol, out)
	if err != nil {
		return err
	}
	vlogger.FromContext(ctx).Debug("mysql extraction complete",
		slog.String("table", m.cfg.Table),
		slog.Int("rows", count),
	)
	return nil
}

func (m *MySQLSource) extractCustomQuery(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	query := m.cfg.Query
	var args []any
	// Custom queries may reference the bounded window with ? placeholders (wm, intervalEnd).
	switch strings.Count(query, "?") {
	case 2:
		args = []any{watermark, intervalEnd}
	case 1:
		args = []any{intervalEnd}
	}
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	colsFromQuery, err := rows.Columns()
	if err != nil {
		return err
	}
	_, err = m.streamRows(ctx, rows, colsFromQuery, nil, m.GetWatermarkColumn(), out)
	return err
}

func colNames(cols []mysqlColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// streamRows scans sql.Rows into standard Rows and sends them to out.
func (m *MySQLSource) streamRows(ctx context.Context, rows *sql.Rows, cols []string, pkCols []string, wmCol string, out chan<- row.Row) (int, error) {
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	count := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if err := rows.Scan(ptrs...); err != nil {
			return count, err
		}

		data := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			data[col] = convertMySQLValue(values[i])
		}

		r := row.Row{
			ID:          uuid.NewString(),
			Source:      m.sourceName(),
			Data:        data,
			ExtractedAt: time.Now(),
		}
		if wm, ok := data[wmCol].(time.Time); ok {
			r.Watermark = wm
		}
		if len(pkCols) > 0 {
			parts := make([]string, len(pkCols))
			for i, pk := range pkCols {
				parts[i] = fmt.Sprintf("%s=%v", pk, data[pk])
			}
			r.PrimaryKey = strings.Join(parts, ",")
		} else {
			r.PrimaryKey = r.ID
		}

		select {
		case out <- r:
		case <-ctx.Done():
			return count, ctx.Err()
		}
		count++
	}
	return count, rows.Err()
}

func (m *MySQLSource) sourceName() string {
	if strings.TrimSpace(m.cfg.Query) != "" {
		return "mysql.custom_query"
	}
	return "mysql." + m.cfg.Table
}

// CursorKind inspects the watermark column and reports the cursor type.
func (m *MySQLSource) CursorKind(ctx context.Context) (string, error) {
	if m.snapshotMode() {
		return "snapshot", nil
	}
	if strings.TrimSpace(m.cfg.Query) != "" {
		return "time", nil
	}
	cols, err := m.introspect(ctx, m.cfg.Table)
	if err != nil {
		return "", err
	}
	wmCol := m.GetWatermarkColumn()
	for _, c := range cols {
		if c.Name != wmCol {
			continue
		}
		dt := strings.ToLower(c.DataType)
		if mysqlIntTypes[dt] {
			return "int", nil
		}
		if mysqlTimeTypes[dt] {
			return "time", nil
		}
		return "", fmt.Errorf("mysql source: watermark column %q has type %q; supported watermark columns are timestamp/datetime/date or integer types — use watermark: none for full-snapshot extraction", wmCol, c.DataType)
	}
	return "", fmt.Errorf("mysql source: watermark column %q not found in table %q", wmCol, m.cfg.Table)
}

// ExtractNumeric fetches rows where cursor column > cursor using keyset
// pagination, ordered ascending. Returns the highest cursor value emitted.
func (m *MySQLSource) ExtractNumeric(ctx context.Context, cursor int64, limit int64, out chan<- row.Row) (int64, error) {
	if m.db == nil {
		return cursor, errors.New("mysql source: not connected")
	}
	defer close(out)

	cols, err := m.introspect(ctx, m.cfg.Table)
	if err != nil {
		return cursor, err
	}
	wmCol := m.GetWatermarkColumn()
	pkCols := make([]string, 0, 2)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
		if c.IsPK {
			pkCols = append(pkCols, c.Name)
		}
	}
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteMySQLIdent(n)
	}

	batchSize := m.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	emitted := int64(0)
	last := cursor
	for {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		pageLimit := int64(batchSize)
		if limit > 0 && limit-emitted < pageLimit {
			pageLimit = limit - emitted
		}
		if pageLimit <= 0 {
			return last, nil
		}
		query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + quoteMySQLIdent(m.cfg.Table) +
			" WHERE " + quoteMySQLIdent(wmCol) + " > ? ORDER BY " + quoteMySQLIdent(wmCol) + " ASC LIMIT ?"
		rows, err := m.db.QueryContext(ctx, query, last, pageLimit)
		if err != nil {
			return last, err
		}
		values := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range values {
			ptrs[i] = &values[i]
		}
		pageCount := int64(0)
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return last, err
			}
			data := make(map[string]interface{}, len(names))
			for i, col := range names {
				data[col] = convertMySQLValue(values[i])
			}
			cur, ok := toCursorInt(data[wmCol])
			if !ok {
				rows.Close()
				return last, fmt.Errorf("mysql source: cursor column %q returned %T, want integer", wmCol, data[wmCol])
			}

			r := row.Row{
				ID:          uuid.NewString(),
				Source:      m.sourceName(),
				Data:        data,
				ExtractedAt: time.Now(),
			}
			if len(pkCols) > 0 {
				parts := make([]string, len(pkCols))
				for i, pk := range pkCols {
					parts[i] = fmt.Sprintf("%s=%v", pk, data[pk])
				}
				r.PrimaryKey = strings.Join(parts, ",")
			} else {
				r.PrimaryKey = r.ID
			}
			select {
			case out <- r:
			case <-ctx.Done():
				rows.Close()
				return last, ctx.Err()
			}
			last = cur
			emitted++
			pageCount++
		}
		if err := rows.Err(); err != nil {
			return last, err
		}
		rows.Close()
		if pageCount < int64(batchSize) || (limit > 0 && emitted >= limit) {
			return last, nil
		}
	}
}

// toCursorInt coerces driver integer representations to int64.
func toCursorInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// convertMySQLValue normalizes driver values into standard Row types.
// go-sql-driver returns []byte for most text/decimal types and time.Time when parseTime=true.
func convertMySQLValue(val any) any {
	switch v := val.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC()
	default:
		return v
	}
}
