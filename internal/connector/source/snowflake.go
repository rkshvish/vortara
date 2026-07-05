package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	gosnowflake "github.com/snowflakedb/gosnowflake"

	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

const snowflakeQueryTagSQL = "ALTER SESSION SET QUERY_TAG = 'vortara'"

// SnowflakeSource implements BatchSource for Snowflake tables and custom SQL queries.
type SnowflakeSource struct {
	cfg      config.SourceConfig
	db       *sql.DB
	dbSchema string
	dbTable  string
	columns  []columnInfo
}

var _ BatchSource = (*SnowflakeSource)(nil)

func init() {
	registry.RegisterBatchSource("snowflake", func() any {
		return NewSnowflakeSource()
	})
}

var openSnowflakeDB = func(dsn string) (*sql.DB, error) {
	return sql.Open("snowflake", dsn)
}

// NewSnowflakeSource returns a new SnowflakeSource.
func NewSnowflakeSource() *SnowflakeSource {
	return &SnowflakeSource{}
}

// Connect opens a Snowflake connection and validates the configured DSN.
func (s *SnowflakeSource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	if strings.EqualFold(strings.TrimSpace(cfg.WatermarkColumn), "none") {
		return errors.New("snowflake source: watermark: none (full-snapshot mode) is not yet supported for snowflake — use a timestamp watermark column or a custom query")
	}
	rawURL := strings.TrimSpace(cfg.URL)
	if rawURL == "" {
		rawURL = strings.TrimSpace(cfg.Connection)
	}
	if rawURL == "" {
		return errors.New("snowflake source: url is required")
	}

	conn, err := parseSnowflakeURL(rawURL)
	if err != nil {
		return err
	}

	dsn, err := gosnowflake.DSN(&gosnowflake.Config{
		Account:   conn.Account,
		User:      conn.User,
		Password:  conn.Password,
		Database:  conn.Database,
		Schema:    conn.Schema,
		Warehouse: conn.Warehouse,
		Role:      conn.Role,
	})
	if err != nil {
		return fmt.Errorf("snowflake source: build dsn: %w", err)
	}

	db, err := openSnowflakeDB(dsn)
	if err != nil {
		return fmt.Errorf("snowflake source: open db: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("snowflake source: ping: %w", err)
	}

	s.cfg = cfg
	s.db = db
	s.dbSchema, s.dbTable = parseSnowflakeTable(cfg.Table)
	s.columns = nil
	return nil
}

// Extract fetches rows incrementally using a watermark window.
func (s *SnowflakeSource) Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	if s.db == nil {
		return errors.New("snowflake source: not connected")
	}
	defer close(out)

	if err := s.setQueryTag(ctx); err != nil {
		return err
	}

	if s.isCustomQuery() {
		return s.runCustomQuery(ctx, watermark, intervalEnd, out)
	}

	schema, err := s.introspectSchema(ctx, s.cfg.Table)
	if err != nil {
		return err
	}
	s.columns = schema.Columns

	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		pageCount, err := s.extractPage(ctx, schema, watermark, intervalEnd, batchSize, offset, out)
		if err != nil {
			return err
		}
		if pageCount < batchSize {
			return nil
		}
		offset += pageCount
	}
}

// GetWatermarkColumn returns the configured watermark column or the Snowflake default.
func (s *SnowflakeSource) GetWatermarkColumn() string {
	if v := strings.TrimSpace(s.cfg.WatermarkColumn); v != "" {
		return v
	}
	return "updated_at"
}

// Close releases the Snowflake connection.
func (s *SnowflakeSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SnowflakeSource) setQueryTag(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, snowflakeQueryTagSQL)
	return err
}

func (s *SnowflakeSource) introspectSchema(ctx context.Context, table string) (*tableSchema, error) {
	schemaName, tableName := parseSnowflakeTable(table)
	query := fmt.Sprintf("DESCRIBE TABLE %s", quoteTableNameWithSchema(schemaName, tableName))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	nameIdx := findColumnIndex(cols, "name")
	typeIdx := findColumnIndex(cols, "type")
	nullIdx := findColumnIndex(cols, "null?")

	var columns []columnInfo
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		if nameIdx < 0 || typeIdx < 0 {
			continue
		}

		name := strings.TrimSpace(fmt.Sprint(values[nameIdx]))
		dataType := normalizeSnowflakeType(fmt.Sprint(values[typeIdx]))
		nullable := true
		if nullIdx >= 0 {
			nullable = strings.EqualFold(strings.TrimSpace(fmt.Sprint(values[nullIdx])), "y")
		}
		if name == "" {
			continue
		}
		columns = append(columns, columnInfo{
			Name:       name,
			DataType:   dataType,
			IsNullable: nullable,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("snowflake source: table %s not found or has no columns", table)
	}

	columns = s.filterExcludedColumns(columns)
	return &tableSchema{Columns: columns}, nil
}

func (s *SnowflakeSource) extractPage(ctx context.Context, schema *tableSchema, watermark, intervalEnd time.Time, batchSize, offset int, out chan<- row.Row) (int, error) {
	colNames := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		colNames = append(colNames, col.Name)
	}

	query, args, err := buildSnowflakeSelectQuery(s.cfg.Table, s.GetWatermarkColumn(), colNames, watermark, intervalEnd, batchSize, offset)
	if err != nil {
		return 0, err
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, nil
	}

	pageCount := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return pageCount, err
		}

		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return pageCount, err
		}

		data := make(map[string]interface{}, len(cols))
		for i, colName := range cols {
			data[colName] = convertSnowflakeValue(values[i])
		}

		wm := extractSnowflakeWatermark(data, s.GetWatermarkColumn())
		pk := extractSnowflakePrimaryKey(data, offset+pageCount+1)
		r := row.NewRow(s.sourceName(), s.pipelineName(), pk, data, wm)
		if r.Watermark.IsZero() {
			r.Watermark = r.ExtractedAt
		}

		select {
		case out <- r:
		case <-ctx.Done():
			return pageCount, ctx.Err()
		}

		pageCount++
	}

	if err := rows.Err(); err != nil {
		return pageCount, err
	}
	return pageCount, nil
}

func (s *SnowflakeSource) runCustomQuery(ctx context.Context, watermark, intervalEnd time.Time, out chan<- row.Row) error {
	query := s.resolveQuery(watermark, intervalEnd)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	rowNum := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		data := make(map[string]interface{}, len(cols))
		for i, colName := range cols {
			data[colName] = convertSnowflakeValue(values[i])
		}

		rowNum++
		wm := extractSnowflakeWatermark(data, s.GetWatermarkColumn())
		pk := extractSnowflakePrimaryKey(data, rowNum)
		r := row.NewRow(s.sourceName(), s.pipelineName(), pk, data, wm)
		if r.Watermark.IsZero() {
			r.Watermark = r.ExtractedAt
		}

		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return rows.Err()
}

func (s *SnowflakeSource) resolveQuery(watermark time.Time, intervalEnd time.Time) string {
	query := s.cfg.Query
	query = strings.ReplaceAll(query, "{{watermark}}", "'"+watermark.UTC().Format("2006-01-02T15:04:05Z07:00")+"'")
	query = strings.ReplaceAll(query, "{{interval_end}}", "'"+intervalEnd.UTC().Format("2006-01-02T15:04:05Z07:00")+"'")
	if s.cfg.Options != nil {
		query = strings.ReplaceAll(query, "{{pipeline}}", s.cfg.Options["pipeline"])
	}
	return query
}

func buildSnowflakeSelectQuery(table, watermarkColumn string, cols []string, watermark, intervalEnd time.Time, batchSize, offset int) (string, []any, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Question)
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}
	if len(quotedCols) == 0 {
		quotedCols = []string{"*"}
	}

	schemaName, tableName := parseSnowflakeTable(table)
	q := psql.Select(quotedCols...).From(quoteTableNameWithSchema(schemaName, tableName))
	if !watermark.IsZero() {
		q = q.Where(sq.Gt{quoteIdentifier(watermarkColumn): watermark})
	}
	if !intervalEnd.IsZero() {
		q = q.Where(sq.LtOrEq{quoteIdentifier(watermarkColumn): intervalEnd})
	}
	q = q.OrderBy(quoteIdentifier(watermarkColumn) + " ASC")
	if batchSize > 0 {
		q = q.Limit(uint64(batchSize))
	}
	if offset > 0 {
		q = q.Offset(uint64(offset))
	}
	return q.ToSql()
}

func parseSnowflakeURL(raw string) (snowflakeConnInfo, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return snowflakeConnInfo{}, fmt.Errorf("snowflake source: parse url: %w", err)
	}

	if u.User == nil {
		return snowflakeConnInfo{}, errors.New("snowflake source: url must include user and password")
	}
	user := u.User.Username()
	password, _ := u.User.Password()
	account := strings.TrimSpace(u.Hostname())
	if user == "" || password == "" || account == "" {
		return snowflakeConnInfo{}, errors.New("snowflake source: url must include user, password, and account")
	}

	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return snowflakeConnInfo{}, errors.New("snowflake source: url must include database in path")
	}

	info := snowflakeConnInfo{
		User:      user,
		Password:  password,
		Account:   account,
		Database:  parts[0],
		Schema:    "PUBLIC",
		Warehouse: u.Query().Get("warehouse"),
		Role:      u.Query().Get("role"),
	}
	if len(parts) >= 2 && strings.TrimSpace(parts[1]) != "" {
		info.Schema = strings.ToUpper(strings.TrimSpace(parts[1]))
	}
	info.Database = strings.TrimSpace(info.Database)
	return info, nil
}

type snowflakeConnInfo struct {
	User      string
	Password  string
	Account   string
	Database  string
	Schema    string
	Warehouse string
	Role      string
}

func parseSnowflakeTable(table string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(table), ".", 2)
	if len(parts) == 2 {
		return strings.ToUpper(strings.TrimSpace(parts[0])), strings.ToUpper(strings.TrimSpace(parts[1]))
	}
	return "PUBLIC", strings.ToUpper(strings.TrimSpace(table))
}

func quoteTableNameWithSchema(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func normalizeSnowflakeType(raw string) string {
	upper := strings.ToUpper(strings.TrimSpace(raw))
	if idx := strings.Index(upper, "("); idx >= 0 {
		upper = upper[:idx]
	}
	switch upper {
	case "VARCHAR", "TEXT", "STRING", "CHAR", "CHARACTER":
		return "string"
	case "NUMBER", "DECIMAL", "NUMERIC":
		return "float64"
	case "INT", "INTEGER", "BIGINT":
		return "int64"
	case "FLOAT", "DOUBLE", "REAL":
		return "float64"
	case "BOOLEAN", "BOOL":
		return "bool"
	case "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME", "TIMESTAMP":
		return "time.Time"
	case "DATE":
		return "time.Time"
	case "VARIANT", "OBJECT", "ARRAY":
		return "string"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func convertSnowflakeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case string:
		return val
	case int64:
		return val
	case float64:
		return val
	case bool:
		return val
	case time.Time:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func extractSnowflakeWatermark(data map[string]interface{}, wmCol string) time.Time {
	if wmCol == "" {
		wmCol = "updated_at"
	}
	if val, ok := lookupCaseInsensitive(data, wmCol); ok {
		if ts, err := asTime(val); err == nil {
			return ts
		}
	}
	if val, ok := lookupCaseInsensitive(data, "watermark"); ok {
		if ts, err := asTime(val); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func extractSnowflakePrimaryKey(data map[string]interface{}, rowNum int) string {
	for _, key := range []string{"id", "row_id"} {
		if val, ok := lookupCaseInsensitive(data, key); ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return fmt.Sprintf("row-%d", rowNum)
}

func lookupCaseInsensitive(data map[string]interface{}, key string) (interface{}, bool) {
	for k, v := range data {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}

func findColumnIndex(cols []string, want string) int {
	for i, col := range cols {
		if strings.EqualFold(strings.TrimSpace(col), want) {
			return i
		}
	}
	return -1
}

func (s *SnowflakeSource) sourceName() string {
	if strings.TrimSpace(s.cfg.Table) != "" {
		return s.cfg.Type + "." + strings.TrimSpace(s.cfg.Table)
	}
	return s.cfg.Type + ".custom_query"
}

func (s *SnowflakeSource) pipelineName() string {
	if s.cfg.Options != nil {
		return strings.TrimSpace(s.cfg.Options["pipeline"])
	}
	return ""
}

func (s *SnowflakeSource) isCustomQuery() bool {
	return strings.TrimSpace(s.cfg.Query) != ""
}

func (s *SnowflakeSource) filterExcludedColumns(cols []columnInfo) []columnInfo {
	if len(s.cfg.ExcludeColumns) == 0 {
		return cols
	}
	excluded := make(map[string]bool, len(s.cfg.ExcludeColumns))
	for _, c := range s.cfg.ExcludeColumns {
		excluded[strings.ToLower(strings.TrimSpace(c))] = true
	}
	filtered := make([]columnInfo, 0, len(cols))
	for _, col := range cols {
		if !excluded[strings.ToLower(strings.TrimSpace(col.Name))] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}
