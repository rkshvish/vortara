package source

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

type columnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	IsPrimaryKey bool
}

type tableSchema struct {
	Columns     []columnInfo
	PrimaryKeys []string
}

// pgxConn is the subset of pgxpool.Conn used by PostgresSource (Prewarm).
type pgxConn interface {
	Release()
}

type pgxPool interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Acquire(context.Context) (pgxConn, error)
	Ping(context.Context) error
	Close()
}

// pgxPoolWrapper adapts *pgxpool.Pool to the pgxPool interface, providing a
// covariant Acquire that returns pgxConn instead of *pgxpool.Conn.
type pgxPoolWrapper struct {
	*pgxpool.Pool
}

func (w *pgxPoolWrapper) Acquire(ctx context.Context) (pgxConn, error) {
	return w.Pool.Acquire(ctx)
}

type pgxRows = pgx.Rows

// PostgresSource implements BatchSource for PostgreSQL tables.
type PostgresSource struct {
	cfg    config.SourceConfig
	pool   pgxPool
	schema *tableSchema // cached after first introspection in parallel path
}

var _ BatchSource = (*PostgresSource)(nil)

func init() {
	registry.RegisterBatchSource("postgres", func() any {
		return NewPostgresSource()
	})
}

var newPgxPool = func(ctx context.Context, cfg *pgxpool.Config) (pgxPool, error) {
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &pgxPoolWrapper{p}, nil
}

// NewPostgresSource returns a new PostgresSource.
// Call Connect() before Extract().
func NewPostgresSource() *PostgresSource {
	return &PostgresSource{}
}

// Connect opens a PostgreSQL connection pool and validates it.
func (p *PostgresSource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	normalized := normalizeConnectionString(cfg.Connection)
	pgxCfg, err := pgxpool.ParseConfig(normalized)
	if err != nil {
		return err
	}
	pgxCfg.MaxConns = 10
	pgxCfg.MinConns = 1
	pgxCfg.MaxConnLifetime = 5 * time.Minute
	pgxCfg.MaxConnIdleTime = 1 * time.Minute
	pgxCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := newPgxPool(ctx, pgxCfg)
	if err != nil {
		return err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return err
	}

	p.cfg = cfg
	p.pool = pool
	return nil
}

// snapshotMode reports whether the source runs without a watermark
// (watermark: none) — a full extract on every run.
func (p *PostgresSource) snapshotMode() bool {
	return strings.EqualFold(strings.TrimSpace(p.cfg.WatermarkColumn), "none")
}

// pgTimeTypes are the column types usable as a time watermark cursor.
var pgTimeTypes = map[string]bool{
	"timestamptz": true,
	"timestamp":   true,
	"date":        true,
}

// pgIntTypes are the column types usable as a numeric cursor.
var pgIntTypes = map[string]bool{
	"int2": true,
	"int4": true,
	"int8": true,
}

// validateWatermarkColumn fails fast with a clear message when the
// configured watermark column is missing or not a timestamp type,
// instead of surfacing a cryptic SQL error mid-extraction.
func (p *PostgresSource) validateWatermarkColumn(schema *tableSchema) error {
	if p.snapshotMode() {
		return nil
	}
	wmCol := p.GetWatermarkColumn()
	for _, col := range schema.Columns {
		if col.Name != wmCol {
			continue
		}
		if pgIntTypes[col.DataType] {
			return fmt.Errorf("postgres source: watermark column %q is an integer; time-window extraction does not apply — the engine routes integer cursors through ExtractNumeric automatically", wmCol)
		}
		if !pgTimeTypes[col.DataType] {
			return fmt.Errorf("postgres source: watermark column %q has type %q; supported watermark columns are timestamp/timestamptz/date or integer types — use watermark: none for full-snapshot extraction", wmCol, col.DataType)
		}
		return nil
	}
	return fmt.Errorf("postgres source: watermark column %q not found in table %q — set watermark: to an existing timestamp column, or watermark: none for full-snapshot extraction", wmCol, p.cfg.Table)
}

// Extract fetches rows incrementally. Uses parallel range scan when ExtractParallelism > 1
// and the table has a numeric primary key; otherwise falls back to sequential offset pagination.
// With watermark: none, every run extracts the full table in one streaming pass.
func (p *PostgresSource) Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	if p.pool == nil {
		return errors.New("postgres source: not connected")
	}
	defer close(out)
	if p.isCustomQuery() {
		return p.runCustomQuery(ctx, watermark, intervalEnd, out)
	}
	if p.snapshotMode() {
		return p.extractSnapshot(ctx, out)
	}
	parallelism := p.cfg.ExtractParallelism
	if parallelism > 1 {
		return p.parallelRangeExtract(ctx, watermark, intervalEnd, out, parallelism)
	}
	return p.extractSequential(ctx, watermark, intervalEnd, out)
}

// CursorKind inspects the watermark column and reports the cursor type.
func (p *PostgresSource) CursorKind(ctx context.Context) (string, error) {
	if p.snapshotMode() {
		return "snapshot", nil
	}
	if p.isCustomQuery() {
		return "time", nil
	}
	schema, err := p.introspectSchema(ctx, p.cfg.Table)
	if err != nil {
		return "", err
	}
	wmCol := p.GetWatermarkColumn()
	for _, col := range schema.Columns {
		if col.Name != wmCol {
			continue
		}
		if pgIntTypes[col.DataType] {
			return "int", nil
		}
		if pgTimeTypes[col.DataType] {
			return "time", nil
		}
		return "", fmt.Errorf("postgres source: watermark column %q has type %q; supported watermark columns are timestamp/timestamptz/date or integer types — use watermark: none for full-snapshot extraction", wmCol, col.DataType)
	}
	return "", fmt.Errorf("postgres source: watermark column %q not found in table %q — set watermark: to an existing column, or watermark: none for full-snapshot extraction", wmCol, p.cfg.Table)
}

// ExtractNumeric fetches rows where cursor column > cursor using keyset
// pagination, ordered ascending. Returns the highest cursor value emitted.
func (p *PostgresSource) ExtractNumeric(ctx context.Context, cursor int64, limit int64, out chan<- row.Row) (int64, error) {
	if p.pool == nil {
		return cursor, errors.New("postgres source: not connected")
	}
	defer close(out)

	schema, err := p.introspectSchema(ctx, p.cfg.Table)
	if err != nil {
		return cursor, err
	}
	wmCol := p.GetWatermarkColumn()
	colTypeMap := make(map[string]string, len(schema.Columns))
	cols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		colTypeMap[col.Name] = col.DataType
		cols = append(cols, col.Name)
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}

	batchSize := p.cfg.BatchSize
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
		query := "SELECT " + strings.Join(quotedCols, ", ") + " FROM " + quoteTableName(p.cfg.Table) +
			" WHERE " + quoteIdentifier(wmCol) + " > $1 ORDER BY " + quoteIdentifier(wmCol) + " ASC LIMIT $2"
		rows, err := p.pool.Query(ctx, query, last, pageLimit)
		if err != nil {
			return last, err
		}
		fields := rows.FieldDescriptions()
		pageCount := int64(0)
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return last, err
			}
			data := make(map[string]interface{}, len(fields))
			for i, field := range fields {
				data[field.Name] = convertPgValue(values[i], colTypeMap[field.Name])
			}
			cur, ok := data[wmCol].(int64)
			if !ok {
				rows.Close()
				return last, fmt.Errorf("postgres source: cursor column %q returned %T, want integer", wmCol, data[wmCol])
			}

			r := row.Get()
			r.ID = uuid.NewString()
			r.Source = p.sourceName()
			r.Pipeline = p.pipelineName()
			r.Data = data
			r.ExtractedAt = time.Now()
			if len(schema.PrimaryKeys) == 0 {
				r.PrimaryKey = r.ID
			} else {
				r.PrimaryKey = buildPrimaryKey(schema, data)
			}
			result := *r
			r.Data = make(map[string]interface{}, 16)
			row.Put(r)
			select {
			case out <- result:
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
		if pageCount < int64(batchSize) || (limit > 0 && emitted >= limit) {
			return last, nil
		}
	}
}

// extractSnapshot streams the whole table without a watermark window or
// pagination; downstream idempotency comes from the delivery log.
func (p *PostgresSource) extractSnapshot(ctx context.Context, out chan<- row.Row) error {
	schema, err := p.introspectSchema(ctx, p.cfg.Table)
	if err != nil {
		return err
	}
	colTypeMap := make(map[string]string, len(schema.Columns))
	cols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		colTypeMap[col.Name] = col.DataType
		cols = append(cols, col.Name)
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}
	query := "SELECT " + strings.Join(quotedCols, ", ") + " FROM " + quoteTableName(p.cfg.Table)

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		values, err := rows.Values()
		if err != nil {
			return err
		}
		data := make(map[string]interface{}, len(fields))
		for i, field := range fields {
			data[field.Name] = convertPgValue(values[i], colTypeMap[field.Name])
		}
		r := row.Get()
		r.ID = uuid.NewString()
		r.Source = p.sourceName()
		r.Pipeline = p.pipelineName()
		r.Data = data
		r.ExtractedAt = time.Now()
		if len(schema.PrimaryKeys) == 0 {
			r.PrimaryKey = r.ID
		} else {
			r.PrimaryKey = buildPrimaryKey(schema, data)
		}
		result := *r
		r.Data = make(map[string]interface{}, 16)
		row.Put(r)
		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

func (p *PostgresSource) extractSequential(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	schema, err := p.introspectSchema(ctx, p.cfg.Table)
	if err != nil {
		return err
	}
	if err := p.validateWatermarkColumn(schema); err != nil {
		return err
	}
	return p.extractWithSchema(ctx, watermark, intervalEnd, out, schema)
}

func (p *PostgresSource) extractWithSchema(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row, schema *tableSchema) error {
	colTypeMap := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		colTypeMap[col.Name] = col.DataType
	}

	batchSize := p.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	watermarkColumn := p.GetWatermarkColumn()

	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		pageCount, err := p.extractPage(ctx, schema, colTypeMap, watermarkColumn, watermark, intervalEnd, batchSize, offset, out)
		if err != nil {
			return err
		}
		if pageCount < batchSize {
			return nil
		}
		offset += pageCount
	}
}

// getPKColumn returns the first primary-key column name if it is a numeric integer
// type suitable for range-based parallel extraction. Returns "" otherwise.
func (p *PostgresSource) getPKColumn() string {
	if p.schema == nil || len(p.schema.PrimaryKeys) == 0 {
		return ""
	}
	pkName := p.schema.PrimaryKeys[0]
	for _, col := range p.schema.Columns {
		if col.Name == pkName {
			switch col.DataType {
			case "int2", "int4", "int8":
				return pkName
			}
			return "" // non-numeric PK — fall back to sequential
		}
	}
	return ""
}

// parallelRangeExtract splits the table's PK range into N chunks and scans them concurrently.
func (p *PostgresSource) parallelRangeExtract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row, parallelism int) error {
	schema, err := p.introspectSchema(ctx, p.cfg.Table)
	if err != nil {
		return err
	}
	if err := p.validateWatermarkColumn(schema); err != nil {
		return err
	}
	p.schema = schema

	pkCol := p.getPKColumn()
	if pkCol == "" {
		return p.extractWithSchema(ctx, watermark, intervalEnd, out, schema)
	}

	// Determine PK range.
	rows, err := p.pool.Query(ctx, fmt.Sprintf(
		"SELECT MIN(%s), MAX(%s) FROM %s",
		quoteIdentifier(pkCol),
		quoteIdentifier(pkCol),
		quoteTableName(p.cfg.Table),
	))
	if err != nil {
		return p.extractWithSchema(ctx, watermark, intervalEnd, out, schema)
	}

	var minPK, maxPK int64
	var hasRange bool
	if rows.Next() {
		vals, vErr := rows.Values()
		rows.Close()
		if vErr == nil && len(vals) == 2 && vals[0] != nil && vals[1] != nil {
			var okMin, okMax bool
			minPK, okMin = pgIntToInt64(vals[0])
			maxPK, okMax = pgIntToInt64(vals[1])
			hasRange = okMin && okMax
		}
	} else {
		rows.Close()
	}

	if !hasRange || minPK >= maxPK {
		return p.extractWithSchema(ctx, watermark, intervalEnd, out, schema)
	}

	// Divide PK space into N ranges.
	type pkRange struct{ min, max int64 }
	rangeSize := (maxPK - minPK + 1) / int64(parallelism)
	if rangeSize == 0 {
		rangeSize = 1
	}
	ranges := make([]pkRange, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		start := minPK + int64(i)*rangeSize
		end := start + rangeSize - 1
		if i == parallelism-1 {
			end = maxPK
		}
		ranges = append(ranges, pkRange{start, end})
	}

	// Scan each range concurrently.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var rangeErr error

	for _, r := range ranges {
		wg.Add(1)
		r := r
		go func() {
			defer wg.Done()
			if err := p.extractRange(ctx, watermark, intervalEnd, out, pkCol, r.min, r.max); err != nil {
				mu.Lock()
				if rangeErr == nil {
					rangeErr = err
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return rangeErr
}

// extractRange fetches all rows in [minPK, maxPK] from the table.
func (p *PostgresSource) extractRange(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row, pkCol string, minPK, maxPK int64) error {
	schema := p.schema
	colTypeMap := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		colTypeMap[col.Name] = col.DataType
	}
	watermarkColumn := p.GetWatermarkColumn()

	cols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		cols = append(cols, col.Name)
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	q := psql.Select(quotedCols...).From(quoteTableName(p.cfg.Table))
	if !watermark.IsZero() {
		q = q.Where(sq.Gt{quoteIdentifier(watermarkColumn): watermark})
	}
	if !intervalEnd.IsZero() {
		q = q.Where(sq.LtOrEq{quoteIdentifier(watermarkColumn): intervalEnd})
	}
	q = q.Where(sq.GtOrEq{quoteIdentifier(pkCol): minPK})
	q = q.Where(sq.LtOrEq{quoteIdentifier(pkCol): maxPK})

	query, args, err := q.ToSql()
	if err != nil {
		return err
	}

	pgRows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer pgRows.Close()

	fields := pgRows.FieldDescriptions()
	for pgRows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		values, err := pgRows.Values()
		if err != nil {
			return err
		}
		data := make(map[string]interface{}, len(fields))
		for i, field := range fields {
			data[field.Name] = convertPgValue(values[i], colTypeMap[field.Name])
		}
		wm, err := watermarkFromValues(values, fields, watermarkColumn)
		if err != nil {
			return err
		}

		r := row.Get()
		r.ID = uuid.NewString()
		r.Source = p.sourceName()
		r.Pipeline = p.pipelineName()
		r.Data = data
		r.ExtractedAt = time.Now()
		r.Watermark = wm
		if len(schema.PrimaryKeys) == 0 {
			r.PrimaryKey = r.ID
		} else {
			r.PrimaryKey = buildPrimaryKey(schema, data)
		}
		result := *r
		r.Data = make(map[string]interface{}, 16)
		row.Put(r)
		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return pgRows.Err()
}

// Prewarm acquires and immediately releases a connection to keep the pool warm between runs.
func (p *PostgresSource) Prewarm(ctx context.Context) error {
	if p.pool == nil {
		return nil
	}
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	conn.Release()
	return nil
}

// GetWatermarkColumn returns the configured watermark column.
func (p *PostgresSource) GetWatermarkColumn() string {
	if strings.TrimSpace(p.cfg.WatermarkColumn) != "" {
		return p.cfg.WatermarkColumn
	}
	return "updated_at"
}

// Close releases the PostgreSQL connection pool.
func (p *PostgresSource) Close() error {
	if p.pool == nil {
		return nil
	}
	p.pool.Close()
	return nil
}

func (p *PostgresSource) introspectSchema(ctx context.Context, table string) (*tableSchema, error) {
	schemaName, tableName := parseTableName(table)

	rows, err := p.pool.Query(ctx, `
SELECT column_name, data_type, udt_name, is_nullable
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &tableSchema{}
	for rows.Next() {
		var name, dataType, udtName, isNullable string
		if err := rows.Scan(&name, &dataType, &udtName, &isNullable); err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, columnInfo{
			Name:       name,
			DataType:   normalizePgTypeName(dataType, udtName),
			IsNullable: strings.EqualFold(isNullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("postgres source: no columns found for %s.%s", schemaName, tableName)
	}
	schema.Columns = p.filterExcludedColumns(schema.Columns)
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("postgres source: no columns remaining for %s.%s after exclude_columns", schemaName, tableName)
	}

	pkRows, err := p.pool.Query(ctx, `
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid
  AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = format('%I.%I', $1::text, $2::text)::regclass
  AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			return nil, err
		}
		schema.PrimaryKeys = append(schema.PrimaryKeys, name)
	}
	if err := pkRows.Err(); err != nil {
		return nil, err
	}
	for i := range schema.Columns {
		for _, pk := range schema.PrimaryKeys {
			if schema.Columns[i].Name == pk {
				schema.Columns[i].IsPrimaryKey = true
				break
			}
		}
	}

	return schema, nil
}

func (p *PostgresSource) filterExcludedColumns(cols []columnInfo) []columnInfo {
	if len(p.cfg.ExcludeColumns) == 0 {
		return cols
	}
	excluded := make(map[string]bool, len(p.cfg.ExcludeColumns))
	for _, col := range p.cfg.ExcludeColumns {
		excluded[strings.ToLower(strings.TrimSpace(col))] = true
	}
	filtered := make([]columnInfo, 0, len(cols))
	for _, col := range cols {
		if excluded[strings.ToLower(col.Name)] {
			continue
		}
		filtered = append(filtered, col)
	}
	return filtered
}

func (p *PostgresSource) extractPage(ctx context.Context, schema *tableSchema, colTypeMap map[string]string, watermarkColumn string, watermark, intervalEnd time.Time, batchSize, offset int, out chan<- row.Row) (int, error) {
	cols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		cols = append(cols, col.Name)
	}
	query, args, err := buildSelectQuery(p.cfg.Table, watermarkColumn, cols, watermark, intervalEnd, batchSize, offset)
	if err != nil {
		return 0, err
	}

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	pageCount := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return pageCount, err
		}

		values, err := rows.Values()
		if err != nil {
			return pageCount, err
		}

		data := make(map[string]interface{}, len(fields))
		for i, field := range fields {
			data[field.Name] = convertPgValue(values[i], colTypeMap[field.Name])
		}

		wm, err := watermarkFromValues(values, fields, watermarkColumn)
		if err != nil {
			return pageCount, err
		}

		r := row.Get()
		r.ID = uuid.NewString()
		r.Source = p.sourceName()
		r.Pipeline = p.pipelineName()
		r.Data = data
		r.ExtractedAt = time.Now()
		r.Watermark = wm
		if len(schema.PrimaryKeys) == 0 {
			r.PrimaryKey = r.ID
		} else {
			r.PrimaryKey = buildPrimaryKey(schema, data)
		}
		result := *r
		// Detach the scanned data map from the pool so reset() on the next
		// Get() does not corrupt result.Data, which the caller still holds.
		r.Data = make(map[string]interface{}, 16)
		row.Put(r)
		select {
		case out <- result:
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

func (p *PostgresSource) runCustomQuery(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	query := p.resolveQuery(watermark, intervalEnd)
	conn, err := p.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer conn.Close()

	fields := conn.FieldDescriptions()
	for conn.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		values, err := conn.Values()
		if err != nil {
			return err
		}
		data := make(map[string]interface{}, len(fields))
		for i, field := range fields {
			data[string(field.Name)] = convertPgValue(values[i], "")
		}

		wm := extractWatermarkFromData(data, p.GetWatermarkColumn())
		r := row.Get()
		r.ID = uuid.NewString()
		r.Source = p.sourceName()
		r.Pipeline = p.pipelineName()
		r.PrimaryKey = extractPKFromData(data)
		if r.PrimaryKey == "" {
			r.PrimaryKey = uuid.NewString()
		}
		r.Data = data
		r.ExtractedAt = time.Now()
		r.Watermark = wm
		if r.Watermark.IsZero() {
			r.Watermark = r.ExtractedAt
		}
		result := *r
		r.Data = make(map[string]interface{}, 16)
		row.Put(r)
		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return conn.Err()
}

func buildSelectQuery(table, watermarkColumn string, cols []string, watermark, intervalEnd time.Time, batchSize, offset int) (string, []any, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}
	if len(quotedCols) == 0 {
		quotedCols = []string{"*"}
	}

	q := psql.Select(quotedCols...).From(quoteTableName(table))
	if !watermark.IsZero() {
		q = q.Where(sq.Gt{quoteIdentifier(watermarkColumn): watermark})
	}
	if !intervalEnd.IsZero() {
		q = q.Where(sq.LtOrEq{quoteIdentifier(watermarkColumn): intervalEnd})
	}
	q = q.OrderBy(quoteIdentifier(watermarkColumn) + " ASC").Limit(uint64(batchSize)).Offset(uint64(offset))
	return q.ToSql()
}

func parseTableName(table string) (schema string, name string) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", table
}

func (p *PostgresSource) sourceName() string {
	if p.cfg.Table == "" {
		return p.cfg.Type
	}
	return p.cfg.Type + "." + p.cfg.Table
}

func (p *PostgresSource) pipelineName() string {
	if p.cfg.Options != nil {
		if name := strings.TrimSpace(p.cfg.Options["pipeline"]); name != "" {
			return name
		}
	}
	return ""
}

func (p *PostgresSource) isCustomQuery() bool {
	return strings.TrimSpace(p.cfg.Query) != ""
}

func (p *PostgresSource) resolveQuery(watermark time.Time, intervalEnd time.Time) string {
	query := p.cfg.Query
	query = strings.ReplaceAll(query, "{{watermark}}", "'"+watermark.UTC().Format(time.RFC3339)+"'")
	query = strings.ReplaceAll(query, "{{interval_end}}", "'"+intervalEnd.UTC().Format(time.RFC3339)+"'")
	pipeline := ""
	if p.cfg.Options != nil {
		pipeline = p.cfg.Options["pipeline"]
	}
	query = strings.ReplaceAll(query, "{{pipeline}}", pipeline)
	return query
}

func extractWatermarkFromData(data map[string]interface{}, wmCol string) time.Time {
	if wmCol == "" {
		wmCol = "updated_at"
	}
	if val, ok := data[wmCol]; ok {
		if ts, err := asTime(val); err == nil {
			return ts
		}
	}
	if val, ok := data["watermark"]; ok {
		if ts, err := asTime(val); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func extractPKFromData(data map[string]interface{}) string {
	for _, key := range []string{"id", "ID", "row_id"} {
		if val, ok := data[key]; ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return uuid.NewString()
}

func normalizeConnectionString(conn string) string {
	idx := strings.Index(conn, "://")
	if idx <= 0 {
		return conn
	}
	scheme := conn[:idx]
	if strings.Contains(scheme, "+") {
		return "postgres://" + conn[idx+3:]
	}
	return conn
}

func normalizePgTypeName(dataType, udtName string) string {
	if name := strings.ToLower(strings.TrimSpace(udtName)); name != "" {
		return name
	}
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "integer":
		return "int4"
	case "smallint":
		return "int2"
	case "bigint":
		return "int8"
	case "real":
		return "float4"
	case "double precision":
		return "float8"
	case "character varying":
		return "varchar"
	case "character":
		return "char"
	case "timestamp with time zone":
		return "timestamptz"
	case "timestamp without time zone":
		return "timestamp"
	case "boolean":
		return "bool"
	default:
		return strings.ToLower(strings.TrimSpace(dataType))
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteTableName(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return quoteIdentifier(parts[0]) + "." + quoteIdentifier(parts[1])
	}
	return `"public".` + quoteIdentifier(table)
}

func buildPrimaryKey(schema *tableSchema, data map[string]interface{}) string {
	if schema == nil || len(schema.PrimaryKeys) == 0 {
		return ""
	}

	parts := make([]string, 0, len(schema.PrimaryKeys))
	for _, pk := range schema.PrimaryKeys {
		parts = append(parts, fmt.Sprintf("%s=%v", pk, data[pk]))
	}
	return strings.Join(parts, ",")
}

func watermarkFromValues(values []any, fields []pgconn.FieldDescription, watermarkColumn string) (time.Time, error) {
	for i, field := range fields {
		if field.Name != watermarkColumn {
			continue
		}
		return asTime(values[i])
	}
	return time.Time{}, fmt.Errorf("postgres source: watermark column %q not found", watermarkColumn)
}

func asTime(v any) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t.UTC(), nil
	case *time.Time:
		if t == nil {
			return time.Time{}, errors.New("postgres source: nil time value")
		}
		return t.UTC(), nil
	case pgtype.Timestamptz:
		if !t.Valid {
			return time.Time{}, errors.New("postgres source: invalid timestamptz")
		}
		return t.Time.UTC(), nil
	case pgtype.Date:
		if !t.Valid {
			return time.Time{}, errors.New("postgres source: invalid date")
		}
		return t.Time.UTC(), nil
	case string:
		return parseTimeString(t)
	case []byte:
		return parseTimeString(string(t))
	case nil:
		return time.Time{}, errors.New("postgres source: nil watermark value")
	default:
		return parseTimeString(fmt.Sprint(t))
	}
}

func parseTimeString(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}

	for _, layout := range layouts {
		if ts, err := time.Parse(layout, s); err == nil {
			return ts.UTC(), nil
		}
	}

	if unix, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(unix, 0).UTC(), nil
	}

	return time.Time{}, fmt.Errorf("postgres source: cannot parse time %q", s)
}

func convertPgValue(val any, pgType string) any {
	switch strings.ToLower(strings.TrimSpace(pgType)) {
	case "int2", "int4", "int8", "serial", "bigserial":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Int2:
			if !v.Valid {
				return nil
			}
			return int64(v.Int16)
		case pgtype.Int4:
			if !v.Valid {
				return nil
			}
			return int64(v.Int32)
		case pgtype.Int8:
			if !v.Valid {
				return nil
			}
			return v.Int64
		case int, int8, int16, int32, int64:
			return toInt64(v)
		default:
			return fmt.Sprintf("%v", v)
		}
	case "float4", "float8", "real", "double precision":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Float4:
			if !v.Valid {
				return nil
			}
			return float64(v.Float32)
		case pgtype.Float8:
			if !v.Valid {
				return nil
			}
			return v.Float64
		case float32:
			return float64(v)
		case float64:
			return v
		default:
			return fmt.Sprintf("%v", v)
		}
	case "numeric", "decimal":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Numeric:
			if !v.Valid {
				return nil
			}
			return numericToFloat64(v)
		case float32:
			return float64(v)
		case float64:
			return v
		default:
			return fmt.Sprintf("%v", v)
		}
	case "bool", "boolean":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Bool:
			if !v.Valid {
				return nil
			}
			return v.Bool
		case bool:
			return v
		default:
			return fmt.Sprintf("%v", v)
		}
	case "timestamptz", "timestamp", "date":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Timestamptz:
			if !v.Valid {
				return nil
			}
			return v.Time.UTC()
		case pgtype.Date:
			if !v.Valid {
				return nil
			}
			return v.Time.UTC()
		case time.Time:
			return v.UTC()
		case string:
			if ts, err := parseTimeString(v); err == nil {
				return ts
			}
			return v
		case []byte:
			if ts, err := parseTimeString(string(v)); err == nil {
				return ts
			}
			return string(v)
		default:
			if ts, err := parseTimeString(fmt.Sprint(v)); err == nil {
				return ts
			}
			return fmt.Sprintf("%v", v)
		}
	case "uuid":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.UUID:
			if !v.Valid {
				return nil
			}
			return v.String()
		case [16]byte:
			// pgx v5 rows.Values() returns uuid columns as raw [16]byte.
			return formatUUID(v)
		case string:
			return v
		default:
			return fmt.Sprintf("%v", v)
		}
	case "text", "varchar", "char", "bpchar", "name":
		switch v := val.(type) {
		case nil:
			return nil
		case pgtype.Text:
			if !v.Valid {
				return nil
			}
			return v.String
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return fmt.Sprintf("%v", v)
		}
	case "jsonb", "json":
		switch v := val.(type) {
		case nil:
			return nil
		case []byte:
			return string(v)
		case string:
			return v
		default:
			// pgx v5 decodes JSON/JSONB into native Go values (map[string]interface{},
			// []interface{}, etc.) — re-marshal so destinations receive valid JSON text.
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Sprintf("%v", v)
			}
			return string(b)
		}
	}

	switch v := val.(type) {
	case nil:
		return nil
	case pgtype.Numeric:
		if !v.Valid {
			return nil
		}
		f8, err := v.Float64Value()
		if err != nil || !f8.Valid {
			return nil
		}
		return f8.Float64
	case pgtype.Timestamptz:
		if !v.Valid {
			return nil
		}
		return v.Time.UTC()
	case pgtype.Date:
		if !v.Valid {
			return nil
		}
		return v.Time.UTC()
	case pgtype.Bool:
		if !v.Valid {
			return nil
		}
		return v.Bool
	case pgtype.Int4:
		if !v.Valid {
			return nil
		}
		return int64(v.Int32)
	case pgtype.Int8:
		if !v.Valid {
			return nil
		}
		return v.Int64
	case pgtype.Float4:
		if !v.Valid {
			return nil
		}
		return float64(v.Float32)
	case pgtype.Float8:
		if !v.Valid {
			return nil
		}
		return v.Float64
	case pgtype.Text:
		if !v.Valid {
			return nil
		}
		return v.String
	case pgtype.UUID:
		if !v.Valid {
			return nil
		}
		return v.String()
	case [16]byte:
		return formatUUID(v)
	case string:
		return v
	case []byte:
		return hex.EncodeToString(v)
	case time.Time:
		return v.UTC()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatUUID renders a raw 16-byte uuid in canonical 8-4-4-4-12 form.
func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func numericToFloat64(n pgtype.Numeric) float64 {
	if !n.Valid {
		return 0
	}
	if n.NaN {
		return math.NaN()
	}
	if n.InfinityModifier == pgtype.Infinity {
		return math.Inf(1)
	}
	if n.InfinityModifier == pgtype.NegativeInfinity {
		return math.Inf(-1)
	}
	if n.Int == nil {
		return 0
	}

	f := new(big.Float).SetInt(n.Int)
	if n.Exp != 0 {
		f.Mul(f, big.NewFloat(math.Pow10(int(n.Exp))))
	}
	out, _ := f.Float64()
	return out
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}

// pgIntToInt64 converts a pgtype or native integer value to int64 for PK range calculations.
func pgIntToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case pgtype.Int8:
		if !n.Valid {
			return 0, false
		}
		return n.Int64, true
	case pgtype.Int4:
		if !n.Valid {
			return 0, false
		}
		return int64(n.Int32), true
	case pgtype.Int2:
		if !n.Valid {
			return 0, false
		}
		return int64(n.Int16), true
	default:
		return 0, false
	}
}
