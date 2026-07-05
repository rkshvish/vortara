package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQuerySource implements BatchSource for BigQuery tables and custom SQL.
type BigQuerySource struct {
	cfg     config.SourceConfig
	client  bqClient
	project string
	dataset string
	table   string
	columns []columnInfo
}

var _ BatchSource = (*BigQuerySource)(nil)

// bqQuerier runs BigQuery SQL queries.
type bqQuerier interface {
	Query(sql string) bqQueryRunner
}

// bqQueryRunner configures query parameters and reads rows.
type bqQueryRunner interface {
	SetParameters(params []bigquery.QueryParameter)
	Read(ctx context.Context) (bqRowIterator, error)
}

// bqRowIterator iterates over query results.
type bqRowIterator interface {
	Next(dst *[]bigquery.Value) error
	Columns() []string
}

// bqClient adds Close to the BigQuery query interface.
type bqClient interface {
	bqQuerier
	Close() error
}

type bigQueryClient struct {
	client *bigquery.Client
}

type bigQueryQuery struct {
	query *bigquery.Query
}

type bigQueryRowIterator struct {
	iter *bigquery.RowIterator
}

// BigQuerySourceOption configures a BigQuerySource for tests.
type BigQuerySourceOption func(*BigQuerySource)

// WithBigQueryClient injects a client for tests.
func WithBigQueryClient(client bqClient) BigQuerySourceOption {
	return func(s *BigQuerySource) {
		s.client = client
	}
}

var openBigQueryClient = func(ctx context.Context, project string, opts ...option.ClientOption) (bqClient, error) {
	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, err
	}
	return &bigQueryClient{client: client}, nil
}

func init() {
	registry.RegisterBatchSource("bigquery", func() any {
		return NewBigQuerySource()
	})
}

// NewBigQuerySource returns a new BigQuerySource.
func NewBigQuerySource(opts ...BigQuerySourceOption) *BigQuerySource {
	src := &BigQuerySource{}
	for _, opt := range opts {
		if opt != nil {
			opt(src)
		}
	}
	return src
}

// Connect validates config and opens a BigQuery client.
func (b *BigQuerySource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	if strings.EqualFold(strings.TrimSpace(cfg.WatermarkColumn), "none") {
		return errors.New("bigquery source: watermark: none (full-snapshot mode) is not yet supported for bigquery — use a timestamp watermark column or a custom query")
	}
	project := strings.TrimSpace(cfg.Connection)
	if project == "" && cfg.Options != nil {
		project = strings.TrimSpace(cfg.Options["project"])
	}
	if project == "" {
		return errors.New("bigquery source: project is required")
	}

	dataset := ""
	credentialsFile := ""
	credentialsJSON := ""
	if cfg.Options != nil {
		dataset = strings.TrimSpace(cfg.Options["dataset"])
		credentialsFile = strings.TrimSpace(cfg.Options["credentials_file"])
		credentialsJSON = strings.TrimSpace(cfg.Options["credentials_json"])
	}
	if dataset == "" {
		return errors.New("bigquery source: dataset is required")
	}

	if b.client == nil {
		var opts []option.ClientOption
		if credentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(credentialsFile))
		} else if credentialsJSON != "" {
			opts = append(opts, option.WithCredentialsJSON([]byte(credentialsJSON)))
		}
		client, err := openBigQueryClient(ctx, project, opts...)
		if err != nil {
			return fmt.Errorf("bigquery source: open client: %w", err)
		}
		b.client = client
	}

	b.cfg = cfg
	b.project = project
	b.dataset = dataset
	b.table = strings.TrimSpace(cfg.Table)
	b.columns = nil
	return nil
}

// Extract streams rows from BigQuery using watermark pagination or a custom query.
func (b *BigQuerySource) Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	if b.client == nil {
		return errors.New("bigquery source: not connected")
	}
	defer close(out)

	if b.isCustomQuery() {
		return b.runCustomQuery(ctx, watermark, intervalEnd, out)
	}

	schema, err := b.introspectSchema(ctx, b.table)
	if err != nil {
		return err
	}
	keys, err := b.primaryKeys(ctx, b.table)
	if err != nil {
		return err
	}
	schema.PrimaryKeys = keys
	b.columns = schema.Columns

	batchSize := b.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		query, params, err := buildBigQuerySelectQuery(b.project, b.dataset, b.table, b.GetWatermarkColumn(), b.columnNames(schema.Columns), watermark, intervalEnd, batchSize, offset)
		if err != nil {
			return err
		}

		it, err := b.query(ctx, query, params)
		if err != nil {
			return err
		}

		pageCount, err := b.emitRows(ctx, it, schema, out, offset)
		if err != nil {
			return err
		}
		if pageCount < batchSize {
			return nil
		}
		offset += pageCount
	}
}

// GetWatermarkColumn returns the configured watermark column or the default.
func (b *BigQuerySource) GetWatermarkColumn() string {
	if v := strings.TrimSpace(b.cfg.WatermarkColumn); v != "" {
		return v
	}
	return "updated_at"
}

// Close releases the BigQuery client.
func (b *BigQuerySource) Close() error {
	if b.client != nil {
		return b.client.Close()
	}
	return nil
}

func (b *BigQuerySource) query(ctx context.Context, sql string, params []bigquery.QueryParameter) (bqRowIterator, error) {
	q := b.client.Query(sql)
	q.SetParameters(params)
	return q.Read(ctx)
}

func (b *BigQuerySource) introspectSchema(ctx context.Context, table string) (*tableSchema, error) {
	q := fmt.Sprintf(`
SELECT column_name, data_type, is_nullable
FROM %s.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = @table
ORDER BY ordinal_position
`, bqQuoteDatasetScope(b.project, b.dataset))
	it, err := b.query(ctx, q, []bigquery.QueryParameter{{Name: "table", Value: table}})
	if err != nil {
		return nil, err
	}

	var columns []columnInfo
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}
		if len(vals) < 3 {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(vals[0]))
		if name == "" {
			continue
		}
		columns = append(columns, columnInfo{
			Name:       name,
			DataType:   normalizeBigQueryType(fmt.Sprint(vals[1])),
			IsNullable: strings.EqualFold(strings.TrimSpace(fmt.Sprint(vals[2])), "yes"),
		})
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("bigquery source: table %s not found or has no columns", table)
	}
	return &tableSchema{Columns: b.filterExcludedColumns(columns)}, nil
}

func (b *BigQuerySource) primaryKeys(ctx context.Context, table string) ([]string, error) {
	q := fmt.Sprintf(`
SELECT kcu.column_name
FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = @table
  AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.ordinal_position
`, bqQuoteDatasetScope(b.project, b.dataset), bqQuoteDatasetScope(b.project, b.dataset))
	it, err := b.query(ctx, q, []bigquery.QueryParameter{{Name: "table", Value: table}})
	if err != nil {
		return nil, err
	}

	var keys []string
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}
		if len(vals) == 0 {
			continue
		}
		if name := strings.TrimSpace(fmt.Sprint(vals[0])); name != "" {
			keys = append(keys, name)
		}
	}
	return keys, nil
}

func (b *BigQuerySource) emitRows(ctx context.Context, it bqRowIterator, schema *tableSchema, out chan<- row.Row, offset int) (int, error) {
	pageCount := 0
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err != nil {
			if errors.Is(err, iterator.Done) {
				return pageCount, nil
			}
			return pageCount, err
		}
		if len(vals) == 0 {
			continue
		}

		data := make(map[string]interface{}, len(schema.Columns))
		for i, col := range schema.Columns {
			if i < len(vals) {
				data[col.Name] = convertBQValue(vals[i], col)
			}
		}

		rowNum := offset + pageCount + 1
		wm := bqWatermarkFromData(data, b.GetWatermarkColumn())
		pk := bqPrimaryKey(schema, data, rowNum)
		r := row.NewRow(b.sourceName(), b.pipelineName(), pk, data, wm).WithContext(ctx)
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
}

func (b *BigQuerySource) runCustomQuery(ctx context.Context, watermark, intervalEnd time.Time, out chan<- row.Row) error {
	query := b.resolveQuery(watermark, intervalEnd)
	it, err := b.query(ctx, query, nil)
	if err != nil {
		return err
	}

	columns := it.Columns()
	var rowIdx int
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err != nil {
			if errors.Is(err, iterator.Done) {
				return nil
			}
			return err
		}
		if len(vals) == 0 {
			continue
		}

		rowIdx++
		data := make(map[string]interface{}, len(vals))
		for i, v := range vals {
			key := fmt.Sprintf("col_%d", i)
			if i < len(columns) && strings.TrimSpace(columns[i]) != "" {
				key = columns[i]
			}
			data[key] = convertBQValue(v, columnInfo{})
		}
		wm := bqWatermarkFromData(data, b.GetWatermarkColumn())
		pk := bqPrimaryKey(&tableSchema{}, data, rowIdx)
		r := row.NewRow(b.sourceName(), b.pipelineName(), pk, data, wm).WithContext(ctx)
		if r.Watermark.IsZero() {
			r.Watermark = r.ExtractedAt
		}

		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *BigQuerySource) resolveQuery(watermark, intervalEnd time.Time) string {
	query := b.cfg.Query
	query = strings.ReplaceAll(query, "{{watermark}}", watermark.UTC().Format(time.RFC3339))
	query = strings.ReplaceAll(query, "{{interval_end}}", intervalEnd.UTC().Format(time.RFC3339))
	return query
}

func (b *BigQuerySource) isCustomQuery() bool {
	return strings.TrimSpace(b.cfg.Query) != ""
}

func (b *BigQuerySource) filterExcludedColumns(cols []columnInfo) []columnInfo {
	if len(b.cfg.ExcludeColumns) == 0 {
		return cols
	}
	excluded := make(map[string]bool, len(b.cfg.ExcludeColumns))
	for _, name := range b.cfg.ExcludeColumns {
		excluded[strings.ToLower(strings.TrimSpace(name))] = true
	}
	filtered := make([]columnInfo, 0, len(cols))
	for _, col := range cols {
		if !excluded[strings.ToLower(col.Name)] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

func (b *BigQuerySource) sourceName() string {
	if strings.TrimSpace(b.cfg.Type) != "" {
		return b.cfg.Type
	}
	return "bigquery"
}

func (b *BigQuerySource) pipelineName() string {
	if b.cfg.Options != nil {
		if name := strings.TrimSpace(b.cfg.Options["pipeline"]); name != "" {
			return name
		}
	}
	return ""
}

func (b *BigQuerySource) columnNames(cols []columnInfo) []string {
	names := make([]string, 0, len(cols))
	for _, col := range cols {
		names = append(names, col.Name)
	}
	return names
}

func bqQuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}

func bqQuoteTable(project, dataset, table string) string {
	return fmt.Sprintf("`%s.%s.%s`", project, dataset, table)
}

func bqQuoteDatasetScope(project, dataset string) string {
	return fmt.Sprintf("`%s.%s`", project, dataset)
}

func buildBigQuerySelectQuery(project, dataset, table, watermarkColumn string, cols []string, watermark, intervalEnd time.Time, batchSize, offset int) (string, []bigquery.QueryParameter, error) {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, bqQuoteIdentifier(col))
	}
	if len(quoted) == 0 {
		quoted = []string{"*"}
	}

	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(strings.Join(quoted, ", "))
	b.WriteString(" FROM ")
	b.WriteString(bqQuoteTable(project, dataset, table))
	params := make([]bigquery.QueryParameter, 0, 4)
	if !watermark.IsZero() {
		b.WriteString(" WHERE ")
		b.WriteString(fmt.Sprintf("%s >= TIMESTAMP(@start)", bqQuoteIdentifier(watermarkColumn)))
		params = append(params, bigquery.QueryParameter{Name: "start", Value: watermark})
	}
	if !intervalEnd.IsZero() {
		if watermark.IsZero() {
			b.WriteString(" WHERE ")
		} else {
			b.WriteString(" AND ")
		}
		b.WriteString(fmt.Sprintf("%s <= TIMESTAMP(@end)", bqQuoteIdentifier(watermarkColumn)))
		params = append(params, bigquery.QueryParameter{Name: "end", Value: intervalEnd})
	}
	b.WriteString(" ORDER BY ")
	b.WriteString(bqQuoteIdentifier(watermarkColumn))
	b.WriteString(" ASC")
	if batchSize > 0 {
		b.WriteString(" LIMIT @limit")
		params = append(params, bigquery.QueryParameter{Name: "limit", Value: batchSize})
	}
	if offset > 0 {
		b.WriteString(" OFFSET @offset")
		params = append(params, bigquery.QueryParameter{Name: "offset", Value: offset})
	}
	return b.String(), params, nil
}

func convertBQValue(v bigquery.Value, col columnInfo) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
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
	case civil.Date:
		return time.Date(val.Year, val.Month, val.Day, 0, 0, 0, 0, time.UTC)
	case []bigquery.Value:
		b, _ := json.Marshal(val)
		return string(b)
	default:
		_ = col
		return fmt.Sprintf("%v", val)
	}
}

func bqWatermarkFromData(data map[string]interface{}, wmCol string) time.Time {
	if wmCol == "" {
		wmCol = "updated_at"
	}
	if val, ok := lookupCaseInsensitive(data, wmCol); ok {
		if ts, err := bqAsTime(val); err == nil {
			return ts
		}
	}
	if val, ok := lookupCaseInsensitive(data, "watermark"); ok {
		if ts, err := bqAsTime(val); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func bqPrimaryKey(schema *tableSchema, data map[string]interface{}, rowNum int) string {
	if schema != nil && len(schema.PrimaryKeys) > 0 {
		parts := make([]string, 0, len(schema.PrimaryKeys))
		for _, key := range schema.PrimaryKeys {
			parts = append(parts, fmt.Sprintf("%s=%v", key, data[key]))
		}
		return strings.Join(parts, ",")
	}
	for _, key := range []string{"id", "row_id"} {
		if val, ok := lookupCaseInsensitive(data, key); ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return fmt.Sprintf("row-%d", rowNum)
}

func bqAsTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val.UTC(), nil
	case *time.Time:
		if val == nil {
			return time.Time{}, errors.New("bigquery source: nil time value")
		}
		return val.UTC(), nil
	case civil.Date:
		return time.Date(val.Year, val.Month, val.Day, 0, 0, 0, 0, time.UTC), nil
	case string:
		return time.Parse(time.RFC3339, val)
	case []byte:
		return time.Parse(time.RFC3339, string(val))
	default:
		return time.Time{}, fmt.Errorf("bigquery source: cannot parse time %T", v)
	}
}

func normalizeBigQueryType(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "STRING", "BYTES":
		return "string"
	case "INTEGER", "INT64":
		return "int64"
	case "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
		return "float64"
	case "BOOLEAN", "BOOL":
		return "bool"
	case "TIMESTAMP", "DATETIME", "DATE":
		return "time.Time"
	case "RECORD", "STRUCT", "JSON", "ARRAY":
		return "string"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func (c *bigQueryClient) Query(sql string) bqQueryRunner {
	return &bigQueryQuery{query: c.client.Query(sql)}
}

func (c *bigQueryClient) Close() error {
	return c.client.Close()
}

func (q *bigQueryQuery) SetParameters(params []bigquery.QueryParameter) {
	q.query.Parameters = params
}

func (q *bigQueryQuery) Read(ctx context.Context) (bqRowIterator, error) {
	it, err := q.query.Read(ctx)
	if err != nil {
		return nil, err
	}
	return &bigQueryRowIterator{iter: it}, nil
}

func (it *bigQueryRowIterator) Next(dst *[]bigquery.Value) error {
	if dst == nil {
		return errors.New("bigquery source: nil destination row")
	}
	var rowVals []bigquery.Value
	if err := it.iter.Next(&rowVals); err != nil {
		return err
	}
	*dst = rowVals
	return nil
}

func (it *bigQueryRowIterator) Columns() []string {
	if it == nil || it.iter == nil || it.iter.Schema == nil {
		return nil
	}
	cols := make([]string, 0, len(it.iter.Schema))
	for _, field := range it.iter.Schema {
		if field != nil {
			cols = append(cols, field.Name)
		}
	}
	return cols
}

var _ bqClient = (*bigQueryClient)(nil)
var _ bqQueryRunner = (*bigQueryQuery)(nil)
var _ bqRowIterator = (*bigQueryRowIterator)(nil)
