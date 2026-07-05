package destination

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	gosnowflake "github.com/snowflakedb/gosnowflake"

	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// SnowflakeDestination writes rows to a Snowflake table.
// Identifiers are uppercased and quoted, matching Snowflake's default
// case-folding for tables created with unquoted DDL.
type SnowflakeDestination struct {
	cfg      config.DestinationConfig
	db       *sql.DB
	schema   string
	table    string
	strategy string
	matchOn  []string

	mu               sync.Mutex
	replaceTruncated map[int64]bool
	truncatedNoRunID bool
}

var _ Destination = (*SnowflakeDestination)(nil)

func init() {
	registry.RegisterDestination("snowflake", func() any {
		return NewSnowflakeDestination()
	})
}

var openSnowflakeDestDB = func(dsn string) (*sql.DB, error) {
	return sql.Open("snowflake", dsn)
}

// NewSnowflakeDestination returns a new SnowflakeDestination.
func NewSnowflakeDestination() *SnowflakeDestination {
	return &SnowflakeDestination{}
}

// Connect opens the Snowflake connection and validates destination settings.
func (s *SnowflakeDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	table := strings.TrimSpace(cfg.Options["table"])
	if table == "" {
		return errors.New("snowflake destination: table is required")
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.Strategy))
	if strategy == "" {
		strategy = "merge"
	}
	switch strategy {
	case "merge", "append", "replace", "delete+insert":
	default:
		return fmt.Errorf("snowflake destination: strategy %q unknown, valid: merge, append, replace, delete+insert", cfg.Strategy)
	}
	var matchOn []string
	for _, k := range strings.Split(cfg.MatchOn, ",") {
		if k = strings.TrimSpace(k); k != "" {
			matchOn = append(matchOn, k)
		}
	}
	if (strategy == "merge" || strategy == "delete+insert") && len(matchOn) == 0 {
		return fmt.Errorf("snowflake destination: strategy %q requires match_on", strategy)
	}

	rawURL := strings.TrimSpace(cfg.URL)
	if rawURL == "" {
		rawURL = strings.TrimSpace(cfg.Connection)
	}
	if rawURL == "" {
		return errors.New("snowflake destination: url is required")
	}
	conn, err := parseSnowflakeDestURL(rawURL)
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
		return fmt.Errorf("snowflake destination: build dsn: %w", err)
	}
	db, err := openSnowflakeDestDB(dsn)
	if err != nil {
		return fmt.Errorf("snowflake destination: open db: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("snowflake destination: ping: %w", err)
	}

	s.cfg = cfg
	s.db = db
	s.schema, s.table = parseSnowflakeDestTable(table)
	s.strategy = strategy
	s.matchOn = matchOn
	s.replaceTruncated = nil
	s.truncatedNoRunID = false
	return nil
}

// Load writes rows using the configured strategy.
func (s *SnowflakeDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if s.db == nil {
		return result, errors.New("snowflake destination: not connected")
	}
	if len(rows) == 0 {
		return result, nil
	}

	if s.strategy == "replace" {
		if err := s.maybeTruncateReplace(ctx); err != nil {
			return result, fmt.Errorf("snowflake destination: truncate: %w", err)
		}
	}

	skipDeliveryCheck := s.strategy == "append" || s.strategy == "replace"
	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !skipDeliveryCheck {
			delivered, err := store.IsDelivered(rw.ID, pipeline, destination)
			if err != nil {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				continue
			}
			if delivered {
				result.Skipped++
				continue
			}
		}

		if err := s.writeRow(ctx, rw); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}

		if !skipDeliveryCheck {
			if err := store.MarkDelivered(rw.ID, pipeline, destination); err != nil {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				continue
			}
		}
		result.Loaded++
	}
	return result, nil
}

// maybeTruncateReplace truncates the target table once per run. The run ID is
// read from the context (set by the engine); when absent, it truncates once
// per connection as a fallback.
func (s *SnowflakeDestination) maybeTruncateReplace(ctx context.Context) error {
	runID := int64(0)
	if v, ok := ctx.Value("vortara_run_id").(int64); ok {
		runID = v
	}
	s.mu.Lock()
	if runID != 0 {
		if s.replaceTruncated[runID] {
			s.mu.Unlock()
			return nil
		}
	} else if s.truncatedNoRunID {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE IF EXISTS "+s.qualifiedTable()); err != nil {
		return err
	}

	s.mu.Lock()
	if runID != 0 {
		if s.replaceTruncated == nil {
			s.replaceTruncated = make(map[int64]bool)
		}
		s.replaceTruncated[runID] = true
	} else {
		s.truncatedNoRunID = true
	}
	s.mu.Unlock()
	return nil
}

func (s *SnowflakeDestination) writeRow(ctx context.Context, rw row.Row) error {
	cols := sortedColumns(rw.Data)
	if len(cols) == 0 {
		return errors.New("snowflake destination: row has no columns")
	}
	var query string
	var args []any
	switch s.strategy {
	case "merge":
		query, args = buildSnowflakeMerge(s.qualifiedTable(), cols, s.matchOn, rw.Data)
		_, err := s.db.ExecContext(ctx, query, args...)
		return err
	case "delete+insert":
		delQuery, delArgs := buildSnowflakeDelete(s.qualifiedTable(), s.matchOn, rw.Data)
		if _, err := s.db.ExecContext(ctx, delQuery, delArgs...); err != nil {
			return err
		}
		query, args = buildSnowflakeInsert(s.qualifiedTable(), cols, rw.Data)
		_, err := s.db.ExecContext(ctx, query, args...)
		return err
	default: // append, replace
		query, args = buildSnowflakeInsert(s.qualifiedTable(), cols, rw.Data)
		_, err := s.db.ExecContext(ctx, query, args...)
		return err
	}
}

// Close closes the database handle.
func (s *SnowflakeDestination) Close() error {
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		return err
	}
	return nil
}

func (s *SnowflakeDestination) qualifiedTable() string {
	return quoteSnowflakeIdent(s.schema) + "." + quoteSnowflakeIdent(s.table)
}

// quoteSnowflakeIdent uppercases and quotes an identifier.
func quoteSnowflakeIdent(name string) string {
	upper := strings.ToUpper(strings.TrimSpace(name))
	return `"` + strings.ReplaceAll(upper, `"`, `""`) + `"`
}

func sortedColumns(data map[string]interface{}) []string {
	cols := make([]string, 0, len(data))
	for c := range data {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

// buildSnowflakeInsert builds INSERT INTO t (cols) VALUES (?, ...).
func buildSnowflakeInsert(table string, cols []string, data map[string]interface{}) (string, []any) {
	quoted := make([]string, len(cols))
	holders := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		quoted[i] = quoteSnowflakeIdent(c)
		holders[i] = "?"
		args[i] = normalizeSnowflakeArg(data[c])
	}
	return "INSERT INTO " + table + " (" + strings.Join(quoted, ", ") + ") VALUES (" + strings.Join(holders, ", ") + ")", args
}

// buildSnowflakeMerge builds a single-row MERGE INTO ... USING (SELECT ? ...) upsert.
func buildSnowflakeMerge(table string, cols []string, matchOn []string, data map[string]interface{}) (string, []any) {
	selects := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		selects[i] = "? AS " + quoteSnowflakeIdent(c)
		args[i] = normalizeSnowflakeArg(data[c])
	}

	matchSet := make(map[string]bool, len(matchOn))
	conds := make([]string, len(matchOn))
	for i, k := range matchOn {
		q := quoteSnowflakeIdent(k)
		conds[i] = "t." + q + " = s." + q
		matchSet[strings.ToUpper(strings.TrimSpace(k))] = true
	}

	var updates []string
	insertCols := make([]string, len(cols))
	insertVals := make([]string, len(cols))
	for i, c := range cols {
		q := quoteSnowflakeIdent(c)
		insertCols[i] = q
		insertVals[i] = "s." + q
		if !matchSet[strings.ToUpper(strings.TrimSpace(c))] {
			updates = append(updates, "t."+q+" = s."+q)
		}
	}

	query := "MERGE INTO " + table + " t USING (SELECT " + strings.Join(selects, ", ") + ") s ON " + strings.Join(conds, " AND ")
	if len(updates) > 0 {
		query += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ", ")
	}
	query += " WHEN NOT MATCHED THEN INSERT (" + strings.Join(insertCols, ", ") + ") VALUES (" + strings.Join(insertVals, ", ") + ")"
	return query, args
}

// buildSnowflakeDelete builds DELETE FROM t WHERE match keys equal the row's values.
func buildSnowflakeDelete(table string, matchOn []string, data map[string]interface{}) (string, []any) {
	conds := make([]string, len(matchOn))
	args := make([]any, len(matchOn))
	for i, k := range matchOn {
		conds[i] = quoteSnowflakeIdent(k) + " = ?"
		args[i] = normalizeSnowflakeArg(data[k])
	}
	return "DELETE FROM " + table + " WHERE " + strings.Join(conds, " AND "), args
}

// normalizeSnowflakeArg converts values the driver cannot bind directly.
// Maps and slices are marshaled to JSON text so VARIANT/OBJECT columns
// receive valid JSON, not Go's fmt representation.
func normalizeSnowflakeArg(v any) any {
	switch val := v.(type) {
	case map[string]interface{}, []interface{}:
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(b)
	case time.Time:
		return val.UTC()
	default:
		return v
	}
}

type snowflakeDestConnInfo struct {
	User      string
	Password  string
	Account   string
	Database  string
	Schema    string
	Warehouse string
	Role      string
}

// parseSnowflakeDestURL parses snowflake://user:pass@account/db[/schema]?warehouse=&role=.
func parseSnowflakeDestURL(raw string) (snowflakeDestConnInfo, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return snowflakeDestConnInfo{}, fmt.Errorf("snowflake destination: parse url: %w", err)
	}
	if u.User == nil {
		return snowflakeDestConnInfo{}, errors.New("snowflake destination: url must include user and password")
	}
	user := u.User.Username()
	password, _ := u.User.Password()
	account := strings.TrimSpace(u.Hostname())
	if user == "" || password == "" || account == "" {
		return snowflakeDestConnInfo{}, errors.New("snowflake destination: url must include user, password, and account")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return snowflakeDestConnInfo{}, errors.New("snowflake destination: url must include database in path")
	}
	info := snowflakeDestConnInfo{
		User:      user,
		Password:  password,
		Account:   account,
		Database:  strings.TrimSpace(parts[0]),
		Schema:    "PUBLIC",
		Warehouse: u.Query().Get("warehouse"),
		Role:      u.Query().Get("role"),
	}
	if len(parts) >= 2 && strings.TrimSpace(parts[1]) != "" {
		info.Schema = strings.ToUpper(strings.TrimSpace(parts[1]))
	}
	return info, nil
}

func parseSnowflakeDestTable(table string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(table), ".", 2)
	if len(parts) == 2 {
		return strings.ToUpper(strings.TrimSpace(parts[0])), strings.ToUpper(strings.TrimSpace(parts[1]))
	}
	return "PUBLIC", strings.ToUpper(strings.TrimSpace(table))
}
