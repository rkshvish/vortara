package destination

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// MySQLDestination writes rows to a MySQL table. Supports all four
// strategies; merge uses INSERT ... ON DUPLICATE KEY UPDATE, which relies
// on a unique index over the match_on columns.
type MySQLDestination struct {
	cfg      config.DestinationConfig
	db       *sql.DB
	table    string
	strategy string
	matchOn  []string

	mu               sync.Mutex
	replaceTruncated map[int64]bool
	truncatedNoRunID bool
}

var _ Destination = (*MySQLDestination)(nil)

func init() {
	registry.RegisterDestination("mysql", func() any {
		return NewMySQLDestination()
	})
}

// NewMySQLDestination returns a new MySQLDestination.
func NewMySQLDestination() *MySQLDestination {
	return &MySQLDestination{}
}

// mysqlDestURLToDSN converts mysql://user:pass@host:port/db to driver DSN form.
func mysqlDestURLToDSN(rawURL string) (string, error) {
	trimmed := strings.TrimPrefix(rawURL, "mysql://")
	slash := strings.Index(trimmed, "/")
	if slash < 0 {
		return "", fmt.Errorf("mysql destination: url %q missing database name", rawURL)
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

// Connect opens the MySQL database and validates destination settings.
func (m *MySQLDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	table := strings.TrimSpace(cfg.Options["table"])
	if table == "" {
		return errors.New("mysql destination: table is required")
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.Strategy))
	if strategy == "" {
		strategy = "merge"
	}
	switch strategy {
	case "merge", "append", "replace", "delete+insert":
	default:
		return fmt.Errorf("mysql destination: strategy %q unknown, valid: merge, append, replace, delete+insert", cfg.Strategy)
	}
	var matchOn []string
	for _, k := range strings.Split(cfg.MatchOn, ",") {
		if k = strings.TrimSpace(k); k != "" {
			matchOn = append(matchOn, k)
		}
	}
	if (strategy == "merge" || strategy == "delete+insert") && len(matchOn) == 0 {
		return fmt.Errorf("mysql destination: strategy %q requires match_on", strategy)
	}

	dsn := strings.TrimSpace(cfg.Connection)
	if dsn == "" {
		dsn = strings.TrimSpace(cfg.URL)
	}
	if dsn == "" {
		return errors.New("mysql destination: url is required")
	}
	if strings.HasPrefix(dsn, "mysql://") {
		converted, err := mysqlDestURLToDSN(dsn)
		if err != nil {
			return err
		}
		dsn = converted
	}
	mycfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("mysql destination: invalid dsn: %w", err)
	}
	mycfg.ParseTime = true

	db, err := sql.Open("mysql", mycfg.FormatDSN())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(8)
	db.SetConnMaxIdleTime(90 * time.Second)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return fmt.Errorf("mysql destination: ping: %w", err)
	}

	m.cfg = cfg
	m.db = db
	m.table = table
	m.strategy = strategy
	m.matchOn = matchOn
	m.replaceTruncated = nil
	m.truncatedNoRunID = false
	return nil
}

// Load writes rows using the configured strategy in multi-row chunks.
func (m *MySQLDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if m.db == nil {
		return result, errors.New("mysql destination: not connected")
	}
	if len(rows) == 0 {
		return result, nil
	}

	if m.strategy == "replace" {
		if err := m.maybeTruncateReplace(ctx); err != nil {
			return result, fmt.Errorf("mysql destination: truncate: %w", err)
		}
	}

	skipDeliveryCheck := m.strategy == "append" || m.strategy == "replace"
	pending := make([]row.Row, 0, len(rows))
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
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	// Group by column signature, write multi-row chunks with per-row fallback.
	type group struct {
		cols []string
		rows []row.Row
	}
	var order []string
	groups := make(map[string]*group)
	for _, rw := range pending {
		cols := make([]string, 0, len(rw.Data))
		for c := range rw.Data {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		sig := strings.Join(cols, "\x00")
		g, ok := groups[sig]
		if !ok {
			g = &group{cols: cols}
			groups[sig] = g
			order = append(order, sig)
		}
		g.rows = append(g.rows, rw)
	}

	const maxChunk = 500
	for _, sig := range order {
		g := groups[sig]
		if len(g.cols) == 0 {
			for _, rw := range g.rows {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: errors.New("mysql destination: row has no columns")})
			}
			continue
		}
		for start := 0; start < len(g.rows); start += maxChunk {
			end := start + maxChunk
			if end > len(g.rows) {
				end = len(g.rows)
			}
			if err := m.writeChunk(ctx, g.cols, g.rows[start:end], store, pipeline, destination, &result); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

// writeChunk executes one multi-row statement; on failure it retries the
// chunk row by row to isolate bad rows.
func (m *MySQLDestination) writeChunk(ctx context.Context, cols []string, batch []row.Row, store state.StateStore, pipeline, destination string, result *LoadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.strategy == "delete+insert" {
		if err := m.deleteMatching(ctx, batch); err != nil {
			return err
		}
	}
	query, args := buildMySQLWriteSQL(m.table, cols, m.matchOn, batch, m.strategy)
	if _, err := m.db.ExecContext(ctx, query, args...); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ctx.Err()
		}
		// Per-row fallback isolates the failing rows.
		for _, rw := range batch {
			q, a := buildMySQLWriteSQL(m.table, cols, m.matchOn, []row.Row{rw}, m.strategy)
			if _, rowErr := m.db.ExecContext(ctx, q, a...); rowErr != nil {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: rowErr})
				continue
			}
			m.markLoaded(rw, store, pipeline, destination, result)
		}
		return nil
	}
	for _, rw := range batch {
		m.markLoaded(rw, store, pipeline, destination, result)
	}
	return nil
}

func (m *MySQLDestination) markLoaded(rw row.Row, store state.StateStore, pipeline, destination string, result *LoadResult) {
	if m.strategy == "merge" || m.strategy == "delete+insert" {
		if err := store.MarkDelivered(rw.ID, pipeline, destination); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			return
		}
	}
	result.Loaded++
}

// deleteMatching removes rows whose match keys equal any batch row's values.
func (m *MySQLDestination) deleteMatching(ctx context.Context, batch []row.Row) error {
	conds := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*len(m.matchOn))
	for _, rw := range batch {
		parts := make([]string, len(m.matchOn))
		for i, k := range m.matchOn {
			parts[i] = quoteMySQLIdent(k) + " = ?"
			args = append(args, normalizeMySQLArg(rw.Data[k]))
		}
		conds = append(conds, "("+strings.Join(parts, " AND ")+")")
	}
	query := "DELETE FROM " + quoteMySQLIdent(m.table) + " WHERE " + strings.Join(conds, " OR ")
	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

// maybeTruncateReplace truncates the target once per run (run ID from the
// engine context) or once per connection for direct callers.
func (m *MySQLDestination) maybeTruncateReplace(ctx context.Context) error {
	runID := int64(0)
	if v, ok := ctx.Value("vortara_run_id").(int64); ok {
		runID = v
	}
	m.mu.Lock()
	if runID != 0 {
		if m.replaceTruncated[runID] {
			m.mu.Unlock()
			return nil
		}
	} else if m.truncatedNoRunID {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	if _, err := m.db.ExecContext(ctx, "TRUNCATE TABLE "+quoteMySQLIdent(m.table)); err != nil {
		return err
	}

	m.mu.Lock()
	if runID != 0 {
		if m.replaceTruncated == nil {
			m.replaceTruncated = make(map[int64]bool)
		}
		m.replaceTruncated[runID] = true
	} else {
		m.truncatedNoRunID = true
	}
	m.mu.Unlock()
	return nil
}

// Close closes the database handle.
func (m *MySQLDestination) Close() error {
	if m.db != nil {
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

// buildMySQLWriteSQL builds a multi-row INSERT, with ON DUPLICATE KEY UPDATE
// for merge. Unlike Postgres, MySQL allows the same key to appear twice in
// one statement, so no chunk splitting on duplicate match keys is needed.
func buildMySQLWriteSQL(table string, cols []string, matchOn []string, batch []row.Row, strategy string) (string, []any) {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteMySQLIdent(c)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteMySQLIdent(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(quoted, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(batch)*len(cols))
	for i, rw := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, c := range cols {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("?")
			args = append(args, normalizeMySQLArg(rw.Data[c]))
		}
		sb.WriteString(")")
	}

	if strategy == "merge" {
		matchSet := make(map[string]bool, len(matchOn))
		for _, k := range matchOn {
			matchSet[strings.ToLower(k)] = true
		}
		var assignments []string
		for _, c := range cols {
			if matchSet[strings.ToLower(c)] {
				continue
			}
			q := quoteMySQLIdent(c)
			assignments = append(assignments, q+" = VALUES("+q+")")
		}
		if len(assignments) == 0 {
			// All columns are match keys: nothing to update on conflict.
			first := quoteMySQLIdent(cols[0])
			assignments = append(assignments, first+" = "+first)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		sb.WriteString(strings.Join(assignments, ", "))
	}
	return sb.String(), args
}

// normalizeMySQLArg converts values the driver cannot bind directly.
// Maps and slices are marshaled to JSON text so JSON columns receive
// valid JSON, not Go's fmt representation.
func normalizeMySQLArg(v any) any {
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

// quoteMySQLIdent quotes an identifier with backticks.
func quoteMySQLIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
