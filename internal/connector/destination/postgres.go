package destination

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// PostgresDestination writes rows to Postgres using batch upsert and COPY.
type PostgresDestination struct {
	cfg  config.DestinationConfig
	db   *sql.DB
	pool *pgxpool.Pool

	mu               sync.RWMutex
	query            map[string]string
	acquireSession   func(context.Context) (copySession, error)
	replaceTruncated map[int64]bool
	replaceStaging   map[int64]string // runID → staging table (atomic replace)
}

var _ Destination = (*PostgresDestination)(nil)

func init() {
	registry.RegisterDestination("postgres", func() any {
		return NewPostgresDestination()
	})
}

type copySession interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	Release()
}

type pgxPoolSession struct {
	conn *pgxpool.Conn
}

func (s *pgxPoolSession) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return s.conn.Conn().Exec(ctx, sql, args...)
}

func (s *pgxPoolSession) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return s.conn.Conn().CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (s *pgxPoolSession) Release() {
	s.conn.Release()
}

// NewPostgresDestination returns a new PostgresDestination.
func NewPostgresDestination() *PostgresDestination {
	return &PostgresDestination{query: make(map[string]string)}
}

// Connect opens the Postgres database and validates destination settings.
func (p *PostgresDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.Options["table"]) == "" {
		return errors.New("postgres destination: table is required")
	}
	strategyName := strings.ToLower(strings.TrimSpace(cfg.Strategy))
	if strategyName == "" {
		strategyName = "merge"
		cfg.Strategy = strategyName
	}
	if (strategyName == "merge" || strategyName == "delete+insert" || strategyName == "scd2") && strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("postgres destination: match_on is required")
	}

	db, err := sql.Open("pgx", cfg.Connection)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	poolCfg, err := pgxpool.ParseConfig(cfg.Connection)
	if err != nil {
		_ = db.Close()
		return err
	}
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 1
	poolCfg.MaxConnLifetime = 5 * time.Minute
	poolCfg.MaxConnIdleTime = 1 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		_ = db.Close()
		return err
	}

	if err := db.PingContext(ctx); err != nil {
		pool.Close()
		_ = db.Close()
		return err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		_ = db.Close()
		return err
	}

	p.cfg = cfg
	p.db = db
	p.pool = pool
	if p.query == nil {
		p.query = make(map[string]string)
	}
	p.acquireSession = func(ctx context.Context) (copySession, error) {
		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		return &pgxPoolSession{conn: conn}, nil
	}

	if strategyName == "scd2" {
		// History columns are added idempotently; pre-existing rows become
		// the current version as of now.
		target := quoteTableName(cfg.Options["table"])
		for _, stmt := range []string{
			`ALTER TABLE ` + target + ` ADD COLUMN IF NOT EXISTS "_scd_valid_from" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
			`ALTER TABLE ` + target + ` ADD COLUMN IF NOT EXISTS "_scd_valid_to" TIMESTAMPTZ`,
			`ALTER TABLE ` + target + ` ADD COLUMN IF NOT EXISTS "_scd_is_current" BOOLEAN NOT NULL DEFAULT TRUE`,
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				pool.Close()
				_ = db.Close()
				return fmt.Errorf("postgres destination: scd2 columns: %w", err)
			}
		}
	}
	return nil
}

// Load writes a batch of rows with per-row idempotency and upsert semantics.
func (p *PostgresDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if p.db == nil && p.pool == nil {
		return result, errors.New("postgres destination: not connected")
	}
	if len(rows) == 0 {
		return result, nil
	}

	stratName := p.strategyName(ctx)
	threshold := p.copyThreshold()

	switch stratName {
	case "append":
		if len(rows) >= threshold {
			return p.copyBatch(ctx, rows, store, pipeline, destination)
		}
		return p.insertBatch(ctx, rows, store, pipeline, destination)
	case "replace":
		if runIDFromContext(ctx) != 0 {
			// Engine-managed run: write to a staging table and swap
			// atomically in FinalizeRun so a failed run never leaves the
			// target truncated or partial.
			return p.replaceStagingLoad(ctx, rows)
		}
		if len(rows) >= threshold {
			return p.copyBatch(ctx, rows, store, pipeline, destination)
		}
		return p.insertBatch(ctx, rows, store, pipeline, destination)
	case "scd2":
		return p.scd2Batch(ctx, rows, store, pipeline, destination)
	case "merge":
		return p.insertBatch(ctx, rows, store, pipeline, destination)
	default:
		return p.insertBatch(ctx, rows, store, pipeline, destination)
	}
}

// scd2Batch applies slowly-changing-dimension type 2 semantics per row:
// when the incoming version differs from the current one, the current row is
// closed (_scd_valid_to, _scd_is_current=false) and a new current version is
// inserted; unchanged rows are left alone. Both statements run in one
// transaction per row.
func (p *PostgresDestination) scd2Batch(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	matchOn := splitMatchOn(p.cfg.MatchOn)
	table := strings.TrimSpace(p.cfg.Options["table"])

	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destination)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}

		cols := make([]string, 0, len(rw.Data))
		for c := range rw.Data {
			if strings.HasPrefix(c, "_scd_") {
				continue
			}
			cols = append(cols, c)
		}
		sort.Strings(cols)
		if len(cols) == 0 {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: errors.New("postgres destination: row has no columns")})
			continue
		}

		closeQ, closeArgs := buildSCD2Close(table, cols, matchOn, rw.Data)
		insertQ, insertArgs := buildSCD2Insert(table, cols, matchOn, rw.Data)

		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, closeQ, closeArgs...); err != nil {
			_ = tx.Rollback()
			if isFatalDBError(err) {
				return result, err
			}
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if _, err := tx.ExecContext(ctx, insertQ, insertArgs...); err != nil {
			_ = tx.Rollback()
			if isFatalDBError(err) {
				return result, err
			}
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if err := tx.Commit(); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}

		if err := store.MarkDelivered(ctx, rw.ID, pipeline, destination); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		result.Loaded++
	}
	return result, nil
}

func splitMatchOn(matchOn string) []string {
	var keys []string
	for _, k := range strings.Split(matchOn, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

// buildSCD2Close closes the current version when any non-key column differs.
func buildSCD2Close(table string, cols, matchOn []string, data map[string]interface{}) (string, []any) {
	matchSet := make(map[string]bool, len(matchOn))
	for _, k := range matchOn {
		matchSet[k] = true
	}
	var sb strings.Builder
	args := make([]any, 0, len(cols))
	sb.WriteString(`UPDATE ` + quoteTableName(table) + ` SET "_scd_valid_to" = NOW(), "_scd_is_current" = FALSE WHERE "_scd_is_current"`)
	ph := 1
	for _, k := range matchOn {
		fmt.Fprintf(&sb, ` AND %s = $%d`, quoteIdentifier(k), ph)
		args = append(args, data[k])
		ph++
	}
	var diffs []string
	for _, c := range cols {
		if matchSet[c] {
			continue
		}
		diffs = append(diffs, fmt.Sprintf(`%s IS DISTINCT FROM $%d`, quoteIdentifier(c), ph))
		args = append(args, data[c])
		ph++
	}
	if len(diffs) > 0 {
		sb.WriteString(" AND (" + strings.Join(diffs, " OR ") + ")")
	}
	return sb.String(), args
}

// buildSCD2Insert inserts a new current version when no current row exists
// for the key (either just closed, or never seen).
func buildSCD2Insert(table string, cols, matchOn []string, data map[string]interface{}) (string, []any) {
	quoted := make([]string, 0, len(cols)+2)
	placeholders := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols)+len(matchOn))
	ph := 1
	for _, c := range cols {
		quoted = append(quoted, quoteIdentifier(c))
		placeholders = append(placeholders, fmt.Sprintf("$%d", ph))
		args = append(args, data[c])
		ph++
	}
	quoted = append(quoted, `"_scd_valid_from"`, `"_scd_is_current"`)

	var sb strings.Builder
	sb.WriteString(`INSERT INTO ` + quoteTableName(table) + ` (` + strings.Join(quoted, ", ") + `) SELECT ` +
		strings.Join(placeholders, ", ") + `, NOW(), TRUE WHERE NOT EXISTS (SELECT 1 FROM ` +
		quoteTableName(table) + ` WHERE "_scd_is_current"`)
	for _, k := range matchOn {
		fmt.Fprintf(&sb, ` AND %s = $%d`, quoteIdentifier(k), ph)
		args = append(args, data[k])
		ph++
	}
	sb.WriteString(`)`)
	return sb.String(), args
}

// copyLoad uses Postgres COPY protocol for bulk inserts.
func (p *PostgresDestination) copyLoad(ctx context.Context, rows []row.Row, table string, cols []string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if len(cols) == 0 {
		return 0, errors.New("postgres destination: row has no columns")
	}
	if p.acquireSession == nil {
		return 0, errors.New("postgres destination: copy session unavailable")
	}

	session, err := p.acquireSession(ctx)
	if err != nil {
		return 0, err
	}
	defer session.Release()

	schemaName, tableName := splitTableName(table)
	values := make([][]any, len(rows))
	for i, rw := range rows {
		rowVals := make([]any, len(cols))
		for j, col := range cols {
			rowVals[j] = rw.Data[col]
		}
		values[i] = rowVals
	}

	n, err := session.CopyFrom(ctx, pgx.Identifier{schemaName, tableName}, cols, pgx.CopyFromRows(values))
	return int(n), err
}

// copyBatch loads rows with COPY and falls back to INSERT on error.
func (p *PostgresDestination) copyBatch(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := p.maybeTruncateReplace(ctx); err != nil {
		return result, err
	}

	cols := sortedRowColumns(rows)
	n, err := p.copyLoad(ctx, rows, p.cfg.Options["table"], cols)
	if err == nil {
		result.Loaded = n
		return result, nil
	}

	return p.insertBatch(ctx, rows, store, pipeline, destination)
}

// insertBatch writes rows using INSERT ... ON CONFLICT. Rows sharing the same
// column set are written in multi-row chunks (one statement per chunk) with a
// per-row fallback when a chunk fails.
func (p *PostgresDestination) insertBatch(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, nil
	}
	if err := p.maybeTruncateReplace(ctx); err != nil {
		return result, err
	}
	stratName := p.strategyName(ctx)
	skipDeliveryCheck := stratName == "append" || stratName == "replace"

	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !skipDeliveryCheck {
			delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destination)
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

	// Multi-column match keys are only supported by the per-row path.
	matchOn := strings.TrimSpace(p.cfg.MatchOn)
	if len(pending) > 1 && !strings.Contains(matchOn, ",") {
		// Sort by match key so ON CONFLICT index probes walk the B-tree in
		// order (better page locality than random-order upserts).
		sortRowsByKey(pending, matchOn)
		if err := p.chunkedWrite(ctx, pending, stratName, store, pipeline, destination, &result); err != nil {
			return result, err
		}
		return result, nil
	}

	for _, rw := range pending {
		if err := p.writeSingleRow(ctx, rw, stratName, store, pipeline, destination, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// writeSingleRow writes one row and records the outcome in result.
// A non-nil return means a fatal error that should abort the batch.
func (p *PostgresDestination) writeSingleRow(ctx context.Context, rw row.Row, stratName string, store state.StateStore, pipeline, destination string, result *LoadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	query, args, err := p.writeQuery(rw, stratName)
	if err != nil {
		result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
		return nil
	}
	if _, err := p.db.ExecContext(ctx, query, args...); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ctx.Err()
		}
		if isFatalDBError(err) {
			return err
		}
		result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
		return nil
	}
	if err := store.MarkDelivered(ctx, rw.ID, pipeline, destination); err != nil {
		result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
		return nil
	}
	result.Loaded++
	return nil
}

// chunkedWrite groups pending rows by column signature and writes each group
// in multi-row statements. Chunks are split when a match-on value repeats
// (ON CONFLICT DO UPDATE cannot touch the same row twice in one statement)
// or when the Postgres parameter limit would be exceeded.
func (p *PostgresDestination) chunkedWrite(ctx context.Context, pending []row.Row, stratName string, store state.StateStore, pipeline, destination string, result *LoadResult) error {
	matchOn := strings.TrimSpace(p.cfg.MatchOn)
	insertOnly := (stratName == "append" || stratName == "replace") && matchOn == ""

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

	for _, sig := range order {
		g := groups[sig]
		if len(g.cols) == 0 {
			for _, rw := range g.rows {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: errors.New("postgres destination: row has no columns")})
			}
			continue
		}
		maxChunk := 65000 / len(g.cols)
		if maxChunk > 1000 {
			maxChunk = 1000
		}
		if maxChunk < 1 {
			maxChunk = 1
		}

		var chunk []row.Row
		seen := make(map[string]bool)
		flush := func() error {
			if len(chunk) == 0 {
				return nil
			}
			batch := chunk
			chunk = nil
			seen = make(map[string]bool)
			if err := ctx.Err(); err != nil {
				return err
			}
			query, args := buildMultiWriteSQL(p.cfg.Options["table"], matchOn, g.cols, batch, insertOnly)
			if _, err := p.db.ExecContext(ctx, query, args...); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return ctx.Err()
				}
				if isFatalDBError(err) {
					return err
				}
				// Chunk failed: retry rows one at a time to isolate bad rows.
				for _, rw := range batch {
					if err := p.writeSingleRow(ctx, rw, stratName, store, pipeline, destination, result); err != nil {
						return err
					}
				}
				return nil
			}
			for _, rw := range batch {
				if err := store.MarkDelivered(ctx, rw.ID, pipeline, destination); err != nil {
					result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
					continue
				}
				result.Loaded++
			}
			return nil
		}

		for _, rw := range g.rows {
			if !insertOnly && matchOn != "" {
				key := fmt.Sprintf("%v", rw.Data[matchOn])
				if seen[key] {
					if err := flush(); err != nil {
						return err
					}
				}
				seen[key] = true
			}
			chunk = append(chunk, rw)
			if len(chunk) >= maxChunk {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}

// sortRowsByKey orders rows by the match-key value with type-aware
// comparison (numeric before lexical fallback).
func sortRowsByKey(rows []row.Row, key string) {
	sort.SliceStable(rows, func(i, j int) bool {
		return lessValue(rows[i].Data[key], rows[j].Data[key])
	})
}

func lessValue(a, b any) bool {
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			return av < bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av < bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av < bv
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Before(bv)
		}
	}
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// buildMultiWriteSQL builds a multi-row INSERT, optionally with an
// ON CONFLICT upsert clause, plus its flattened argument list.
func buildMultiWriteSQL(table, matchOn string, cols []string, batch []row.Row, insertOnly bool) (string, []any) {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quoteIdentifier(c)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteTableName(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(quotedCols, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(batch)*len(cols))
	ph := 1
	for i, rw := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, c := range cols {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("$")
			sb.WriteString(strconv.Itoa(ph))
			ph++
			args = append(args, rw.Data[c])
		}
		sb.WriteString(")")
	}

	if !insertOnly && matchOn != "" {
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(quoteIdentifier(matchOn))
		sb.WriteString(") ")
		var assignments []string
		for _, c := range cols {
			if c == matchOn {
				continue
			}
			q := quoteIdentifier(c)
			assignments = append(assignments, q+" = EXCLUDED."+q)
		}
		if len(assignments) == 0 {
			sb.WriteString("DO NOTHING")
		} else {
			sb.WriteString("DO UPDATE SET ")
			sb.WriteString(strings.Join(assignments, ", "))
		}
	}
	return sb.String(), args
}

func (p *PostgresDestination) writeQuery(rw row.Row, strategyName string) (string, []any, error) {
	if strategyName == "append" || strategyName == "replace" {
		if strings.TrimSpace(p.cfg.MatchOn) == "" {
			return p.insertOnlyQuery(rw)
		}
	}
	return p.upsertQuery(rw)
}

// ensureReplaceStaging returns (creating if needed) the run's staging table.
func (p *PostgresDestination) ensureReplaceStaging(ctx context.Context, runID int64) (string, error) {
	p.mu.RLock()
	staging, ok := p.replaceStaging[runID]
	p.mu.RUnlock()
	if ok {
		return staging, nil
	}
	target := strings.TrimSpace(p.cfg.Options["table"])
	staging = fmt.Sprintf("%s_vstg_%d", strings.ReplaceAll(target, ".", "_"), runID)
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS)",
		quoteTableName(staging), quoteTableName(target))
	if _, err := p.db.ExecContext(ctx, query); err != nil {
		return "", err
	}
	p.mu.Lock()
	if p.replaceStaging == nil {
		p.replaceStaging = make(map[int64]string)
	}
	p.replaceStaging[runID] = staging
	p.mu.Unlock()
	return staging, nil
}

// replaceStagingLoad writes replace-strategy rows into the run's staging table.
func (p *PostgresDestination) replaceStagingLoad(ctx context.Context, rows []row.Row) (LoadResult, error) {
	var result LoadResult
	runID := runIDFromContext(ctx)
	staging, err := p.ensureReplaceStaging(ctx, runID)
	if err != nil {
		return result, fmt.Errorf("postgres destination: staging table: %w", err)
	}
	cols := sortedRowColumns(rows)
	if len(rows) >= p.copyThreshold() {
		n, err := p.copyLoad(ctx, rows, staging, cols)
		if err == nil {
			result.Loaded = n
			return result, nil
		}
		// fall through to multi-row insert on COPY failure
	}
	query, args := buildMultiWriteSQL(staging, "", cols, rows, true)
	if _, err := p.db.ExecContext(ctx, query, args...); err != nil {
		for _, rw := range rows {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
		}
		return result, nil
	}
	result.Loaded = len(rows)
	return result, nil
}

// FinalizeRun completes a replace run: on success the staging table is
// swapped into the target in one transaction (truncate + insert-select);
// on failure the staging table is dropped and the target is untouched.
func (p *PostgresDestination) FinalizeRun(ctx context.Context, runID int64, succeeded bool) error {
	p.mu.Lock()
	staging, ok := p.replaceStaging[runID]
	delete(p.replaceStaging, runID)
	delete(p.replaceTruncated, runID)
	p.mu.Unlock()
	if !ok {
		return nil
	}
	dropStaging := func() {
		_, _ = p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteTableName(staging))
	}
	if !succeeded {
		dropStaging()
		return nil
	}

	target := strings.TrimSpace(p.cfg.Options["table"])
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		dropStaging()
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "TRUNCATE TABLE "+quoteTableName(target)); err != nil {
		dropStaging()
		return fmt.Errorf("postgres destination: replace swap truncate: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+quoteTableName(target)+" SELECT * FROM "+quoteTableName(staging)); err != nil {
		dropStaging()
		return fmt.Errorf("postgres destination: replace swap insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		dropStaging()
		return err
	}
	dropStaging()
	return nil
}

// createStagingTable creates a temporary staging table name for bulk merge loads.
func (p *PostgresDestination) createStagingTable(ctx context.Context, table string, cols []string) (string, error) {
	staging := table + "_vortara_staging_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	query := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS)", quoteIdentifier(staging), quoteTableName(table))
	if _, err := p.db.ExecContext(ctx, query); err != nil {
		return "", err
	}
	return staging, nil
}

// mergeStagingToTarget merges a staging table back into the target table.
func (p *PostgresDestination) mergeStagingToTarget(ctx context.Context, staging, target string, cols []string, matchOn string) error {
	if len(cols) == 0 {
		return errors.New("postgres destination: no columns to merge")
	}

	quotedCols := make([]string, len(cols))
	selectCols := make([]string, len(cols))
	assignments := make([]string, 0, len(cols))
	for i, col := range cols {
		quoted := quoteIdentifier(col)
		quotedCols[i] = quoted
		selectCols[i] = quoted
		if col != matchOn {
			assignments = append(assignments, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
		}
	}

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteTableName(target))
	b.WriteString(" (")
	b.WriteString(strings.Join(quotedCols, ", "))
	b.WriteString(") SELECT ")
	b.WriteString(strings.Join(selectCols, ", "))
	b.WriteString(" FROM ")
	b.WriteString(quoteIdentifier(staging))
	b.WriteString(" ON CONFLICT (")
	b.WriteString(quoteIdentifier(matchOn))
	b.WriteString(") ")
	if len(assignments) == 0 {
		b.WriteString("DO NOTHING")
	} else {
		b.WriteString("DO UPDATE SET ")
		b.WriteString(strings.Join(assignments, ", "))
	}

	_, err := p.db.ExecContext(ctx, b.String())
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdentifier(staging))
	return err
}

// Close releases all resources held by the destination.
func (p *PostgresDestination) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgresDestination) upsertQuery(rw row.Row) (string, []any, error) {
	table := strings.TrimSpace(p.cfg.Options["table"])
	matchOn := strings.TrimSpace(p.cfg.MatchOn)

	cols := make([]string, 0, len(rw.Data))
	for col := range rw.Data {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	if len(cols) == 0 {
		return "", nil, errors.New("postgres destination: row has no columns")
	}

	foundMatch := false
	for _, col := range cols {
		if col == matchOn {
			foundMatch = true
			break
		}
	}
	if !foundMatch {
		return "", nil, fmt.Errorf("postgres destination: match_on column %q missing from row", matchOn)
	}

	key := strings.Join(cols, "\x00")
	p.mu.RLock()
	query, ok := p.query[key]
	p.mu.RUnlock()
	if !ok {
		query = buildUpsertSQL(table, matchOn, cols)
		p.mu.Lock()
		p.query[key] = query
		p.mu.Unlock()
	}

	args := make([]any, 0, len(cols))
	for _, col := range cols {
		args = append(args, rw.Data[col])
	}
	return query, args, nil
}

func (p *PostgresDestination) insertOnlyQuery(rw row.Row) (string, []any, error) {
	table := strings.TrimSpace(p.cfg.Options["table"])
	cols := make([]string, 0, len(rw.Data))
	for col := range rw.Data {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	if len(cols) == 0 {
		return "", nil, errors.New("postgres destination: row has no columns")
	}

	quotedCols := make([]string, 0, len(cols))
	vals := make([]any, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
		vals = append(vals, rw.Data[col])
	}

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	query, args, err := psql.Insert(quoteTableName(table)).Columns(quotedCols...).Values(vals...).ToSql()
	if err != nil {
		return "", nil, err
	}
	return query, args, nil
}

func buildUpsertSQL(table, matchOn string, cols []string) string {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, quoteIdentifier(col))
	}
	insertBuilder := psql.Insert(quoteTableName(table)).Columns(quotedCols...)
	vals := make([]any, len(cols))
	insertBuilder = insertBuilder.Values(vals...)

	assignments := make([]string, 0, len(cols))
	for _, col := range cols {
		if col == matchOn {
			continue
		}
		quoted := quoteIdentifier(col)
		assignments = append(assignments, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
	}
	suffix := "ON CONFLICT (" + quoteIdentifier(matchOn) + ") "
	if len(assignments) == 0 {
		suffix += "DO NOTHING"
	} else {
		suffix += "DO UPDATE SET " + strings.Join(assignments, ", ")
	}
	sql, _, err := insertBuilder.Suffix(suffix).ToSql()
	if err != nil {
		return ""
	}
	return sql
}

// quoteIdentifier safely quotes a Postgres identifier.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteTableName quotes schema.table or just table.
func quoteTableName(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return quoteIdentifier(parts[0]) + "." + quoteIdentifier(parts[1])
	}
	return `"public".` + quoteIdentifier(table)
}

func splitTableName(table string) (string, string) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", table
}

func sortedRowColumns(rows []row.Row) []string {
	if len(rows) == 0 {
		return nil
	}
	cols := make([]string, 0, len(rows[0].Data))
	for col := range rows[0].Data {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

func strategyNameFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value("vortara_strategy").(string); ok {
		return v
	}
	return ""
}

// strategyName resolves the effective strategy: the context value (set by the
// engine per destination) wins; direct API callers fall back to the config.
func (p *PostgresDestination) strategyName(ctx context.Context) string {
	if s := strings.ToLower(strings.TrimSpace(strategyNameFromContext(ctx))); s != "" {
		return s
	}
	return strings.ToLower(strings.TrimSpace(p.cfg.Strategy))
}

func runIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx.Value("vortara_run_id").(int64); ok {
		return v
	}
	return 0
}

func (p *PostgresDestination) maybeTruncateReplace(ctx context.Context) error {
	if p.strategyName(ctx) != "replace" {
		return nil
	}

	runID := runIDFromContext(ctx)
	if runID != 0 {
		p.mu.RLock()
		seen := p.replaceTruncated != nil && p.replaceTruncated[runID]
		p.mu.RUnlock()
		if seen {
			return nil
		}
	}

	if _, err := p.db.ExecContext(ctx, "TRUNCATE TABLE "+quoteTableName(p.cfg.Options["table"])); err != nil {
		return err
	}

	if runID != 0 {
		p.mu.Lock()
		if p.replaceTruncated == nil {
			p.replaceTruncated = make(map[int64]bool)
		}
		p.replaceTruncated[runID] = true
		p.mu.Unlock()
	}
	return nil
}

func (p *PostgresDestination) copyThreshold() int {
	if p.cfg.Options != nil {
		if raw := strings.TrimSpace(p.cfg.Options["copy_threshold"]); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil && n > 0 {
				return n
			}
		}
	}
	return 100
}

func isFatalDBError(err error) bool {
	return errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn)
}
