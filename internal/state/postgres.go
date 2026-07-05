package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/rkshvish/vortara/pkg/config"
)

// PostgresStore is a StateStore backed by PostgreSQL — for deployments where
// multiple Vortara instances (or instances without durable local disk) need
// shared state. Tables are created on connect, named <prefix>_watermarks,
// <prefix>_numeric_watermarks, <prefix>_kafka_offsets, <prefix>_run_log, and
// <prefix>_delivery_log (prefix from settings.state.key_prefix, default
// "vortara").
type PostgresStore struct {
	db     *sql.DB
	prefix string

	batchMu      sync.Mutex
	batchPending []deliveryEntry
	batchSeen    map[string]struct{}
	inBatch      bool
}

var _ StateStore = (*PostgresStore)(nil)

func init() {
	Register("postgres", func(cfg config.StateConfig) (StateStore, error) {
		return NewPostgresStore(cfg.Connection, cfg.KeyPrefix)
	})
}

var validPrefix = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// NewPostgresStore connects and ensures the state schema exists.
func NewPostgresStore(dsn, prefix string) (*PostgresStore, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, errors.New("postgres state backend: settings.state.connection is required")
	}
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	if prefix == "" {
		prefix = "vortara"
	}
	if !validPrefix.MatchString(prefix) {
		return nil, fmt.Errorf("postgres state backend: invalid key_prefix %q (lowercase letters, digits, underscores)", prefix)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres state backend: ping: %w", err)
	}

	s := &PostgresStore{db: db, prefix: prefix}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres state backend: schema: %w", err)
	}
	return s, nil
}

func (s *PostgresStore) table(name string) string {
	return s.prefix + "_" + name
}

func (s *PostgresStore) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + s.table("watermarks") + ` (
			pipeline   TEXT NOT NULL,
			source     TEXT NOT NULL,
			watermark  TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (pipeline, source)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.table("numeric_watermarks") + ` (
			pipeline   TEXT NOT NULL,
			source     TEXT NOT NULL,
			watermark  BIGINT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (pipeline, source)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.table("kafka_offsets") + ` (
			pipeline   TEXT NOT NULL,
			topic      TEXT NOT NULL,
			partition  INTEGER NOT NULL,
			commit_offset BIGINT NOT NULL,
			PRIMARY KEY (pipeline, topic, partition)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.table("run_log") + ` (
			id             BIGSERIAL PRIMARY KEY,
			pipeline       TEXT NOT NULL,
			mode           TEXT NOT NULL,
			started_at     TIMESTAMPTZ NOT NULL,
			finished_at    TIMESTAMPTZ,
			rows_extracted INTEGER DEFAULT 0,
			rows_loaded    INTEGER DEFAULT 0,
			rows_skipped   INTEGER DEFAULT 0,
			rows_errored   INTEGER DEFAULT 0,
			status         TEXT NOT NULL DEFAULT 'running',
			error          TEXT DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS ` + s.table("run_log_pipeline_idx") + ` ON ` + s.table("run_log") + ` (pipeline, id DESC)`,
		`CREATE TABLE IF NOT EXISTS ` + s.table("delivery_log") + ` (
			row_id       TEXT NOT NULL,
			pipeline     TEXT NOT NULL,
			destination  TEXT NOT NULL,
			delivered_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (row_id, pipeline, destination)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// GetWatermark returns the last processed watermark for a pipeline+source.
func (s *PostgresStore) GetWatermark(ctx context.Context, pipeline, source string) (time.Time, error) {
	var wm time.Time
	err := s.db.QueryRowContext(ctx, 
		`SELECT watermark FROM `+s.table("watermarks")+` WHERE pipeline=$1 AND source=$2`,
		pipeline, source,
	).Scan(&wm)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return wm.UTC(), nil
}

// SetWatermark saves the watermark for a pipeline+source.
func (s *PostgresStore) SetWatermark(ctx context.Context, pipeline, source string, wm time.Time) error {
	_, err := s.db.ExecContext(ctx, 
		`INSERT INTO `+s.table("watermarks")+` (pipeline, source, watermark, updated_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (pipeline, source) DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()`,
		pipeline, source, wm.UTC(),
	)
	return err
}

// GetNumericWatermark returns the last integer cursor for a pipeline+source.
func (s *PostgresStore) GetNumericWatermark(ctx context.Context, pipeline, source string) (int64, error) {
	var wm int64
	err := s.db.QueryRowContext(ctx, 
		`SELECT watermark FROM `+s.table("numeric_watermarks")+` WHERE pipeline=$1 AND source=$2`,
		pipeline, source,
	).Scan(&wm)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return wm, err
}

// SetNumericWatermark saves the integer cursor for a pipeline+source.
func (s *PostgresStore) SetNumericWatermark(ctx context.Context, pipeline, source string, wm int64) error {
	_, err := s.db.ExecContext(ctx, 
		`INSERT INTO `+s.table("numeric_watermarks")+` (pipeline, source, watermark, updated_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (pipeline, source) DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()`,
		pipeline, source, wm,
	)
	return err
}

// GetOffset returns the last committed offset, or -1 when unset.
func (s *PostgresStore) GetOffset(ctx context.Context, pipeline, topic string, partition int) (int64, error) {
	var offset int64
	err := s.db.QueryRowContext(ctx, 
		`SELECT commit_offset FROM `+s.table("kafka_offsets")+` WHERE pipeline=$1 AND topic=$2 AND partition=$3`,
		pipeline, topic, partition,
	).Scan(&offset)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	return offset, nil
}

// SetOffset saves the committed offset for a topic+partition.
func (s *PostgresStore) SetOffset(ctx context.Context, pipeline, topic string, partition int, offset int64) error {
	_, err := s.db.ExecContext(ctx, 
		`INSERT INTO `+s.table("kafka_offsets")+` (pipeline, topic, partition, commit_offset)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (pipeline, topic, partition) DO UPDATE SET commit_offset = EXCLUDED.commit_offset`,
		pipeline, topic, partition, offset,
	)
	return err
}

// StartRun creates a new run log entry and returns its ID.
func (s *PostgresStore) StartRun(ctx context.Context, pipeline, mode string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, 
		`INSERT INTO `+s.table("run_log")+` (pipeline, mode, started_at, status)
		 VALUES ($1, $2, NOW(), 'running') RETURNING id`,
		pipeline, mode,
	).Scan(&id)
	return id, err
}

// FinishRun updates the run log entry with final stats.
func (s *PostgresStore) FinishRun(ctx context.Context, runID int64, stats RunStats) error {
	_, err := s.db.ExecContext(ctx, 
		`UPDATE `+s.table("run_log")+`
		 SET finished_at=NOW(), rows_extracted=$1, rows_loaded=$2, rows_skipped=$3,
		     rows_errored=$4, status=$5, error=$6
		 WHERE id=$7`,
		stats.RowsExtracted, stats.RowsLoaded, stats.RowsSkipped,
		stats.RowsErrored, stats.Status, stats.Error, runID,
	)
	return err
}

// GetLastRun returns the most recent run log entry for a pipeline.
func (s *PostgresStore) GetLastRun(ctx context.Context, pipeline string) (RunLog, error) {
	runs, err := s.GetRunHistory(ctx, pipeline, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(runs) == 0 {
		return RunLog{}, fmt.Errorf("no runs found for pipeline %q", pipeline)
	}
	return runs[0], nil
}

// GetRunHistory returns the most recent run log entries for a pipeline.
func (s *PostgresStore) GetRunHistory(ctx context.Context, pipeline string, limit int) ([]RunLog, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, 
		`SELECT id, pipeline, mode, started_at, COALESCE(finished_at, 'epoch'::timestamptz),
		        rows_extracted, rows_loaded, rows_skipped, rows_errored, status, error
		 FROM `+s.table("run_log")+` WHERE pipeline=$1 ORDER BY id DESC LIMIT $2`,
		pipeline, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []RunLog
	for rows.Next() {
		var l RunLog
		if err := rows.Scan(&l.ID, &l.Pipeline, &l.Mode, &l.StartedAt, &l.FinishedAt,
			&l.RowsExtracted, &l.RowsLoaded, &l.RowsSkipped, &l.RowsErrored, &l.Status, &l.Error); err != nil {
			return nil, err
		}
		l.StartedAt = l.StartedAt.UTC()
		if l.FinishedAt.Unix() == 0 {
			l.FinishedAt = time.Time{}
		} else {
			l.FinishedAt = l.FinishedAt.UTC()
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

// IsDelivered reports whether a row was already delivered.
func (s *PostgresStore) IsDelivered(ctx context.Context, rowID, pipeline, destination string) (bool, error) {
	s.batchMu.Lock()
	if s.inBatch {
		if _, seen := s.batchSeen[deliveryKey(rowID, pipeline, destination)]; seen {
			s.batchMu.Unlock()
			return true, nil
		}
	}
	s.batchMu.Unlock()

	var one int
	err := s.db.QueryRowContext(ctx, 
		`SELECT 1 FROM `+s.table("delivery_log")+` WHERE row_id=$1 AND pipeline=$2 AND destination=$3`,
		rowID, pipeline, destination,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// MarkDelivered records that a row was successfully delivered.
func (s *PostgresStore) MarkDelivered(ctx context.Context, rowID, pipeline, destination string) error {
	s.batchMu.Lock()
	if s.inBatch {
		s.batchPending = append(s.batchPending, deliveryEntry{
			RowID:       rowID,
			Pipeline:    pipeline,
			Destination: destination,
		})
		if s.batchSeen == nil {
			s.batchSeen = make(map[string]struct{})
		}
		s.batchSeen[deliveryKey(rowID, pipeline, destination)] = struct{}{}
		s.batchMu.Unlock()
		return nil
	}
	s.batchMu.Unlock()

	_, err := s.db.ExecContext(ctx, 
		`INSERT INTO `+s.table("delivery_log")+` (row_id, pipeline, destination)
		 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		rowID, pipeline, destination,
	)
	return err
}

// PruneDelivered deletes delivery-log entries older than the cutoff.
func (s *PostgresStore) PruneDelivered(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, 
		`DELETE FROM `+s.table("delivery_log")+` WHERE delivered_at < $1`,
		olderThan.UTC(),
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// BeginBatch starts buffering delivery writes in memory.
func (s *PostgresStore) BeginBatch(ctx context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = make(map[string]struct{})
	s.inBatch = true
	return nil
}

// CommitBatch flushes buffered delivery writes in chunked multi-row inserts
// inside one transaction.
func (s *PostgresStore) CommitBatch(ctx context.Context) error {
	s.batchMu.Lock()
	pending := append([]deliveryEntry(nil), s.batchPending...)
	s.batchPending = nil
	s.batchSeen = nil
	s.inBatch = false
	s.batchMu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const chunk = 1000
	now := time.Now().UTC()
	for start := 0; start < len(pending); start += chunk {
		end := start + chunk
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[start:end]
		var sb strings.Builder
		sb.WriteString(`INSERT INTO ` + s.table("delivery_log") + ` (row_id, pipeline, destination, delivered_at) VALUES `)
		args := make([]any, 0, len(batch)*4)
		for i, e := range batch {
			if i > 0 {
				sb.WriteString(", ")
			}
			base := i * 4
			fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4)
			args = append(args, e.RowID, e.Pipeline, e.Destination, now)
		}
		sb.WriteString(" ON CONFLICT DO NOTHING")
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("batch delivery write: %w", err)
		}
	}
	return tx.Commit()
}

// RollbackBatch discards buffered delivery writes.
func (s *PostgresStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = nil
	s.batchSeen = nil
	s.inBatch = false
	return nil
}

// Close releases the database handle.
func (s *PostgresStore) Close() error {
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		return err
	}
	return nil
}
