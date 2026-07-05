// Package state defines the storage contract used by Vortara to persist
// batch watermarks, streaming offsets, run history, and delivery idempotency.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rkshvish/vortaraos/pkg/config"
)

func init() {
	Register("sqlite", func(cfg config.StateConfig) (StateStore, error) {
		return NewSQLiteStore(cfg.Path)
	})
}

const (
	journalModeWAL  = "PRAGMA journal_mode = WAL;"
	synchronousNorm = "PRAGMA synchronous = NORMAL;"
	foreignKeysOn   = "PRAGMA foreign_keys = ON;"
	busyTimeout     = "PRAGMA busy_timeout = 5000;"
)

const (
	createWatermarksTable = `
CREATE TABLE IF NOT EXISTS watermarks (
    pipeline   TEXT NOT NULL,
    source     TEXT NOT NULL,
    watermark  DATETIME NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline, source)
);`
	createNumericWatermarksTable = `
CREATE TABLE IF NOT EXISTS numeric_watermarks (
    pipeline   TEXT NOT NULL,
    source     TEXT NOT NULL,
    watermark  BIGINT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline, source)
);`
	createKafkaOffsetsTable = `
CREATE TABLE IF NOT EXISTS kafka_offsets (
    pipeline   TEXT NOT NULL,
    topic      TEXT NOT NULL,
    partition  INTEGER NOT NULL,
    offset     BIGINT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline, topic, partition)
);`
	createRunLogTable = `
CREATE TABLE IF NOT EXISTS run_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline       TEXT NOT NULL,
    mode           TEXT NOT NULL,
    started_at     DATETIME NOT NULL,
    finished_at    DATETIME,
    rows_extracted INTEGER DEFAULT 0,
    rows_loaded    INTEGER DEFAULT 0,
    rows_skipped   INTEGER DEFAULT 0,
    rows_errored   INTEGER DEFAULT 0,
    status         TEXT,
    error          TEXT
);`
	createDeliveryLogTable = `
CREATE TABLE IF NOT EXISTS delivery_log (
    row_id       TEXT NOT NULL,
    pipeline     TEXT NOT NULL,
    destination  TEXT NOT NULL,
    delivered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (row_id, pipeline, destination)
);`
)

// SQLiteStore implements StateStore using a SQLite database.
type SQLiteStore struct {
	db           *sql.DB
	batchMu      sync.Mutex
	batchPending []deliveryEntry
	batchSeen    map[string]struct{}
	inBatch      bool
}

var _ StateStore = (*SQLiteStore)(nil)

type deliveryEntry struct {
	RowID       string
	Pipeline    string
	Destination string
}

// NewSQLiteStore opens or creates the SQLite database at path and initializes
// the required schema and pragmas.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		return nil, errors.New("state: empty sqlite path")
	}

	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		path = abs
	}
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return nil, err
	}

	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + "?parseTime=true&_loc=auto"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func ensureDir(dir string) error {
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state: create sqlite directory: %w", err)
	}
	return nil
}

func (s *SQLiteStore) init() error {
	pragmas := []string{journalModeWAL, synchronousNorm, foreignKeysOn, busyTimeout}
	for _, pragma := range pragmas {
		if _, err := s.db.Exec(pragma); err != nil {
			return fmt.Errorf("state: apply pragma %q: %w", pragma, err)
		}
	}

	stmts := []string{
		createWatermarksTable,
		createNumericWatermarksTable,
		createKafkaOffsetsTable,
		createRunLogTable,
		createDeliveryLogTable,
	}

	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("state: create schema: %w", err)
		}
	}

	return nil
}

// Close releases all resources held by the store.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// GetWatermark returns the last processed watermark for a pipeline and source.
func (s *SQLiteStore) GetWatermark(pipeline, source string) (time.Time, error) {
	row := s.db.QueryRow(`SELECT watermark FROM watermarks WHERE pipeline=? AND source=?`, pipeline, source)
	var wm time.Time
	if err := row.Scan(&wm); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	return wm, nil
}

// SetWatermark saves the watermark for a pipeline and source.
func (s *SQLiteStore) SetWatermark(pipeline, source string, wm time.Time) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO watermarks (pipeline, source, watermark) VALUES (?, ?, ?)`, pipeline, source, wm.UTC())
	return err
}

// GetNumericWatermark returns the last integer cursor for a pipeline+source.
func (s *SQLiteStore) GetNumericWatermark(pipeline, source string) (int64, error) {
	row := s.db.QueryRow(`SELECT watermark FROM numeric_watermarks WHERE pipeline=? AND source=?`, pipeline, source)
	var wm int64
	if err := row.Scan(&wm); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return wm, nil
}

// SetNumericWatermark saves the integer cursor for a pipeline+source.
func (s *SQLiteStore) SetNumericWatermark(pipeline, source string, wm int64) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO numeric_watermarks (pipeline, source, watermark) VALUES (?, ?, ?)`, pipeline, source, wm)
	return err
}

// GetOffset returns the last committed offset for a pipeline, topic, and partition.
func (s *SQLiteStore) GetOffset(pipeline, topic string, partition int) (int64, error) {
	row := s.db.QueryRow(`SELECT offset FROM kafka_offsets WHERE pipeline=? AND topic=? AND partition=?`, pipeline, topic, partition)
	var offset int64
	if err := row.Scan(&offset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return -1, nil
		}
		return -1, err
	}
	return offset, nil
}

// SetOffset saves the committed offset for a pipeline, topic, and partition.
func (s *SQLiteStore) SetOffset(pipeline, topic string, partition int, offset int64) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO kafka_offsets (pipeline, topic, partition, offset) VALUES (?, ?, ?, ?)`, pipeline, topic, partition, offset)
	return err
}

// StartRun creates a new run log entry and returns its ID.
func (s *SQLiteStore) StartRun(pipeline, mode string) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO run_log (pipeline, mode, started_at, status) VALUES (?, ?, ?, 'running')`, pipeline, mode, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishRun updates a run log entry with final statistics.
func (s *SQLiteStore) FinishRun(runID int64, stats RunStats) error {
	_, err := s.db.Exec(
		`UPDATE run_log SET finished_at=?, rows_extracted=?, rows_loaded=?, rows_skipped=?, rows_errored=?, status=?, error=? WHERE id=?`,
		time.Now().UTC(),
		stats.RowsExtracted,
		stats.RowsLoaded,
		stats.RowsSkipped,
		stats.RowsErrored,
		stats.Status,
		stats.Error,
		runID,
	)
	return err
}

// GetLastRun returns the most recent run log entry for a pipeline.
func (s *SQLiteStore) GetLastRun(pipeline string) (RunLog, error) {
	history, err := s.GetRunHistory(pipeline, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(history) == 0 {
		return RunLog{}, sql.ErrNoRows
	}
	return history[0], nil
}

// GetRunHistory returns the most recent run log entries for a pipeline.
func (s *SQLiteStore) GetRunHistory(pipeline string, limit int) ([]RunLog, error) {
	if limit <= 0 {
		return []RunLog{}, nil
	}

	rows, err := s.db.Query(
		`SELECT id, pipeline, mode, started_at, finished_at, rows_extracted, rows_loaded, rows_skipped, rows_errored, status, error
		 FROM run_log WHERE pipeline=? ORDER BY started_at DESC, id DESC LIMIT ?`,
		pipeline,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []RunLog
	for rows.Next() {
		log, err := scanRunLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}

// IsDelivered reports whether a row has already been delivered.
func (s *SQLiteStore) IsDelivered(rowID, pipeline, destination string) (bool, error) {
	s.batchMu.Lock()
	if s.inBatch {
		if _, seen := s.batchSeen[deliveryKey(rowID, pipeline, destination)]; seen {
			s.batchMu.Unlock()
			return true, nil
		}
	}
	s.batchMu.Unlock()

	row := s.db.QueryRow(`SELECT COUNT(*) FROM delivery_log WHERE row_id=? AND pipeline=? AND destination=?`, rowID, pipeline, destination)
	var count int
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// MarkDelivered records that a row was successfully delivered.
func (s *SQLiteStore) MarkDelivered(rowID, pipeline, destination string) error {
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

	_, err := s.db.Exec(`INSERT OR IGNORE INTO delivery_log (row_id, pipeline, destination) VALUES (?, ?, ?)`, rowID, pipeline, destination)
	return err
}

// BeginBatch starts buffering delivery writes in memory.
func (s *SQLiteStore) BeginBatch(ctx context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = make(map[string]struct{})
	s.inBatch = true
	return nil
}

// CommitBatch flushes buffered delivery writes atomically.
func (s *SQLiteStore) CommitBatch(ctx context.Context) error {
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

	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO delivery_log (row_id, pipeline, destination, delivered_at) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC()
	for _, e := range pending {
		if _, err := stmt.ExecContext(ctx, e.RowID, e.Pipeline, e.Destination, now); err != nil {
			return fmt.Errorf("batch delivery write: %w", err)
		}
	}
	return tx.Commit()
}

// PruneDelivered deletes delivery-log entries older than the cutoff.
func (s *SQLiteStore) PruneDelivered(olderThan time.Time) (int64, error) {
	// datetime() normalizes both CURRENT_TIMESTAMP and driver-written
	// time.Time formats before comparison.
	res, err := s.db.Exec(
		`DELETE FROM delivery_log WHERE datetime(delivered_at) < datetime(?)`,
		olderThan.UTC().Format("2006-01-02 15:04:05"),
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// RollbackBatch discards buffered delivery writes.
func (s *SQLiteStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = nil
	s.batchSeen = nil
	s.inBatch = false
	return nil
}

func scanRunLog(scanner interface {
	Scan(dest ...any) error
}) (RunLog, error) {
	var log RunLog
	var finishedAt sql.NullTime
	var status sql.NullString
	var runError sql.NullString
	if err := scanner.Scan(
		&log.ID,
		&log.Pipeline,
		&log.Mode,
		&log.StartedAt,
		&finishedAt,
		&log.RowsExtracted,
		&log.RowsLoaded,
		&log.RowsSkipped,
		&log.RowsErrored,
		&status,
		&runError,
	); err != nil {
		return RunLog{}, err
	}
	if finishedAt.Valid {
		log.FinishedAt = finishedAt.Time
	}
	if status.Valid {
		log.Status = status.String
	}
	if runError.Valid {
		log.Error = runError.String
	}
	return log, nil
}
