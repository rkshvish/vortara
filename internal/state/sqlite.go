package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func init() {
	Register("sqlite", func(cfg stateConfig) (StateStore, error) {
		return NewSQLiteStore(cfg.Path)
	})
}

// SQLiteStore is a StateStore backed by SQLite.
type SQLiteStore struct {
	db           *sql.DB
	batchMu      sync.Mutex
	batchPending []deliveryEntry
	batchSeen    map[string]struct{}
}

type deliveryEntry struct {
	rowID       string
	syncName    string
	destination string
	deliveredAt time.Time
}

// NewSQLiteStore opens (or creates) a SQLite state database at path.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		path = "./state/sync.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("state: mkdir %s: %w", filepath.Dir(path), err)
	}
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("state: open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db: db}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) initSchema() error {
	ctx := context.Background()
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, p := range pragmas {
		if _, err := s.db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("state: pragma: %w", err)
		}
	}
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS entity_state (
			sync_name            TEXT NOT NULL,
			destination          TEXT NOT NULL,
			entity_key           TEXT NOT NULL,
			destination_id       TEXT NOT NULL DEFAULT '',
			current_fingerprint  TEXT NOT NULL DEFAULT '',
			previous_fingerprint TEXT NOT NULL DEFAULT '',
			current_payload      TEXT NOT NULL DEFAULT '{}',
			previous_payload     TEXT NOT NULL DEFAULT '{}',
			remembered_state     TEXT NOT NULL DEFAULT '{}',
			last_decision        TEXT NOT NULL DEFAULT '',
			last_status          TEXT NOT NULL DEFAULT '',
			consecutive_missing  INTEGER NOT NULL DEFAULT 0,
			version              INTEGER NOT NULL DEFAULT 0,
			created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (sync_name, destination, entity_key)
		)`,
		`CREATE TABLE IF NOT EXISTS rule_firings (
			sync_name    TEXT NOT NULL,
			destination  TEXT NOT NULL,
			entity_key   TEXT NOT NULL,
			rule_name    TEXT NOT NULL,
			fired_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (sync_name, destination, entity_key, rule_name)
		)`,
		`CREATE TABLE IF NOT EXISTS decision_events (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			sync_name       TEXT NOT NULL,
			destination     TEXT NOT NULL,
			entity_key      TEXT NOT NULL,
			run_id          INTEGER NOT NULL DEFAULT 0,
			decision        TEXT NOT NULL,
			triggered_rules TEXT NOT NULL DEFAULT '[]',
			reasons         TEXT NOT NULL DEFAULT '[]',
			created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_decision_events_entity
			ON decision_events (sync_name, destination, entity_key)`,
		`CREATE TABLE IF NOT EXISTS run_log (
			id             INTEGER PRIMARY KEY AUTOINCREMENT,
			sync_name      TEXT NOT NULL,
			mode           TEXT NOT NULL DEFAULT 'batch',
			started_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			finished_at    DATETIME,
			rows_extracted INTEGER NOT NULL DEFAULT 0,
			rows_loaded    INTEGER NOT NULL DEFAULT 0,
			rows_skipped   INTEGER NOT NULL DEFAULT 0,
			rows_errored   INTEGER NOT NULL DEFAULT 0,
			high_watermark DATETIME,
			status         TEXT NOT NULL DEFAULT 'running',
			error          TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS delivery_log (
			row_id       TEXT NOT NULL,
			sync_name    TEXT NOT NULL,
			destination  TEXT NOT NULL,
			delivered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (row_id, sync_name, destination)
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_locks (
			sync_name    TEXT PRIMARY KEY,
			owner        TEXT NOT NULL,
			acquired_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			heartbeat_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at   DATETIME NOT NULL
		)`,
	}
	for _, stmt := range ddl {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("state: ddl: %w", err)
		}
	}
	// Idempotent column additions for databases created before this column existed.
	_, _ = s.db.ExecContext(ctx, `ALTER TABLE run_log ADD COLUMN high_watermark DATETIME`)
	return nil
}

// --- Entity state ---

func (s *SQLiteStore) GetEntityState(ctx context.Context, syncName, destination, entityKey string) (*EntityState, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT destination_id, current_fingerprint, previous_fingerprint,
		       current_payload, previous_payload, remembered_state,
		       last_decision, last_status, consecutive_missing,
		       version, created_at, updated_at
		FROM entity_state
		WHERE sync_name=? AND destination=? AND entity_key=?`,
		syncName, destination, entityKey)

	var (
		destID, curFP, prevFP            string
		curPayJSON, prevPayJSON, remJSON string
		lastDecision, lastStatus         string
		consMissing, version             int
		createdAt, updatedAt             time.Time
	)
	err := row.Scan(&destID, &curFP, &prevFP,
		&curPayJSON, &prevPayJSON, &remJSON,
		&lastDecision, &lastStatus, &consMissing,
		&version, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("state: get entity: %w", err)
	}
	var curPay, prevPay, remembered map[string]any
	_ = json.Unmarshal([]byte(curPayJSON), &curPay)
	_ = json.Unmarshal([]byte(prevPayJSON), &prevPay)
	_ = json.Unmarshal([]byte(remJSON), &remembered)
	return &EntityState{
		SyncName: syncName, Destination: destination, EntityKey: entityKey,
		DestinationID: destID, CurrentFingerprint: curFP, PreviousFingerprint: prevFP,
		CurrentPayload: curPay, PreviousPayload: prevPay, RememberedState: remembered,
		LastDecision: lastDecision, LastStatus: lastStatus,
		ConsecutiveMissing: consMissing, Version: version,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func (s *SQLiteStore) SaveEntityState(ctx context.Context, es *EntityState) error {
	curPay, _ := json.Marshal(es.CurrentPayload)
	prevPay, _ := json.Marshal(es.PreviousPayload)
	remembered, _ := json.Marshal(es.RememberedState)
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO entity_state
			(sync_name, destination, entity_key, destination_id,
			 current_fingerprint, previous_fingerprint,
			 current_payload, previous_payload, remembered_state,
			 last_decision, last_status, consecutive_missing, version,
			 created_at, updated_at)
		VALUES (?,?,?,?, ?,?, ?,?,?, ?,?,?,?, ?,?)
		ON CONFLICT(sync_name, destination, entity_key) DO UPDATE SET
			destination_id       = excluded.destination_id,
			current_fingerprint  = excluded.current_fingerprint,
			previous_fingerprint = excluded.previous_fingerprint,
			current_payload      = excluded.current_payload,
			previous_payload     = excluded.previous_payload,
			remembered_state     = excluded.remembered_state,
			last_decision        = excluded.last_decision,
			last_status          = excluded.last_status,
			consecutive_missing  = excluded.consecutive_missing,
			version              = excluded.version,
			updated_at           = excluded.updated_at`,
		es.SyncName, es.Destination, es.EntityKey, es.DestinationID,
		es.CurrentFingerprint, es.PreviousFingerprint,
		string(curPay), string(prevPay), string(remembered),
		es.LastDecision, es.LastStatus, es.ConsecutiveMissing, es.Version,
		now, now,
	)
	return err
}

func (s *SQLiteStore) ListEntityStates(ctx context.Context, syncName, destination string, limit, offset int) ([]*EntityState, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT entity_key, destination_id, current_fingerprint, previous_fingerprint,
		       current_payload, previous_payload, remembered_state,
		       last_decision, last_status, consecutive_missing,
		       version, created_at, updated_at
		FROM entity_state
		WHERE sync_name=? AND destination=?
		ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
		syncName, destination, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EntityState
	for rows.Next() {
		var (
			ek, destID, curFP, prevFP        string
			curPayJSON, prevPayJSON, remJSON string
			lastDecision, lastStatus         string
			consMissing, version             int
			createdAt, updatedAt             time.Time
		)
		if err := rows.Scan(&ek, &destID, &curFP, &prevFP,
			&curPayJSON, &prevPayJSON, &remJSON,
			&lastDecision, &lastStatus, &consMissing,
			&version, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		var curPay, prevPay, remembered map[string]any
		_ = json.Unmarshal([]byte(curPayJSON), &curPay)
		_ = json.Unmarshal([]byte(prevPayJSON), &prevPay)
		_ = json.Unmarshal([]byte(remJSON), &remembered)
		out = append(out, &EntityState{
			SyncName: syncName, Destination: destination, EntityKey: ek,
			DestinationID: destID, CurrentFingerprint: curFP, PreviousFingerprint: prevFP,
			CurrentPayload: curPay, PreviousPayload: prevPay, RememberedState: remembered,
			LastDecision: lastDecision, LastStatus: lastStatus,
			ConsecutiveMissing: consMissing, Version: version,
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		})
	}
	return out, rows.Err()
}

func (s *SQLiteStore) ResetEntityState(ctx context.Context, syncName, destination, entityKey string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM entity_state WHERE sync_name=? AND destination=? AND entity_key=?`,
		syncName, destination, entityKey)
	return err
}

// --- Rule firings ---

func (s *SQLiteStore) HasRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rule_firings WHERE sync_name=? AND destination=? AND entity_key=? AND rule_name=?`,
		syncName, destination, entityKey, rule).Scan(&n)
	return n > 0, err
}

func (s *SQLiteStore) MarkRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO rule_firings (sync_name, destination, entity_key, rule_name) VALUES (?,?,?,?)`,
		syncName, destination, entityKey, rule)
	return err
}

// --- Decision events ---

func (s *SQLiteStore) RecordDecision(ctx context.Context, event *DecisionEvent) error {
	rules, _ := json.Marshal(event.TriggeredRules)
	reasons, _ := json.Marshal(event.Reasons)
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO decision_events (sync_name, destination, entity_key, run_id, decision, triggered_rules, reasons, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		event.SyncName, event.Destination, event.EntityKey,
		event.RunID, event.Decision, string(rules), string(reasons), createdAt)
	return err
}

func (s *SQLiteStore) GetDecisionHistory(ctx context.Context, syncName, destination, entityKey string, limit int) ([]*DecisionEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, decision, triggered_rules, reasons, created_at
		FROM decision_events
		WHERE sync_name=? AND destination=? AND entity_key=?
		ORDER BY created_at DESC LIMIT ?`,
		syncName, destination, entityKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DecisionEvent
	for rows.Next() {
		var (
			id, runID                     int64
			decision, rulesJSON, reasJSON string
			createdAt                     time.Time
		)
		if err := rows.Scan(&id, &runID, &decision, &rulesJSON, &reasJSON, &createdAt); err != nil {
			return nil, err
		}
		var triggered, reasonsList []string
		_ = json.Unmarshal([]byte(rulesJSON), &triggered)
		_ = json.Unmarshal([]byte(reasJSON), &reasonsList)
		out = append(out, &DecisionEvent{
			ID: id, SyncName: syncName, Destination: destination,
			EntityKey: entityKey, RunID: runID, Decision: decision,
			TriggeredRules: triggered, Reasons: reasonsList, CreatedAt: createdAt,
		})
	}
	return out, rows.Err()
}

// --- Run log ---

func (s *SQLiteStore) StartRun(ctx context.Context, syncName, mode string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO run_log (sync_name, mode, started_at, status) VALUES (?,?,?,?)`,
		syncName, mode, time.Now().UTC(), "running")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *SQLiteStore) FinishRun(ctx context.Context, runID int64, stats RunStats) error {
	var wm any
	if !stats.HighWatermark.IsZero() {
		wm = stats.HighWatermark.UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE run_log SET
			finished_at=?, rows_extracted=?, rows_loaded=?,
			rows_skipped=?, rows_errored=?, high_watermark=?, status=?, error=?
		WHERE id=?`,
		time.Now().UTC(),
		stats.RowsExtracted, stats.RowsLoaded,
		stats.RowsSkipped, stats.RowsErrored,
		wm, stats.Status, stats.Error, runID)
	return err
}

func (s *SQLiteStore) GetLastRun(ctx context.Context, syncName string) (RunLog, error) {
	logs, err := s.GetRunHistory(ctx, syncName, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(logs) == 0 {
		return RunLog{}, fmt.Errorf("no runs found for %q", syncName)
	}
	return logs[0], nil
}

func (s *SQLiteStore) GetRunHistory(ctx context.Context, syncName string, limit int) ([]RunLog, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, sync_name, mode, started_at, finished_at,
		       rows_extracted, rows_loaded, rows_skipped, rows_errored,
		       high_watermark, status, error
		FROM run_log WHERE sync_name=?
		ORDER BY started_at DESC LIMIT ?`, syncName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunLog
	for rows.Next() {
		var (
			rl            RunLog
			finishedAt    sql.NullTime
			highWatermark sql.NullTime
		)
		if err := rows.Scan(&rl.ID, &rl.SyncName, &rl.Mode,
			&rl.StartedAt, &finishedAt,
			&rl.RowsExtracted, &rl.RowsLoaded, &rl.RowsSkipped, &rl.RowsErrored,
			&highWatermark, &rl.Status, &rl.Error); err != nil {
			return nil, err
		}
		if finishedAt.Valid {
			rl.FinishedAt = finishedAt.Time
		}
		if highWatermark.Valid {
			rl.HighWatermark = highWatermark.Time
		}
		out = append(out, rl)
	}
	return out, rows.Err()
}

// --- Delivery idempotency ---

func (s *SQLiteStore) IsDelivered(ctx context.Context, rowID, syncName, destination string) (bool, error) {
	s.batchMu.Lock()
	if s.batchSeen != nil {
		if _, ok := s.batchSeen[deliveryKey(rowID, syncName, destination)]; ok {
			s.batchMu.Unlock()
			return true, nil
		}
	}
	s.batchMu.Unlock()

	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM delivery_log WHERE row_id=? AND sync_name=? AND destination=?`,
		rowID, syncName, destination).Scan(&n)
	return n > 0, err
}

func (s *SQLiteStore) MarkDelivered(ctx context.Context, rowID, syncName, destination string) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	if s.batchSeen != nil {
		key := deliveryKey(rowID, syncName, destination)
		if _, already := s.batchSeen[key]; already {
			return nil
		}
		s.batchSeen[key] = struct{}{}
		s.batchPending = append(s.batchPending, deliveryEntry{
			rowID: rowID, syncName: syncName, destination: destination,
			deliveredAt: time.Now().UTC(),
		})
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO delivery_log (row_id, sync_name, destination) VALUES (?,?,?)`,
		rowID, syncName, destination)
	return err
}

func (s *SQLiteStore) BeginBatch(ctx context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = make(map[string]struct{})
	return nil
}

func (s *SQLiteStore) CommitBatch(ctx context.Context) error {
	s.batchMu.Lock()
	pending := append([]deliveryEntry(nil), s.batchPending...)
	s.batchPending = s.batchPending[:0]
	s.batchSeen = nil
	s.batchMu.Unlock()

	if len(pending) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO delivery_log (row_id, sync_name, destination, delivered_at) VALUES (?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range pending {
		if _, err := stmt.ExecContext(ctx, e.rowID, e.syncName, e.destination, e.deliveredAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = nil
	return nil
}

func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func deliveryKey(rowID, syncName, destination string) string {
	return strings.Join([]string{rowID, syncName, destination}, "\x00")
}

// --- Pipeline locks ---

func (s *SQLiteStore) LockRun(ctx context.Context, syncName, owner string, ttl time.Duration) error {
	now := time.Now().UTC()
	expires := now.Add(ttl)
	// Remove any expired lock for this sync.
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM pipeline_locks WHERE sync_name=? AND expires_at<=?`,
		syncName, now); err != nil {
		return fmt.Errorf("state: lock cleanup: %w", err)
	}
	// Try to insert; OR IGNORE means 0 rows affected if lock already held.
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO pipeline_locks (sync_name, owner, acquired_at, heartbeat_at, expires_at) VALUES (?,?,?,?,?)`,
		syncName, owner, now, now, expires)
	if err != nil {
		return fmt.Errorf("state: acquire lock: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var curOwner string
		_ = s.db.QueryRowContext(ctx,
			`SELECT owner FROM pipeline_locks WHERE sync_name=?`, syncName).Scan(&curOwner)
		return fmt.Errorf("state: sync %q is already running (lock held by %q) — use 'vortara state unlock' to clear a stale lock", syncName, curOwner)
	}
	return nil
}

func (s *SQLiteStore) UnlockRun(ctx context.Context, syncName string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pipeline_locks WHERE sync_name=?`, syncName)
	return err
}

func (s *SQLiteStore) HeartbeatLock(ctx context.Context, syncName, owner string, ttl time.Duration) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_locks SET heartbeat_at=?, expires_at=? WHERE sync_name=? AND owner=?`,
		now, now.Add(ttl), syncName, owner)
	return err
}
