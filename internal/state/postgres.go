package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func init() {
	Register("postgres", func(cfg stateConfig) (StateStore, error) {
		return NewPostgresStore(cfg.Connection, cfg.KeyPrefix)
	})
}

// PostgresOption configures the Postgres connection pool.
type PostgresOption func(*postgresPoolCfg)

type postgresPoolCfg struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
}

// WithMaxOpenConns sets the maximum number of open connections.
func WithMaxOpenConns(n int) PostgresOption {
	return func(c *postgresPoolCfg) { c.maxOpenConns = n }
}

// WithMaxIdleConns sets the maximum number of idle connections.
func WithMaxIdleConns(n int) PostgresOption {
	return func(c *postgresPoolCfg) { c.maxIdleConns = n }
}

// WithConnMaxLifetime sets the maximum lifetime for a connection.
func WithConnMaxLifetime(d time.Duration) PostgresOption {
	return func(c *postgresPoolCfg) { c.connMaxLifetime = d }
}

// PostgresStore is a StateStore backed by PostgreSQL.
type PostgresStore struct {
	db           *sql.DB
	prefix       string
	batchMu      sync.Mutex
	batchPending []deliveryEntry
	batchSeen    map[string]struct{}
}

// NewPostgresStore opens a Postgres connection and creates the required tables.
func NewPostgresStore(dsn, prefix string, opts ...PostgresOption) (*PostgresStore, error) {
	if prefix == "" {
		prefix = "v"
	}
	cfg := postgresPoolCfg{maxOpenConns: 5, maxIdleConns: 5, connMaxLifetime: 5 * time.Minute}
	for _, opt := range opts {
		opt(&cfg)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("state: open postgres: %w", err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConns)
	db.SetMaxIdleConns(cfg.maxIdleConns)
	db.SetConnMaxLifetime(cfg.connMaxLifetime)

	s := &PostgresStore{db: db, prefix: prefix}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStore) tbl(name string) string {
	return s.prefix + "_" + name
}

func (s *PostgresStore) initSchema() error {
	ctx := context.Background()
	ddl := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
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
			created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (sync_name, destination, entity_key)
		)`, s.tbl("entity_state")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			sync_name   TEXT NOT NULL,
			destination TEXT NOT NULL,
			entity_key  TEXT NOT NULL,
			rule_name   TEXT NOT NULL,
			fired_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (sync_name, destination, entity_key, rule_name)
		)`, s.tbl("rule_firings")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id              BIGSERIAL PRIMARY KEY,
			sync_name       TEXT NOT NULL,
			destination     TEXT NOT NULL,
			entity_key      TEXT NOT NULL,
			run_id          BIGINT NOT NULL DEFAULT 0,
			decision        TEXT NOT NULL,
			triggered_rules TEXT NOT NULL DEFAULT '[]',
			reasons         TEXT NOT NULL DEFAULT '[]',
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`, s.tbl("decision_events")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (sync_name, destination, entity_key)`,
			s.tbl("idx_de_entity"), s.tbl("decision_events")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id             BIGSERIAL PRIMARY KEY,
			sync_name      TEXT NOT NULL,
			mode           TEXT NOT NULL DEFAULT 'batch',
			started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			finished_at    TIMESTAMPTZ,
			rows_extracted INTEGER NOT NULL DEFAULT 0,
			rows_loaded    INTEGER NOT NULL DEFAULT 0,
			rows_skipped   INTEGER NOT NULL DEFAULT 0,
			rows_errored   INTEGER NOT NULL DEFAULT 0,
			status         TEXT NOT NULL DEFAULT 'running',
			error          TEXT NOT NULL DEFAULT ''
		)`, s.tbl("run_log")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			row_id       TEXT NOT NULL,
			sync_name    TEXT NOT NULL,
			destination  TEXT NOT NULL,
			delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (row_id, sync_name, destination)
		)`, s.tbl("delivery_log")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			sync_name    TEXT PRIMARY KEY,
			owner        TEXT NOT NULL,
			acquired_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at   TIMESTAMPTZ NOT NULL
		)`, s.tbl("pipeline_locks")),
	}
	for _, stmt := range ddl {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("state: postgres ddl: %w", err)
		}
	}
	return nil
}

// --- Entity state ---

func (s *PostgresStore) GetEntityState(ctx context.Context, syncName, destination, entityKey string) (*EntityState, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT destination_id, current_fingerprint, previous_fingerprint,
		       current_payload, previous_payload, remembered_state,
		       last_decision, last_status, consecutive_missing,
		       version, created_at, updated_at
		FROM %s WHERE sync_name=$1 AND destination=$2 AND entity_key=$3`,
		s.tbl("entity_state")), syncName, destination, entityKey)
	var (
		destID, curFP, prevFP                 string
		curPayJSON, prevPayJSON, remJSON       string
		lastDecision, lastStatus              string
		consMissing, version                  int
		createdAt, updatedAt                  time.Time
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

func (s *PostgresStore) SaveEntityState(ctx context.Context, es *EntityState) error {
	curPay, _ := json.Marshal(es.CurrentPayload)
	prevPay, _ := json.Marshal(es.PreviousPayload)
	remembered, _ := json.Marshal(es.RememberedState)
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s
			(sync_name, destination, entity_key, destination_id,
			 current_fingerprint, previous_fingerprint,
			 current_payload, previous_payload, remembered_state,
			 last_decision, last_status, consecutive_missing, version,
			 created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (sync_name, destination, entity_key) DO UPDATE SET
			destination_id       = EXCLUDED.destination_id,
			current_fingerprint  = EXCLUDED.current_fingerprint,
			previous_fingerprint = EXCLUDED.previous_fingerprint,
			current_payload      = EXCLUDED.current_payload,
			previous_payload     = EXCLUDED.previous_payload,
			remembered_state     = EXCLUDED.remembered_state,
			last_decision        = EXCLUDED.last_decision,
			last_status          = EXCLUDED.last_status,
			consecutive_missing  = EXCLUDED.consecutive_missing,
			version              = EXCLUDED.version,
			updated_at           = EXCLUDED.updated_at`,
		s.tbl("entity_state")),
		es.SyncName, es.Destination, es.EntityKey, es.DestinationID,
		es.CurrentFingerprint, es.PreviousFingerprint,
		string(curPay), string(prevPay), string(remembered),
		es.LastDecision, es.LastStatus, es.ConsecutiveMissing, es.Version,
		now, now,
	)
	return err
}

func (s *PostgresStore) ListEntityStates(ctx context.Context, syncName, destination string, limit, offset int) ([]*EntityState, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT entity_key, destination_id, current_fingerprint, previous_fingerprint,
		       current_payload, previous_payload, remembered_state,
		       last_decision, last_status, consecutive_missing,
		       version, created_at, updated_at
		FROM %s WHERE sync_name=$1 AND destination=$2
		ORDER BY updated_at DESC LIMIT $3 OFFSET $4`,
		s.tbl("entity_state")), syncName, destination, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EntityState
	for rows.Next() {
		var (
			ek, destID, curFP, prevFP             string
			curPayJSON, prevPayJSON, remJSON       string
			lastDecision, lastStatus              string
			consMissing, version                  int
			createdAt, updatedAt                  time.Time
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

func (s *PostgresStore) ResetEntityState(ctx context.Context, syncName, destination, entityKey string) error {
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE sync_name=$1 AND destination=$2 AND entity_key=$3`,
			s.tbl("entity_state")), syncName, destination, entityKey)
	return err
}

func (s *PostgresStore) HasRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE sync_name=$1 AND destination=$2 AND entity_key=$3 AND rule_name=$4`,
			s.tbl("rule_firings")), syncName, destination, entityKey, rule).Scan(&n)
	return n > 0, err
}

func (s *PostgresStore) MarkRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) error {
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (sync_name, destination, entity_key, rule_name) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			s.tbl("rule_firings")), syncName, destination, entityKey, rule)
	return err
}

func (s *PostgresStore) RecordDecision(ctx context.Context, event *DecisionEvent) error {
	rules, _ := json.Marshal(event.TriggeredRules)
	reasons, _ := json.Marshal(event.Reasons)
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (sync_name, destination, entity_key, run_id, decision, triggered_rules, reasons) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			s.tbl("decision_events")),
		event.SyncName, event.Destination, event.EntityKey,
		event.RunID, event.Decision, string(rules), string(reasons))
	return err
}

func (s *PostgresStore) GetDecisionHistory(ctx context.Context, syncName, destination, entityKey string, limit int) ([]*DecisionEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id, run_id, decision, triggered_rules, reasons, created_at
		FROM %s WHERE sync_name=$1 AND destination=$2 AND entity_key=$3
		ORDER BY created_at DESC LIMIT $4`, s.tbl("decision_events")),
		syncName, destination, entityKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DecisionEvent
	for rows.Next() {
		var (
			id, runID                      int64
			decision, rulesJSON, reasJSON  string
			createdAt                      time.Time
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

func (s *PostgresStore) StartRun(ctx context.Context, syncName, mode string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (sync_name, mode, started_at, status) VALUES ($1,$2,NOW(),$3) RETURNING id`,
			s.tbl("run_log")), syncName, mode, "running").Scan(&id)
	return id, err
}

func (s *PostgresStore) FinishRun(ctx context.Context, runID int64, stats RunStats) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET finished_at=NOW(), rows_extracted=$1, rows_loaded=$2,
		rows_skipped=$3, rows_errored=$4, status=$5, error=$6 WHERE id=$7`,
		s.tbl("run_log")),
		stats.RowsExtracted, stats.RowsLoaded, stats.RowsSkipped, stats.RowsErrored,
		stats.Status, stats.Error, runID)
	return err
}

func (s *PostgresStore) GetLastRun(ctx context.Context, syncName string) (RunLog, error) {
	logs, err := s.GetRunHistory(ctx, syncName, 1)
	if err != nil {
		return RunLog{}, err
	}
	if len(logs) == 0 {
		return RunLog{}, fmt.Errorf("no runs found for %q", syncName)
	}
	return logs[0], nil
}

func (s *PostgresStore) GetRunHistory(ctx context.Context, syncName string, limit int) ([]RunLog, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, sync_name, mode, started_at, finished_at,
		       rows_extracted, rows_loaded, rows_skipped, rows_errored, status, error
		FROM %s WHERE sync_name=$1 ORDER BY started_at DESC LIMIT $2`,
		s.tbl("run_log")), syncName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunLog
	for rows.Next() {
		var (
			rl         RunLog
			finishedAt sql.NullTime
		)
		if err := rows.Scan(&rl.ID, &rl.SyncName, &rl.Mode,
			&rl.StartedAt, &finishedAt,
			&rl.RowsExtracted, &rl.RowsLoaded, &rl.RowsSkipped, &rl.RowsErrored,
			&rl.Status, &rl.Error); err != nil {
			return nil, err
		}
		if finishedAt.Valid {
			rl.FinishedAt = finishedAt.Time
		}
		out = append(out, rl)
	}
	return out, rows.Err()
}

func (s *PostgresStore) IsDelivered(ctx context.Context, rowID, syncName, destination string) (bool, error) {
	s.batchMu.Lock()
	if s.batchSeen != nil {
		if _, ok := s.batchSeen[pgDeliveryKey(rowID, syncName, destination)]; ok {
			s.batchMu.Unlock()
			return true, nil
		}
	}
	s.batchMu.Unlock()
	var n int
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE row_id=$1 AND sync_name=$2 AND destination=$3`,
			s.tbl("delivery_log")), rowID, syncName, destination).Scan(&n)
	return n > 0, err
}

func (s *PostgresStore) MarkDelivered(ctx context.Context, rowID, syncName, destination string) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	if s.batchSeen != nil {
		key := pgDeliveryKey(rowID, syncName, destination)
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
		fmt.Sprintf(`INSERT INTO %s (row_id, sync_name, destination) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
			s.tbl("delivery_log")), rowID, syncName, destination)
	return err
}

func (s *PostgresStore) BeginBatch(_ context.Context) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = make(map[string]struct{})
	return nil
}

func (s *PostgresStore) CommitBatch(ctx context.Context) error {
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
		fmt.Sprintf(`INSERT INTO %s (row_id, sync_name, destination, delivered_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			s.tbl("delivery_log")))
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

func (s *PostgresStore) RollbackBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batchPending = s.batchPending[:0]
	s.batchSeen = nil
	return nil
}

func (s *PostgresStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func pgDeliveryKey(rowID, syncName, destination string) string {
	return strings.Join([]string{rowID, syncName, destination}, "\x00")
}

// --- Pipeline locks ---

func (s *PostgresStore) LockRun(ctx context.Context, syncName, owner string, ttl time.Duration) error {
	now := time.Now().UTC()
	expires := now.Add(ttl)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: lock tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Remove expired lock if present.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE sync_name=$1 AND expires_at<=$2`,
		s.tbl("pipeline_locks")), syncName, now); err != nil {
		return fmt.Errorf("state: lock cleanup: %w", err)
	}
	// Insert if no live lock exists.
	res, err := tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (sync_name, owner, acquired_at, heartbeat_at, expires_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (sync_name) DO NOTHING`,
		s.tbl("pipeline_locks")), syncName, owner, now, now, expires)
	if err != nil {
		return fmt.Errorf("state: acquire lock: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var curOwner string
		_ = tx.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT owner FROM %s WHERE sync_name=$1`,
			s.tbl("pipeline_locks")), syncName).Scan(&curOwner)
		return fmt.Errorf("state: sync %q is already running (lock held by %q) — use 'vortara state unlock' to clear a stale lock", syncName, curOwner)
	}
	return tx.Commit()
}

func (s *PostgresStore) UnlockRun(ctx context.Context, syncName string) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE sync_name=$1`, s.tbl("pipeline_locks")), syncName)
	return err
}

func (s *PostgresStore) HeartbeatLock(ctx context.Context, syncName, owner string, ttl time.Duration) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET heartbeat_at=$1, expires_at=$2 WHERE sync_name=$3 AND owner=$4`,
		s.tbl("pipeline_locks")), now, now.Add(ttl), syncName, owner)
	return err
}
