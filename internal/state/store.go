// Package state defines the storage contract for sync state.
package state

import (
	"context"
	"time"
)

// StateStore is the interface all state backends must implement.
type StateStore interface {
	// --- Entity state ---

	// GetEntityState retrieves the current state for one entity.
	// Returns nil, nil when the entity has never been seen.
	GetEntityState(ctx context.Context, syncName, destination, entityKey string) (*EntityState, error)

	// SaveEntityState persists entity state. Upserts on (syncName, destination, entityKey).
	SaveEntityState(ctx context.Context, s *EntityState) error

	// ListEntityStates returns stored entity states for a sync+destination, paginated.
	ListEntityStates(ctx context.Context, syncName, destination string, limit, offset int) ([]*EntityState, error)

	// ResetEntityState removes the stored state for one entity, causing it to
	// be treated as first_seen on the next run.
	ResetEntityState(ctx context.Context, syncName, destination, entityKey string) error

	// --- Rule firings (once: true tracking) ---

	// HasRuleFired returns true if the named rule has already fired for this entity.
	HasRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) (bool, error)

	// MarkRuleFired records that a rule fired for this entity.
	MarkRuleFired(ctx context.Context, syncName, destination, entityKey, rule string) error

	// --- Decision events (for explain / history) ---

	// RecordDecision persists a decision event for audit and explain.
	RecordDecision(ctx context.Context, event *DecisionEvent) error

	// GetDecisionHistory returns recent decision events for one entity.
	GetDecisionHistory(ctx context.Context, syncName, destination, entityKey string, limit int) ([]*DecisionEvent, error)

	// --- Run log ---

	// StartRun creates a new run log entry and returns its ID.
	StartRun(ctx context.Context, syncName, mode string) (int64, error)

	// FinishRun updates the run log entry with final stats.
	FinishRun(ctx context.Context, runID int64, stats RunStats) error

	// GetLastRun returns the most recent run log entry for a sync.
	GetLastRun(ctx context.Context, syncName string) (RunLog, error)

	// GetRunHistory returns the most recent run log entries for a sync.
	GetRunHistory(ctx context.Context, syncName string, limit int) ([]RunLog, error)

	// --- Delivery idempotency (used by destination connectors) ---

	// IsDelivered returns true if this row key was already successfully delivered.
	IsDelivered(ctx context.Context, rowID, syncName, destination string) (bool, error)

	// MarkDelivered records successful delivery of a row.
	MarkDelivered(ctx context.Context, rowID, syncName, destination string) error

	// BeginBatch starts buffering delivery writes.
	BeginBatch(ctx context.Context) error

	// CommitBatch atomically flushes buffered delivery writes.
	CommitBatch(ctx context.Context) error

	// RollbackBatch discards buffered delivery writes.
	RollbackBatch() error

	// Close releases all resources.
	Close() error

	// --- Pipeline locks (prevents concurrent runs of the same sync) ---

	// LockRun acquires an exclusive run lock for the named sync.
	// Returns an error if the sync is already locked and the lock has not expired.
	LockRun(ctx context.Context, syncName, owner string, ttl time.Duration) error

	// UnlockRun releases the run lock for the named sync.
	UnlockRun(ctx context.Context, syncName string) error

	// HeartbeatLock extends the expiry of the lock held by owner.
	HeartbeatLock(ctx context.Context, syncName, owner string, ttl time.Duration) error
}
