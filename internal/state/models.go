package state

import "time"

// RunStats holds the result of a completed sync run.
// Fields marked "persisted" are written to the run_log table.
// All other fields are in-memory only for the current run.
type RunStats struct {
	// Persisted fields.
	RowsExtracted int
	RowsLoaded    int
	RowsSkipped   int
	RowsErrored   int
	HighWatermark time.Time // max watermark observed in this run
	Status        string    // "success" | "failed" | "timeout"
	Error         string

	// In-memory only: breakdown of delivery decisions.
	Creates int
	Updates int
	Deletes int

	// In-memory only: per-field change frequency (field name → count).
	FieldChangeCounts map[string]int

	// In-memory only: approval state.
	ApprovalRequired bool
	ApprovalHash     string // short hash the operator can pass to --approve-snapshot
}

// RunLog is one entry from the run history.
type RunLog struct {
	ID            int64
	SyncName      string
	Mode          string
	StartedAt     time.Time
	FinishedAt    time.Time
	RowsExtracted int
	RowsLoaded    int
	RowsSkipped   int
	RowsErrored   int
	HighWatermark time.Time
	Status        string
	Error         string
}

// EntityState holds the durable per-entity state for one sync+destination pair.
type EntityState struct {
	SyncName            string
	Destination         string
	EntityKey           string
	DestinationID       string // set after first successful delivery
	CurrentFingerprint  string
	PreviousFingerprint string
	CurrentPayload      map[string]any // last successfully delivered payload
	PreviousPayload     map[string]any
	RememberedState     map[string]any // user-programmable key/value store
	LastDecision        string         // "create", "update", "upsert", "skip", etc.
	LastStatus          string         // "success" | "failed" | "pending"
	ConsecutiveMissing  int            // runs where entity was absent from source
	Version             int
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// DecisionEvent records what happened to one entity in one run.
type DecisionEvent struct {
	ID             int64
	SyncName       string
	Destination    string
	EntityKey      string
	RunID          int64
	Decision       string   // the Action taken
	TriggeredRules []string // names of rules that fired
	Reasons        []string // human-readable explanation
	CreatedAt      time.Time
}
