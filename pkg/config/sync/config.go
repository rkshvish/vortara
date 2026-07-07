// Package sync defines the configuration types for the v2 sync YAML format.
package sync

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// SyncFile is the root of a sync YAML file.
type SyncFile struct {
	Sync SyncSpec `yaml:"sync"`
}

// SyncSpec is the top-level sync specification.
type SyncSpec struct {
	Name          string            `yaml:"name"`
	Source        SourceConfig      `yaml:"source"`
	Mapping       []MappingEntry    `yaml:"mapping,omitempty"`
	Required      []string          `yaml:"required,omitempty"` // source fields that must be present
	Destination   DestinationConfig `yaml:"destination"`
	State         StateConfig       `yaml:"state,omitempty"`
	Decisions     DecisionsConfig   `yaml:"decisions,omitempty"`
	OnMissingFrom OnMissingConfig   `yaml:"on_missing_from_source,omitempty"`
	Safety        SafetyConfig      `yaml:"safety,omitempty"`
	Errors        ErrorsConfig      `yaml:"errors,omitempty"`
	Artifacts     ArtifactsConfig   `yaml:"artifacts,omitempty"`
	Metrics       MetricsConfig     `yaml:"metrics,omitempty"`
	Cron          string            `yaml:"cron,omitempty"`
	Tests         []TestCase        `yaml:"tests,omitempty"`
}

// ArtifactsConfig controls per-run artifact output.
type ArtifactsConfig struct {
	Path       string `yaml:"path,omitempty"`        // base directory for artifact files
	MaxSamples int    `yaml:"max_samples,omitempty"` // max sample rows per category (default 10)
}

// MetricsConfig controls Prometheus textfile output (node_exporter compatible).
type MetricsConfig struct {
	Path string `yaml:"path,omitempty"` // directory where .prom files are written
}

// SourceConfig describes where to read entity data from.
type SourceConfig struct {
	Type      string           `yaml:"type"`
	URL       string           `yaml:"url,omitempty"`
	Table     string           `yaml:"table,omitempty"`
	Query     string           `yaml:"query,omitempty"`
	EntityKey string           `yaml:"entity_key"`
	Watermark *WatermarkConfig `yaml:"watermark,omitempty"`
	BatchSize int              `yaml:"batch_size,omitempty"`
	Auth      *AuthConfig      `yaml:"auth,omitempty"`
}

// WatermarkConfig enables incremental extraction instead of full snapshots.
type WatermarkConfig struct {
	Column string `yaml:"column"`
	Type   string `yaml:"type,omitempty"` // "timestamp" (default) or "int"
}

// MappingEntry projects a source field to a destination field name.
type MappingEntry struct {
	Source                 string `yaml:"source"`
	Dest                   string `yaml:"dest,omitempty"` // defaults to Source
	ExcludeFromFingerprint bool   `yaml:"exclude_from_fingerprint,omitempty"`
	Redacted               bool   `yaml:"redacted,omitempty"` // mask this field in logs, artifacts, and CLI output
}

// DestName returns the effective destination field name.
func (m MappingEntry) DestName() string {
	if m.Dest != "" {
		return m.Dest
	}
	return m.Source
}

// DestinationConfig describes where to write entity data to.
type DestinationConfig struct {
	Type    string            `yaml:"type"`
	URL     string            `yaml:"url,omitempty"`
	Auth    *AuthConfig       `yaml:"auth,omitempty"`
	Object  string            `yaml:"object,omitempty"`
	Table   string            `yaml:"table,omitempty"`
	MatchOn []string          `yaml:"match_on,omitempty"`
	Method  string            `yaml:"method,omitempty"`
	Options map[string]string `yaml:"options,omitempty"`
}

// AuthConfig holds authentication parameters.
type AuthConfig struct {
	Type         string   `yaml:"type"`
	Token        string   `yaml:"token,omitempty"`
	ClientID     string   `yaml:"client_id,omitempty"`
	ClientSecret string   `yaml:"client_secret,omitempty"`
	TokenURL     string   `yaml:"token_url,omitempty"`
	Scopes       []string `yaml:"scopes,omitempty"`
	Key          string   `yaml:"key,omitempty"`
	Value        string   `yaml:"value,omitempty"`
	InHeader     bool     `yaml:"in_header,omitempty"`
	Username     string   `yaml:"username,omitempty"`
	Password     string   `yaml:"password,omitempty"`
}

// StateConfig describes where to store sync state.
type StateConfig struct {
	Backend     string            `yaml:"backend,omitempty"`
	Path        string            `yaml:"path,omitempty"`
	Connection  string            `yaml:"connection,omitempty"`
	KeyPrefix   string            `yaml:"key_prefix,omitempty"`
	Fingerprint FingerprintConfig `yaml:"fingerprint,omitempty"`
}

// FingerprintConfig controls which fields contribute to the entity fingerprint.
// If Include is set, only those fields are hashed. Exclude is applied after Include.
type FingerprintConfig struct {
	Include []string `yaml:"include,omitempty"` // whitelist: only hash these fields
	Exclude []string `yaml:"exclude,omitempty"` // always exclude these fields from hash
}

// DecisionsConfig holds the decision rules for this sync.
type DecisionsConfig struct {
	Default string       `yaml:"default,omitempty"` // "skip" (default) or "upsert"
	Rules   []RuleConfig `yaml:"rules,omitempty"`
}

// RuleConfig is a single named decision rule.
type RuleConfig struct {
	Name     string            `yaml:"name"`
	When     WhenConfig        `yaml:"when"`
	Once     bool              `yaml:"once,omitempty"`
	Action   string            `yaml:"action"` // "upsert", "update", "create", "skip", "delete"
	Remember map[string]string `yaml:"remember,omitempty"`
}

// WhenConfig is a union predicate. Exactly one field should be set.
type WhenConfig struct {
	// Leaf predicates — set via string shorthand "first_seen()" or structured form
	FirstSeen          *struct{} `yaml:"first_seen,omitempty"`
	FingerprintChanged *struct{} `yaml:"fingerprint_changed,omitempty"`

	// Structured predicates
	Transitioned *TransitionedConfig `yaml:"transitioned,omitempty"`
	OnlyChanged  []string            `yaml:"only_changed,omitempty"`

	// Logical combinators
	Any []*WhenConfig `yaml:"any,omitempty"`
	All []*WhenConfig `yaml:"all,omitempty"`
	Not *WhenConfig   `yaml:"not,omitempty"`
}

// UnmarshalYAML supports "first_seen()" and "fingerprint_changed()" shorthands.
func (w *WhenConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		switch strings.TrimSpace(value.Value) {
		case "first_seen()":
			v := struct{}{}
			w.FirstSeen = &v
		case "fingerprint_changed()":
			v := struct{}{}
			w.FingerprintChanged = &v
		default:
			return fmt.Errorf("unknown predicate shorthand %q", value.Value)
		}
		return nil
	}
	type plain WhenConfig
	return value.Decode((*plain)(w))
}

// TransitionedConfig matches a specific field value transition.
type TransitionedConfig struct {
	Field string `yaml:"field"`
	From  string `yaml:"from"`
	To    string `yaml:"to"`
}

// OnMissingConfig controls behavior for entities absent from the current source snapshot.
type OnMissingConfig struct {
	Action           string   `yaml:"action,omitempty"`             // "skip" | "clear_fields" | "delete"
	Fields           []string `yaml:"fields,omitempty"`             // for clear_fields
	AfterMissingRuns int      `yaml:"after_missing_runs,omitempty"` // default 1
}

// SafetyConfig defines blast-radius limits.
type SafetyConfig struct {
	MaxCreatesPerRun              int                `yaml:"max_creates_per_run,omitempty"`
	MaxUpdatesPerRun              int                `yaml:"max_updates_per_run,omitempty"`
	MaxDeletesPerRun              int                `yaml:"max_deletes_per_run,omitempty"`
	RequireApprovalAbove          float64            `yaml:"require_approval_above,omitempty"`
	DryRunRequired                bool               `yaml:"dry_run_required,omitempty"`
	BlockIfChangedFieldRatioAbove map[string]float64 `yaml:"block_if_changed_field_ratio_above,omitempty"`
	RequireApprovalFor            []string           `yaml:"require_approval_for,omitempty"`
}

// RetryConfig describes retry policy for delivery errors.
type RetryConfig struct {
	OnStatus     []int  `yaml:"on_status,omitempty"`
	Attempts     int    `yaml:"attempts,omitempty"`
	Backoff      string `yaml:"backoff,omitempty"` // "exponential" | "linear"
	InitialDelay string `yaml:"initial_delay,omitempty"`
	MaxDelay     string `yaml:"max_delay,omitempty"`
}

// DLQErrorConfig describes which errors route to the dead-letter queue.
type DLQErrorConfig struct {
	Path     string `yaml:"path,omitempty"`
	OnStatus []int  `yaml:"on_status,omitempty"`
}

// ErrorsConfig defines error handling policy.
type ErrorsConfig struct {
	OnError           string         `yaml:"on_error,omitempty"`            // "skip" | "retry" | "dlq"
	MaxRetries        int            `yaml:"max_retries,omitempty"`         // deprecated: use retry.attempts
	DLQPath           string         `yaml:"dlq_path,omitempty"`            // deprecated: use dlq.path
	FailureWebhookURL string         `yaml:"failure_webhook_url,omitempty"` // POST JSON on run failure
	Retry             RetryConfig    `yaml:"retry,omitempty"`
	DLQ               DLQErrorConfig `yaml:"dlq,omitempty"`
}

// TestCase defines an inline state unit test for the sync (run via vortara test --state-tests).
type TestCase struct {
	Name     string         `yaml:"name"`
	Previous map[string]any `yaml:"previous,omitempty"` // nil means first_seen
	Current  map[string]any `yaml:"current"`
	Expect   TestExpect     `yaml:"expect"`
}

// TestExpect describes the expected outcome of a TestCase.
type TestExpect struct {
	Decision       string   `yaml:"decision"`
	TriggeredRules []string `yaml:"triggered_rules,omitempty"`
	ChangedFields  []string `yaml:"changed_fields,omitempty"` // field names that must appear in the diff
}

// ResolvedDLQPath returns dlq.path if set, falling back to the top-level dlq_path.
func (e ErrorsConfig) ResolvedDLQPath() string {
	if e.DLQ.Path != "" {
		return e.DLQ.Path
	}
	return e.DLQPath
}
