package v2

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	config "github.com/rkshvish/vortara/pkg/config"
	"gopkg.in/yaml.v3"
)

// PipelineConfig is the root config struct.
type PipelineConfig struct {
	Name         string              `yaml:"name"`
	Source       SourceConfig        `yaml:"source"`
	Also         *AlsoConfig         `yaml:"also,omitempty"`
	Transform    []TransformStep     `yaml:"transform,omitempty"`
	Destinations []DestinationConfig `yaml:"destinations"`
	Cron         string              `yaml:"cron,omitempty"`
	Settings     SettingsConfig      `yaml:"settings,omitempty"`
	Alerts       *AlertsConfig       `yaml:"alerts,omitempty"`
}

// AlertsConfig defines failure notification hooks.
type AlertsConfig struct {
	OnFailure *AlertTarget `yaml:"on_failure,omitempty"`
}

// AlertTarget is a webhook notification target.
type AlertTarget struct {
	WebhookURL string `yaml:"webhook_url"`
}

type SourceConfig struct {
	Type            string       `yaml:"type"`
	URL             string       `yaml:"url,omitempty"`
	Project         string       `yaml:"project,omitempty"`
	Dataset         string       `yaml:"dataset,omitempty"`
	Table           string       `yaml:"table,omitempty"`
	Query           string       `yaml:"query,omitempty"`
	Watermark       string       `yaml:"watermark,omitempty"`
	Exclude         []string     `yaml:"exclude,omitempty"`
	BatchSize       int          `yaml:"batch_size,omitempty"`
	Parallelism     int          `yaml:"parallelism,omitempty"`
	Auth            *AuthConfig  `yaml:"auth,omitempty"`
	Brokers         []string     `yaml:"brokers,omitempty"`
	Topic           string       `yaml:"topic,omitempty"`
	GroupID         string       `yaml:"group_id,omitempty"`
	Path            string       `yaml:"path,omitempty"`
	Port            int          `yaml:"port,omitempty"`
	Secret          string       `yaml:"secret,omitempty"`
	Flush           *FlushConfig `yaml:"flush,omitempty"`
	Dedup           *DedupConfig `yaml:"dedup,omitempty"`
	CredentialsFile string       `yaml:"credentials_file,omitempty"`
	CredentialsJSON string       `yaml:"credentials_json,omitempty"`
	Slot            string       `yaml:"slot,omitempty"`
	Publication     string       `yaml:"publication,omitempty"`
}

type AlsoConfig struct {
	Type    string       `yaml:"type"`
	Brokers []string     `yaml:"brokers,omitempty"`
	Topic   string       `yaml:"topic,omitempty"`
	GroupID string       `yaml:"group_id,omitempty"`
	Path    string       `yaml:"path,omitempty"`
	Port    int          `yaml:"port,omitempty"`
	Secret  string       `yaml:"secret,omitempty"`
	Flush   *FlushConfig `yaml:"flush,omitempty"`
	Dedup   *DedupConfig `yaml:"dedup,omitempty"`
}

type FlushConfig struct {
	Interval string `yaml:"interval"`
	Records  int    `yaml:"records"`
}

type DedupConfig struct {
	Window string `yaml:"window"`
	Key    string `yaml:"key"`
}

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

type TransformStep struct {
	Filter  string            `yaml:"filter,omitempty"`
	Rename  map[string]string `yaml:"rename,omitempty"`
	Add     map[string]string `yaml:"add,omitempty"`
	Drop    []string          `yaml:"drop,omitempty"`
	Mask    []string          `yaml:"mask,omitempty"`
	Trim    []string          `yaml:"trim,omitempty"`    // ["*"] = all string fields
	Flatten string            `yaml:"flatten,omitempty"` // separator for nested maps, e.g. "_"
}

type DestinationConfig struct {
	Type      string      `yaml:"type"`
	URL       string      `yaml:"url,omitempty"`
	Webhook   string      `yaml:"webhook,omitempty"`
	Auth      *AuthConfig `yaml:"auth,omitempty"`
	Table     string      `yaml:"table,omitempty"`
	MatchOn   []string    `yaml:"match_on,omitempty"`
	Strategy  string      `yaml:"strategy,omitempty"`
	Object    string      `yaml:"object,omitempty"`
	Message   string      `yaml:"message,omitempty"`
	When      string      `yaml:"when,omitempty"`
	RateLimit string      `yaml:"rate_limit,omitempty"`
}

type SettingsConfig struct {
	State       StateSettings       `yaml:"state,omitempty"`
	Log         LogSettings         `yaml:"log,omitempty"`
	Limits      LimitsSettings      `yaml:"limits,omitempty"`
	OnError     string              `yaml:"on_error,omitempty"`
	DLQPath     string              `yaml:"dlq_path,omitempty"`
	Concurrency ConcurrencySettings `yaml:"concurrency,omitempty"`
}

type StateSettings struct {
	Backend      string `yaml:"backend,omitempty"`
	Path         string `yaml:"path,omitempty"`
	Connection   string `yaml:"connection,omitempty"`
	DeliveredTTL string `yaml:"delivered_ttl,omitempty"`
	KeyPrefix    string `yaml:"key_prefix,omitempty"`
}

type LogSettings struct {
	Level  string `yaml:"level,omitempty"`
	Format string `yaml:"format,omitempty"`
}

type LimitsSettings struct {
	MaxRuntime string `yaml:"max_runtime,omitempty"`
	MaxRows    int64  `yaml:"max_rows,omitempty"`
}

type ConcurrencySettings struct {
	Workers   int `yaml:"workers,omitempty"`
	BatchSize int `yaml:"batch_size,omitempty"`
}

// Load reads and parses a pipeline config file.
// Applies env interpolation and defaults.
func Load(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse parses raw YAML bytes into PipelineConfig.
// Applies env interpolation then defaults.
func Parse(data []byte) (*PipelineConfig, error) {
	var cfg PipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	applyEnv(&cfg)
	applyDefaults(&cfg)
	return &cfg, nil
}

func applyDefaults(cfg *PipelineConfig) {
	if cfg.Source.BatchSize == 0 {
		cfg.Source.BatchSize = 1000
	}
	if cfg.Source.Parallelism == 0 {
		cfg.Source.Parallelism = 1
	}
	if cfg.Source.Watermark == "" {
		cfg.Source.Watermark = "updated_at"
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Source.Watermark), "none") {
		cfg.Source.Watermark = "none"
	}

	for i := range cfg.Destinations {
		if cfg.Destinations[i].Strategy == "" {
			switch cfg.Destinations[i].Type {
			case "restapi", "slack", "googlesheets", "mixpanel":
				cfg.Destinations[i].Strategy = "append"
			default:
				cfg.Destinations[i].Strategy = "merge"
			}
		}
	}

	if cfg.Settings.State.Backend == "" {
		cfg.Settings.State.Backend = "sqlite"
	}
	if cfg.Settings.State.Path == "" && cfg.Settings.State.Backend == "sqlite" {
		cfg.Settings.State.Path = "./vortara/state.db"
	}
	if cfg.Settings.State.KeyPrefix == "" {
		cfg.Settings.State.KeyPrefix = "vortara"
	}
	if cfg.Settings.Log.Level == "" {
		cfg.Settings.Log.Level = "info"
	}
	if cfg.Settings.Log.Format == "" {
		cfg.Settings.Log.Format = "text"
	}
	if cfg.Settings.OnError == "" {
		cfg.Settings.OnError = "skip"
	}
	if cfg.Settings.Concurrency.Workers == 0 {
		cfg.Settings.Concurrency.Workers = runtime.NumCPU()
	}
	if cfg.Settings.Concurrency.BatchSize == 0 {
		cfg.Settings.Concurrency.BatchSize = 1000
	}
}

// ToSourceConfig converts a v2 source config to the connector-level config.
func ToSourceConfig(s SourceConfig) config.SourceConfig {
	cfg := config.SourceConfig{
		Type:               s.Type,
		Connection:         s.URL,
		URL:                s.URL,
		Table:              s.Table,
		Query:              s.Query,
		WatermarkColumn:    s.Watermark,
		ExcludeColumns:     append([]string(nil), s.Exclude...),
		BatchSize:          s.BatchSize,
		ExtractParallelism: s.Parallelism,
		Options: map[string]string{
			"project":          s.Project,
			"dataset":          s.Dataset,
			"credentials_file": s.CredentialsFile,
			"credentials_json": s.CredentialsJSON,
			"group_id":         s.GroupID,
			"path":             s.Path,
			"secret":           s.Secret,
		},
	}
	if s.Auth != nil {
		cfg.Auth = config.AuthConfig{
			Type:         s.Auth.Type,
			Token:        s.Auth.Token,
			Key:          s.Auth.Key,
			Value:        s.Auth.Value,
			InHeader:     s.Auth.InHeader,
			Username:     s.Auth.Username,
			Password:     s.Auth.Password,
			ClientID:     s.Auth.ClientID,
			ClientSecret: s.Auth.ClientSecret,
			TokenURL:     s.Auth.TokenURL,
			Scopes:       append([]string(nil), s.Auth.Scopes...),
		}
	}
	return cfg
}

// ToStreamingConfig converts a v2 source config to the connector-level streaming config.
func ToStreamingConfig(s SourceConfig) config.StreamingConfig {
	cfg := config.StreamingConfig{
		Type:     s.Type,
		Broker:   strings.Join(s.Brokers, ","),
		GroupID:  s.GroupID,
		Topic:    s.Topic,
		Endpoint: s.URL,
		Path:     s.Path,
		Secret:   s.Secret,
		Options: map[string]string{
			"table":       s.Table,
			"slot":        s.Slot,
			"publication": s.Publication,
		},
	}
	if s.Dedup != nil {
		cfg.Dedup = config.DedupConfig{
			WindowMs: durationToMillis(s.Dedup.Window),
			Key:      s.Dedup.Key,
		}
	}
	if s.Flush != nil {
		cfg.Flush = config.FlushConfig{
			IntervalMs: durationToMillis(s.Flush.Interval),
			Records:    s.Flush.Records,
		}
	}
	return cfg
}

// AlsoToStreamingConfig converts a v2 also config to the connector-level streaming config.
func AlsoToStreamingConfig(a AlsoConfig) config.StreamingConfig {
	cfg := config.StreamingConfig{
		Type:    a.Type,
		Broker:  strings.Join(a.Brokers, ","),
		GroupID: a.GroupID,
		Topic:   a.Topic,
		Path:    a.Path,
		Secret:  a.Secret,
		Options: map[string]string{},
	}
	if a.Dedup != nil {
		cfg.Dedup = config.DedupConfig{
			WindowMs: durationToMillis(a.Dedup.Window),
			Key:      a.Dedup.Key,
		}
	}
	if a.Flush != nil {
		cfg.Flush = config.FlushConfig{
			IntervalMs: durationToMillis(a.Flush.Interval),
			Records:    a.Flush.Records,
		}
	}
	return cfg
}

// ToStateConfig converts v2 settings to the connector-level state config.
func ToStateConfig(s SettingsConfig) config.StateConfig {
	return config.StateConfig{
		Backend:      s.State.Backend,
		Path:         s.State.Path,
		Connection:   s.State.Connection,
		DeliveredTTL: s.State.DeliveredTTL,
		KeyPrefix:    s.State.KeyPrefix,
	}
}

func durationToMillis(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return int(d / time.Millisecond)
}

// ToDestinationConfig converts a v2 destination config to the connector-level config.
func ToDestinationConfig(d DestinationConfig) config.DestinationConfig {
	cfg := config.DestinationConfig{
		Type:       d.Type,
		Connection: d.URL,
		URL:        d.URL,
		MatchOn:    strings.Join(d.MatchOn, ","),
		Strategy:   d.Strategy,
		Options: map[string]string{
			"table":   d.Table,
			"object":  d.Object,
			"message": d.Message,
			"webhook": d.Webhook,
		},
	}
	if d.Auth != nil {
		cfg.Auth = config.AuthConfig{
			Type:         d.Auth.Type,
			Token:        d.Auth.Token,
			Key:          d.Auth.Key,
			Value:        d.Auth.Value,
			InHeader:     d.Auth.InHeader,
			Username:     d.Auth.Username,
			Password:     d.Auth.Password,
			ClientID:     d.Auth.ClientID,
			ClientSecret: d.Auth.ClientSecret,
			TokenURL:     d.Auth.TokenURL,
			Scopes:       append([]string(nil), d.Auth.Scopes...),
		}
	}
	if d.RateLimit != "" {
		parts := strings.SplitN(d.RateLimit, "/", 2)
		if len(parts) == 2 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				cfg.RateLimit = config.RateLimitConfig{
					Requests: n,
					Period:   strings.TrimSpace(parts[1]),
				}
			}
		}
	}
	return cfg
}
