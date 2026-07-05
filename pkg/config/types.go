package config

// SourceConfig is the connector-level source configuration used by batch sources.
// Populated by pipeline.ToSourceConfig — never unmarshaled from YAML directly.
type SourceConfig struct {
	Type               string
	Connection         string // v2: source.url
	URL                string // v2: source.url (alias; some connectors prefer URL)
	Table              string
	Query              string
	WatermarkColumn    string
	ExcludeColumns     []string
	BatchSize          int
	ExtractParallelism int
	Options            map[string]string
	Auth               AuthConfig
}

// StreamingConfig is the connector-level streaming configuration.
// Populated by pipeline.ToStreamingConfig — never unmarshaled from YAML directly.
type StreamingConfig struct {
	Type     string
	Broker   string
	GroupID  string
	Topic    string
	Endpoint string // v2: source.url for non-broker streaming (e.g. webhook)
	Path     string
	Secret   string
	Dedup    DedupConfig
	Flush    FlushConfig
	Options  map[string]string
}

// DestinationConfig is the connector-level destination configuration.
// Populated by pipeline.ToDestinationConfig — never unmarshaled from YAML directly.
type DestinationConfig struct {
	Type             string
	Connection       string // v2: destination.url (alias for DB destinations)
	URL              string // v2: destination.url
	MatchOn          string // comma-separated match keys
	Strategy         string
	WriteParallelism int
	Method           string
	Headers          map[string]string
	Options          map[string]string
	Auth             AuthConfig
	Retry            RetryConfig
	RateLimit        RateLimitConfig
	CircuitBreaker   CircuitBreakerConfig
}

// AuthConfig holds auth credentials for any connector.
type AuthConfig struct {
	Type         string
	Token        string
	Key          string
	Value        string
	InHeader     bool
	Username     string
	Password     string
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
	inHeaderSet  bool
}

// InHeaderSpecified reports whether InHeader was explicitly provided.
// Always false for configs created via pipeline.ToDestinationConfig; used by
// internal/connector/http/auth.go to fall back to type-based defaults.
func (a AuthConfig) InHeaderSpecified() bool { return a.inHeaderSet }

// RetryConfig defines retry and backoff behavior for HTTP destinations.
type RetryConfig struct {
	Attempts  int
	BackoffMs int
	BackoffOn []int
	DropOn    []int
}

// RateLimitConfig defines token-bucket rate limiting for HTTP destinations.
type RateLimitConfig struct {
	Requests int
	Period   string
}

// CircuitBreakerConfig defines consecutive-failure protection for HTTP destinations.
type CircuitBreakerConfig struct {
	Threshold  int
	CooldownMs int
}

// FlushConfig defines streaming batch flush behavior.
type FlushConfig struct {
	IntervalMs int
	Records    int
}

// DedupConfig defines the streaming event deduplication window.
type DedupConfig struct {
	WindowMs int
	Key      string
}

// StateConfig defines where pipeline state is stored.
type StateConfig struct {
	Backend      string
	Path         string
	Connection   string
	DeliveredTTL string
	KeyPrefix    string
}
