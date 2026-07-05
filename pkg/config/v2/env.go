package v2

import (
	"os"
	"strconv"
	"strings"
)

func resolveStr(value string, keyPath ...string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") && len(value) > 3 {
		varName := value[2 : len(value)-1]
		if v := os.Getenv(varName); v != "" {
			return v
		}
		return value
	}

	if len(keyPath) > 0 {
		if v := os.Getenv(buildEnvKey(keyPath...)); v != "" {
			return v
		}
	}

	return value
}

func buildEnvKey(parts ...string) string {
	if len(parts) == 0 {
		return "VORTARA__"
	}

	upper := make([]string, len(parts))
	for i, p := range parts {
		upper[i] = strings.ToUpper(strings.ReplaceAll(p, "-", "_"))
	}
	return "VORTARA__" + strings.Join(upper, "__")
}

func resolveList(values []string, keyPath ...string) []string {
	if len(keyPath) > 0 {
		if v := os.Getenv(buildEnvKey(keyPath...)); v != "" {
			parts := strings.Split(v, ",")
			out := make([]string, 0, len(parts))
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					out = append(out, part)
				}
			}
			return out
		}
	}

	out := make([]string, len(values))
	for i, value := range values {
		out[i] = resolveStr(value)
	}
	return out
}

func applyEnv(cfg *PipelineConfig) {
	cfg.Name = resolveStr(cfg.Name, "name")

	cfg.Source.Type = resolveStr(cfg.Source.Type, "source", "type")
	cfg.Source.URL = resolveStr(cfg.Source.URL, "source", "url")
	cfg.Source.Project = resolveStr(cfg.Source.Project, "source", "project")
	cfg.Source.Dataset = resolveStr(cfg.Source.Dataset, "source", "dataset")
	cfg.Source.Table = resolveStr(cfg.Source.Table, "source", "table")
	cfg.Source.Query = resolveStr(cfg.Source.Query, "source", "query")
	cfg.Source.Watermark = resolveStr(cfg.Source.Watermark, "source", "watermark")
	cfg.Source.Exclude = resolveList(cfg.Source.Exclude, "source", "exclude")
	cfg.Source.BatchSize = cfg.Source.BatchSize
	cfg.Source.Parallelism = cfg.Source.Parallelism
	cfg.Source.Brokers = resolveList(cfg.Source.Brokers, "source", "brokers")
	cfg.Source.Topic = resolveStr(cfg.Source.Topic, "source", "topic")
	cfg.Source.GroupID = resolveStr(cfg.Source.GroupID, "source", "group_id")
	cfg.Source.Path = resolveStr(cfg.Source.Path, "source", "path")
	cfg.Source.Secret = resolveStr(cfg.Source.Secret, "source", "secret")
	cfg.Source.CredentialsFile = resolveStr(cfg.Source.CredentialsFile, "source", "credentials_file")
	cfg.Source.CredentialsJSON = resolveStr(cfg.Source.CredentialsJSON, "source", "credentials_json")
	if cfg.Source.Auth != nil {
		resolveAuth(cfg.Source.Auth, "source", "auth")
	}
	if cfg.Source.Flush != nil {
		resolveFlush(cfg.Source.Flush, "source", "flush")
	}
	if cfg.Source.Dedup != nil {
		resolveDedup(cfg.Source.Dedup, "source", "dedup")
	}

	if cfg.Also != nil {
		cfg.Also.Type = resolveStr(cfg.Also.Type, "also", "type")
		cfg.Also.Brokers = resolveList(cfg.Also.Brokers, "also", "brokers")
		cfg.Also.Topic = resolveStr(cfg.Also.Topic, "also", "topic")
		cfg.Also.GroupID = resolveStr(cfg.Also.GroupID, "also", "group_id")
		cfg.Also.Path = resolveStr(cfg.Also.Path, "also", "path")
		cfg.Also.Secret = resolveStr(cfg.Also.Secret, "also", "secret")
		if cfg.Also.Flush != nil {
			resolveFlush(cfg.Also.Flush, "also", "flush")
		}
		if cfg.Also.Dedup != nil {
			resolveDedup(cfg.Also.Dedup, "also", "dedup")
		}
	}

	for i := range cfg.Destinations {
		idx := strconv.Itoa(i)
		d := &cfg.Destinations[i]
		d.Type = resolveStr(d.Type, "destinations", idx, "type")
		d.URL = resolveStr(d.URL, "destinations", idx, "url")
		d.Webhook = resolveStr(d.Webhook, "destinations", idx, "webhook")
		d.Table = resolveStr(d.Table, "destinations", idx, "table")
		d.Object = resolveStr(d.Object, "destinations", idx, "object")
		d.Message = resolveStr(d.Message, "destinations", idx, "message")
		d.Strategy = resolveStr(d.Strategy, "destinations", idx, "strategy")
		d.When = resolveStr(d.When, "destinations", idx, "when")
		d.RateLimit = resolveStr(d.RateLimit, "destinations", idx, "rate_limit")
		d.MatchOn = resolveList(d.MatchOn, "destinations", idx, "match_on")
		if d.Auth != nil {
			resolveAuth(d.Auth, "destinations", idx, "auth")
		}
	}

	for i := range cfg.Transform {
		step := &cfg.Transform[i]
		step.Filter = resolveStr(step.Filter, "transform", strconv.Itoa(i), "filter")
		if step.Rename != nil {
			for k, v := range step.Rename {
				step.Rename[k] = resolveStr(v, "transform", strconv.Itoa(i), "rename", k)
			}
		}
		if step.Add != nil {
			for k, v := range step.Add {
				step.Add[k] = resolveStr(v, "transform", strconv.Itoa(i), "add", k)
			}
		}
		step.Drop = resolveList(step.Drop, "transform", strconv.Itoa(i), "drop")
		step.Mask = resolveList(step.Mask, "transform", strconv.Itoa(i), "mask")
	}

	cfg.Settings.State.Backend = resolveStr(cfg.Settings.State.Backend, "settings", "state", "backend")
	cfg.Settings.State.Path = resolveStr(cfg.Settings.State.Path, "settings", "state", "path")
	cfg.Settings.State.Connection = resolveStr(cfg.Settings.State.Connection, "settings", "state", "connection")
	cfg.Settings.State.DeliveredTTL = resolveStr(cfg.Settings.State.DeliveredTTL, "settings", "state", "delivered_ttl")
	cfg.Settings.State.KeyPrefix = resolveStr(cfg.Settings.State.KeyPrefix, "settings", "state", "key_prefix")
	cfg.Settings.Log.Level = resolveStr(cfg.Settings.Log.Level, "settings", "log", "level")
	cfg.Settings.Log.Format = resolveStr(cfg.Settings.Log.Format, "settings", "log", "format")
	cfg.Settings.Limits.MaxRuntime = resolveStr(cfg.Settings.Limits.MaxRuntime, "settings", "limits", "max_runtime")
	cfg.Settings.OnError = resolveStr(cfg.Settings.OnError, "settings", "on_error")
}

func resolveAuth(a *AuthConfig, prefix ...string) {
	a.Type = resolveStr(a.Type, append(prefix, "type")...)
	a.Token = resolveStr(a.Token, append(prefix, "token")...)
	a.ClientID = resolveStr(a.ClientID, append(prefix, "client_id")...)
	a.ClientSecret = resolveStr(a.ClientSecret, append(prefix, "client_secret")...)
	a.TokenURL = resolveStr(a.TokenURL, append(prefix, "token_url")...)
	a.Scopes = resolveList(a.Scopes, append(prefix, "scopes")...)
	a.Key = resolveStr(a.Key, append(prefix, "key")...)
	a.Value = resolveStr(a.Value, append(prefix, "value")...)
	a.Username = resolveStr(a.Username, append(prefix, "username")...)
	a.Password = resolveStr(a.Password, append(prefix, "password")...)
}

func resolveFlush(f *FlushConfig, prefix ...string) {
	f.Interval = resolveStr(f.Interval, append(prefix, "interval")...)
}

func resolveDedup(d *DedupConfig, prefix ...string) {
	d.Window = resolveStr(d.Window, append(prefix, "window")...)
	d.Key = resolveStr(d.Key, append(prefix, "key")...)
}
