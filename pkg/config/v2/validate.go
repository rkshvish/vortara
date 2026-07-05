package v2

import (
	"fmt"
	"sort"
	"strings"

	"github.com/robfig/cron/v3"
)

func Validate(cfg *PipelineConfig) error {
	var errs []string

	if cfg.Name == "" {
		errs = append(errs, "name is required")
	}

	if cfg.Source.Type == "" {
		errs = append(errs, "source.type is required")
	}

	validSources := map[string]bool{
		"postgres":  true,
		"mysql":     true,
		"redshift":  true,
		"snowflake": true,
		"bigquery":  true,
		"restapi":   true,
		"kafka":        true,
		"webhook":      true,
		"postgres_cdc": true,
	}
	if cfg.Source.Type != "" && !validSources[cfg.Source.Type] {
		errs = append(errs, fmt.Sprintf("source.type %q unknown, valid: %s", cfg.Source.Type, strings.Join(sortedKeys(validSources), ", ")))
	}

	batchSources := map[string]bool{
		"postgres":  true,
		"mysql":     true,
		"redshift":  true,
		"snowflake": true,
		"bigquery":  true,
		"restapi":   true,
	}
	if batchSources[cfg.Source.Type] {
		if cfg.Source.Table == "" && cfg.Source.Query == "" {
			errs = append(errs, "source: table or query is required")
		}
		if cfg.Source.Table != "" && cfg.Source.Query != "" {
			errs = append(errs, "source: set either table or query, not both")
		}
	}

	if cfg.Source.Type == "kafka" && cfg.Source.Topic == "" {
		errs = append(errs, "source.topic is required for kafka")
	}
	if cfg.Source.Type == "webhook" && cfg.Source.Path == "" {
		errs = append(errs, "source.path is required for webhook")
	}
	if cfg.Source.Type == "postgres_cdc" {
		if cfg.Source.URL == "" {
			errs = append(errs, "source.url is required for postgres_cdc")
		}
		if cfg.Source.Table == "" {
			errs = append(errs, "source.table is required for postgres_cdc")
		}
	}

	if cfg.Also != nil {
		if cfg.Also.Type != "kafka" && cfg.Also.Type != "webhook" {
			errs = append(errs, "also.type must be kafka or webhook")
		}
	}

	if len(cfg.Destinations) == 0 {
		errs = append(errs, "at least one destination is required")
	}

	validDests := map[string]bool{
		"salesforce":   true,
		"mysql":        true,
		"hubspot":      true,
		"slack":        true,
		"postgres":     true,
		"snowflake":    true,
		"restapi":      true,
		"googlesheets": true,
	}
	validStrategies := map[string]bool{
		"merge":         true,
		"append":        true,
		"replace":       true,
		"delete+insert": true,
		"scd2":          true,
	}
	validAuth := map[string]bool{
		"oauth2":  true,
		"bearer":  true,
		"api_key": true,
		"basic":   true,
	}

	for i, d := range cfg.Destinations {
		prefix := fmt.Sprintf("destinations[%d]", i)
		if d.Type == "" {
			errs = append(errs, prefix+".type is required")
		} else if !validDests[d.Type] {
			errs = append(errs, fmt.Sprintf("%s.type %q unknown, valid: %s", prefix, d.Type, strings.Join(sortedKeys(validDests), ", ")))
		}
		strategy := d.Strategy
		if strategy == "" {
			if d.Type == "restapi" || d.Type == "slack" || d.Type == "googlesheets" {
				strategy = "append"
			} else {
				strategy = "merge"
			}
		}
		if !validStrategies[strategy] {
			errs = append(errs, fmt.Sprintf("%s.strategy %q unknown, valid: merge, append, replace, delete+insert, scd2", prefix, d.Strategy))
		}
		if strategy == "scd2" && d.Type != "postgres" {
			errs = append(errs, fmt.Sprintf("%s: strategy scd2 is currently supported only by the postgres destination", prefix))
		}
		if (strategy == "merge" || strategy == "delete+insert" || strategy == "scd2") && len(d.MatchOn) == 0 {
			errs = append(errs, fmt.Sprintf("%s: strategy %q requires match_on", prefix, d.Strategy))
		}
		if d.Auth != nil && !validAuth[d.Auth.Type] {
			errs = append(errs, fmt.Sprintf("%s.auth.type %q unknown, valid: oauth2, bearer, api_key, basic", prefix, d.Auth.Type))
		}
	}

	if cfg.Cron != "" {
		if _, err := cron.ParseStandard(cfg.Cron); err != nil {
			errs = append(errs, fmt.Sprintf("cron %q invalid: %v", cfg.Cron, err))
		}
	}

	if cfg.Source.Watermark == "none" {
		for i, d := range cfg.Destinations {
			strategy := d.Strategy
			if strategy == "" {
				if d.Type == "restapi" || d.Type == "slack" || d.Type == "googlesheets" {
					strategy = "append"
				} else {
					strategy = "merge"
				}
			}
			if strategy == "append" {
				errs = append(errs, fmt.Sprintf("destinations[%d]: watermark: none re-extracts every row each run; strategy %q would duplicate them — use merge, replace, or delete+insert", i, strategy))
			}
		}
	}

	switch strings.ToLower(strings.TrimSpace(cfg.Settings.OnError)) {
	case "", "skip", "retry", "dlq":
	default:
		errs = append(errs, fmt.Sprintf("settings.on_error %q unknown, valid: skip, retry, dlq", cfg.Settings.OnError))
	}

	if cfg.Alerts != nil && cfg.Alerts.OnFailure != nil && strings.TrimSpace(cfg.Alerts.OnFailure.WebhookURL) == "" {
		errs = append(errs, "alerts.on_failure.webhook_url is required when alerts.on_failure is set")
	}


	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
