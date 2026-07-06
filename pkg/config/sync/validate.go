package sync

import (
	"fmt"
	"strings"
)

var validActions = map[string]bool{
	"upsert": true,
	"update": true,
	"create": true,
	"delete": true,
	"skip":   true,
}

var validOnError = map[string]bool{
	"skip":  true,
	"retry": true,
	"dlq":   true,
}

// Validate checks a SyncFile for required fields and invalid values.
func Validate(f *SyncFile) error {
	s := f.Sync
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("sync.name is required")
	}
	if err := validateSource(s.Source); err != nil {
		return err
	}
	if err := validateDest(s.Destination); err != nil {
		return err
	}
	for i, r := range s.Decisions.Rules {
		if err := validateRule(i, r); err != nil {
			return err
		}
	}
	if s.Decisions.Default != "" && !validActions[s.Decisions.Default] {
		return fmt.Errorf("sync.decisions.default %q must be one of: upsert, update, create, skip, delete", s.Decisions.Default)
	}
	if s.Errors.OnError != "" && !validOnError[s.Errors.OnError] {
		return fmt.Errorf("sync.errors.on_error %q must be one of: skip, retry, dlq", s.Errors.OnError)
	}
	return nil
}

func validateSource(s SourceConfig) error {
	if strings.TrimSpace(s.Type) == "" {
		return fmt.Errorf("sync.source.type is required")
	}
	if strings.TrimSpace(s.URL) == "" {
		return fmt.Errorf("sync.source.url is required")
	}
	if strings.TrimSpace(s.EntityKey) == "" {
		return fmt.Errorf("sync.source.entity_key is required")
	}
	return nil
}

func validateDest(d DestinationConfig) error {
	if strings.TrimSpace(d.Type) == "" {
		return fmt.Errorf("sync.destination.type is required")
	}
	return nil
}

func validateRule(idx int, r RuleConfig) error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("sync.decisions.rules[%d].name is required", idx)
	}
	if !validActions[r.Action] {
		return fmt.Errorf("sync.decisions.rules[%d] (%s): action %q must be one of: upsert, update, create, skip, delete", idx, r.Name, r.Action)
	}
	return nil
}
