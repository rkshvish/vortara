package sync

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load reads and parses a sync YAML file.
func Load(path string) (*SyncFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse parses raw YAML bytes into a SyncFile.
func Parse(data []byte) (*SyncFile, error) {
	var f SyncFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	applyEnv(&f)
	applyDefaults(&f)
	return &f, nil
}

func applyDefaults(f *SyncFile) {
	s := &f.Sync
	if s.Source.BatchSize == 0 {
		s.Source.BatchSize = 1000
	}
	if s.State.Backend == "" {
		s.State.Backend = "sqlite"
	}
	if s.State.Path == "" && s.State.Backend == "sqlite" {
		s.State.Path = "./state/sync.db"
	}
	if s.Decisions.Default == "" {
		s.Decisions.Default = "skip"
	}
	if s.Errors.OnError == "" {
		s.Errors.OnError = "skip"
	}
	if s.Errors.MaxRetries == 0 {
		s.Errors.MaxRetries = 3
	}
	if s.OnMissingFrom.Action == "" {
		s.OnMissingFrom.Action = "skip"
	}
	if s.OnMissingFrom.AfterMissingRuns == 0 {
		s.OnMissingFrom.AfterMissingRuns = 1
	}
}
