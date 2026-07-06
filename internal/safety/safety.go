// Package safety enforces blast-radius limits on a sync run.
package safety

import (
	"fmt"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

// RunCounts tracks how many of each action have been executed in the current run.
type RunCounts struct {
	Creates int
	Updates int
	Deletes int
}

// Evaluator checks per-run action counts against configured limits.
type Evaluator struct {
	cfg synccfg.SafetyConfig
}

// New creates an Evaluator from a SafetyConfig.
func New(cfg synccfg.SafetyConfig) *Evaluator {
	return &Evaluator{cfg: cfg}
}

// Allow returns nil if the proposed action is within limits, or an error
// describing which limit would be exceeded.
func (e *Evaluator) Allow(action string, counts RunCounts) error {
	switch action {
	case "create", "upsert":
		if e.cfg.MaxCreatesPerRun > 0 && counts.Creates >= e.cfg.MaxCreatesPerRun {
			return fmt.Errorf("safety: max_creates_per_run (%d) reached", e.cfg.MaxCreatesPerRun)
		}
	case "update":
		if e.cfg.MaxUpdatesPerRun > 0 && counts.Updates >= e.cfg.MaxUpdatesPerRun {
			return fmt.Errorf("safety: max_updates_per_run (%d) reached", e.cfg.MaxUpdatesPerRun)
		}
	case "delete":
		if e.cfg.MaxDeletesPerRun > 0 && counts.Deletes >= e.cfg.MaxDeletesPerRun {
			return fmt.Errorf("safety: max_deletes_per_run (%d) reached", e.cfg.MaxDeletesPerRun)
		}
	}
	return nil
}

// CheckFieldRatios returns an error if any configured field change ratio would be exceeded.
// Call after all decisions are collected, before delivery, so the run can be aborted cleanly.
func (e *Evaluator) CheckFieldRatios(fieldChangeCounts map[string]int, totalEntities int) error {
	if totalEntities == 0 || len(e.cfg.BlockIfChangedFieldRatioAbove) == 0 {
		return nil
	}
	for field, threshold := range e.cfg.BlockIfChangedFieldRatioAbove {
		changed := fieldChangeCounts[field]
		ratio := float64(changed) / float64(totalEntities)
		if ratio > threshold {
			return fmt.Errorf("safety: field %q changed in %.1f%% of entities (limit %.1f%%) — use dry-run to review before proceeding",
				field, ratio*100, threshold*100)
		}
	}
	return nil
}

// Record increments the appropriate counter after an action succeeds.
func (e *Evaluator) Record(action string, counts *RunCounts) {
	switch action {
	case "create", "upsert":
		counts.Creates++
	case "update":
		counts.Updates++
	case "delete":
		counts.Deletes++
	}
}
