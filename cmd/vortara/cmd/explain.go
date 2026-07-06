package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/decision"
	"github.com/rkshvish/vortara/internal/diff"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var (
	explainEntityKey string
	explainJSON      bool
)

var explainCmd = &cobra.Command{
	Use:   "explain <sync.yaml>",
	Short: "Explain the current decision and field changes for one entity",
	Args:  cobra.ExactArgs(1),
	RunE:  runExplain,
}

func init() {
	explainCmd.Flags().StringVar(&explainEntityKey, "key", "", "Entity key to explain (required)")
	_ = explainCmd.MarkFlagRequired("key")
	explainCmd.Flags().BoolVar(&explainJSON, "json", false, "Output as JSON")
}

// --- output types (also used as JSON schema) ---

type explainFieldChange struct {
	Previous any `json:"previous"`
	Current  any `json:"current"`
}

type explainRuleTrace struct {
	Rule        string `json:"rule"`
	Action      string `json:"action,omitempty"`
	Matched     bool   `json:"matched"`
	Reason      string `json:"reason"`
	FiredBefore bool   `json:"fired_before,omitempty"`
	Winner      bool   `json:"winner,omitempty"`
}

type explainSafety struct {
	MaxCreatesPerRun int `json:"max_creates_per_run,omitempty"`
	MaxUpdatesPerRun int `json:"max_updates_per_run,omitempty"`
	MaxDeletesPerRun int `json:"max_deletes_per_run,omitempty"`
}

type explainHistoryItem struct {
	RunID    int64    `json:"run_id"`
	Decision string   `json:"decision"`
	Rules    []string `json:"triggered_rules,omitempty"`
	Reasons  []string `json:"reasons,omitempty"`
	At       string   `json:"at"`
}

type explainOutput struct {
	EntityKey          string                        `json:"entity_key"`
	SyncName           string                        `json:"sync_name"`
	Destination        string                        `json:"destination"`
	DestID             string                        `json:"dest_id,omitempty"`
	Version            int                           `json:"version"`
	LastDecision       string                        `json:"last_decision"`
	LastStatus         string                        `json:"last_status"`
	UpdatedAt          string                        `json:"updated_at"`
	FingerprintChanged bool                          `json:"fingerprint_changed"`
	Decision           string                        `json:"decision"` // what would happen if run now
	Rules              []explainRuleTrace            `json:"rules,omitempty"`
	FieldChanges       map[string]explainFieldChange `json:"field_changes,omitempty"`
	RememberedState    map[string]any                `json:"remembered_state,omitempty"`
	Safety             *explainSafety                `json:"safety,omitempty"`
	RecentHistory      []explainHistoryItem          `json:"recent_history,omitempty"`
}

func runExplain(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	s := f.Sync
	destType := s.Destination.Type

	es, err := store.GetEntityState(ctx, s.Name, destType, explainEntityKey)
	if err != nil {
		return err
	}
	if es == nil {
		fmt.Fprintf(os.Stderr, "Entity %q has not been seen yet in sync %q.\n", explainEntityKey, s.Name)
		os.Exit(1)
	}

	// Reconstruct the decision input from stored state.
	fpChanged := es.PreviousFingerprint != es.CurrentFingerprint && es.PreviousFingerprint != ""
	fieldDiff := diff.Compute(es.PreviousPayload, es.CurrentPayload)
	in := decision.Input{
		IsFirstSeen:        es.PreviousFingerprint == "",
		FingerprintChanged: fpChanged,
		Diff:               fieldDiff,
		PreviousPayload:    es.PreviousPayload,
		CurrentPayload:     es.CurrentPayload,
		RememberedState:    es.RememberedState,
	}

	plan, traces, err := decision.Trace(ctx, s.Decisions, in, s.Name, destType, explainEntityKey, store)
	if err != nil {
		return fmt.Errorf("decision trace: %w", err)
	}

	history, err := store.GetDecisionHistory(ctx, s.Name, destType, explainEntityKey, 10)
	if err != nil {
		return err
	}

	// Build output struct.
	out := explainOutput{
		EntityKey:          explainEntityKey,
		SyncName:           s.Name,
		Destination:        destType,
		DestID:             es.DestinationID,
		Version:            es.Version,
		LastDecision:       es.LastDecision,
		LastStatus:         es.LastStatus,
		UpdatedAt:          es.UpdatedAt.UTC().Format(time.RFC3339),
		FingerprintChanged: fpChanged,
		Decision:           string(plan.Action),
	}

	for _, tr := range traces {
		out.Rules = append(out.Rules, explainRuleTrace{
			Rule:        tr.Rule,
			Action:      tr.Action,
			Matched:     tr.Matched,
			Reason:      tr.Reason,
			FiredBefore: tr.FiredBefore,
			Winner:      tr.Winner,
		})
	}
	// Default action appended as a synthetic rule.
	if len(traces) == 0 {
		out.Rules = append(out.Rules, explainRuleTrace{
			Rule:    "(default)",
			Action:  string(plan.Action),
			Matched: true,
			Reason:  "no rules configured, using default",
			Winner:  true,
		})
	}

	if !fieldDiff.IsEmpty() {
		out.FieldChanges = make(map[string]explainFieldChange, len(fieldDiff))
		for field, ch := range fieldDiff {
			out.FieldChanges[field] = explainFieldChange{Previous: ch.Previous, Current: ch.Current}
		}
	}

	if len(es.RememberedState) > 0 {
		out.RememberedState = es.RememberedState
	}

	if s.Safety.MaxCreatesPerRun > 0 || s.Safety.MaxUpdatesPerRun > 0 || s.Safety.MaxDeletesPerRun > 0 {
		out.Safety = &explainSafety{
			MaxCreatesPerRun: s.Safety.MaxCreatesPerRun,
			MaxUpdatesPerRun: s.Safety.MaxUpdatesPerRun,
			MaxDeletesPerRun: s.Safety.MaxDeletesPerRun,
		}
	}

	for _, ev := range history {
		out.RecentHistory = append(out.RecentHistory, explainHistoryItem{
			RunID:    ev.RunID,
			Decision: ev.Decision,
			Rules:    ev.TriggeredRules,
			Reasons:  ev.Reasons,
			At:       ev.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	if explainJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	printExplain(out, s)
	return nil
}

func printExplain(out explainOutput, s synccfg.SyncSpec) {
	fmt.Printf("Entity:      %s\n", out.EntityKey)
	fmt.Printf("Sync:        %s\n", out.SyncName)
	fmt.Printf("Destination: %s\n", out.Destination)
	if out.DestID != "" {
		fmt.Printf("Dest ID:     %s\n", out.DestID)
	}
	fmt.Printf("Version:     %d\n", out.Version)
	fmt.Printf("Last action: %s (%s)\n", out.LastDecision, out.LastStatus)
	fmt.Printf("Updated:     %s\n", out.UpdatedAt)

	// Field changes.
	if len(out.FieldChanges) > 0 {
		fmt.Println("\nField changes (last run → current):")
		fields := make([]string, 0, len(out.FieldChanges))
		for f := range out.FieldChanges {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		for _, f := range fields {
			ch := out.FieldChanges[f]
			fmt.Printf("  %-30s %v → %v\n", f, formatVal(ch.Previous), formatVal(ch.Current))
		}
	}

	// Decision trace.
	fmt.Printf("\nDecision: %s\n", strings.ToUpper(out.Decision))
	if len(out.Rules) > 0 {
		fmt.Println("\nRules evaluated:")
		for _, tr := range out.Rules {
			if tr.FiredBefore {
				fmt.Printf("  ✗ %-30s [skipped: once:true already fired]\n", tr.Rule)
				continue
			}
			mark := "✗"
			extra := ""
			if tr.Matched {
				mark = "✓"
			}
			if tr.Winner {
				extra = " ← winner"
			}
			action := ""
			if tr.Action != "" {
				action = fmt.Sprintf("[→ %s]  ", tr.Action)
			}
			fmt.Printf("  %s %-30s %s%s%s\n", mark, tr.Rule, action, tr.Reason, extra)
		}
	}

	// Remembered state.
	if len(out.RememberedState) > 0 {
		fmt.Println("\nRemembered state:")
		keys := make([]string, 0, len(out.RememberedState))
		for k := range out.RememberedState {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %-30s %v\n", k, out.RememberedState[k])
		}
	}

	// Safety limits.
	if out.Safety != nil {
		fmt.Println("\nSafety limits:")
		if out.Safety.MaxCreatesPerRun > 0 {
			fmt.Printf("  max_creates_per_run:  %d\n", out.Safety.MaxCreatesPerRun)
		}
		if out.Safety.MaxUpdatesPerRun > 0 {
			fmt.Printf("  max_updates_per_run:  %d\n", out.Safety.MaxUpdatesPerRun)
		}
		if out.Safety.MaxDeletesPerRun > 0 {
			fmt.Printf("  max_deletes_per_run:  %d\n", out.Safety.MaxDeletesPerRun)
		}
	}
	if s.Safety.RequireApprovalAbove > 0 {
		fmt.Printf("  require_approval_above: %.0f\n", s.Safety.RequireApprovalAbove)
	}

	// Decision history.
	if len(out.RecentHistory) > 0 {
		fmt.Println("\nRecent decisions:")
		for _, h := range out.RecentHistory {
			rules := ""
			if len(h.Rules) > 0 {
				rules = " [" + strings.Join(h.Rules, ", ") + "]"
			}
			reasons := ""
			if len(h.Reasons) > 0 {
				reasons = "  — " + strings.Join(h.Reasons, "; ")
			}
			fmt.Printf("  %s  run=%-6d  %-10s%s%s\n", h.At, h.RunID, h.Decision, rules, reasons)
		}
	}
}

func formatVal(v any) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", v)
}
