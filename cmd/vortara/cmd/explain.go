package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/connector/source"
	"github.com/rkshvish/vortara/internal/decision"
	"github.com/rkshvish/vortara/internal/diff"
	"github.com/rkshvish/vortara/internal/engine"
	"github.com/rkshvish/vortara/internal/fingerprint"
	"github.com/rkshvish/vortara/internal/registry"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

var (
	explainEntityKey string
	explainJSON      bool
)

var explainCmd = &cobra.Command{
	Use:   "explain <sync.yaml>",
	Short: "Explain the planned decision for one entity against its current source row",
	Args:  cobra.ExactArgs(1),
	RunE:  runExplain,
}

func init() {
	explainCmd.Flags().StringVar(&explainEntityKey, "key", "", "Entity key to explain (required)")
	_ = explainCmd.MarkFlagRequired("key")
	explainCmd.Flags().BoolVar(&explainJSON, "json", false, "Output as JSON")
}

// --- output types ---

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
	MaxCreatesPerRun     int     `json:"max_creates_per_run,omitempty"`
	MaxUpdatesPerRun     int     `json:"max_updates_per_run,omitempty"`
	MaxDeletesPerRun     int     `json:"max_deletes_per_run,omitempty"`
	RequireApprovalAbove float64 `json:"require_approval_above,omitempty"`
}

type explainMissingInfo struct {
	ConsecutiveMissing int    `json:"consecutive_missing"`
	Threshold          int    `json:"threshold"`
	ConfiguredAction   string `json:"configured_action"`
	AllowDestructive   bool   `json:"allow_destructive_actions"`
	Blocked            bool   `json:"blocked,omitempty"` // true when delete but allow_destructive_actions not set
}

type explainHistoryItem struct {
	RunID    int64    `json:"run_id"`
	Decision string   `json:"decision"`
	Rules    []string `json:"triggered_rules,omitempty"`
	Reasons  []string `json:"reasons,omitempty"`
	At       string   `json:"at"`
}

type explainOutput struct {
	EntityKey       string                        `json:"entity_key"`
	SyncName        string                        `json:"sync_name"`
	Destination     string                        `json:"destination"`
	DestURL         string                        `json:"destination_url,omitempty"`
	DestID          string                        `json:"dest_id,omitempty"`
	IdempotencyKey  string                        `json:"idempotency_key,omitempty"`
	Version         int                           `json:"version"`
	LastDecision    string                        `json:"last_decision,omitempty"`
	LastStatus      string                        `json:"last_status,omitempty"`
	UpdatedAt       string                        `json:"updated_at,omitempty"`
	SourceFound     bool                          `json:"source_found"`
	Decision        string                        `json:"decision"`
	MissingInfo     *explainMissingInfo           `json:"missing_info,omitempty"`
	Rules           []explainRuleTrace            `json:"rules,omitempty"`
	FieldChanges    map[string]explainFieldChange `json:"field_changes,omitempty"`
	RememberedState map[string]any                `json:"remembered_state,omitempty"`
	Safety          *explainSafety                `json:"safety,omitempty"`
	RecentHistory   []explainHistoryItem          `json:"recent_history,omitempty"`
}

func runExplain(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	s := f.Sync
	destType := s.Destination.Type

	// --- load existing entity state (may be nil for first_seen) ---
	es, err := store.GetEntityState(ctx, s.Name, destType, explainEntityKey)
	if err != nil {
		return err
	}

	// --- extract current row from the source ---
	var currentMapped map[string]any
	currentMapped, err = explainFetchCurrentRow(ctx, s, explainEntityKey)
	if err != nil {
		// Non-fatal: fall back to stored state if source is unavailable.
		fmt.Fprintf(os.Stderr, "warn: could not fetch current source row (%v); showing stored state only\n", err)
	}

	if currentMapped == nil && es == nil {
		fmt.Fprintf(os.Stderr, "Entity %q not found in source or state for sync %q.\n", explainEntityKey, s.Name)
		os.Exit(1)
	}

	// --- missing-from-source: entity is in state but not in the source ---
	// Compute what the engine would do and return early with a focused summary.
	if currentMapped == nil && es != nil {
		out := explainOutput{
			EntityKey:   explainEntityKey,
			SyncName:    s.Name,
			Destination: destType,
			DestURL:     s.Destination.URL,
			SourceFound: false,
			DestID:      es.DestinationID,
			Version:     es.Version,
			LastDecision: es.LastDecision,
			LastStatus:  es.LastStatus,
			UpdatedAt:   es.UpdatedAt.UTC().Format(time.RFC3339),
		}

		threshold := s.OnMissingFrom.AfterMissingRuns
		if threshold <= 0 {
			threshold = 1
		}
		consecutive := es.ConsecutiveMissing + 1
		configuredAction := strings.ToLower(s.OnMissingFrom.Action)
		if configuredAction == "" {
			configuredAction = "skip"
		}

		mi := &explainMissingInfo{
			ConsecutiveMissing: consecutive,
			Threshold:          threshold,
			ConfiguredAction:   configuredAction,
			AllowDestructive:   s.OnMissingFrom.AllowDestructive,
		}

		if consecutive >= threshold && configuredAction == "delete" {
			if !s.OnMissingFrom.AllowDestructive {
				mi.Blocked = true
				out.Decision = "blocked"
			} else if isArchiveDestination(s.Destination.Type) {
				out.Decision = "archive"
			} else {
				out.Decision = "delete"
			}
		} else if consecutive >= threshold {
			out.Decision = configuredAction
		} else {
			out.Decision = fmt.Sprintf("missing (%d/%d runs)", consecutive, threshold)
		}
		out.MissingInfo = mi

		history, _ := store.GetDecisionHistory(ctx, s.Name, destType, explainEntityKey, 10)
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

	// --- build the decision input ---
	var prevPayload map[string]any
	var prevFP string
	if es != nil {
		prevPayload = fingerprint.NormalizePayload(es.CurrentPayload)
		prevFP = es.CurrentFingerprint
	}

	// If we could not reach the source, reconstruct from stored state (last-run diff).
	if currentMapped == nil {
		currentMapped = fingerprint.NormalizePayload(es.CurrentPayload)
	}

	// Build fingerprint exclusion set matching engine logic.
	var fpExclude []string
	for _, m := range s.Mapping {
		if m.ExcludeFromFingerprint {
			fpExclude = append(fpExclude, m.DestName())
		}
	}
	fpExclude = append(fpExclude, s.State.Fingerprint.Exclude...)
	fpInclude := explainFPIncludeSet(s.State.Fingerprint.Include)

	curFP := fingerprint.Of(explainFPFields(currentMapped, fpInclude), fpExclude...)
	fieldDiff := diff.Compute(prevPayload, currentMapped)

	in := decision.Input{
		IsFirstSeen:        es == nil,
		FingerprintChanged: curFP != prevFP,
		Diff:               fieldDiff,
		PreviousPayload:    prevPayload,
		CurrentPayload:     currentMapped,
	}
	if es != nil {
		in.RememberedState = es.RememberedState
	}

	plan, traces, err := decision.Trace(ctx, s.Decisions, in, s.Name, destType, explainEntityKey, store)
	if err != nil {
		return fmt.Errorf("decision trace: %w", err)
	}

	// Deterministic idempotency key the engine would use.
	idempotencyKey := engine.ExplainDeliveryKey(s.Name, destType, explainEntityKey, string(plan.Action), curFP)

	history, _ := store.GetDecisionHistory(ctx, s.Name, destType, explainEntityKey, 10)

	// --- assemble output ---
	out := explainOutput{
		EntityKey:      explainEntityKey,
		SyncName:       s.Name,
		Destination:    destType,
		DestURL:        s.Destination.URL,
		IdempotencyKey: idempotencyKey,
		SourceFound:    currentMapped != nil,
		Decision:       string(plan.Action),
	}
	if es != nil {
		out.DestID = es.DestinationID
		out.Version = es.Version
		out.LastDecision = es.LastDecision
		out.LastStatus = es.LastStatus
		out.UpdatedAt = es.UpdatedAt.UTC().Format(time.RFC3339)
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
	if es != nil && len(es.RememberedState) > 0 {
		out.RememberedState = es.RememberedState
	}

	if s.Safety.MaxCreatesPerRun > 0 || s.Safety.MaxUpdatesPerRun > 0 ||
		s.Safety.MaxDeletesPerRun > 0 || s.Safety.RequireApprovalAbove > 0 {
		out.Safety = &explainSafety{
			MaxCreatesPerRun:     s.Safety.MaxCreatesPerRun,
			MaxUpdatesPerRun:     s.Safety.MaxUpdatesPerRun,
			MaxDeletesPerRun:     s.Safety.MaxDeletesPerRun,
			RequireApprovalAbove: s.Safety.RequireApprovalAbove,
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

// explainFetchCurrentRow extracts only the row matching entityKey from the source.
func explainFetchCurrentRow(ctx context.Context, s synccfg.SyncSpec, entityKey string) (map[string]any, error) {
	rawSrc, err := registry.GetBatchSource(s.Source.Type)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	src, ok := rawSrc.(source.BatchSource)
	if !ok {
		return nil, fmt.Errorf("source %q is not a BatchSource", s.Source.Type)
	}

	cfg := conncfg.SourceConfig{
		Type:       s.Source.Type,
		URL:        s.Source.URL,
		Connection: s.Source.URL,
		Table:      s.Source.Table,
		Query:      s.Source.Query,
		BatchSize:  s.Source.BatchSize,
		Options:    map[string]string{},
	}
	if s.Source.Watermark != nil {
		cfg.WatermarkColumn = s.Source.Watermark.Column
	}
	if s.Source.Auth != nil {
		cfg.Auth = conncfg.AuthConfig{
			Type: s.Source.Auth.Type, Token: s.Source.Auth.Token,
			ClientID: s.Source.Auth.ClientID, ClientSecret: s.Source.Auth.ClientSecret,
			TokenURL: s.Source.Auth.TokenURL,
			Username: s.Source.Auth.Username, Password: s.Source.Auth.Password,
		}
	}
	if err := src.Connect(ctx, cfg); err != nil {
		return nil, fmt.Errorf("source connect: %w", err)
	}
	defer src.Close()

	out := make(chan row.Row, 256)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	var found map[string]any
	for r := range out {
		k := fmt.Sprintf("%v", r.Data[s.Source.EntityKey])
		if k == entityKey {
			found = fingerprint.NormalizePayload(explainApplyMapping(r.Data, s.Mapping))
			// Drain the channel so the goroutine can finish.
			go func() {
				for range out {
				}
			}()
			break
		}
	}
	if err := <-done; err != nil && ctx.Err() == nil {
		return found, err
	}
	return found, nil
}

func explainApplyMapping(data map[string]any, mapping []synccfg.MappingEntry) map[string]any {
	if len(mapping) == 0 {
		out := make(map[string]any, len(data))
		for k, v := range data {
			out[k] = v
		}
		return out
	}
	out := make(map[string]any, len(mapping))
	for _, m := range mapping {
		if v, ok := data[m.Source]; ok {
			out[m.DestName()] = v
		}
	}
	return out
}

func explainFPIncludeSet(include []string) map[string]struct{} {
	if len(include) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(include))
	for _, f := range include {
		s[f] = struct{}{}
	}
	return s
}

func explainFPFields(data map[string]any, include map[string]struct{}) map[string]any {
	if include == nil {
		return data
	}
	out := make(map[string]any, len(include))
	for k, v := range data {
		if _, ok := include[k]; ok {
			out[k] = v
		}
	}
	return out
}

func printExplain(out explainOutput, s synccfg.SyncSpec) {
	fmt.Printf("Entity:      %s\n", out.EntityKey)
	fmt.Printf("Sync:        %s\n", out.SyncName)
	fmt.Printf("Destination: %s", out.Destination)
	if out.DestURL != "" {
		fmt.Printf("  (%s)", out.DestURL)
	}
	fmt.Println()
	if out.DestID != "" {
		fmt.Printf("Dest ID:     %s\n", out.DestID)
	}
	if !out.SourceFound && out.MissingInfo != nil {
		mi := out.MissingInfo
		fmt.Printf("\n  [entity not found in source — missing %d of %d run(s) required]\n", mi.ConsecutiveMissing, mi.Threshold)
		fmt.Printf("  Configured action: %s\n", mi.ConfiguredAction)
		if mi.AllowDestructive {
			fmt.Printf("  allow_destructive_actions: true\n")
		} else if mi.ConfiguredAction == "delete" {
			fmt.Printf("  allow_destructive_actions: NOT SET — archive is blocked\n")
		}
	} else if !out.SourceFound {
		fmt.Println("\n  [source row not available — showing stored state]")
	}
	if out.Version > 0 {
		fmt.Printf("Version:     %d\n", out.Version)
	}
	if out.LastDecision != "" {
		fmt.Printf("Last action: %s (%s) at %s\n", out.LastDecision, out.LastStatus, out.UpdatedAt)
	}

	// Field changes.
	if len(out.FieldChanges) > 0 {
		fmt.Println("\nField changes:")
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

	// Decision — with archive context for missing entities.
	decisionLabel := strings.ToUpper(out.Decision)
	if out.MissingInfo != nil && out.Decision == "archive" && out.DestID != "" {
		destLabel := out.Destination
		if isArchiveDestination(out.Destination) {
			destLabel = strings.Title(out.Destination) //nolint:staticcheck // Title is fine here
		}
		decisionLabel += fmt.Sprintf("\nReason:   missing from source for %d run(s)\nDestination: %s contact %s",
			out.MissingInfo.ConsecutiveMissing, destLabel, out.DestID)
	} else if out.MissingInfo != nil && out.Decision == "blocked" {
		decisionLabel = "BLOCKED\nReason:   on_missing_from_source.allow_destructive_actions not set to true"
	}
	fmt.Printf("\nDecision: %s\n", decisionLabel)

	// Rule trace.
	if len(out.Rules) > 0 {
		fmt.Println("\nRules evaluated:")
		for _, tr := range out.Rules {
			action := ""
			if tr.Action != "" {
				action = fmt.Sprintf("[→ %s] ", tr.Action)
			}
			switch {
			case tr.FiredBefore:
				fmt.Printf("  - %-30s %sskipped (once:true already fired)\n", tr.Rule, action)
			case tr.Winner:
				fmt.Printf("  ✓ %-30s %smatched, selected\n", tr.Rule, action)
			case tr.Matched:
				fmt.Printf("  ✓ %-30s %smatched, not selected (first-match-wins)\n", tr.Rule, action)
			default:
				fmt.Printf("  ✗ %-30s %sdid not match\n", tr.Rule, action)
			}
		}
	}

	// Idempotency key.
	if out.IdempotencyKey != "" {
		fmt.Printf("\nIdempotency key: %s\n", out.IdempotencyKey)
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

	// Safety.
	if out.Safety != nil {
		fmt.Println("\nSafety limits:")
		if out.Safety.MaxCreatesPerRun > 0 {
			fmt.Printf("  max_creates_per_run:    %d\n", out.Safety.MaxCreatesPerRun)
		}
		if out.Safety.MaxUpdatesPerRun > 0 {
			fmt.Printf("  max_updates_per_run:    %d\n", out.Safety.MaxUpdatesPerRun)
		}
		if out.Safety.MaxDeletesPerRun > 0 {
			fmt.Printf("  max_deletes_per_run:    %d\n", out.Safety.MaxDeletesPerRun)
		}
		if out.Safety.RequireApprovalAbove > 0 {
			fmt.Printf("  require_approval_above: %.0f\n", out.Safety.RequireApprovalAbove)
		}
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
