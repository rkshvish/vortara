package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/connector/source"
	"github.com/rkshvish/vortara/internal/decision"
	"github.com/rkshvish/vortara/internal/diff"
	"github.com/rkshvish/vortara/internal/fingerprint"
	"github.com/rkshvish/vortara/internal/registry"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

var diffOutputJSON bool

var diffCmd = &cobra.Command{
	Use:   "diff <sync.yaml>",
	Short: "Show what would change if run now (structured comparison vs stored state)",
	Args:  cobra.ExactArgs(1),
	RunE:  runDiff,
}

func init() {
	diffCmd.Flags().BoolVar(&diffOutputJSON, "json", false, "Output results as JSON")
}

// diffRecord holds one entity's diff summary.
type diffRecord struct {
	EntityKey string        `json:"entity_key"`
	Action    string        `json:"action"` // "create" | "update" | "skip" | "missing" | "would-delete"
	Rules     []string      `json:"rules,omitempty"`
	Fields    []fieldChange `json:"fields,omitempty"`
}

type fieldChange struct {
	Field string `json:"field"`
	From  any    `json:"from,omitempty"`
	To    any    `json:"to,omitempty"`
}

func runDiff(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	if err := synccfg.Validate(f); err != nil {
		return err
	}

	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	s := f.Sync
	destName := s.Destination.Type

	rawSrc, err := registry.GetBatchSource(s.Source.Type)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	src, ok := rawSrc.(source.BatchSource)
	if !ok {
		return fmt.Errorf("source %q is not a BatchSource", s.Source.Type)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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
		return fmt.Errorf("source connect: %w", err)
	}
	defer src.Close()

	// Build fingerprint exclusions from mapping and state config
	var fpExclude []string
	for _, m := range s.Mapping {
		if m.ExcludeFromFingerprint {
			fpExclude = append(fpExclude, m.DestName())
		}
	}
	fpExclude = append(fpExclude, s.State.Fingerprint.Exclude...)
	fpInclude := diffFPIncludeSet(s.State.Fingerprint.Include)

	extractCh := make(chan row.Row, 256)
	extractDone := make(chan error, 1)
	go func() {
		extractDone <- src.Extract(ctx, time.Time{}, time.Time{}, extractCh)
	}()

	var records []diffRecord
	seenKeys := make(map[string]struct{})
	var creates, updates, skips int

	for r := range extractCh {
		if ctx.Err() != nil {
			break
		}
		mapped := fingerprint.NormalizePayload(diffApplyMapping(r.Data, s.Mapping))
		entityKey := fmt.Sprintf("%v", r.Data[s.Source.EntityKey])
		if entityKey == "" || entityKey == "<nil>" {
			continue
		}
		seenKeys[entityKey] = struct{}{}

		es, _ := store.GetEntityState(ctx, s.Name, destName, entityKey)
		isFirstSeen := es == nil

		curFP := fingerprint.Of(diffFPFields(mapped, fpInclude), fpExclude...)
		var prevFP string
		var prevPayload map[string]any
		if !isFirstSeen {
			prevFP = es.CurrentFingerprint
			// Normalize prevPayload too: old state rows may pre-date normalization,
			// and JSON round-trip can produce json.Number instead of int/float.
			prevPayload = fingerprint.NormalizePayload(es.CurrentPayload)
		}

		fieldDiff := diff.Compute(prevPayload, mapped)
		in := decision.Input{
			IsFirstSeen:        isFirstSeen,
			FingerprintChanged: curFP != prevFP,
			Diff:               fieldDiff,
			PreviousPayload:    prevPayload,
			CurrentPayload:     mapped,
		}
		if !isFirstSeen {
			in.RememberedState = es.RememberedState
		}
		plan, err := decision.Evaluate(ctx, s.Decisions, in, s.Name, destName, entityKey, store)
		if err != nil {
			continue
		}

		rec := diffRecord{
			EntityKey: entityKey,
			Action:    string(plan.Action),
			Rules:     plan.TriggeredRules,
		}
		if !isFirstSeen && !plan.Skipped() {
			for field, fc := range fieldDiff {
				rec.Fields = append(rec.Fields, fieldChange{Field: field, From: fc.Previous, To: fc.Current})
			}
		}

		switch {
		case plan.Skipped():
			skips++
		case isFirstSeen:
			creates++
		default:
			updates++
		}
		records = append(records, rec)
	}
	<-extractDone

	// Entities in state but absent from this extraction pass
	knownStates, _ := store.ListEntityStates(ctx, s.Name, destName, 10000, 0)
	var missing int
	for _, es := range knownStates {
		if _, seen := seenKeys[es.EntityKey]; seen {
			continue
		}
		missing++
		consecutive := es.ConsecutiveMissing + 1
		action := "missing"
		if s.OnMissingFrom.Action != "" {
			threshold := s.OnMissingFrom.AfterMissingRuns
			if threshold <= 0 {
				threshold = 1
			}
			if consecutive >= threshold {
				action = "would-" + s.OnMissingFrom.Action
			} else {
				action = fmt.Sprintf("missing (%d/%d runs)", consecutive, threshold)
			}
		}
		records = append(records, diffRecord{EntityKey: es.EntityKey, Action: action})
	}

	if diffOutputJSON {
		return json.NewEncoder(os.Stdout).Encode(records)
	}

	fmt.Printf("Diff: %s → %s\n\n", s.Name, destName)
	fmt.Printf("  creates:  %d\n  updates:  %d\n  skips:    %d\n  missing:  %d\n\n",
		creates, updates, skips, missing)
	for _, rec := range records {
		if rec.Action == "skip" {
			continue
		}
		fmt.Printf("[%s] %s\n", rec.Action, rec.EntityKey)
		for _, fc := range rec.Fields {
			fmt.Printf("   %-20s  %v → %v\n", fc.Field, fc.From, fc.To)
		}
	}
	return nil
}

func diffFPIncludeSet(include []string) map[string]struct{} {
	if len(include) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(include))
	for _, f := range include {
		s[f] = struct{}{}
	}
	return s
}

func diffFPFields(data map[string]any, include map[string]struct{}) map[string]any {
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

func diffApplyMapping(data map[string]any, mapping []synccfg.MappingEntry) map[string]any {
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
