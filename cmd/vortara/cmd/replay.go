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

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/engine"
	"github.com/rkshvish/vortara/internal/fingerprint"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

var (
	replayDLQFile string
	replayKey     string
	replayDryRun  bool
)

var replayCmd = &cobra.Command{
	Use:   "replay <sync.yaml>",
	Short: "Re-deliver failed entities from DLQ or by entity key",
	Args:  cobra.ExactArgs(1),
	RunE:  runReplay,
}

func init() {
	replayCmd.Flags().StringVar(&replayDLQFile, "dlq", "", "DLQ file to replay (default: sync's configured DLQ path)")
	replayCmd.Flags().StringVar(&replayKey, "key", "", "Replay a single entity by key")
	replayCmd.Flags().BoolVar(&replayDryRun, "dry-run", false, "Print rows that would be replayed without delivering")
}

func runReplay(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	if err := synccfg.Validate(f); err != nil {
		return err
	}

	s := f.Sync

	// Determine which records to replay
	var records []engine.DLQRecord

	if replayKey != "" {
		// Replay a single entity by key — load its state payload
		store, err := openStateStore(f)
		if err != nil {
			return err
		}
		defer store.Close()

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		es, err := store.GetEntityState(ctx, s.Name, s.Destination.Type, replayKey)
		if err != nil {
			return fmt.Errorf("get entity state: %w", err)
		}
		if es == nil {
			return fmt.Errorf("entity %q not found in state for sync %q", replayKey, s.Name)
		}
		records = []engine.DLQRecord{{
			SyncName:  s.Name,
			EntityKey: replayKey,
			Data:      es.CurrentPayload,
		}}
	} else {
		// Replay from DLQ file
		dlqPath := replayDLQFile
		if dlqPath == "" {
			dlqPath = engine.ResolveDLQPath(s.Name, s.Errors.ResolvedDLQPath())
		}
		records, err = loadDLQRecords(dlqPath)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			fmt.Printf("No records to replay from %s\n", dlqPath)
			return nil
		}
	}

	if replayDryRun {
		fmt.Printf("Would replay %d record(s) from sync %q:\n", len(records), s.Name)
		for _, rec := range records {
			b, _ := json.MarshalIndent(rec.Data, "  ", "  ")
			fmt.Printf("  [%s]\n  %s\n", rec.EntityKey, b)
		}
		return nil
	}

	// Open destination and deliver
	rawDest, err := registry.GetDestination(s.Destination.Type)
	if err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	dest, ok := rawDest.(destination.Destination)
	if !ok {
		return fmt.Errorf("destination %q is not a Destination", s.Destination.Type)
	}

	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	baseCtx := vlogger.WithContext(context.Background(), vlogger.New("info", "text"))
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	destCfg := conncfg.DestinationConfig{
		Type:       s.Destination.Type,
		URL:        s.Destination.URL,
		Connection: s.Destination.URL,
		Options:    map[string]string{"object": s.Destination.Object, "table": s.Destination.Table},
	}
	if s.Destination.Auth != nil {
		destCfg.Auth = conncfg.AuthConfig{
			Type: s.Destination.Auth.Type, Token: s.Destination.Auth.Token,
			ClientID: s.Destination.Auth.ClientID, ClientSecret: s.Destination.Auth.ClientSecret,
			TokenURL: s.Destination.Auth.TokenURL,
			Username: s.Destination.Auth.Username, Password: s.Destination.Auth.Password,
		}
	}
	if err := dest.Connect(ctx, destCfg); err != nil {
		return fmt.Errorf("destination connect: %w", err)
	}
	defer dest.Close()

	// Reset delivery idempotency for these rows so they can be re-sent.
	if err := store.BeginBatch(ctx); err != nil {
		return err
	}

	var loaded, errored int
	for _, rec := range records {
		// Use the same deterministic key the engine would generate. We re-derive
		// it from the stored payload fingerprint so idempotency works across retries.
		data := fingerprint.NormalizePayload(rec.Data)
		curFP := fingerprint.Of(data)
		opKey := engine.ExplainDeliveryKey(rec.SyncName, s.Destination.Type, rec.EntityKey, "replay", curFP)

		r := row.Row{
			ID:         opKey,
			PrimaryKey: rec.EntityKey,
			Data:       data,
		}
		res, loadErr := dest.Load(ctx, []row.Row{r}, store, s.Name, s.Destination.Type)
		loaded += res.Loaded

		if loadErr != nil {
			errored++
			fmt.Fprintf(os.Stderr, "error replaying %s: %v\n", rec.EntityKey, loadErr)
			continue
		}
		for _, re := range res.Errors {
			errored++
			fmt.Fprintf(os.Stderr, "error replaying %s: %v\n", rec.EntityKey, re.Err)
		}

		// Update entity state to "success" only when delivery actually landed.
		if res.Loaded > 0 && len(res.Errors) == 0 {
			es, _ := store.GetEntityState(ctx, s.Name, s.Destination.Type, rec.EntityKey)
			var version int
			if es != nil {
				version = es.Version
			}
			newState := &state.EntityState{
				SyncName:           s.Name,
				Destination:        s.Destination.Type,
				EntityKey:          rec.EntityKey,
				CurrentFingerprint: curFP,
				CurrentPayload:     data,
				LastDecision:       "replay",
				LastStatus:         "success",
				Version:            version + 1,
				UpdatedAt:          time.Now().UTC(),
			}
			if es != nil {
				newState.PreviousFingerprint = es.CurrentFingerprint
				newState.PreviousPayload = es.CurrentPayload
				newState.RememberedState = es.RememberedState
				newState.DestinationID = es.DestinationID
			}
			_ = store.SaveEntityState(ctx, newState)
		}
	}

	if err := store.CommitBatch(ctx); err != nil {
		_ = store.RollbackBatch()
		return err
	}

	fmt.Printf("Replay complete: loaded=%d errors=%d\n", loaded, errored)
	return nil
}

func loadDLQRecords(path string) ([]engine.DLQRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var records []engine.DLQRecord
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var rec engine.DLQRecord
		if err := json.Unmarshal(line, &rec); err == nil {
			records = append(records, rec)
		}
	}
	return records, nil
}
