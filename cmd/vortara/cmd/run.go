package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/engine"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/state"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var (
	runForceUnlock bool
	runApproveHash string
)

var runCmd = &cobra.Command{
	Use:   "run <sync.yaml>",
	Short: "Run a sync",
	Args:  cobra.ExactArgs(1),
	RunE:  runSync,
}

func init() {
	runCmd.Flags().BoolVar(&runForceUnlock, "force-unlock", false, "Clear a stale pipeline lock before running")
	runCmd.Flags().StringVar(&runApproveHash, "approve-snapshot", "", "Approval hash from a previous dry-run to bypass the approval gate")
}

func runSync(cmd *cobra.Command, args []string) error {
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

	if runForceUnlock {
		ctx := context.Background()
		if unlockErr := store.UnlockRun(ctx, f.Sync.Name); unlockErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: force-unlock failed: %v\n", unlockErr)
		} else {
			fmt.Printf("Force-unlocked sync %q\n", f.Sync.Name)
		}
	}

	eng := engine.NewEngine(store)
	if runApproveHash != "" {
		eng.SetApprovalHash(runApproveHash)
	}
	defer eng.Close()

	baseCtx := vlogger.WithContext(
		context.Background(),
		vlogger.New("info", "text"),
	)
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Running sync: %s\n", f.Sync.Name)
	start := time.Now()
	if err := eng.Run(ctx, f); err != nil {
		fmt.Fprintf(os.Stderr, "Sync error: %v\n", err)
		return err
	}

	if lastRun, err := eng.Store().GetLastRun(ctx, f.Sync.Name); err == nil {
		fmt.Printf("Done: loaded=%d skipped=%d errors=%d duration=%s\n",
			lastRun.RowsLoaded,
			lastRun.RowsSkipped,
			lastRun.RowsErrored,
			lastRun.FinishedAt.Sub(lastRun.StartedAt).Round(time.Millisecond),
		)
	} else {
		fmt.Printf("Done in %s\n", time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// openStateStore creates a StateStore from sync config.
func openStateStore(f *synccfg.SyncFile) (state.StateStore, error) {
	s := f.Sync.State
	store, err := state.Build(s.Backend, s.Path, s.Connection)
	if err != nil {
		return nil, fmt.Errorf("state store: %w", err)
	}
	return store, nil
}
