package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/engine"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/state"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

var dryRunCmd = &cobra.Command{
	Use:   "dry-run <sync.yaml>",
	Short: "Show what would happen without writing to the destination",
	Args:  cobra.ExactArgs(1),
	RunE:  runDryRun,
}

func runDryRun(_ *cobra.Command, args []string) error {
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

	eng := engine.NewEngine(store)
	eng.SetDryRunDestination(&printDestination{name: f.Sync.Destination.Type})
	defer eng.Close()

	baseCtx := vlogger.WithContext(context.Background(), vlogger.New("warn", "text"))
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Dry run: %s (no writes to %s)\n\n", f.Sync.Name, f.Sync.Destination.Type)
	if err := eng.Run(ctx, f); err != nil {
		// Show approval info and then surface the error.
		if st := eng.LastStats(); st != nil && st.ApprovalRequired {
			fmt.Printf("\n--- Approval Required ---\n")
			fmt.Printf("Re-run with: vortara run --approve-snapshot %s %s\n", st.ApprovalHash, f.Sync.Name)
		}
		return err
	}
	printDryRunSummary(eng.LastStats(), f)
	return nil
}

func printDryRunSummary(st *state.RunStats, f *synccfg.SyncFile) {
	if st == nil {
		return
	}
	fmt.Printf("\n--- Summary ---\n")
	fmt.Printf("  %-12s %d\n", "extracted:", st.RowsExtracted)
	fmt.Printf("  %-12s %d\n", "create:", st.Creates)
	fmt.Printf("  %-12s %d\n", "update:", st.Updates)
	fmt.Printf("  %-12s %d\n", "delete:", st.Deletes)
	fmt.Printf("  %-12s %d\n", "skip:", st.RowsSkipped)
	fmt.Printf("  %-12s %d\n", "error:", st.RowsErrored)

	if len(st.FieldChangeCounts) > 0 {
		fmt.Printf("\n--- Changed Fields ---\n")
		type kv struct {
			field string
			count int
		}
		var pairs []kv
		for f, c := range st.FieldChangeCounts {
			pairs = append(pairs, kv{f, c})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].count != pairs[j].count {
				return pairs[i].count > pairs[j].count
			}
			return pairs[i].field < pairs[j].field
		})
		for _, p := range pairs {
			pct := 0.0
			if st.RowsExtracted > 0 {
				pct = float64(p.count) / float64(st.RowsExtracted) * 100
			}
			fmt.Printf("  %-30s %d (%.1f%%)\n", p.field, p.count, pct)
		}
	}

	// Safety projection.
	safety := f.Sync.Safety
	if safety.RequireApprovalAbove > 0 || len(safety.RequireApprovalFor) > 0 {
		total := st.Creates + st.Updates + st.Deletes
		fmt.Printf("\n--- Safety ---\n")
		if safety.RequireApprovalAbove > 0 {
			status := "OK"
			if float64(total) > safety.RequireApprovalAbove {
				status = "REQUIRES APPROVAL"
			}
			fmt.Printf("  require_approval_above %.0f: %d deliveries — %s\n", safety.RequireApprovalAbove, total, status)
		}
		for _, action := range safety.RequireApprovalFor {
			var count int
			switch action {
			case "delete":
				count = st.Deletes
			case "create":
				count = st.Creates
			case "update":
				count = st.Updates
			}
			status := "OK"
			if count > 0 {
				status = "REQUIRES APPROVAL"
			}
			fmt.Printf("  require_approval_for %s: %d — %s\n", action, count, status)
		}
	}
}

// printDestination prints rows to stdout without delivering them.
type printDestination struct {
	name string
}

var _ destination.Destination = (*printDestination)(nil)

func (d *printDestination) Connect(_ context.Context, _ conncfg.DestinationConfig) error { return nil }

func (d *printDestination) Load(_ context.Context, rows []row.Row, _ state.StateStore, _, _ string) (destination.LoadResult, error) {
	for _, r := range rows {
		b, _ := json.MarshalIndent(r.Data, "  ", "  ")
		fmt.Printf("[dry-run -> %s] entity=%s\n  %s\n", d.name, r.PrimaryKey, string(b))
	}
	return destination.LoadResult{Loaded: len(rows)}, nil
}

func (d *printDestination) Close() error { return nil }
