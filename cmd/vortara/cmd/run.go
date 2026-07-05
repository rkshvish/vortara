package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/engine"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/state"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	v2config "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

var runOnce bool
var flagFullRefresh bool
var flagDryRun bool
var flagSince string

var runCmd = &cobra.Command{
	Use:   "run <pipeline.yaml>",
	Short: "Run a pipeline once",
	Args:  cobra.ExactArgs(1),
	RunE:  runPipeline,
}

func init() {
	runCmd.Flags().BoolVar(&runOnce, "once", true, "Run the pipeline once and exit")
	runCmd.Flags().BoolVar(&flagFullRefresh, "full-refresh", false, "Ignore watermark and extract all rows from the beginning")
	runCmd.Flags().BoolVar(&flagDryRun, "dry-run", false, "Extract and transform rows but skip destination writes. Prints rows to stdout.")
	runCmd.Flags().StringVar(&flagSince, "since", "", "Backfill: override the watermark with an explicit start date (RFC3339 or YYYY-MM-DD) for this run")
}

func runPipeline(cmd *cobra.Command, args []string) error {
	path := args[0]
	if !runOnce {
		runOnce = true
	}

	cfg, err := v2config.Load(path)
	if err != nil {
		return err
	}
	if err := v2config.Validate(cfg); err != nil {
		return err
	}

	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	if flagFullRefresh && flagSince != "" {
		_ = store.Close()
		return fmt.Errorf("--full-refresh and --since are mutually exclusive")
	}
	if flagFullRefresh {
		if err := store.SetWatermark(cmd.Context(), cfg.Name, sourceName(cfg), time.Time{}); err != nil {
			_ = store.Close()
			return err
		}
		if err := store.SetNumericWatermark(cmd.Context(), cfg.Name, sourceName(cfg), 0); err != nil {
			_ = store.Close()
			return err
		}
		fmt.Println("Full refresh: watermark reset, extracting all rows")
	}
	if flagSince != "" {
		if n, err := strconv.ParseInt(flagSince, 10, 64); err == nil {
			// Integer value: numeric-cursor backfill (extract ids > n).
			if err := store.SetNumericWatermark(cmd.Context(), cfg.Name, sourceName(cfg), n); err != nil {
				_ = store.Close()
				return err
			}
			fmt.Printf("Backfill: numeric cursor set to %d\n", n)
		} else {
			since, err := parseSince(flagSince)
			if err != nil {
				_ = store.Close()
				return err
			}
			if err := store.SetWatermark(cmd.Context(), cfg.Name, sourceName(cfg), since); err != nil {
				_ = store.Close()
				return err
			}
			fmt.Printf("Backfill: watermark set to %s\n", since.Format(time.RFC3339))
		}
	}

	eng := engine.NewEngine(store)
	defer eng.Close()

	if flagDryRun {
		for i, destCfg := range cfg.Destinations {
			eng.SetDestination(strconv.Itoa(i), &DryRunDestination{name: destCfg.Type})
		}
		fmt.Println("Dry run: no data will be written to destinations")
	}

	baseCtx := vlogger.WithContext(
		context.Background(),
		vlogger.New(cfg.Settings.Log.Level, cfg.Settings.Log.Format),
	)
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Running pipeline: %s\n", cfg.Name)
	start := time.Now()
	if err := eng.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Pipeline error: %v\n", err)
		return err
	}

	lastRun, err := eng.Store().GetLastRun(ctx, cfg.Name)
	if err == nil {
		fmt.Printf("✓ Done: loaded=%d skipped=%d errors=%d duration=%s\n",
			lastRun.RowsLoaded,
			lastRun.RowsSkipped,
			lastRun.RowsErrored,
			lastRun.FinishedAt.Sub(lastRun.StartedAt).Round(time.Millisecond),
		)
	} else {
		fmt.Printf("✓ Done in %s\n", time.Since(start).Round(time.Millisecond))
	}

	return nil
}

func parseSince(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("--since %q: expected RFC3339 or YYYY-MM-DD", value)
}

// DryRunDestination prints rows without writing anywhere.
type DryRunDestination struct {
	name string
}

func (d *DryRunDestination) Connect(context.Context, conncfg.DestinationConfig) error { return nil }

func (d *DryRunDestination) Close() error { return nil }

func (d *DryRunDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, dest string) (destination.LoadResult, error) {
	for _, r := range rows {
		b, _ := json.MarshalIndent(r.Data, "", "  ")
		fmt.Printf("[DRY RUN -> %s] %s\n%s\n", d.name, r.PrimaryKey, string(b))
	}
	return destination.LoadResult{Loaded: len(rows)}, nil
}
