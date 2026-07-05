package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	v2config "github.com/rkshvish/vortara/pkg/config/v2"
)

var historyLimit int

var historyCmd = &cobra.Command{
	Use:   "history <pipeline.yaml>",
	Short: "Show recent pipeline runs",
	Args:  cobra.ExactArgs(1),
	RunE:  runHistory,
}

func init() {
	historyCmd.Flags().IntVar(&historyLimit, "limit", 10, "Number of runs to show")
}

func runHistory(cmd *cobra.Command, args []string) error {
	cfg, err := v2config.Load(args[0])
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
	defer store.Close()

	runs, err := store.GetRunHistory(cfg.Name, historyLimit)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("No runs yet")
		return nil
	}

	for _, run := range runs {
		duration := run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond)
		if run.Error != "" {
			fmt.Printf("#%d  %s  %-7s error: %s  %s\n",
				run.ID,
				run.StartedAt.UTC().Format("2006-01-02 15:04:05"),
				run.Status,
				run.Error,
				duration,
			)
			continue
		}
		fmt.Printf("#%d  %s  %-7s loaded=%d skipped=%d errors=%d  %s\n",
			run.ID,
			run.StartedAt.UTC().Format("2006-01-02 15:04:05"),
			run.Status,
			run.RowsLoaded,
			run.RowsSkipped,
			run.RowsErrored,
			duration,
		)
	}
	return nil
}
