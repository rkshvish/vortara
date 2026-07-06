package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var historyLimit int

var historyCmd = &cobra.Command{
	Use:   "history <sync.yaml>",
	Short: "Show recent sync runs",
	Args:  cobra.ExactArgs(1),
	RunE:  runHistory,
}

func init() {
	historyCmd.Flags().IntVar(&historyLimit, "limit", 10, "Number of runs to show")
}

func runHistory(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	runs, err := store.GetRunHistory(cmd.Context(), f.Sync.Name, historyLimit)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("No runs yet")
		return nil
	}

	for _, run := range runs {
		dur := run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond)
		if run.Error != "" {
			fmt.Printf("#%d  %s  %-7s error: %s  %s\n",
				run.ID, run.StartedAt.UTC().Format("2006-01-02 15:04:05"),
				run.Status, run.Error, dur)
			continue
		}
		fmt.Printf("#%d  %s  %-7s loaded=%d skipped=%d errors=%d  %s\n",
			run.ID, run.StartedAt.UTC().Format("2006-01-02 15:04:05"),
			run.Status, run.RowsLoaded, run.RowsSkipped, run.RowsErrored, dur)
	}
	return nil
}
