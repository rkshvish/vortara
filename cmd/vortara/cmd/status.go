package cmd

import (
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"

	v2config "github.com/rkshvish/vortara/pkg/config/v2"
)

var statusCmd = &cobra.Command{
	Use:   "status <pipeline.yaml>",
	Short: "Show the last run status",
	Args:  cobra.ExactArgs(1),
	RunE:  runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
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

	fmt.Printf("Pipeline:  %s\n", cfg.Name)
	if cfg.Cron != "" {
		fmt.Printf("Cron:      %s\n", cfg.Cron)
	}
	destNames := make([]string, len(cfg.Destinations))
	for i, dest := range cfg.Destinations {
		destNames[i] = fmt.Sprintf("%d:%s", i, dest.Type)
	}
	sort.Strings(destNames)
	for _, name := range destNames {
		fmt.Printf("Dest:      %s\n", name)
	}

	lastRun, err := store.GetLastRun(cfg.Name)
	if err != nil {
		fmt.Println("Status:    never run")
		return nil
	}

	fmt.Printf("Last run:  %s\n", lastRun.StartedAt.UTC().Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("Status:    %s\n", lastRun.Status)
	fmt.Printf("Loaded:    %d\n", lastRun.RowsLoaded)
	fmt.Printf("Skipped:   %d\n", lastRun.RowsSkipped)
	fmt.Printf("Errors:    %d\n", lastRun.RowsErrored)
	fmt.Printf("Duration:  %s\n", lastRun.FinishedAt.Sub(lastRun.StartedAt).Round(time.Millisecond))
	return nil
}
